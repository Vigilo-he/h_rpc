// 服务端 RPC 分发实现：从 reactor 的 Buffer 中切出完整帧，解析
// RpcHeader 与 args，把业务调用投递到线程池，并通过 TcpConnection 回包。
#include "h_rpc/rpc_provider.h"

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>
#include <google/protobuf/service.h>
#include <google/protobuf/stubs/callback.h>
#include <netinet/in.h>

#include <cstdint>
#include <cstring>
#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <utility>

#include "h_reactor/buffer.h"
#include "h_reactor/event_loop.h"
#include "h_reactor/tcp_connection.h"
#include "h_reactor/tcp_server.h"
#include "h_rpc/callback_executor.h"
#include "h_rpc/coro_executor.h"
#include "rpc_header.pb.h"

namespace h_rpc {
namespace {

// 与客户端保持一致的上限，避免异常长度导致超大分配。
constexpr uint32_t kMaxFrameSize = 64 * 1024 * 1024;

// 业务线程数。与 TcpServer 的 IO loop 数量一致；吞吐上限约为
// 线程数 / 单次业务耗时，调大前先确认业务本身可并发。
constexpr int kExecutorThreads = 4;

}  // namespace

class RpcClosure : public google::protobuf::Closure {
 public:
    explicit RpcClosure(std::function<void()> cb) : cb_(std::move(cb)) {}

    void Run() override {
        cb_();
        delete this;
    }

 private:
    std::function<void()> cb_;
};

RpcProvider::RpcProvider(ExecutorType type) : executor_type_(type) {
    if (type == ExecutorType::kCallback) {
        executor_ = std::make_unique<CallbackExecutor>(kExecutorThreads);
    }
}

void RpcProvider::NotifyService(google::protobuf::Service* service) {
    const google::protobuf::ServiceDescriptor* psd = service->GetDescriptor();
    service_map_[psd->name()] = service;
}

void RpcProvider::Run(h_reactor::EventLoop* loop, int port) {
    // 协程执行器依赖 EventLoop，只能等 Run 拿到 loop 后再创建。
    if (executor_type_ == ExecutorType::kCoro && !executor_) {
        executor_ = std::make_unique<CoroExecutor>(loop);
    }

    tcp_server_ = std::make_unique<h_reactor::TcpServer>(loop, port, 4);

    tcp_server_->SetMessageCallback(
        [this](const std::shared_ptr<h_reactor::TcpConnection>& conn,
               h_reactor::Buffer* buffer) { this->OnMessage(conn, buffer); });

    tcp_server_->Start();
    std::cout << "[RpcProvider] Server started on port " << port << std::endl;
}

void RpcProvider::OnMessage(
    const std::shared_ptr<h_reactor::TcpConnection>& conn,
    h_reactor::Buffer* buffer) {
    // 帧格式（大端）：
    //   body_len(4) | header_len(4) | RpcHeader(header_len) | args
    // body_len 是其后内容的长度，即 4 + header_len + args_size。
    while (buffer->ReadableBytes() >= 4) {
        uint32_t net_body_len = 0;
        std::memcpy(&net_body_len, buffer->Peek(), sizeof(net_body_len));
        const uint32_t body_len = ntohl(net_body_len);

        if (body_len < 4 || body_len > kMaxFrameSize) {
            std::cerr << "[RpcProvider] Invalid body length: " << body_len
                      << std::endl;
            return;
        }

        // 坑：必须等整帧到齐再消费，TCP 会把一次 send 拆成多段；
        // 提前 Retrieve 会丢掉半包，导致后续帧全部错位。
        if (buffer->ReadableBytes() < sizeof(net_body_len) + body_len) {
            return;
        }

        buffer->Retrieve(sizeof(net_body_len));
        HandleFrame(conn, buffer->RetrieveAsString(body_len));
    }
}

void RpcProvider::HandleFrame(
    const std::shared_ptr<h_reactor::TcpConnection>& conn,
    const std::string& body) {
    if (body.size() < 4) {
        std::cerr << "[RpcProvider] Frame too short: " << body.size()
                  << std::endl;
        return;
    }

    uint32_t header_len = 0;
    std::memcpy(&header_len, body.data(), sizeof(header_len));
    header_len = ntohl(header_len);

    if (header_len > body.size() - 4) {
        std::cerr << "[RpcProvider] Invalid header length: " << header_len
                  << std::endl;
        return;
    }

    std::string header_str = body.substr(4, header_len);
    std::string args_str = body.substr(4 + header_len);

    RpcHeader header;
    if (!header.ParseFromString(header_str)) {
        std::cerr << "[RpcProvider] Parse Header Failed" << std::endl;
        return;
    }

    if (header.args_size() != args_str.size()) {
        std::cerr << "[RpcProvider] Args size mismatch: " << header.args_size()
                  << " != " << args_str.size() << std::endl;
        return;
    }

    auto it = service_map_.find(header.service_name());
    if (it == service_map_.end()) {
        std::cerr << "[RpcProvider] Service not found: "
                  << header.service_name() << std::endl;
        return;
    }
    google::protobuf::Service* service = it->second;

    const google::protobuf::MethodDescriptor* method =
        service->GetDescriptor()->FindMethodByName(header.method_name());
    if (method == nullptr) {
        std::cerr << "[RpcProvider] Method not found: " << header.method_name()
                  << std::endl;
        return;
    }

    google::protobuf::Message* request =
        service->GetRequestPrototype(method).New();
    google::protobuf::Message* response =
        service->GetResponsePrototype(method).New();

    if (!request->ParseFromString(args_str)) {
        std::cerr << "[RpcProvider] Parse Args Failed" << std::endl;
        delete request;
        delete response;
        return;
    }

    // 坑：response 要等到 SendRpcResponse 之后才能释放，所以闭包里连同
    // request 一起 delete；只释放 request 会让每次请求泄漏一份 response。
    auto* done = new RpcClosure([this, conn, request, response]() {
        this->SendRpcResponse(conn, response);
        delete request;
        delete response;
    });

    // 业务方法可能包含数据库等待，丢给线程池执行，避免阻塞 reactor 的 IO
    // 线程。request/response/done 的所有权随闭包转移，done->Run 里统一归还。
    executor_->Execute([service, method, request, response, done]() {
        service->CallMethod(method, nullptr, request, response, done);
    });
}

void RpcProvider::SendRpcResponse(
    const std::shared_ptr<h_reactor::TcpConnection>& conn,
    google::protobuf::Message* response) {
    std::string response_str;
    if (!response->SerializeToString(&response_str)) {
        std::cerr << "[RpcProvider] Serialize response failed" << std::endl;
        return;
    }

    const uint32_t total_len = static_cast<uint32_t>(response_str.size());
    const uint32_t net_len = htonl(total_len);

    std::string send_data;
    send_data.reserve(sizeof(net_len) + total_len);
    send_data.append(reinterpret_cast<const char*>(&net_len), sizeof(net_len));
    send_data.append(response_str);

    conn->Send(send_data);
}

}  // namespace h_rpc
