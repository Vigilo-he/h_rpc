// 客户端 RPC 通道实现：把 protobuf 请求编码为
// [body_len][header_len][RpcHeader][args] 帧，经短连接发送并同步读回响应。
#include "h_rpc/rpc_channel.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <string>

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>
#include <google/protobuf/service.h>

#include "rpc_header.pb.h"

namespace h_rpc {
namespace {

// 正常 RPC 报文远小于该值，用上限挡住异常长度导致的超大分配。
constexpr uint32_t kMaxFrameSize = 64 * 1024 * 1024;

// 对端无响应时不要无限阻塞；超时后 recv 返回 -1，调用方可以报错退出。
constexpr int kRecvTimeoutSec = 5;

// 写满 len 字节才算成功，处理短写和 EINTR。
bool SendAll(int fd, const char* data, size_t len) {
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = ::send(fd, data + sent, len - sent, MSG_NOSIGNAL);
        if (n > 0) {
            sent += static_cast<size_t>(n);
        } else if (n < 0 && errno == EINTR) {
            continue;
        } else {
            return false;
        }
    }
    return true;
}

// 读满 len 字节才算成功；对端关闭或超时返回 false。
bool RecvAll(int fd, char* data, size_t len) {
    size_t received = 0;
    while (received < len) {
        ssize_t n = ::recv(fd, data + received, len - received, 0);
        if (n > 0) {
            received += static_cast<size_t>(n);
        } else if (n == 0) {
            return false;
        } else if (errno == EINTR) {
            continue;
        } else {
            return false;
        }
    }
    return true;
}

}  // namespace

RpcChannel::RpcChannel(const std::string& ip, int port)
    : ip_(ip)
    , port_(port)
{}

void RpcChannel::CallMethod(const google::protobuf::MethodDescriptor* method,
                            google::protobuf::RpcController* controller,
                            const google::protobuf::Message* request,
                            google::protobuf::Message* response,
                            google::protobuf::Closure* done) {
    auto finish = [&](const char* error) {
        if (error != nullptr && controller != nullptr) {
            controller->SetFailed(error);
        }
        // 同步实现，返回前统一触发回调，保持与 protobuf 异步语义一致。
        if (done != nullptr) {
            done->Run();
        }
    };

    std::string args_str;
    if (!request->SerializeToString(&args_str)) {
        finish("Serialize request failed");
        return;
    }

    RpcHeader header;
    header.set_method_name(method->name());
    header.set_service_name(method->service()->name());
    header.set_args_size(static_cast<uint32_t>(args_str.size()));

    std::string header_str;
    if (!header.SerializeToString(&header_str)) {
        finish("Serialize request failed");
        return;
    }

    const uint32_t header_len = static_cast<uint32_t>(header_str.size());
    const uint32_t body_len =
        4 + header_len + static_cast<uint32_t>(args_str.size());
    const uint32_t net_body_len = htonl(body_len);
    const uint32_t net_header_len = htonl(header_len);

    std::string send_str;
    send_str.reserve(sizeof(net_body_len) + body_len);
    send_str.append(reinterpret_cast<const char*>(&net_body_len),
                    sizeof(net_body_len));
    send_str.append(reinterpret_cast<const char*>(&net_header_len),
                    sizeof(net_header_len));
    // 坑：这里必须用 string 的 data()/长度；传 &header_str 只是把
    // std::string 对象自身的内存当成内容，发出去的是指针字节。
    send_str.append(header_str);
    send_str.append(args_str);

    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        finish("Create socket failed");
        return;
    }

    timeval timeout{};
    timeout.tv_sec = kRecvTimeoutSec;
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port_));
    if (::inet_pton(AF_INET, ip_.c_str(), &addr.sin_addr) != 1) {
        ::close(fd);
        finish("Invalid server address");
        return;
    }

    if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        ::close(fd);
        finish("Connect to server failed");
        return;
    }

    // 坑：只构造 send_str 而不写 socket，会让客户端卡在 recv、服务端
    // 卡在读事件上，双方都以为在等对方。
    if (!SendAll(fd, send_str.data(), send_str.size())) {
        ::close(fd);
        finish("Send request failed");
        return;
    }

    uint32_t net_response_len = 0;
    if (!RecvAll(fd, reinterpret_cast<char*>(&net_response_len),
                 sizeof(net_response_len))) {
        ::close(fd);
        finish("Receive response length failed");
        return;
    }

    const uint32_t response_len = ntohl(net_response_len);
    if (response_len == 0 || response_len > kMaxFrameSize) {
        ::close(fd);
        finish("Invalid response length");
        return;
    }

    std::string response_str(response_len, '\0');
    // 坑：必须传缓冲区首地址；传 &response_str 会把响应写进
    // std::string 对象本身，破坏栈上的对象。
    if (!RecvAll(fd, response_str.data(), response_str.size())) {
        ::close(fd);
        finish("Receive response body failed");
        return;
    }
    ::close(fd);

    if (!response->ParseFromArray(response_str.data(),
                                  static_cast<int>(response_str.size()))) {
        finish("Parse response failed");
        return;
    }

    finish(nullptr);
}

}  // namespace h_rpc
