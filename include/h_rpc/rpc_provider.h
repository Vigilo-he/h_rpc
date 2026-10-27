// 服务端 RPC 分发器声明：注册 protobuf service，按帧解析请求并回包。
#pragma once

#include <google/protobuf/message.h>
#include <google/protobuf/service.h>

#include <memory>
#include <string>
#include <unordered_map>

#include "h_reactor/buffer.h"
#include "h_reactor/event_loop.h"
#include "h_reactor/tcp_connection.h"
#include "h_reactor/tcp_server.h"
#include "h_rpc/executor.h"

namespace h_rpc {

// 业务执行引擎：回调版使用固定线程池，协程版使用 EventLoop 调度协程。
enum class ExecutorType {
    kCallback,
    kCoro,
};

// RPC 服务注册与分发器。NotifyService 必须在 Run 之前调用；service 对象由
// 调用方持有，必须比 RpcProvider 活得久。
// 线程模型：OnMessage/HandleFrame 运行在 h_reactor 的 IO 线程，只做拆帧和
// 反序列化；业务方法经 executor_ 投递到执行引擎，避免 IO 线程被业务耗时
// 阻塞。kCallback 使用 4 线程 CallbackExecutor；kCoro 在 Run 时用传入的
// EventLoop 创建 CoroExecutor，业务里可以用 co_await AsyncSleep 挂起。
// 响应由执行引擎线程发出，h_reactor 会把 write 调度回连接所属的 IO 线程。
class RpcProvider {
 public:
    explicit RpcProvider(ExecutorType type = ExecutorType::kCallback);
    ~RpcProvider() = default;

    RpcProvider(const RpcProvider&) = delete;
    RpcProvider& operator=(const RpcProvider&) = delete;

    void NotifyService(google::protobuf::Service* service);

    void Run(h_reactor::EventLoop*, int);

 private:
    void OnMessage(const std::shared_ptr<h_reactor::TcpConnection>&,
                   h_reactor::Buffer*);

    // 解析一帧的 body（header_len + RpcHeader + args）并分发到对应 service。
    void HandleFrame(const std::shared_ptr<h_reactor::TcpConnection>&,
                     const std::string& body);

    void SendRpcResponse(const std::shared_ptr<h_reactor::TcpConnection>&,
                         google::protobuf::Message*);

    std::unordered_map<std::string, google::protobuf::Service*> service_map_;

    ExecutorType executor_type_;

    // 析构顺序是声明顺序的逆序：executor_ 必须比 tcp_server_ 先析构，否则
    // 执行引擎里在途的任务会在 TcpServer 销毁后还去操作连接对象。
    std::unique_ptr<h_reactor::TcpServer> tcp_server_;
    std::unique_ptr<Executor> executor_;
};

}  // namespace h_rpc