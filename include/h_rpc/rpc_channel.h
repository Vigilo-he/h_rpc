// 客户端 RPC 通道声明：定义同步调用接口，每次调用使用一条短连接。
#pragma once

#include <google/protobuf/service.h>
#include <string>

namespace h_rpc {

// 同步 RPC 通道：把一次 CallMethod 编码成请求帧发送，并阻塞等待响应。
// 不保存连接状态，ip_/port_ 只在构造时写入，因此可在多线程中并发调用。
class RpcChannel : public google::protobuf::RpcChannel {
public:
    explicit RpcChannel(const std::string& ip, int port);
    ~RpcChannel() override = default;

    void CallMethod(const google::protobuf::MethodDescriptor* method,
                    google::protobuf::RpcController* controller, 
                    const google::protobuf::Message* request,
                    google::protobuf::Message* response,
                    google::protobuf::Closure* done) override;

private:
    std::string ip_;
    int port_;       
};

}