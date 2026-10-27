#include <iostream>
#include "h_rpc/rpc_channel.h"
#include "user.pb.h"

int main() {
    h_rpc::RpcChannel channel("127.0.0.1", 8000);
    
    h_rpc::UserServiceRpc_Stub stub(&channel);
    
    h_rpc::LoginRequest request;
    request.set_name("Vigilo");
    request.set_pwd("123456");
    
    h_rpc::LoginResponse response;
    
    std::cout << "[Client] 发送 Login 请求..." << std::endl;
    stub.Login(nullptr, &request, &response, nullptr);
    
    if (response.result().errcode() == 0) {
        std::cout << "[Client] 登录成功 success = " << response.success() << std::endl;
    } else {
        std::cout << "[Client] 登录失败 错误信息: " << response.result().errmsg() << std::endl;
    }
    
    return 0;
}