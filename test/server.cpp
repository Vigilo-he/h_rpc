#include <chrono>
#include <iostream>
#include <string>
#include <thread>

#include "h_coro/task.h"
#include "h_reactor/event_loop.h"
#include "h_rpc/async_sleep.h"
#include "h_rpc/coro_executor.h"
#include "h_rpc/rpc_provider.h"
#include "user.pb.h"

namespace {

// 模拟一次数据库查询的耗时；压测时用它估算执行引擎的吞吐上限。
constexpr int kLoginDelayMs = 10;

void FillLoginResponse(const h_rpc::LoginRequest& request,
                       h_rpc::LoginResponse* response) {
    if (request.pwd() == "123456") {
        response->mutable_result()->set_errcode(0);
        response->mutable_result()->set_errmsg("");
        response->set_success(true);
    } else {
        response->mutable_result()->set_errcode(1);
        response->mutable_result()->set_errmsg("密码错误");
        response->set_success(false);
    }
}

}  // namespace

// 回调版：业务方法在 CallbackExecutor 的工作线程里阻塞 10ms。
class CallbackUserService : public h_rpc::UserServiceRpc {
 public:
    void Login(::google::protobuf::RpcController* controller,
               const ::h_rpc::LoginRequest* request,
               ::h_rpc::LoginResponse* response,
               ::google::protobuf::Closure* done) override {
        std::this_thread::sleep_for(std::chrono::milliseconds(kLoginDelayMs));
        FillLoginResponse(*request, response);
        done->Run();
    }
};

// 协程版：Login 只是启动协程的入口，真正耗时逻辑在 DoLogin 里 co_await。
class CoroUserService : public h_rpc::UserServiceRpc {
 public:
    void Login(::google::protobuf::RpcController* controller,
               const ::h_rpc::LoginRequest* request,
               ::h_rpc::LoginResponse* response,
               ::google::protobuf::Closure* done) override {
        h_rpc::CoroExecutor* executor = h_rpc::CoroExecutor::Current();
        if (executor == nullptr) {
            std::cerr << "[CoroServer] Login 不在 CoroExecutor 中运行"
                      << std::endl;
            done->Run();
            return;
        }

        executor->Spawn(DoLogin(request, response, done));
    }

 private:
    h_coro::Task<> DoLogin(const h_rpc::LoginRequest* request,
                           h_rpc::LoginResponse* response,
                           google::protobuf::Closure* done) {
        // 挂起当前协程，把线程让给 EventLoop；10ms 后自动恢复。
        co_await h_rpc::AsyncSleep(std::chrono::milliseconds(kLoginDelayMs));

        FillLoginResponse(*request, response);
        done->Run();
    }
};

int main(int argc, char** argv) {
    bool use_coro = false;
    if (argc > 1) {
        const std::string mode = argv[1];
        if (mode == "coro") {
            use_coro = true;
        } else if (mode != "callback") {
            std::cerr << "用法: " << argv[0] << " [callback|coro]" << std::endl;
            return 1;
        }
    }

    h_reactor::EventLoop loop;

    h_rpc::RpcProvider provider(use_coro ? h_rpc::ExecutorType::kCoro
                                         : h_rpc::ExecutorType::kCallback);

    if (use_coro) {
        provider.NotifyService(new CoroUserService());
    } else {
        provider.NotifyService(new CallbackUserService());
    }

    provider.Run(&loop, 8000);

    std::cout << "[Server] mode=" << (use_coro ? "coro" : "callback")
              << ", port=8000" << std::endl;
    loop.Loop();

    return 0;
}
