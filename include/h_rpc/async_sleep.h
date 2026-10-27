// 协程版异步睡眠：co_await AsyncSleep(10ms) 把当前协程挂到 EventLoop 的
// 定时器上，到期后在 loop 线程恢复，不阻塞任何线程。
#pragma once

#include <chrono>
#include <coroutine>
#include <thread>

#include "h_rpc/coro_executor.h"

namespace h_rpc {

class AsyncSleepAwaiter {
 public:
    explicit AsyncSleepAwaiter(std::chrono::milliseconds delay)
        : delay_(delay) {}

    bool await_ready() const noexcept { return delay_.count() <= 0; }

    bool await_suspend(std::coroutine_handle<> handle) const {
        CoroExecutor* executor = CoroExecutor::Current();
        if (executor == nullptr) {
            // 不在 CoroExecutor 的协程里，没有事件循环可以挂起。退化成阻塞
            // 等待并返回 false（不挂起），避免调用方永远不被唤醒。
            std::this_thread::sleep_for(delay_);
            return false;
        }

        executor->ResumeAfter(delay_, handle);
        return true;
    }

    void await_resume() const noexcept {}

 private:
    std::chrono::milliseconds delay_;
};

template <typename Rep, typename Period>
AsyncSleepAwaiter AsyncSleep(std::chrono::duration<Rep, Period> delay) {
    return AsyncSleepAwaiter(
        std::chrono::duration_cast<std::chrono::milliseconds>(delay));
}

}  // namespace h_rpc
