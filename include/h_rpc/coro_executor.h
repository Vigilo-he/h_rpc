// 协程任务执行器：把 Executor 的普通任务包装成 h_coro::Task<>，交给
// EventLoop 调度；协程挂起后由 EventLoop 定时器/网络事件恢复，不占用线程。
#pragma once

#include <chrono>
#include <coroutine>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "h_coro/task.h"
#include "h_reactor/event_loop.h"
#include "h_reactor/event_loop_scheduler.h"
#include "h_rpc/executor.h"

namespace h_rpc {

// 单 EventLoop 协程执行器。Execute 可在任意线程调用，任务会被包装成协程并
// 投递到 loop_ 所属线程启动；协程的每次恢复都经过本类，完成后自动回收。
//
// 生命周期：executor 必须比 loop_ 先销毁；析构时会取消尚未触发的定时器并
// 销毁仍挂起的协程帧，因此要求调用方在 loop 停止后再销毁 executor。
class CoroExecutor : public Executor {
 public:
    explicit CoroExecutor(h_reactor::EventLoop* loop);
    ~CoroExecutor() override;

    CoroExecutor(const CoroExecutor&) = delete;
    CoroExecutor& operator=(const CoroExecutor&) = delete;

    void Execute(std::function<void()> task) override;

    // 在“当前协程”里启动一个子协程；子协程由本 executor 接管生命周期。
    // 通常在 service 的协程上下文中调用；从其他线程调用时 Resume 会投递到
    // loop。
    void Spawn(h_coro::Task<> task);

    // 把 handle 的恢复动作注册到 EventLoop 定时器上，AsyncSleep 使用。
    // 只能在当前 executor 的协程里调用。
    void ResumeAfter(std::chrono::milliseconds delay,
                     std::coroutine_handle<> handle);

    // 当前线程正在运行的 CoroExecutor；不在协程调度中时返回 nullptr。
    static CoroExecutor* Current();

 private:
    struct TaskState {
        std::unique_ptr<h_coro::Task<>> task;
        std::uint64_t timer_id{0};
    };

    h_coro::Task<> RunWrapper(std::function<void()> task);
    void Resume(std::coroutine_handle<> handle);
    void Reap(std::coroutine_handle<> handle);

    h_reactor::EventLoop* loop_;
    h_reactor::EventLoopScheduler scheduler_;

    // 保护 tasks_。协程句柄地址是 key，timer_id 用于析构时取消未触发的 sleep。
    std::mutex mutex_;
    std::unordered_map<void*, TaskState> tasks_;

    static thread_local CoroExecutor* current_;
};

}  // namespace h_rpc
