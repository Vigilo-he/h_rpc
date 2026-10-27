// CoroExecutor 实现：包装任务为协程、在 EventLoop 上恢复、完成后回收。
#include "h_rpc/coro_executor.h"

#include <iostream>
#include <utility>

namespace h_rpc {

thread_local CoroExecutor* CoroExecutor::current_ = nullptr;

CoroExecutor::CoroExecutor(h_reactor::EventLoop* loop)
    : loop_(loop), scheduler_(loop) {}

CoroExecutor::~CoroExecutor() {
    std::unordered_map<void*, TaskState> pending;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        pending.swap(tasks_);
    }

    // 先取消未触发的定时器，再销毁协程帧；否则回调会在 executor 析构后
    // 访问 this。这里假定析构发生在 loop 停止之后。
    for (auto& entry : pending) {
        if (entry.second.timer_id != 0) {
            loop_->CancelTimer(entry.second.timer_id);
        }
    }
    // pending 析构时销毁仍挂起的 h_coro::Task<>，即销毁协程帧。
}

void CoroExecutor::Execute(std::function<void()> task) {
    auto coroutine =
        std::make_unique<h_coro::Task<>>(RunWrapper(std::move(task)));
    auto handle = coroutine->GetHandle();
    if (!handle) {
        return;
    }

    {
        std::lock_guard<std::mutex> lock(mutex_);
        tasks_[handle.address()] = TaskState{std::move(coroutine), 0};
    }

    Resume(handle);
}

void CoroExecutor::Spawn(h_coro::Task<> task) {
    auto handle = task.GetHandle();
    if (!handle) {
        return;
    }

    auto coroutine = std::make_unique<h_coro::Task<>>(std::move(task));
    {
        std::lock_guard<std::mutex> lock(mutex_);
        tasks_[handle.address()] = TaskState{std::move(coroutine), 0};
    }

    Resume(handle);
}

void CoroExecutor::ResumeAfter(std::chrono::milliseconds delay,
                               std::coroutine_handle<> handle) {
    const std::uint64_t timer_id =
        loop_->RunAfter(delay.count(), [this, handle]() {
            {
                std::lock_guard<std::mutex> lock(mutex_);
                auto it = tasks_.find(handle.address());
                if (it != tasks_.end()) {
                    it->second.timer_id = 0;
                }
            }
            Resume(handle);
        });

    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = tasks_.find(handle.address());
        if (it != tasks_.end()) {
            it->second.timer_id = timer_id;
        }
    }
}

h_coro::Task<> CoroExecutor::RunWrapper(std::function<void()> task) {
    try {
        task();
    } catch (const std::exception& e) {
        std::cerr << "[CoroExecutor] task threw: " << e.what() << std::endl;
    } catch (...) {
        std::cerr << "[CoroExecutor] task threw unknown exception" << std::endl;
    }
    co_return;
}

void CoroExecutor::Resume(std::coroutine_handle<> handle) {
    if (!handle) {
        return;
    }

    scheduler_.Schedule([this, handle]() {
        if (handle.done()) {
            // 已经结束但还留在表里（例如重复投递），直接回收避免泄漏。
            Reap(handle);
            return;
        }

        CoroExecutor* previous = current_;
        current_ = this;
        handle.resume();
        current_ = previous;

        if (handle.done()) {
            Reap(handle);
        }
    });
}

void CoroExecutor::Reap(std::coroutine_handle<> handle) {
    std::unique_ptr<h_coro::Task<>> finished;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = tasks_.find(handle.address());
        if (it == tasks_.end()) {
            return;
        }
        finished = std::move(it->second.task);
        tasks_.erase(it);
    }

    // finished 在锁外析构，协程帧里的析构函数不会长时间持锁。
}

CoroExecutor* CoroExecutor::Current() { return current_; }

}  // namespace h_rpc
