// 任务执行器抽象：把业务处理从 reactor 的 IO 线程剥离，交给独立的执行线程。
#pragma once

#include <functional>

namespace h_rpc {

// 单任务执行器。Execute 接管 task 的所有权，可能在本线程同步执行，也可能
// 排队到其他线程异步执行；返回不代表任务已经完成。
// 线程安全：实现必须保证 Execute 可在任意线程并发调用。
class Executor {
 public:
    virtual ~Executor() = default;

    virtual void Execute(std::function<void()> task) = 0;
};

}  // namespace h_rpc
