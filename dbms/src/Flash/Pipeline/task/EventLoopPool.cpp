// Copyright 2022 PingCAP, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Common/setThreadName.h>
#include <Flash/Pipeline/PipelineManager.h>
#include <Flash/Pipeline/dag/DAGScheduler.h>
#include <Flash/Pipeline/dag/Event.h>
#include <Flash/Pipeline/task/EventLoopPool.h>
#include <errno.h>

namespace DB
{
IOPoller::IOPoller(EventLoopPool & pool_): pool(pool_)
{
    io_thread = std::thread(&IOPoller::ioModeLoop, this);

}

void IOPoller::finish()
{
    LOG_DEBUG(logger, "call finish for IOPoller");
    if (this->is_shutdown.load() == false) {
        this->is_shutdown.store(true, std::memory_order_release);
        cond.notify_one();
    }
}

void IOPoller::submit(PipelineTask && task)
{
    std::unique_lock<std::mutex> lock(mutex);
    blocked_tasks.emplace_back(std::move(task));
    cond.notify_one();
}

IOPoller::~IOPoller()
{
    io_thread.join();
    LOG_INFO(logger, "stop io event loop");
}

void IOPoller::ioModeLoop() noexcept
{
    LOG_DEBUG(logger, "start io mode loop");

    std::list<PipelineTask> local_blocked_tasks;
    int spin_count = 0;
    std::vector<PipelineTask> ready_tasks;
    while (!is_shutdown.load(std::memory_order_acquire))
    {
#ifndef NDEBUG
        LOG_TRACE(logger, "getting io task from blocked_tasks");
#endif
        {
            std::unique_lock<std::mutex> lock(this->mutex);
            local_blocked_tasks.splice(local_blocked_tasks.end(), blocked_tasks);
            if (local_blocked_tasks.empty() && blocked_tasks.empty()) {
                std::cv_status cv_status = std::cv_status::no_timeout;
                while (!is_shutdown.load(std::memory_order_acquire) && this->blocked_tasks.empty()) {
                    cv_status = cond.wait_for(lock, std::chrono::milliseconds(10));
                }
                if (cv_status == std::cv_status::timeout) {
                    continue;
                }
                if (is_shutdown.load(std::memory_order_acquire)) {
                    break;
                }
                local_blocked_tasks.splice(local_blocked_tasks.end(), blocked_tasks);
            }
        }

        auto task_it = local_blocked_tasks.begin();
        while (task_it != local_blocked_tasks.end()) {
            auto & task = *task_it;

#ifndef NDEBUG
        LOG_TRACE(logger, "handle io mode task: {}", task.toString());
#endif
            auto pre_status = task.status;
            if (task.tryToCpuMode(pool.pipeline_manager))
            {
                if (pre_status == PipelineTaskStatus::io_wait)
                    ready_tasks.emplace_back(std::move(task));
                task_it = local_blocked_tasks.erase(task_it);
            }
            else
            {
                ++task_it;
            }
        }

        if (ready_tasks.empty()) {
            spin_count += 1;
        } else {
            spin_count = 0;
            pool.batchSubmitCPU(ready_tasks);
            ready_tasks.clear();
        }

        if (spin_count != 0 && spin_count % 64 == 0) {
#ifdef __x86_64__
            _mm_pause();
#else
            // TODO: Maybe there's a better intrinsic like _mm_pause on non-x86_64 architecture.
            sched_yield();
#endif
        }
        if (spin_count == 640) {
            spin_count = 0;
            sched_yield();
        }
    }
    LOG_DEBUG(logger, "finish io mode loop");
}

EventLoop::EventLoop(
    size_t loop_id_,
    EventLoopPool & pool_)
    : loop_id(loop_id_)
    , pool(pool_)
{
    cpu_thread = std::thread(&EventLoop::cpuModeLoop, this);
}

void EventLoop::finish() {}

void EventLoop::submit(PipelineTask && task)
{
    pool.submitCPU(std::move(task));
}

EventLoop::~EventLoop()
{
    cpu_thread.join();
    LOG_INFO(logger, "stop event loop");
}

void EventLoop::handleCpuModeTask(PipelineTask && task) noexcept
{
#ifndef NDEBUG
    LOG_TRACE(logger, "handle cpu mode task: {}", task.toString());
#endif
    auto result = task.execute(pool.pipeline_manager);
    switch (result.type)
    {
    case PipelineTaskResultType::running:
    {
        if (task.status == PipelineTaskStatus::io_wait || task.status == PipelineTaskStatus::io_finishing)
            pool.submitIO(std::move(task));
        else
            submit(std::move(task));
        break;
    }
    case PipelineTaskResultType::finished:
    {
        break;
    }
    case PipelineTaskResultType::error:
    {
        pool.handleErrTask(task, result);
        break;
    }
    }
}

void EventLoop::cpuModeLoop() noexcept
{
#ifdef __linux__
    struct sched_param param;
    param.__sched_priority = sched_get_priority_max(sched_getscheduler(0));
    sched_setparam(0, &param);
#endif
    setThreadName("EventLoop");
    PipelineTask task;
    while (likely(popTask(task)))
    {
        handleCpuModeTask(std::move(task));
    }
}

bool EventLoop::popTask(PipelineTask & task)
{
    {
        std::unique_lock<std::mutex> lock(pool.global_mutex);
        while (true)
        {
            if (unlikely(pool.is_closed))
                return false;
            if (!pool.cpu_event_queue.empty())
                break;
            pool.cv.wait(lock);
        }

        task = std::move(pool.cpu_event_queue.front());
        pool.cpu_event_queue.pop_front();
    }
    return true;
}

EventLoopPool::EventLoopPool(
    size_t loop_num,
    PipelineManager & pipeline_manager_)
    : pipeline_manager(pipeline_manager_)
    , io_poller(*this)
{
    RUNTIME_ASSERT(loop_num > 0);
    cpu_loops.reserve(loop_num);
    for (size_t i = 0; i < loop_num; ++i)
        cpu_loops.emplace_back(std::make_unique<EventLoop>(i, *this));
}

void EventLoopPool::submit(std::vector<PipelineTask> & tasks)
{
    for (auto & task : tasks)
    {
        task.prepare();
        if (task.tryToIOMode())
        {
#ifndef NDEBUG
            LOG_TRACE(logger, "submit {} task to io loop", task.toString());
#endif
            submitIO(std::move(task));
        } 
        else
        {
#ifndef NDEBUG
            LOG_TRACE(logger, "submit {} task to cpu loop", task.toString());
#endif
            submitCPU(std::move(task));
        }
    }
}

void EventLoopPool::submitCPU(PipelineTask && task)
{
    std::lock_guard<std::mutex> lock(global_mutex);
    cpu_event_queue.emplace_back(std::move(task));
    cv.notify_one();
}

void EventLoopPool::batchSubmitCPU(std::vector<PipelineTask> & tasks)
{
    std::lock_guard<std::mutex> lock(global_mutex);
    for (auto & task : tasks)
    {
        cpu_event_queue.emplace_back(std::move(task));
        cv.notify_one();
    }
}

void EventLoopPool::submitIO(PipelineTask && task)
{
    io_poller.submit(std::move(task));
}

void EventLoopPool::finish()
{
    {
        std::lock_guard<std::mutex> lock(global_mutex);
        is_closed = true;
        cv.notify_all();
    }
    for (auto & cpu_loop : cpu_loops)
        cpu_loop->finish();
    io_poller.finish();
}

EventLoopPool::~EventLoopPool()
{
    cpu_loops.clear();
    LOG_INFO(logger, "stop event loop pool");
}

void EventLoopPool::handleErrTask(const PipelineTask & task, const PipelineTaskResult & result)
{
    if (auto dag_scheduler = pipeline_manager.getDAGScheduler(task.mpp_task_id); likely(dag_scheduler))
        dag_scheduler->submit(PipelineEvent::fail(result.err_msg));
}
} // namespace DB
