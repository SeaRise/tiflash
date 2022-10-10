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
EventLoop::EventLoop(
    size_t loop_id_,
    EventLoopPool & pool_)
    : loop_id(loop_id_)
    , pool(pool_)
{
    cpu_thread = std::thread(&EventLoop::cpuModeLoop, this);
}

EventLoop::~EventLoop()
{
    cpu_thread.join();
    LOG_INFO(logger, "stop event loop");
}

void EventLoop::handleCpuModeTask(PipelineTask && task)
{
    LOG_DEBUG(logger, "handle cpu mode task {}", task.toString());
    auto result = task.execute();
    switch (result.type)
    {
    case PipelineTaskResultType::running:
    {
        if (task.status == PipelineTaskStatus::io_wait)
            pool.io_poller.submit(std::move(task));
        else
            pool.submitCPU(std::move(task));
        break;
    }
    case PipelineTaskResultType::finished:
    {
        if (task.status == PipelineTaskStatus::io_finish)
            pool.io_poller.submit(std::move(task));
        else
            pool.handleFinishTask(task);
        break;
    }
    case PipelineTaskResultType::error:
    {
        pool.handleErrTask(task, result);
        break;
    }
    }
}

void EventLoop::cpuModeLoop()
{
#ifdef __linux__
    struct sched_param param;
    param.__sched_priority = sched_get_priority_max(sched_getscheduler(0));
    sched_setparam(0, &param);
#endif
    setThreadName("EventLoop");
    LOG_INFO(logger, "start cpu event loop {}", loop_id);
    PipelineTask task;
    while (likely(pool.popTask(task)))
    {
        handleCpuModeTask(std::move(task));
    }
    LOG_INFO(logger, "cpu event loop {} finished", loop_id);
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

bool EventLoopPool::popTask(PipelineTask & task)
{
    {
        std::unique_lock<std::mutex> lock(global_mutex);
        while (true)
        {
            if (unlikely(is_closed))
                return false;
            if (!cpu_event_queue.empty())
                break;
            cv.wait(lock);
        }

        task = std::move(cpu_event_queue.front());
        cpu_event_queue.pop_front();
    }
    return true;
}

void EventLoopPool::submit(std::vector<PipelineTask> & tasks)
{
    if (tasks.empty())
        return;
    std::vector<PipelineTask> io_tasks;
    io_tasks.reserve(tasks.size());
    std::vector<PipelineTask> cpu_tasks;
    cpu_tasks.reserve(tasks.size());
    for (auto & task : tasks)
    {
        task.prepare();
        if (task.tryToIOMode())
            io_tasks.emplace_back(std::move(task));
        else
            cpu_tasks.emplace_back(std::move(task));
    }
    io_poller.submit(io_tasks);
    submitCPU(cpu_tasks);
}

void EventLoopPool::submitCPU(PipelineTask && task)
{
    {
        std::lock_guard<std::mutex> lock(global_mutex);
        cpu_event_queue.emplace_back(std::move(task));
    }
    cv.notify_one();
    LOG_DEBUG(logger, "submit {} to cpu event loop", task.toString());
}

void EventLoopPool::submitCPU(std::vector<PipelineTask> & tasks)
{
    if (tasks.empty())
        return;
    size_t notify_count = std::min(tasks.size(), cpu_loops.size());
    {
        std::lock_guard<std::mutex> lock(global_mutex);
        for (auto & task : tasks)
        {
            LOG_DEBUG(logger, "submit {} to cpu event loop", task.toString());
            cpu_event_queue.emplace_back(std::move(task));
        }
    }
    for (size_t i = 0; i < notify_count; ++i)
        cv.notify_one();
}

void EventLoopPool::finish()
{
    {
        std::lock_guard<std::mutex> lock(global_mutex);
        is_closed = true;
    }
    cv.notify_all();
    io_poller.finish();
}

EventLoopPool::~EventLoopPool()
{
    cpu_loops.clear();
    LOG_INFO(logger, "stop event loop pool");
}

void EventLoopPool::handleFinishTask(const PipelineTask & task)
{
    LOG_INFO(logger, "pipeline task {} finished", task.toString());
    if (auto dag_scheduler = pipeline_manager.getDAGScheduler(task.mpp_task_id); likely(dag_scheduler))
        dag_scheduler->submit(PipelineEvent::finish(task.task_id, task.pipeline_id));
}
void EventLoopPool::handleErrTask(const PipelineTask & task, const PipelineTaskResult & result)
{
    LOG_INFO(logger, "pipeline task {} occur error", task.toString());
    if (auto dag_scheduler = pipeline_manager.getDAGScheduler(task.mpp_task_id); likely(dag_scheduler))
        dag_scheduler->submit(PipelineEvent::fail(result.err_msg));
}
} // namespace DB
