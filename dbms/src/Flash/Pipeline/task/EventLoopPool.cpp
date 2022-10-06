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
EventLoopPool::EventLoopPool(
    size_t loop_num,
    PipelineManager & pipeline_manager_)
    : pipeline_manager(pipeline_manager_)
{
    cpu_threads.reserve(loop_num);
    for (size_t i = 0; i < loop_num; ++i)
        cpu_threads.emplace_back(std::thread(&EventLoopPool::cpuModeLoop, this));
    io_thread = std::thread(&EventLoopPool::ioModeLoop, this);
}

void EventLoopPool::submit(std::vector<PipelineTask> & tasks)
{
    for (auto & task : tasks)
    {
        task.prepare();
        if (task.tryToIOMode())
            submitIO(std::move(task));
        else
            submitCPU(std::move(task));
    }
}

void EventLoopPool::submitCPU(PipelineTask && task)
{
    RUNTIME_ASSERT(
        cpu_event_queue.tryPush(std::move(task)) != MPMCQueueResult::FULL,
        "EventLoopPool cpu event queue full");
}

void EventLoopPool::submitIO(PipelineTask && task)
{
    RUNTIME_ASSERT(
        io_event_queue.tryPush(std::move(task)) != MPMCQueueResult::FULL,
        "EventLoopPool io event queue full");
}

void EventLoopPool::finish()
{
    cpu_event_queue.finish();
    io_event_queue.finish();
}

EventLoopPool::~EventLoopPool()
{
    for (auto & cpu_thread : cpu_threads)
        cpu_thread.join();
    io_thread.join();
    LOG_INFO(logger, "stop event loop pool");
}

void EventLoopPool::handleFinishTask(const PipelineTask & task)
{
    if (auto dag_scheduler = pipeline_manager.getDAGScheduler(task.mpp_task_id); likely(dag_scheduler))
        dag_scheduler->submit(PipelineEvent::finish(task.task_id, task.pipeline_id));
}
void EventLoopPool::handleErrTask(const PipelineTask & task, const PipelineTaskResult & result)
{
    if (auto dag_scheduler = pipeline_manager.getDAGScheduler(task.mpp_task_id); likely(dag_scheduler))
        dag_scheduler->submit(PipelineEvent::fail(result.err_msg));
}

void EventLoopPool::handleCpuModeTask(PipelineTask && task)
{
#ifndef NDEBUG
    LOG_TRACE(logger, "handle cpu mode task: {}", task.toString());
#endif
    auto result = task.execute();
    switch (result.type)
    {
    case PipelineTaskResultType::running:
    {
        if (task.status == PipelineTaskStatus::io_wait)
            submitIO(std::move(task));
        else
            submitCPU(std::move(task));
        break;
    }
    case PipelineTaskResultType::finished:
    {
        if (task.status == PipelineTaskStatus::io_finish)
            submitIO(std::move(task));
        else
            handleFinishTask(task);
        break;
    }
    case PipelineTaskResultType::error:
    {
        handleErrTask(task, result);
        break;
    }
    default:
        throw Exception("Unknown PipelineTaskResultType");
    }
}

void EventLoopPool::handleIOModeTask(PipelineTask && task)
{
#ifndef NDEBUG
    LOG_TRACE(logger, "handle io mode task: {}", task.toString());
#endif
    switch(task.status)
    {
    case PipelineTaskStatus::io_wait:
    {
        if (task.tryToCpuMode())
            submitCPU(std::move(task));
        else
            submitIO(std::move(task));
        break;
    }
    case PipelineTaskStatus::io_finish:
    {
        if (task.tryToCpuMode())
            handleFinishTask(task);
        else
            submitIO(std::move(task));
        break;
    }
    default:
        throw Exception("just throw");
    }
}

void EventLoopPool::cpuModeLoop()
{
    setThreadName("EventLoopPool");
    PipelineTask task;
    while (likely(cpu_event_queue.pop(task) == MPMCQueueResult::OK))
    {
        handleCpuModeTask(std::move(task));
    }
}

void EventLoopPool::ioModeLoop()
{
    setThreadName("EventLoopPool");
    PipelineTask task;
    while (likely(io_event_queue.pop(task) == MPMCQueueResult::OK))
    {
        handleIOModeTask(std::move(task));
    }
}
} // namespace DB
