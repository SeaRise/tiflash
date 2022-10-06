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
    size_t loop_id_,
    size_t cpu_thread_count,
    const std::vector<int> & cpus_,
    PipelineManager & pipeline_manager_)
    : loop_id(loop_id_)
    , cpus(cpus_)
    , pipeline_manager(pipeline_manager_)
{
    cpu_threads.reserve(cpu_thread_count);
    for (size_t i = 0; i < cpu_thread_count; ++i)
        cpu_threads.emplace_back(std::thread(&EventLoopPool::cpuModeLoop, this));
    io_thread = std::thread(&EventLoopPool::ioModeLoop, this);
}

void EventLoopPool::submit(std::vector<PipelineTask> & tasks)
{
    for (size_t i = 0; i < cpu_threads.size(); ++i)
    {
        if (!tasks.empty())
        {
            submitCPU(std::move(tasks.back()));
            tasks.pop_back();
        }
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
    LOG_INFO(logger, "stop event loop pool {}", loop_id);
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
    setCPUAffinity();
    setThreadName(fmt::format("loop_{}", loop_id).c_str());
    PipelineTask task;
    while (likely(cpu_event_queue.pop(task) == MPMCQueueResult::OK))
    {
        handleCpuModeTask(std::move(task));
    }
}

void EventLoopPool::ioModeLoop()
{
    setCPUAffinity();
    setThreadName(fmt::format("loop_{}", loop_id).c_str());
    PipelineTask task;
    while (likely(io_event_queue.pop(task) == MPMCQueueResult::OK))
    {
        handleIOModeTask(std::move(task));
    }
}

void EventLoopPool::setCPUAffinity()
{
    if (cpus.empty())
    {
        return;
    }
#ifdef __linux__
    cpu_set_t cpu_set;
    CPU_ZERO(&cpu_set);
    for (int i : cpus)
    {
        CPU_SET(i, &cpu_set);
    }
    int ret = sched_setaffinity(0, sizeof(cpu_set), &cpu_set);
    if (ret != 0)
    {
        // It can be failed due to some CPU core cannot access, such as CPU offline.
        LOG_WARNING(logger, "sched_setaffinity fail, cpus={} errno={}", cpus, std::strerror(errno));
    }
    else
    {
        LOG_FMT_DEBUG(logger, "sched_setaffinity succ, cpus={}", cpus);
    }
#endif
}
} // namespace DB
