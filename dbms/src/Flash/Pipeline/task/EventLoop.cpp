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
#include <Flash/Pipeline/task/EventLoop.h>
#include <errno.h>

namespace DB
{
EventLoop::EventLoop(
    size_t loop_id_, 
    const std::vector<int> & cpus_,
    PipelineManager & pipeline_manager_)
    : loop_id(loop_id_)
    , cpus(cpus_)
    , pipeline_manager(pipeline_manager_)
{
    // TODO 2 thread for per event loop.
    t = std::thread(&EventLoop::loop, this);
}

void EventLoop::submit(PipelineTask && task)
{
    RUNTIME_ASSERT(
        event_queue.tryPush(std::move(task)) != MPMCQueueResult::FULL,
        "EventLoop event queue full");
}

void EventLoop::finish()
{
    event_queue.finish();
}

EventLoop::~EventLoop()
{
    t.join();
    LOG_INFO(logger, "stop event loop {}", loop_id);
}

void EventLoop::handleCpuModeTask(PipelineTask && task)
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
            io_wait_queue.emplace_back(std::move(task));
        else
            submit(std::move(task));
        break;
    }
    case PipelineTaskResultType::finished:
    {
        if (task.status == PipelineTaskStatus::io_finish)
            io_wait_queue.emplace_back(std::move(task));
        else
        {
            if (auto dag_scheduler = pipeline_manager.getDAGScheduler(task.mpp_task_id); likely(dag_scheduler))
            {
                dag_scheduler->submit(PipelineEvent::finish(task.task_id, task.pipeline_id));
            }
        }
        break;
    }
    case PipelineTaskResultType::error:
    {
        if (auto dag_scheduler = pipeline_manager.getDAGScheduler(task.mpp_task_id); likely(dag_scheduler))
        {
            dag_scheduler->submit(PipelineEvent::fail(result.err_msg));
        }
        break;
    }
    default:
        throw Exception("Unknown PipelineTaskResultType");
    }
}

bool EventLoop::cpuModeLoop(PipelineTask & task)
{
    while (likely(event_queue.pop(task) == MPMCQueueResult::OK))
    {
        handleCpuModeTask(std::move(task));
        // switch to cpu and io loop.
        if (!io_wait_queue.empty())
            return true;
    }
    return false;
}

void EventLoop::handleIOModeTask(PipelineTask && task)
{
#ifndef NDEBUG
    LOG_TRACE(logger, "handle io mode task: {}", task.toString());
#endif
    switch(task.status)
    {
    case PipelineTaskStatus::io_wait:
    {
        if (task.tryToCpuMode())
            submit(std::move(task));
        else
            io_wait_queue.emplace_back(std::move(task));
        break;
    }
    case PipelineTaskStatus::io_finish:
    {
        if (task.tryToCpuMode())
        {
            if (auto dag_scheduler = pipeline_manager.getDAGScheduler(task.mpp_task_id); likely(dag_scheduler))
            {
                dag_scheduler->submit(PipelineEvent::finish(task.task_id, task.pipeline_id));
            }
        }
        else
            io_wait_queue.emplace_back(std::move(task));
        break;
    }
    default:
        throw Exception("just throw");
    }
}

bool EventLoop::cpuAndIOModeLoop(PipelineTask & task)
{
    for (size_t i = 0; i < 60; ++i)
    {
        size_t cpu_mode_tasks = event_queue.size();
        if (cpu_mode_tasks == 0)
        {
            if (unlikely(event_queue.getStatus() != MPMCQueueStatus::NORMAL))
                return false;
            else
                break;
        }
        for (size_t j = 0; j < cpu_mode_tasks; ++j)
        {
            auto res = event_queue.tryPop(task);
            switch (res)
            {
            case MPMCQueueResult::OK:
                handleCpuModeTask(std::move(task));
                break;
            case MPMCQueueResult::EMPTY:
                goto io_mode;
            default:
                return false;
            }
        }
    }
    io_mode:
    size_t io_mode_tasks = io_wait_queue.size();
    for (size_t i = 0; i < io_mode_tasks; ++i)
    {
        task = std::move(io_wait_queue.front());
        io_wait_queue.pop_front();
        handleIOModeTask(std::move(task));
    }
    return true;
}

void EventLoop::setCPUAffinity()
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

void EventLoop::loop()
{
    setCPUAffinity();
    setThreadName(fmt::format("loop_{}", loop_id).c_str());

    PipelineTask task;
    while (true)
    {
        if (io_wait_queue.empty())
        {
            if (unlikely(!cpuModeLoop(task)))
                return;
        }
        else
        {
            if (unlikely(!cpuAndIOModeLoop(task)))
                return;
        }
    }
}
} // namespace DB
