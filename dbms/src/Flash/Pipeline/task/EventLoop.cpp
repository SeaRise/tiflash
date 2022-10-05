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
#include <Flash/Pipeline/task/IOReactor.h>
#include <errno.h>

namespace DB
{
EventLoop::EventLoop(
    size_t loop_id_, 
    int core_, 
    PipelineManager & pipeline_manager_, 
    IOReactor & io_reactor_)
    : loop_id(loop_id_)
    , core(core_)
    , pipeline_manager(pipeline_manager_)
    , io_reactor(io_reactor_)
{
    // TODO 2 thread for per event loop.
    t = std::thread(&EventLoop::loop, this);
}

void EventLoop::submit(PipelineTask & task)
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
    LOG_INFO(logger, "stop event loop with cpu core: {}", core);
}

void EventLoop::handleTask(PipelineTask & task)
{
#ifndef NDEBUG
    LOG_TRACE(logger, "handle task: {}", task.toString());
#endif
    auto result = task.execute();
    switch (result.type)
    {
    case PipelineTaskResultType::running:
    {
#ifndef NDEBUG
        LOG_TRACE(logger, "task: {} is running", task.toString());
#endif
        RUNTIME_ASSERT(
            event_queue.tryPush(std::move(task)) != MPMCQueueResult::FULL,
            "EventLoop event queue full");
        io_reactor.submit(loop_id, task);
        break;
    }
    case PipelineTaskResultType::finished:
    {
#ifndef NDEBUG
        LOG_TRACE(logger, "task: {} is finished", task.toString());
#endif
        if (auto dag_scheduler = pipeline_manager.getDAGScheduler(task.mpp_task_id); likely(dag_scheduler))
        {
            dag_scheduler->submit(PipelineEvent::finish(task.task_id, task.pipeline_id));
        }
        break;
    }
    case PipelineTaskResultType::error:
    {
#ifndef NDEBUG
        LOG_TRACE(logger, "task: {} occur error", task.toString());
#endif
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

void EventLoop::loop()
{
#ifdef __linux__
    cpu_set_t cpu_set;
    CPU_ZERO(&cpu_set);
    CPU_SET(core, &cpu_set);
    int ret = sched_setaffinity(0, sizeof(cpu_set), &cpu_set);
    if (unlikely(ret != 0))
        throw Exception(fmt::format("sched_setaffinity fail: {}", std::strerror(errno)));
#endif
    setThreadName(fmt::format("el<{},{}>", loop_id, core).c_str());
    LOG_INFO(logger, "start event loop {} with cpu core: {}", loop_id, core);

    PipelineTask task;
    while (likely(event_queue.pop(task) == MPMCQueueResult::OK))
    {
        handleTask(task);
    }
}
} // namespace DB
