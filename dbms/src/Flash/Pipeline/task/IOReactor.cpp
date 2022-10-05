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
#include <Flash/Pipeline/task/TaskScheduler.h>
#include <Flash/Pipeline/task/IOReactor.h>

namespace DB
{
IOReactor::IOReactor(TaskScheduler & task_scheduler_)
    : task_scheduler(task_scheduler_)
{
    t = std::thread(&IOReactor::loop, this);
}

void IOReactor::submit(size_t loop_id, PipelineTask & task)
{
    RUNTIME_ASSERT(
        event_queue.tryPush(std::pair{loop_id, std::move(task)}) != MPMCQueueResult::FULL,
        "IOReactor event queue full");
}

void IOReactor::finish()
{
    event_queue.finish();
}

IOReactor::~IOReactor()
{
    t.join();
    LOG_INFO(logger, "stop IOReactor loop");
}

void IOReactor::handleTask(std::pair<size_t, PipelineTask> & task)
{
#ifndef NDEBUG
    LOG_TRACE(logger, "handle task: {}", task.second.toString());
#endif
    if (task.second.execute().type == PipelineTaskResultType::running)
        task_scheduler.submit(task.first, task.second);
    else
        RUNTIME_ASSERT(
            event_queue.tryPush(std::move(task)) != MPMCQueueResult::FULL,
            "IOReactor event queue full");
}

void IOReactor::loop()
{
    setThreadName("IOReactor");
    LOG_INFO(logger, "start IOReactor");

    std::pair<size_t, PipelineTask> task;
    while (likely(event_queue.pop(task) == MPMCQueueResult::OK))
    {
        handleTask(task);
    }
}
} // namespace DB
