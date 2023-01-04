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

#include <Common/Exception.h>
#include <Common/setThreadName.h>
#include <Flash/Pipeline/FIFOTaskQueue.h>
#include <Flash/Pipeline/TaskExecutor.h>
#include <Flash/Pipeline/TaskHelper.h>
#include <Flash/Pipeline/TaskScheduler.h>
#include <common/likely.h>
#include <common/logger_useful.h>

namespace DB
{
TaskExecutor::TaskExecutor(TaskScheduler & scheduler_, size_t thread_num)
    : task_queue(std::make_unique<FIFOTaskQueue>())
    , scheduler(scheduler_)
{
    RUNTIME_CHECK(thread_num > 0);
    threads.reserve(thread_num);
    for (size_t i = 0; i < thread_num; ++i)
        threads.emplace_back(&TaskExecutor::loop, this);
}

void TaskExecutor::close()
{
    task_queue->close();
}

void TaskExecutor::waitForStop()
{
    for (auto & thread : threads)
        thread.join();
    LOG_INFO(logger, "task executor is stopped");
}

void TaskExecutor::loop() noexcept
{
    setThreadName("TaskExecutor");
    LOG_INFO(logger, "start task executor loop");
    ASSERT_MEMORY_TRACKER

    TaskPtr task;
    while (likely(task_queue->take(task)))
    {
        handleTask(task);
        assert(!task);
        ASSERT_MEMORY_TRACKER
    }

    LOG_INFO(logger, "task executor loop finished");
}

void TaskExecutor::handleTask(TaskPtr & task)
{
    assert(task);
    task->profile_info.addExecutePendingTime();
    TRACE_MEMORY(task);
    UInt64 time_spent = 0;
    while (true)
    {
        assert(task);
        auto status = task->execute();
        time_spent += task->profile_info.elapsedFromPrev();
        switch (status)
        {
        case ExecTaskStatus::RUNNING:
        {
            static constexpr UInt64 YIELD_MAX_TIME_SPENT = 100'000'000L;
            if (time_spent >= YIELD_MAX_TIME_SPENT)
            {
                task_queue->updateStatistics(task, time_spent);
                task->profile_info.addExecuteTime(time_spent);
                submit(std::move(task));
                return;
            }
            break;
        }
        case ExecTaskStatus::WAITING:
            task_queue->updateStatistics(task, time_spent);
            task->profile_info.addExecuteTime(time_spent);
            scheduler.wait_reactor.submit(std::move(task));
            return;
        case ExecTaskStatus::SPILLING:
            task_queue->updateStatistics(task, time_spent);
            task->profile_info.addExecuteTime(time_spent);
            scheduler.spill_executor.submit(std::move(task));
            return;
        case FINISH_STATUS:
            task_queue->updateStatistics(task, time_spent);
            task->profile_info.addExecuteTime(time_spent);
            task.reset();
            return;
        default:
            __builtin_unreachable();
        }
    }
}

void TaskExecutor::submit(TaskPtr && task)
{
    task_queue->submit(std::move(task));
}

void TaskExecutor::submit(std::vector<TaskPtr> & tasks)
{
    task_queue->submit(tasks);
}
} // namespace DB
