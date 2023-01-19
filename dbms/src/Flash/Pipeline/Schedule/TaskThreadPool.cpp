// Copyright 2023 PingCAP, Ltd.
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
#include <Flash/Pipeline/Schedule/TaskQueues/MultiLevelFeedbackQueue.h>
#include <Flash/Pipeline/Schedule/TaskQueues/getTaskQueue.h>
#include <Flash/Pipeline/Schedule/TaskScheduler.h>
#include <Flash/Pipeline/Schedule/TaskThreadPool.h>
#include <Flash/Pipeline/Schedule/Tasks/TaskHelper.h>
#include <common/likely.h>
#include <common/logger_useful.h>

namespace DB
{
TaskThreadPool::TaskThreadPool(TaskScheduler & scheduler_, const ThreadPoolConfig & config)
    : scheduler(scheduler_)
{
    task_queue = getTaskQueue<ExecuteMultiLevelFeedbackQueue>(config.queue_type);
    auto thread_num = config.pool_size;
    RUNTIME_CHECK(thread_num > 0);
    threads.reserve(thread_num);
    for (size_t i = 0; i < thread_num; ++i)
        threads.emplace_back(&TaskThreadPool::loop, this, i);
}

void TaskThreadPool::close()
{
    task_queue->close();
}

void TaskThreadPool::waitForStop()
{
    for (auto & thread : threads)
        thread.join();
    LOG_INFO(logger, "task thread pool is stopped");
}

void TaskThreadPool::loop(size_t thread_no) noexcept
{
    auto thread_no_str = fmt::format("thread_no={}", thread_no);
    auto thread_logger = logger->getChild(thread_no_str);
    setThreadName(thread_no_str.c_str());
    LOG_INFO(thread_logger, "start loop");
    ASSERT_MEMORY_TRACKER

    TaskPtr task;
    while (likely(task_queue->take(task)))
    {
        handleTask(task, thread_logger);
        assert(!task);
        ASSERT_MEMORY_TRACKER
    }

    LOG_INFO(thread_logger, "loop finished");
}

void TaskThreadPool::handleTask(TaskPtr & task, const LoggerPtr & log)
{
    assert(task);
    TRACE_MEMORY(task);

    task->profile_info.elapsedExecutePendingTime();
    UInt64 total_time_spent = 0;
    ExecTaskStatus status;
    while (true)
    {
        status = task->execute();
        auto inc_time_spent = task->profile_info.elapsedFromPrev();
        task_queue->updateStatistics(task, inc_time_spent);
        total_time_spent += inc_time_spent;
        // The executing task should yield if it takes more than `YIELD_MAX_TIME_SPENT_NS`.
        if (status != ExecTaskStatus::RUNNING || total_time_spent >= YIELD_MAX_TIME_SPENT_NS)
            break;
    }
    task->profile_info.addExecuteTime(total_time_spent);

    switch (status)
    {
    case ExecTaskStatus::RUNNING:
        submit(std::move(task));
        break;
    case ExecTaskStatus::WAITING:
        scheduler.wait_reactor.submit(std::move(task));
        break;
    case ExecTaskStatus::SPILLING:
        scheduler.spill_thread_pool.submit(std::move(task));
        break;
    case FINISH_STATUS:
        task.reset();
        break;
    default:
        UNEXPECTED_STATUS(log, status);
    }
}

void TaskThreadPool::submit(TaskPtr && task)
{
    task_queue->submit(std::move(task));
}

void TaskThreadPool::submit(std::vector<TaskPtr> & tasks)
{
    task_queue->submit(tasks);
}
} // namespace DB
