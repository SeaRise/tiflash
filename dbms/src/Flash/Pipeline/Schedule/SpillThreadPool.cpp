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
#include <Flash/Pipeline/Schedule/SpillThreadPool.h>
#include <Flash/Pipeline/Schedule/Task/TaskHelper.h>
#include <Flash/Pipeline/Schedule/TaskQueue/FIFOTaskQueue.h>
#include <Flash/Pipeline/Schedule/TaskQueue/MultiLevelFeedbackQueue.h>
#include <Flash/Pipeline/Schedule/TaskQueue/getTaskQueue.h>
#include <Flash/Pipeline/Schedule/TaskScheduler.h>
#include <assert.h>
#include <common/likely.h>
#include <common/logger_useful.h>
#include <errno.h>

namespace DB
{
SpillThreadPool::SpillThreadPool(TaskScheduler & scheduler_, const ThreadPoolConfig & config)
    : scheduler(scheduler_)
{
    task_queue = getTaskQueue<SpillMultiLevelFeedbackQueue, FIFOTaskQueue>(config.queue_type);
    auto thread_num = config.pool_size;
    RUNTIME_CHECK(thread_num > 0);
    threads.reserve(thread_num);
    for (size_t i = 0; i < thread_num; ++i)
        threads.emplace_back(&SpillThreadPool::loop, this);
}

void SpillThreadPool::close()
{
    task_queue->close();
}

void SpillThreadPool::waitForStop()
{
    for (auto & thread : threads)
        thread.join();
    LOG_INFO(logger, "spill thread pool is stopped");
}

void SpillThreadPool::submit(TaskPtr && task)
{
    task_queue->submit(std::move(task));
}

void SpillThreadPool::handleTask(TaskPtr && task)
{
    assert(task);
    TRACE_MEMORY(task);

    task->profile_info.addSpillPendingTime();
    UInt64 total_time_spent = 0;
    ExecTaskStatus status;
    while (true)
    {
        status = task->spill();
        auto inc_time_spent = task->profile_info.elapsedFromPrev();
        task_queue->updateStatistics(task, inc_time_spent);
        total_time_spent += inc_time_spent;
        if (status != ExecTaskStatus::SPILLING || total_time_spent >= YIELD_MAX_TIME_SPENT)
            break;
    }
    task->profile_info.addSpillTime(total_time_spent);

    switch (status)
    {
    case ExecTaskStatus::SPILLING:
        submit(std::move(task));
        break;
    case ExecTaskStatus::RUNNING:
        scheduler.task_thread_pool.submit(std::move(task));
        break;
    case FINISH_STATUS:
        task.reset();
        break;
    default:
        RUNTIME_ASSERT(true, logger, "Unexpected task state {}", magic_enum::enum_name(status));
    }
}

void SpillThreadPool::loop() noexcept
{
    setThreadName("SpillThreadPool");
    LOG_INFO(logger, "start spill thread pool loop");
    ASSERT_MEMORY_TRACKER

    TaskPtr task;
    while (likely(task_queue->take(task)))
    {
        handleTask(std::move(task));
        assert(!task);
        ASSERT_MEMORY_TRACKER
    }

    LOG_INFO(logger, "spill thread pool loop finished");
}
} // namespace DB
