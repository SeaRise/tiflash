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

#pragma once

#include <Common/Logger.h>
#include <Flash/Pipeline/TaskQueue.h>

#include <deque>
#include <mutex>

namespace DB
{
class UnitQueue
{
public:
    UnitQueue(UInt64 time_slice_): time_slice(time_slice_) {}

    void submit(TaskPtr && task);

    void take(TaskPtr & task);

    bool empty();

    double accuTimeAfterDivisor();

public:
    std::atomic_uint64_t accu_consume_time{0};

    const UInt64 time_slice;

    // factor for normalization
    double factor_for_normal{0};

private:
    std::deque<TaskPtr> task_queue;
};
using UnitQueuePtr = std::unique_ptr<UnitQueue>;

template<typename TimeGetter>
class MultiLevelFeedbackQueue : public TaskQueue
{
public:
    MultiLevelFeedbackQueue()
    {
        level_queues.reserve(QUEUE_SIZE);
        UInt64 time_slice = 0;
        for (size_t i = 0; i < QUEUE_SIZE; ++i)
        {
            time_slice += LEVEL_TIME_SLICE_BASE_NS * (i + 1);
            level_queues.push_back(std::make_unique<UnitQueue>(time_slice));
        }

        double factor = 1;
        for (int i = QUEUE_SIZE - 1; i >= 0; --i) {
            // initialize factor for every unit queue,
            // Higher priority queues have more execution time,
            // so they have a larger factor.
            level_queues[i]->factor_for_normal = factor;
            factor *= RATIO_OF_ADJACENT_QUEUE;
        }
    }

    void submit(TaskPtr && task) override
    {
        assert(task);
        size_t level = computeQueueLevel(task);
        {
            std::lock_guard lock(mu);
            level_queues[level]->submit(std::move(task));
        }
        assert(!task);
        cv.notify_one();
    }

    void submit(std::vector<TaskPtr> & tasks) override
    {
        if (tasks.empty())
            return;

        std::vector<size_t> levels;
        levels.reserve(tasks.size());
        for (auto & task : tasks)
            levels.push_back(computeQueueLevel(task));

        std::lock_guard lock(mu);
        for (size_t i = 0; i < tasks.size(); ++i)
        {
            level_queues[levels[i]]->submit(std::move(tasks[i]));
            cv.notify_one();
        }
    }

    bool take(TaskPtr & task) override
    {
        assert(!task);
        // -1 means no candidates; else has candidate.
        int queue_idx = -1;
        double target_accu_time = 0;

        {
            std::unique_lock lock(mu);
            while (true)
            {
                if (unlikely(is_closed))
                    return false;
    
                // Find the queue with the smallest execution time.
                for (size_t i = 0; i < QUEUE_SIZE; ++i)
                {
                    // we just search for queue has element
                    const auto & cur_queue = level_queues[i];
                    if (!cur_queue->empty())
                    {
                        double local_target_time = cur_queue->accuTimeAfterDivisor();
                        if (queue_idx < 0 || local_target_time < target_accu_time)
                        {
                            target_accu_time = local_target_time;
                            queue_idx = i;
                        }
                    }
                }
    
                if (queue_idx >= 0)
                    break;
                cv.wait(lock);
            }
            level_queues[queue_idx]->take(task);
        }

        assert(task);
        return true;
    }

    void updateStatistics(const TaskPtr & task, size_t value) override
    {
        assert(task);
        size_t level = computeQueueLevel(task);
        assert(level < level_queues.size());
        level_queues[level]->accu_consume_time += value;
    }

    bool empty() override
    {
        std::lock_guard lock(mu);
        for (const auto & queue : level_queues)
        {
            if (!queue->empty())
                return false;
        }
        return true;
    }

    void close() override
    {
        {
            std::lock_guard lock(mu);
            is_closed = true;
        }
        cv.notify_all();
    }

private:
    size_t computeQueueLevel(const TaskPtr & task)
    {
        auto time_spent = TimeGetter::get(task);
        for (size_t i = 0; i < QUEUE_SIZE; ++i)
        {
            if (time_spent < level_queues[i]->time_slice)
                return i;
        }
        return QUEUE_SIZE - 1;
    }

private:
    std::mutex mu;
    std::condition_variable cv;
    bool is_closed = false;

    LoggerPtr logger = Logger::get("MultiLevelFeedbackQueue");

    static constexpr size_t QUEUE_SIZE = 8;

    // The time slice of the i-th level is (i+1)*LEVEL_TIME_SLICE_BASE ns,
    // so when a driver's execution time exceeds 0.2s, 0.6s, 1.2s, 2s, 3s, 4.2s, 5.6s, and 7.2s,
    // it will move to next level.
    static constexpr int64_t LEVEL_TIME_SLICE_BASE_NS = 200'000'000L;

    static constexpr double RATIO_OF_ADJACENT_QUEUE = 1.2;

    std::vector<UnitQueuePtr> level_queues;
};

struct ExecuteTimeGetter
{
    static UInt64 get(const TaskPtr & task)
    {
        assert(task);
        return task->profile_info.execute_time;
    }
};

struct SpillTimeGetter
{
    static UInt64 get(const TaskPtr & task)
    {
        assert(task);
        return task->profile_info.spill_time;
    }
};
} // namespace DB
