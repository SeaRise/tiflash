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

#include <Common/ThreadManager.h>
#include <Flash/Pipeline/Schedule/TaskQueue/FIFOTaskQueue.h>
#include <Flash/Pipeline/Schedule/TaskQueue/MultiLevelFeedbackQueue.h>
#include <Flash/Pipeline/Schedule/TaskQueue/TaskQueue.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <gtest/gtest.h>

#include <thread>
#include <unordered_set>

namespace DB::tests
{
namespace
{
class IndexTask : public Task
{
public:
    explicit IndexTask(size_t index_)
        : Task(nullptr)
        , index(index_)
    {}

    ExecTaskStatus executeImpl() override { return ExecTaskStatus::FINISHED; }

    size_t index;
};
} // namespace

class TaskQueueTestRunner : public ::testing::Test
{
};

// fifo
TEST_F(TaskQueueTestRunner, fifo)
try
{
    TaskQueuePtr queue = std::make_unique<FIFOTaskQueue>();

    auto thread_manager = newThreadManager();
    size_t valid_task_num = 1000;

    // submit valid task
    thread_manager->schedule(false, "submit", [&]() {
        for (size_t i = 0; i < valid_task_num; ++i)
            queue->submit(std::make_unique<IndexTask>(i));
        // Close the queue after all valid tasks have been consumed.
        while (!queue->empty())
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        queue->close();
    });
    // take valid task
    thread_manager->schedule(false, "take", [&]() {
        TaskPtr task;
        size_t expect_index = 0;
        while (queue->take(task))
        {
            ASSERT_TRUE(task);
            auto * index_task = static_cast<IndexTask *>(task.get());
            ASSERT_EQ(index_task->index, expect_index++);
            task.reset();
        }
        ASSERT_EQ(expect_index, valid_task_num);
    });
    thread_manager->wait();

    // No tasks are taken after the queue is closed.
    queue->submit(std::make_unique<IndexTask>(valid_task_num));
    TaskPtr task;
    ASSERT_FALSE(queue->take(task));
}
CATCH

// mlfq
TEST_F(TaskQueueTestRunner, mlfq_init)
try
{
    TaskQueuePtr queue = std::make_unique<ExecuteMultiLevelFeedbackQueue>();
    size_t valid_task_num = 1000;
    // submit
    for (size_t i = 0; i < valid_task_num; ++i)
        queue->submit(std::make_unique<IndexTask>(i));
    // take
    for (size_t i = 0; i < valid_task_num; ++i)
    {
        TaskPtr task;
        queue->take(task);
        ASSERT_EQ(task->mlfq_level, 0);
    }
    ASSERT_TRUE(queue->empty());
    queue->close();
    // No tasks are taken after the queue is closed.
    queue->submit(std::make_unique<IndexTask>(valid_task_num));
    TaskPtr task;
    ASSERT_FALSE(queue->take(task));
}
CATCH

TEST_F(TaskQueueTestRunner, mlfq_random)
try
{
    TaskQueuePtr queue = std::make_unique<ExecuteMultiLevelFeedbackQueue>();

    auto thread_manager = newThreadManager();
    size_t valid_task_num = 1000;
    auto mock_value = []() {
        return ExecuteMultiLevelFeedbackQueue::LEVEL_TIME_SLICE_BASE_NS * (1 + random() % 100);
    };

    // submit valid task
    thread_manager->schedule(false, "submit", [&]() {
        for (size_t i = 0; i < valid_task_num; ++i)
        {
            TaskPtr task = std::make_unique<IndexTask>(i);
            auto value = mock_value();
            queue->updateStatistics(task, value);
            task->profile_info.addExecuteTime(value);
            queue->submit(std::move(task));
        }
        // Close the queue after all valid tasks have been consumed.
        while (!queue->empty())
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        queue->close();
    });
    // take valid task
    thread_manager->schedule(false, "take", [&]() {
        std::unordered_set<size_t> indexes;
        for (size_t i = 0; i < valid_task_num; ++i)
            indexes.insert(i);
        TaskPtr task;
        while (queue->take(task))
        {
            ASSERT_TRUE(task);
            auto * index_task = static_cast<IndexTask *>(task.get());
            ASSERT_TRUE(indexes.erase(index_task->index));
            task.reset();
        }
        ASSERT_TRUE(indexes.empty());
    });
    thread_manager->wait();
}
CATCH

TEST_F(TaskQueueTestRunner, mlfq_level)
try
{
    ExecuteMultiLevelFeedbackQueue queue;
    TaskPtr task = std::make_unique<IndexTask>(0);
    queue.submit(std::move(task));
    for (size_t level = 0; level < ExecuteMultiLevelFeedbackQueue::QUEUE_SIZE; ++level)
    {
        while (queue.take(task))
        {
            ASSERT_EQ(task->mlfq_level, level);
            ASSERT_TRUE(task);
            auto value = ExecuteMultiLevelFeedbackQueue::LEVEL_TIME_SLICE_BASE_NS;
            queue.updateStatistics(task, value);
            task->profile_info.addExecuteTime(value);
            bool need_break = ExecuteTimeGetter::get(task) >= queue.getUnitQueueInfo(level).time_slice;
            queue.submit(std::move(task));
            if (need_break)
                break;
        }
    }
    queue.take(task);
    ASSERT_TRUE(queue.empty());
    ASSERT_EQ(task->mlfq_level, ExecuteMultiLevelFeedbackQueue::QUEUE_SIZE - 1);
}
CATCH

TEST_F(TaskQueueTestRunner, mlfq_feedback)
try
{
    ExecuteMultiLevelFeedbackQueue queue;

    // The case that low level > high level
    {
        // level `QUEUE_SIZE - 1`
        TaskPtr task = std::make_unique<IndexTask>(0);
        task->mlfq_level = ExecuteMultiLevelFeedbackQueue::QUEUE_SIZE - 1;
        auto value = queue.getUnitQueueInfo(task->mlfq_level).time_slice;
        queue.updateStatistics(task, value);
        task->profile_info.addExecuteTime(value);
        queue.submit(std::move(task));
    }
    {
        // level `0`
        TaskPtr task = std::make_unique<IndexTask>(0);
        auto value = queue.getUnitQueueInfo(0).time_slice - 1;
        queue.updateStatistics(task, value);
        task->profile_info.addExecuteTime(value);
        queue.submit(std::move(task));
    }
    // the first task will be level `0`.
    {
        TaskPtr task;
        queue.take(task);
        ASSERT_EQ(task->mlfq_level, 0);
    }
    {
        TaskPtr task;
        queue.take(task);
        ASSERT_EQ(task->mlfq_level, ExecuteMultiLevelFeedbackQueue::QUEUE_SIZE - 1);
    }
    ASSERT_TRUE(queue.empty());

    // The case that low level < high level
    size_t task_num = 1000;
    for (size_t i = 0; i < task_num; ++i)
    {
        // level `0`
        TaskPtr task = std::make_unique<IndexTask>(0);
        auto value = queue.getUnitQueueInfo(0).time_slice - 1;
        queue.updateStatistics(task, value);
        task->profile_info.addExecuteTime(value);
        queue.submit(std::move(task));
    }
    {
        // level `QUEUE_SIZE - 1`
        TaskPtr task = std::make_unique<IndexTask>(0);
        task->mlfq_level = ExecuteMultiLevelFeedbackQueue::QUEUE_SIZE - 1;
        auto value = queue.getUnitQueueInfo(task->mlfq_level).time_slice;
        queue.updateStatistics(task, value);
        task->profile_info.addExecuteTime(value);
        queue.submit(std::move(task));
    }
    // the first task will be level `QUEUE_SIZE - 1`.
    {
        TaskPtr task;
        queue.take(task);
        ASSERT_EQ(task->mlfq_level, ExecuteMultiLevelFeedbackQueue::QUEUE_SIZE - 1);
    }
    for (size_t i = 0; i < task_num; ++i)
    {
        TaskPtr task;
        queue.take(task);
        ASSERT_EQ(task->mlfq_level, 0);
    }
    ASSERT_TRUE(queue.empty());
}
CATCH

} // namespace DB::tests
