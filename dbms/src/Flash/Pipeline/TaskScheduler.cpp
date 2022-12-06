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

#include <Flash/Pipeline/TaskScheduler.h>

namespace DB
{
void TaskCounter::finishOne()
{
    auto pre_count = counter.fetch_sub(1);
    if (pre_count <= 0)
    {
        ++counter;
        cv.notify_one();
    }
    else if (pre_count == 1)
    {
        cv.notify_one();
    }
}

void TaskCounter::waitAllFinished()
{
    while (0 != counter)
    {
        std::unique_lock<std::mutex> lock(global_mutex);
        cv.wait(lock);
    }
}

TaskScheduler::TaskScheduler(size_t thread_num, std::vector<TaskPtr> & tasks)
    : io_runner(*this, thread_num)
    , task_counter(tasks.size())
{
    RUNTIME_ASSERT(thread_num > 0);
    task_runners.reserve(thread_num);
    for (size_t i = 0; i < thread_num; ++i)
        task_runners.emplace_back(std::make_unique<TaskRunner>(*this));

    submit(tasks);
}

bool TaskScheduler::popTask(TaskPtr & task)
{
    {
        std::unique_lock<std::mutex> lock(global_mutex);
        while (true)
        {
            if (unlikely(is_closed))
                return false;
            if (!task_queue.empty())
                break;
            cv.wait(lock);
        }

        task = std::move(task_queue.front());
        assert(task);
        task_queue.pop_front();
    }
    return true;
}

void TaskScheduler::submit(std::vector<TaskPtr> & tasks)
{
    if (tasks.empty())
        return;
    std::lock_guard<std::mutex> lock(global_mutex);
    while (!tasks.empty())
    {
        auto & task = tasks.back();
        assert(task);
        task_queue.emplace_back(std::move(task));
        tasks.pop_back();
        cv.notify_one();
    }
}

void TaskScheduler::submit(TaskPtr && task)
{
    assert(task);
    {
        std::lock_guard<std::mutex> lock(global_mutex);
        task_queue.emplace_back(std::move(task));
    }
    cv.notify_one();
}

void TaskScheduler::submitJob(TaskPtr && task)
{
    io_runner.submit(std::move(task));
}

void TaskScheduler::finishOneTask()
{
    task_counter.finishOne();
}

void TaskScheduler::waitForFinish()
{
    task_counter.waitAllFinished();
    {
        std::lock_guard<std::mutex> lock(global_mutex);
        is_closed = true;
    }
    cv.notify_all();
}
} // namespace DB
