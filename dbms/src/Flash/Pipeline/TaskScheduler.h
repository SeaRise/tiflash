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

#include <Flash/Pipeline/IORunner.h>
#include <Flash/Pipeline/Task.h>
#include <Flash/Pipeline/TaskRunner.h>

#include <atomic>
#include <deque>
#include <list>
#include <mutex>

namespace DB
{
struct TaskCounter
{
    void init(size_t count)
    {
        counter = count;
    }

    void finishOne();

    void waitAllFinished();

    std::atomic_int64_t counter = 0;

    mutable std::mutex global_mutex;
    std::condition_variable cv;
};

class TaskScheduler
{
public:
    TaskScheduler(size_t cpu_thread_num, size_t io_thread_num);
    void submit(std::vector<TaskPtr> & tasks);
    void submit(TaskPtr && task);
    void submitJob(TaskPtr && task);
    bool popTask(TaskPtr & task);
    void finishOneTask();
    void waitForFinish();

private:
    mutable std::mutex global_mutex;
    std::condition_variable cv;
    bool is_closed = false;
    std::deque<TaskPtr> task_queue;

    std::vector<TaskRunnerPtr> task_runners;

    IORunner io_runner;

    TaskCounter task_counter;

    LoggerPtr logger = Logger::get("TaskScheduler");
};
} // namespace DB
