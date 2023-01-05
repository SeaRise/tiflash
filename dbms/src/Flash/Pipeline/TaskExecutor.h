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
#include <Flash/Pipeline/Task.h>
#include <Flash/Pipeline/TaskQueue.h>

#include <thread>
#include <vector>

namespace DB
{
struct ExecutorConfig;
class TaskScheduler;

class TaskExecutor
{
public:
    TaskExecutor(TaskScheduler & scheduler_, const ExecutorConfig & config);

    void close();

    void waitForStop();

    void submit(TaskPtr && task);

    void submit(std::vector<TaskPtr> & tasks);

private:
    void loop() noexcept;

    void handleTask(TaskPtr & task);

private:
    TaskQueuePtr task_queue;

    LoggerPtr logger = Logger::get("TaskExecutor");

    TaskScheduler & scheduler;

    std::vector<std::thread> threads;
};
} // namespace DB
