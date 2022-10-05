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
#include <Common/MPMCQueue.h>
#include <Flash/Pipeline/task/PipelineTask.h>

#include <memory>
#include <thread>

namespace DB
{
class TaskScheduler;

class IOReactor
{
public:
    explicit IOReactor(TaskScheduler & task_scheduler_);

    void finish();

    void submit(size_t loop_id, PipelineTask & task);

    ~IOReactor();

private:
    void loop();

    void handleTask(std::pair<size_t, PipelineTask> & task);

private:
    MPMCQueue<std::pair<size_t, PipelineTask>> event_queue{499999};

    TaskScheduler & task_scheduler;
    LoggerPtr logger = Logger::get("IOReactor");
    std::thread t;
};

using IOReactorPtr = std::unique_ptr<IOReactor>;
} // namespace DB
