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
struct PipelineManager;
class IOReactor;

class EventLoop
{
public:
    EventLoop(
        size_t loop_id_, 
        int core_, 
        PipelineManager & pipeline_manager_, 
        IOReactor & io_reactor_);

    void finish();

    void submit(PipelineTask & task);

    ~EventLoop();

private:
    void loop();

    void handleTask(PipelineTask & task);

private:
    size_t loop_id;
    int core;
    MPMCQueue<PipelineTask> event_queue{499999};

    PipelineManager & pipeline_manager;
    IOReactor & io_reactor;
    LoggerPtr logger = Logger::get(fmt::format("event loop {} with cpu_core {}", loop_id, core));
    std::thread t;
};

using EventLoopPtr = std::unique_ptr<EventLoop>;
} // namespace DB
