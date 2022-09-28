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

namespace DB
{
struct PipelineManager;

class EventLoop
{
public:
    EventLoop(size_t loop_id_, PipelineManager & pipeline_manager_);

    void loop();

    void finish();

    void submit(PipelineTask && task);

private:
    void handleTask(PipelineTask & task);

private:
    size_t loop_id;
    MPMCQueue<PipelineTask> event_queue{499999};

    PipelineManager & pipeline_manager;

    LoggerPtr logger = Logger::get(fmt::format("event loop {}", loop_id));
};

using EventLoopPtr = std::shared_ptr<EventLoop>;
} // namespace DB
