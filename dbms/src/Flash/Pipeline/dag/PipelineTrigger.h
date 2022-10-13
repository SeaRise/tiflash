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

#include <atomic>
#include <memory>
#include <mutex>
#include <vector>

namespace DB
{
class PipelineSignal;
using PipelineSignalPtr = std::shared_ptr<PipelineSignal>;
class Pipeline;
using PipelinePtr = std::shared_ptr<Pipeline>;
class TaskScheduler;
class Context;
class PipelineTrigger
{
public:
    PipelineTrigger(
        const PipelinePtr pipeline_,
        TaskScheduler & task_scheduler_,
        Context & context_)
        : pipeline(pipeline_)
        , task_scheduler(task_scheduler_)
        , context(context_)
    {}

    void trigger();

    void addDependency(const PipelineSignalPtr & signal);

private:
    PipelinePtr pipeline;
    std::vector<PipelineSignalPtr> dependency;
    TaskScheduler & task_scheduler;
    Context & context;
    
    std::atomic_bool is_triggered{false};
};

using PipelineTriggerPtr = std::shared_ptr<PipelineTrigger>;
} // namespace DB
