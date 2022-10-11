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

#include <Common/MemoryTracker.h>
#include <Flash/Mpp/MPPTaskId.h>
#include <Transforms/Transforms.h>

#include <memory>

namespace DB
{
enum class PipelineTaskStatus
{
    // cpu mode
    cpu_run,
    // io mode
    io_wait,
    io_finishing,
    // finish
    finish,
};

enum class PipelineTaskResultType
{
    running,
    finished,
    error,
};

struct PipelineTaskResult
{
    PipelineTaskResultType type;
    String err_msg;
};

PipelineTaskResult toFinish();
PipelineTaskResult toFail(const String & err_msg);
PipelineTaskResult toRunning();

class PipelineTask
{
public:
    PipelineTask(
        UInt32 task_id_,
        UInt32 pipeline_id_,
        const MPPTaskId & mpp_task_id_,
        const TransformsPtr & transforms_)
        : task_id(task_id_)
        , pipeline_id(pipeline_id_)
        , mpp_task_id(mpp_task_id_)
        , transforms(transforms_)
        , mem_tracker(current_memory_tracker ? current_memory_tracker->shared_from_this() : nullptr)
        , logger(Logger::get("PipelineTask", fmt::format("{{mpp_task_id: {}, pipeline_id: {}, task_id: {}}}", mpp_task_id.toString(), pipeline_id, task_id)))
    {}

    void prepare();

    PipelineTaskResult execute();

    void finish();

    bool tryToCpuMode();
    bool tryToIOMode();

    String toString() const;

    void changeStatus(PipelineTaskStatus new_status);

public:
    UInt32 task_id;
    UInt32 pipeline_id;
    MPPTaskId mpp_task_id;
    TransformsPtr transforms;

    PipelineTaskStatus status = PipelineTaskStatus::cpu_run;

    std::shared_ptr<MemoryTracker> mem_tracker;

private:
    MemoryTracker * getMemTracker()
    {
        return mem_tracker ? mem_tracker.get() : nullptr;
    }

private:
    LoggerPtr logger;
};

using PipelineTaskPtr = std::unique_ptr<PipelineTask>;
} // namespace DB
