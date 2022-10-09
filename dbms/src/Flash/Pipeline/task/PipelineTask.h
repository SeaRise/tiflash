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
#include <Flash/Pipeline/task/PipelineFinishCounter.h>

namespace DB
{
enum class PipelineTaskStatus
{
    // cpu mode
    cpu_run,
    // io mode
    io_wait,
    io_finishing,
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

struct PipelineManager;

class PipelineTask
{
public:
    PipelineTask() = default;

    PipelineTask(
        UInt32 pipeline_id_,
        const MPPTaskId & mpp_task_id_,
        const TransformsPtr & transforms_,
        bool is_final_task_,
        const PipelineFinishCounterPtr & pipeline_finish_counter_,
        const std::vector<PipelineFinishCounterPtr> & depends_)
        : pipeline_id(pipeline_id_)
        , mpp_task_id(mpp_task_id_)
        , transforms(transforms_)
        , mem_tracker(current_memory_tracker ? current_memory_tracker->shared_from_this() : nullptr)
        , is_final_task(is_final_task_)
        , pipeline_finish_counter(pipeline_finish_counter_)
        , depends(depends_)
    {}

    PipelineTask(PipelineTask && task)
        : pipeline_id(std::move(task.pipeline_id))
        , mpp_task_id(std::move(task.mpp_task_id))
        , transforms(std::move(task.transforms))
        , status(std::move(task.status))
        , mem_tracker(std::move(task.mem_tracker))
        , is_final_task(std::move(task.is_final_task))
        , pipeline_finish_counter(std::move(task.pipeline_finish_counter))
        , depends(std::move(task.depends))
    {}

    PipelineTask & operator=(PipelineTask && task)
    {
        if (this != &task)
        {
            pipeline_id = std::move(task.pipeline_id);
            mpp_task_id = std::move(task.mpp_task_id);
            transforms = std::move(task.transforms);
            status = std::move(task.status);
            mem_tracker = std::move(task.mem_tracker);
            is_final_task = std::move(task.is_final_task);
            pipeline_finish_counter = std::move(task.pipeline_finish_counter);
            depends = std::move(task.depends);
        }
        return *this;
    }

    void prepare();

    PipelineTaskResult execute(PipelineManager & pipeline_manager);

    MemoryTracker * getMemTracker()
    {
        return mem_tracker ? mem_tracker.get() : nullptr;
    }

    bool tryToCpuMode(PipelineManager & pipeline_manager);
    bool tryToIOMode();

    String toString() const;

    bool isDependsReady();

public:
    UInt32 pipeline_id;
    MPPTaskId mpp_task_id;
    TransformsPtr transforms;

    PipelineTaskStatus status = PipelineTaskStatus::cpu_run;

    std::shared_ptr<MemoryTracker> mem_tracker;

    bool is_final_task = false;

    PipelineFinishCounterPtr pipeline_finish_counter;
    std::vector<PipelineFinishCounterPtr> depends;

private:
    void doFinish(PipelineManager & pipeline_manager);

    static PipelineTaskResult finish();
    static PipelineTaskResult fail(const String & err_msg);
    static PipelineTaskResult running();
};
} // namespace DB
