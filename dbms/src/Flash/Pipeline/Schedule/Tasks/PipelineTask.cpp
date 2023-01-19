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

#include <Common/Exception.h>
#include <Common/MemoryTrackerSetter.h>
#include <Flash/Pipeline/Schedule/Events/Event.h>
#include <Flash/Pipeline/Schedule/Tasks/PipelineTask.h>

#include <magic_enum.hpp>

namespace DB
{
PipelineTask::PipelineTask(
    MemoryTrackerPtr mem_tracker_,
    const EventPtr & event_,
    PipelineExecPtr && pipeline_exec_)
    : Task(std::move(mem_tracker_))
    , event(event_)
    , pipeline_exec(std::move(pipeline_exec_))
{
    assert(event);
    assert(pipeline_exec);
}

PipelineTask::~PipelineTask()
{
    pipeline_exec.reset();
    assert(event);
    event->onTaskFinish();
    event.reset();
}

#define HANDLE_CANCELLED                  \
    if (unlikely(event->isCancelled()))   \
    {                                     \
        pipeline_exec.reset();            \
        return ExecTaskStatus::CANCELLED; \
    }

#define HANDLE_ERROR                                            \
    catch (...)                                                 \
    {                                                           \
        pipeline_exec.reset();                                  \
        assert(event);                                          \
        event->toError(getCurrentExceptionMessage(true, true)); \
        return ExecTaskStatus::ERROR;                           \
    }

#define HANDLE_FINISH_STATUS              \
    case OperatorStatus::FINISHED:        \
    {                                     \
        pipeline_exec.reset();            \
        return ExecTaskStatus::FINISHED;  \
    }                                     \
    case OperatorStatus::CANCELLED:       \
    {                                     \
        pipeline_exec.reset();            \
        return ExecTaskStatus::CANCELLED; \
    }

#define HANDLE_SPILLING_AND_WAITING_STATUS \
    case OperatorStatus::WAITING:          \
    {                                      \
        return ExecTaskStatus::WAITING;    \
    }                                      \
    case OperatorStatus::SPILLING:         \
    {                                      \
        return ExecTaskStatus::SPILLING;   \
    }

#define UNEXPECTED_OP_STATUS(op_status, function_name) \
    throw Exception(fmt::format("Unexpected op state {} at {}", magic_enum::enum_name(op_status), (function_name)));

ExecTaskStatus PipelineTask::executeImpl()
{
    HANDLE_CANCELLED
    try
    {
        assert(event);
        assert(pipeline_exec);
        auto op_status = pipeline_exec->execute(event->getExecStatus());
        switch (op_status)
        {
            HANDLE_FINISH_STATUS
            HANDLE_SPILLING_AND_WAITING_STATUS
        // After `pipeline_exec->execute`, `NEED_INPUT` means that pipeline_exec need data to do the calculations and expect the next call to `execute`
        // And other states are unexpected.
        case OperatorStatus::NEED_INPUT:
            return ExecTaskStatus::RUNNING;
        default:
            UNEXPECTED_OP_STATUS(op_status, "PipelineTask::execute");
        }
    }
    HANDLE_ERROR
}

ExecTaskStatus PipelineTask::awaitImpl()
{
    HANDLE_CANCELLED
    try
    {
        assert(event);
        assert(pipeline_exec);
        auto op_status = pipeline_exec->await(event->getExecStatus());
        switch (op_status)
        {
            HANDLE_FINISH_STATUS
            HANDLE_SPILLING_AND_WAITING_STATUS
        // After `pipeline_exec->await`, `HAS_OUTPUT` means that pipeline_exec has data to do the calculations and expect the next call to `execute`
        // And other states are unexpected.
        case OperatorStatus::HAS_OUTPUT:
            return ExecTaskStatus::RUNNING;
        default:
            UNEXPECTED_OP_STATUS(op_status, "PipelineTask::await");
        }
    }
    HANDLE_ERROR
}

ExecTaskStatus PipelineTask::spillImpl()
{
    HANDLE_CANCELLED
    try
    {
        assert(event);
        assert(pipeline_exec);
        auto op_status = pipeline_exec->spill(event->getExecStatus());
        switch (op_status)
        {
            HANDLE_FINISH_STATUS
            HANDLE_SPILLING_AND_WAITING_STATUS
        // After `pipeline_exec->spill`,
        // - `NEED_INPUT` means that pipeline_exec need data to spill.
        // - `HAS_OUTPUT` means that pipeline_exec has restored data, and ready for ouput.
        // And other states are unexpected.
        case OperatorStatus::NEED_INPUT:
        case OperatorStatus::HAS_OUTPUT:
            return ExecTaskStatus::RUNNING;
        default:
            UNEXPECTED_OP_STATUS(op_status, "PipelineTask::spill");
        }
    }
    HANDLE_ERROR
}

#undef HANDLE_CANCELLED
#undef HANDLE_ERROR
#undef HANDLE_FINISH_STATUS
#undef HANDLE_SPILLING_AND_WAITING_STATUS
#undef UNEXPECTED_OP_STATUS

} // namespace DB
