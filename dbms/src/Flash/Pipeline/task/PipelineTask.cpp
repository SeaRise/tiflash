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

#include <Common/Stopwatch.h>
#include <Common/Exception.h>
#include <Common/MemoryTrackerSetter.h>
#include <Flash/Pipeline/task/PipelineTask.h>
#include <Flash/Pipeline/dag/PipelineSignal.h>
#include <magic_enum.hpp>

namespace DB
{
String PipelineTask::toString() const
{
    return fmt::format("{{mpp_task_id: {}, pipeline_id: {}, task_id: {}}}", mpp_task_id.toString(), pipeline_id, task_id);
}

// must in io mode.
bool PipelineTask::tryToCpuMode()
{
    assert(status == PipelineTaskStatus::io_wait || status == PipelineTaskStatus::io_finishing);
    if (unlikely(signal->isCancelled()))
    {
        cancel(signal->isKilled());
        return true;
    }
    try
    {
        MemoryTrackerSetter setter(true, getMemTracker());
        if (transforms->isIOReady())
        {
            if (status == PipelineTaskStatus::io_finishing)
                finish();
            else
            {
                assert(status == PipelineTaskStatus::io_wait);
                changeStatus(PipelineTaskStatus::cpu_run);
            }
            return true;
        }
    }
    catch (...)
    {
        error(getCurrentExceptionMessage(true));
        return true;
    }
    return false;
}
// must call in submit
bool PipelineTask::tryToIOMode()
{
    MemoryTrackerSetter setter(true, getMemTracker());
    assert(status == PipelineTaskStatus::cpu_run);
    if (!transforms->isIOReady())
    {
        changeStatus(PipelineTaskStatus::io_wait);
        return true;
    }
    return false;
}

void PipelineTask::prepare()
{
    transforms->prepare();
}

// must in cpu mode.
void PipelineTask::execute()
{
    try
    {
        MemoryTrackerSetter setter(true, getMemTracker());
        assert(status == PipelineTaskStatus::cpu_run);
        int64_t time_spent = 0;
        while (true)
        {
            if (unlikely(signal->isCancelled()))
            {
                cancel(signal->isKilled());
                return;
            }
            Stopwatch stopwatch {CLOCK_MONOTONIC_COARSE};
            if (!transforms->execute())
            {
                transforms->finish();
                if (transforms->isIOReady())
                    finish();
                else
                    changeStatus(PipelineTaskStatus::io_finishing);
                return;
            }
            else if (!transforms->isIOReady())
            {
                changeStatus(PipelineTaskStatus::io_wait);
                return;
            }
            else
            {
                time_spent += stopwatch.elapsed();
                static constexpr int64_t YIELD_MAX_TIME_SPENT = 100'000'000L;
                if (time_spent >= YIELD_MAX_TIME_SPENT)
                    return;
            }
        }
    }
    catch (...)
    {
        error(getCurrentExceptionMessage(true));
    }
}

void PipelineTask::finish()
{
    changeStatus(PipelineTaskStatus::finish);
    signal->finish();
}

void PipelineTask::error(const String & err_msg)
{
    changeStatus(PipelineTaskStatus::error);
    signal->error(err_msg);
}

void PipelineTask::cancel(bool is_kill)
{
    changeStatus(PipelineTaskStatus::cancelled);
    transforms->cancel(is_kill);
}

void PipelineTask::changeStatus(PipelineTaskStatus new_status)
{
    auto pre_status = status;
    status = new_status;
    LOG_DEBUG(logger, "change status: {} -> {}", magic_enum::enum_name(pre_status), magic_enum::enum_name(status));
}
} // namespace DB
