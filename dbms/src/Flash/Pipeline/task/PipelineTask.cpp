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

namespace DB
{
String PipelineTask::toString() const
{
    return fmt::format("{{task_id: {}, pipeline_id: {}, mpp_task_id: {}}}", task_id, pipeline_id, mpp_task_id.toString());
}

// must in io mode.
bool PipelineTask::tryToCpuMode()
{
    MemoryTrackerSetter setter(true, getMemTracker());
    assert(status == PipelineTaskStatus::io_wait || status == PipelineTaskStatus::io_finish);
    if (transforms->isIOReady())
    {
        status = PipelineTaskStatus::cpu_run;
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
        status = PipelineTaskStatus::io_wait;
        return true;
    }
    return false;
}

void PipelineTask::prepare()
{
    transforms->prepare();
}

// must in cpu mode.
PipelineTaskResult PipelineTask::execute()
{
    try
    {
        MemoryTrackerSetter setter(true, getMemTracker());
        assert(status == PipelineTaskStatus::cpu_run);
        int64_t time_spent = 0;
        while (true)
        {
            Stopwatch stopwatch {CLOCK_MONOTONIC_COARSE};
            if (!transforms->execute())
            {
                transforms->finish();
                if (!transforms->isIOReady())
                    status = PipelineTaskStatus::io_finish;
                return finish();
            }
            else if (!transforms->isIOReady())
            {
                status = PipelineTaskStatus::io_wait;
                return running();
            }
            else
            {
                time_spent += stopwatch.elapsed();
                static constexpr int64_t YIELD_MAX_TIME_SPENT = 100'000'000L;
                if (time_spent >= YIELD_MAX_TIME_SPENT)
                    return running();
            }
        }
    }
    catch (...)
    {
        return fail(getCurrentExceptionMessage(true));
    }
}

PipelineTaskResult PipelineTask::finish()
{
    return PipelineTaskResult{PipelineTaskResultType::finished, ""};
}
PipelineTaskResult PipelineTask::fail(const String & err_msg)
{
    return PipelineTaskResult{PipelineTaskResultType::error, err_msg};
}
PipelineTaskResult PipelineTask::running()
{
    return PipelineTaskResult{PipelineTaskResultType::running, ""};
}
} // namespace DB
