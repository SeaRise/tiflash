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
#include <Flash/Pipeline/PipelineManager.h>
#include <Flash/Pipeline/dag/DAGScheduler.h>
#include <Flash/Pipeline/dag/Event.h>

namespace DB
{
String PipelineTask::toString() const
{
    return fmt::format("{{pipeline_id: {}, mpp_task_id: {}}}", pipeline_id, mpp_task_id.toString());
}

// must in io mode.
bool PipelineTask::tryToCpuMode(PipelineManager & pipeline_manager)
{
    MemoryTrackerSetter setter(true, getMemTracker());
    assert(status == PipelineTaskStatus::io_wait || status == PipelineTaskStatus::io_finishing);
    if (isDependsReady() && transforms->isIOReady())
    {
        if (status == PipelineTaskStatus::io_finishing)
            doFinish(pipeline_manager);
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
    if (!isDependsReady() || !transforms->isIOReady())
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

bool PipelineTask::isDependsReady()
{
    for (auto & depend : depends)
    {
        if (!depend->isFinished())
            return false;
    }
    return true;
}

// must in cpu mode.
PipelineTaskResult PipelineTask::execute(PipelineManager & pipeline_manager)
{
    try
    {
        MemoryTrackerSetter setter(true, getMemTracker());
        assert(status == PipelineTaskStatus::cpu_run);
        int64_t time_spent = 0;
        while (true)
        {
            // isDependsReady() must be true here.
            Stopwatch stopwatch {CLOCK_MONOTONIC_COARSE};
            if (!transforms->execute())
            {
                transforms->finish();
                if (!transforms->isIOReady())
                {
                    status = PipelineTaskStatus::io_finishing;
                    return running();
                }
                doFinish(pipeline_manager);
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

void PipelineTask::doFinish(PipelineManager & pipeline_manager)
{
    assert(pipeline_finish_counter);
#ifndef NDEBUG
    LOG_TRACE(pipeline_manager.log, "task {} finish", toString());
#endif
    if (0 == pipeline_finish_counter->finish())
    {
        assert(pipeline_finish_counter->isFinished());
#ifndef NDEBUG
        LOG_TRACE(pipeline_manager.log, "pipeline {} finish", pipeline_id);
#endif
        if (is_final_task)
        {
#ifndef NDEBUG
            LOG_TRACE(pipeline_manager.log, "final pipeline {} finish", pipeline_id);
#endif
            if (auto dag_scheduler = pipeline_manager.getDAGScheduler(mpp_task_id); likely(dag_scheduler))
                dag_scheduler->submit(PipelineEvent::finish());
        }
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
