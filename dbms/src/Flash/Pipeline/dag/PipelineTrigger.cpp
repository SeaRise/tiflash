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

#include <Flash/Pipeline/dag/PipelineTrigger.h>
#include <Flash/Pipeline/dag/PipelineSignal.h>
#include <Flash/Pipeline/dag/Pipeline.h>
#include <Flash/Pipeline/task/TaskScheduler.h>
#include <Interpreters/Context.h>

namespace DB
{
void PipelineTrigger::trigger()
{
    for (const auto & signal : dependency)
    {
        if (!signal->isFinished())
            return;
    }

    bool expect_false = false;
    if (is_triggered.compare_exchange_strong(expect_false, true))
    {
        task_scheduler.submit(pipeline, context);
    }
}

void PipelineTrigger::addDependency(const PipelineSignalPtr & signal)
{
    dependency.emplace_back(signal);
}
} // namespace DB
