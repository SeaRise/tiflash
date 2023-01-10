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

#include <Flash/Pipeline/Pipeline.h>
#include <Flash/Pipeline/Schedule/Event/PlainPipelineEvent.h>
#include <Flash/Pipeline/Schedule/Task/PipelineTask.h>

namespace DB
{
bool PlainPipelineEvent::scheduleImpl()
{
    assert(pipeline);
    auto pipline_exec_group = pipeline->toExec(context, concurrency);
    pipeline.reset();

    if (pipline_exec_group.empty())
        return true;

    std::vector<TaskPtr> tasks;
    tasks.reserve(pipline_exec_group.size());
    for (auto & pipline_exec : pipline_exec_group)
        tasks.push_back(std::make_unique<PipelineTask>(mem_tracker, shared_from_this(), std::move(pipline_exec)));
    scheduleTask(tasks);
    return false;
}
} // namespace DB
