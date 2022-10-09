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

#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Pipeline/task/TaskScheduler.h>
#include <Interpreters/Context.h>
#include <Storages/Transaction/TMTContext.h>

namespace DB
{
TaskScheduler::TaskScheduler(
    PipelineManager & pipeline_manager, 
    const ServerInfo & server_info)
    : event_loop_pool(std::make_unique<EventLoopPool>(
        // server_info.cpu_info.logical_cores, 
        server_info.cpu_info.physical_cores, 
        pipeline_manager))
{}

TaskScheduler::~TaskScheduler()
{
    event_loop_pool->finish();
    event_loop_pool = nullptr;
}

void TaskScheduler::submit(std::vector<PipelineTask> & tasks)
{
    if (unlikely(tasks.empty()))
        return;
    event_loop_pool->submit(tasks);
}
} // namespace DB
