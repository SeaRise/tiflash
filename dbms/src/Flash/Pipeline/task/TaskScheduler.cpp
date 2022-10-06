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
#include <Storages/DeltaMerge/ReadThread/CPU.h>
#include <Storages/Transaction/TMTContext.h>

namespace DB
{
TaskScheduler::TaskScheduler(PipelineManager & pipeline_manager, const ServerInfo & server_info)
{
    auto numa_nodes = DM::getNumaNodes(log->getLog());
    LOG_FMT_INFO(log, "numa_nodes {} => {}", numa_nodes.size(), numa_nodes);
    RUNTIME_ASSERT(!numa_nodes.empty());
    for (size_t loop_id = 0; loop_id < numa_nodes.size(); ++loop_id)
    {
        const auto & node = numa_nodes[loop_id];
        size_t thread_count = node.empty() ? server_info.cpu_info.logical_cores : node.size();
        event_loop_pools.emplace_back(std::make_unique<EventLoopPool>(loop_id, thread_count, node, pipeline_manager));
        loop_count += thread_count;
    }
}

TaskScheduler::~TaskScheduler()
{
    for (const auto & event_loop_pool : event_loop_pools)
        event_loop_pool->finish();
    event_loop_pools.clear();
}

void TaskScheduler::submit(std::vector<PipelineTask> & tasks)
{
    if (unlikely(tasks.empty()))
        return;

    size_t loop_id = tasks.back().mpp_task_id.start_ts % event_loop_pools.size();
    assert(loop_id < event_loop_pools.size());
    auto next_loop = [&]() -> EventLoopPool & {
        assert(loop_id < event_loop_pools.size());
        EventLoopPool & loop = *event_loop_pools[loop_id++];
        if (loop_id == event_loop_pools.size())
            loop_id = 0;
        return loop;
    };

    while(!tasks.empty())
        next_loop().submit(tasks);
}

size_t TaskScheduler::concurrency() const
{
    return loop_count;
}
} // namespace DB
