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
    if (numa_nodes.size() == 1 && numa_nodes.back().empty())
    {
        numa_indexes.emplace_back(0);
        int logical_cores = server_info.cpu_info.logical_cores;
        RUNTIME_ASSERT(logical_cores > 0);
        event_loops.reserve(logical_cores);
        for (int core = 0; core < logical_cores; ++core)
            event_loops.emplace_back(std::make_unique<EventLoop>(core, core, pipeline_manager));
        numa_indexes.emplace_back(event_loops.size());
    }
    else
    {
        RUNTIME_ASSERT(!numa_nodes.empty());
        numa_indexes.emplace_back(0);
        size_t loop_id = 0;
        for (const auto & node : numa_nodes)
        {
            RUNTIME_ASSERT(!node.empty());
            event_loops.reserve(event_loops.size() + node.size());
            for (auto core : node)
                event_loops.emplace_back(std::make_unique<EventLoop>(loop_id++, core, pipeline_manager));
            numa_indexes.emplace_back(event_loops.size());
        }
    }
    numa_num = numa_indexes.size() - 1;
    LOG_DEBUG(log, "init {} event loop success", event_loops.size());
}

TaskScheduler::~TaskScheduler()
{
    for (const auto & event_loop : event_loops)
        event_loop->finish();
    event_loops.clear();
}

void TaskScheduler::submit(std::vector<PipelineTask> & tasks)
{
    if (unlikely(tasks.empty()))
        return;
    auto tso = tasks.back().mpp_task_id.start_ts;
    auto pipeline_id = tasks.back().pipeline_id;

    size_t i = 0;
    while ((tasks.size() - i) >= event_loops.size())
    {
        for (const auto & event_loop : event_loops)
            event_loop->submit(std::move(tasks[i++]));
    }

    static size_t numa_id_counter = tso % numa_num;
    assert(numa_id_counter < numa_num);
    auto next_numa_id = [&]() {
        size_t numa_id = numa_id_counter++;
        assert(numa_id < numa_indexes.size());
        if (numa_id_counter == numa_num)
            numa_id_counter = 0;
        return numa_id;
    };
    while (i < tasks.size())
    {
        auto numa_id = next_numa_id();
        auto numa_index = numa_indexes[numa_id];
        auto numa_end = numa_indexes[numa_id + 1];
        size_t numa_core_num = numa_end - numa_index;
        assert(numa_core_num > 0);
        if ((tasks.size() - i) >= numa_core_num)
        {
            while (numa_index < numa_end)
            {
                assert(numa_index < event_loops.size());
                event_loops[numa_index++]->submit(std::move(tasks[i++]));
            }
        }
        else
        {
            size_t loop_index = numa_index + ((pipeline_id + tso) % numa_core_num);
            assert(loop_index < numa_end);
            auto next_loop = [&]() -> EventLoop & {
                assert(loop_index < numa_end);
                EventLoop & loop = *event_loops[loop_index++];
                if (loop_index == numa_end)
                    loop_index = numa_index;
                return loop;
            };
            while (i < tasks.size())
                next_loop().submit(std::move(tasks[i++]));
        }
    }
}

size_t TaskScheduler::concurrency() const
{
    return event_loops.size();
}
} // namespace DB
