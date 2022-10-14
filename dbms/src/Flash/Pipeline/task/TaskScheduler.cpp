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

#include <Flash/Pipeline/dag/Pipeline.h>
#include <Flash/Pipeline/task/TaskScheduler.h>
#include <Interpreters/Context.h>
#include <common/logger_useful.h>

namespace DB
{
TaskScheduler::TaskScheduler(
    const ServerInfo & server_info)
    // Assume that the lock on the cpu event loop blocks for no more than 50%, 
    // so a thread count of 2 * cpu_info.physical_cores can theoretically make full use of the cpu.
    : event_loop_pool(2 * server_info.cpu_info.physical_cores)
{}

TaskScheduler::~TaskScheduler()
{
    event_loop_pool.finish();
}

void TaskScheduler::submit(const PipelinePtr & pipeline, Context & context)
{
    size_t concurrency = std::min(event_loop_pool.concurrency(), context.getMaxStreams());
    if (0 == concurrency)
        return;

    auto tasks = pipeline->transform(context, concurrency);
    assert(!tasks.empty());
    LOG_DEBUG(log, "submit pipeline {} with task num {}", pipeline->toString(), tasks.size());
    event_loop_pool.submit(tasks);
}
} // namespace DB
