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

#pragma once

#include <Common/Logger.h>
#include <Common/MPMCQueue.h>
#include <Flash/Pipeline/task/PipelineTask.h>

#include <memory>
#include <thread>

namespace DB
{
struct PipelineManager;

class EventLoopPool
{
public:
    EventLoopPool(
        size_t loop_id_,
        size_t cpu_thread_count,
        const std::vector<int> & cpus_,
        PipelineManager & pipeline_manager_);

    void finish();

    void submit(std::vector<PipelineTask> & tasks);

    ~EventLoopPool();

private:
    void submitCPU(PipelineTask && task);
    void submitIO(PipelineTask && task);

    void handleCpuModeTask(PipelineTask && task);
    void handleIOModeTask(PipelineTask && task);

    void cpuModeLoop();
    void ioModeLoop();

    void handleFinishTask(const PipelineTask & task);
    void handleErrTask(const PipelineTask & task, const PipelineTaskResult & result);

    void setCPUAffinity();

private:
    size_t loop_id;
    std::vector<int> cpus;
    PipelineManager & pipeline_manager;
    MPMCQueue<PipelineTask> cpu_event_queue{499999};
    MPMCQueue<PipelineTask> io_event_queue{499999};
    std::vector<std::thread> cpu_threads;
    std::thread io_thread;

    LoggerPtr logger = Logger::get(fmt::format("event loop pool {}", loop_id));
};

using EventLoopPoolPtr = std::unique_ptr<EventLoopPool>;
} // namespace DB
