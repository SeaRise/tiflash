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
#include <Flash/Pipeline/task/PipelineTask.h>
#include <Flash/Pipeline/task/IOPoller.h>

#include <memory>
#include <thread>

namespace DB
{
class EventLoop
{
public:
    EventLoop(
        size_t loop_id_,
        EventLoopPool & pool_);
    ~EventLoop();
private:
    void handleCpuModeTask(PipelineTask && task);
    void cpuModeLoop();
private:
    size_t loop_id;
    EventLoopPool & pool;
    std::thread cpu_thread;
    LoggerPtr logger = Logger::get(fmt::format("event loop {}", loop_id));
};
using EventLoopPtr = std::unique_ptr<EventLoop>;

struct PipelineManager;
class EventLoopPool
{
public:
    EventLoopPool(
        size_t loop_num,
        PipelineManager & pipeline_manager_);

    void finish();

    void submit(std::vector<PipelineTask> & tasks);

    size_t concurrency() const { return cpu_loops.size(); }

    ~EventLoopPool();

private:
    void submitCPU(PipelineTask && task);
    void submitCPU(std::vector<PipelineTask> & tasks);

    bool popTask(PipelineTask & task);

    void handleFinishTask(const PipelineTask & task);
    void handleErrTask(const PipelineTask & task, const PipelineTaskResult & result);
private:
    PipelineManager & pipeline_manager;

    IOPoller io_poller;

    mutable std::mutex global_mutex;
    std::condition_variable cv;
    bool is_closed = false;
    std::deque<PipelineTask> cpu_event_queue;

    std::vector<EventLoopPtr> cpu_loops;

    LoggerPtr logger = Logger::get("event loop pool");

    friend class EventLoop;
    friend class IOPoller;
};
using EventLoopPoolPtr = std::unique_ptr<EventLoopPool>;
} // namespace DB
