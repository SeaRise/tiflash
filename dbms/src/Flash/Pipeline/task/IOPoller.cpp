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

#include <Common/setThreadName.h>
#include <Flash/Pipeline/task/IOPoller.h>
#include <Flash/Pipeline/task/EventLoopPool.h>
#include <errno.h>

namespace DB
{
IOPoller::IOPoller(EventLoopPool & pool_): pool(pool_)
{
    io_thread = std::thread(&IOPoller::ioModeLoop, this);
}

void IOPoller::finish()
{
    if (this->is_shutdown.load() == false)
    {
        this->is_shutdown.store(true, std::memory_order_release);
        cond.notify_one();
    }
}

void IOPoller::submit(PipelineTaskPtr && task)
{
    assert(task);
    LOG_DEBUG(logger, "submit {} to io event loop", task->toString());
    {
        std::lock_guard lock(mutex);
        blocked_tasks.emplace_back(std::move(task));
    }
    cond.notify_one();
}

void IOPoller::submit(std::vector<PipelineTaskPtr> & tasks)
{
    if (tasks.empty())
        return;
    {
        std::lock_guard<std::mutex> lock(mutex);
        while (!tasks.empty())
        {
            auto & task = tasks.back();
            assert(task);
            LOG_DEBUG(logger, "submit {} to io event loop", task->toString());
            blocked_tasks.emplace_back(std::move(task));
            tasks.pop_back();
        }
    }
    cond.notify_one();
}

IOPoller::~IOPoller()
{
    io_thread.join();
    LOG_INFO(logger, "stop io event loop");
}

// return true to remove task in blocked_tasks.
bool IOPoller::handleIOModeTask(std::vector<PipelineTaskPtr> & ready_tasks, PipelineTaskPtr && task)
{
    assert(task);
    if (task->tryToCpuMode())
    {
        if (task->status == PipelineTaskStatus::cpu_run)
            ready_tasks.emplace_back(std::move(task));
        return true;
    }
    return false;
}

void IOPoller::ioModeLoop()
{
    setThreadName("IOPoller");
    LOG_INFO(logger, "start io event loop");
    std::list<PipelineTaskPtr> local_blocked_tasks;
    int spin_count = 0;
    std::vector<PipelineTaskPtr> ready_tasks;
    while (!is_shutdown.load(std::memory_order_acquire))
    {
        if (local_blocked_tasks.empty())
        {
            std::unique_lock<std::mutex> lock(mutex);
            while (!is_shutdown.load(std::memory_order_acquire) && blocked_tasks.empty())
                cond.wait(lock);
            if (is_shutdown.load(std::memory_order_acquire))
                break;
            assert(!blocked_tasks.empty());
            local_blocked_tasks.splice(local_blocked_tasks.end(), blocked_tasks);
        }
        else
        {
            std::lock_guard lock(mutex);
            if (!blocked_tasks.empty())
                local_blocked_tasks.splice(local_blocked_tasks.end(), blocked_tasks);
        }

        auto task_it = local_blocked_tasks.begin();
        while (task_it != local_blocked_tasks.end())
        {
            auto & task = *task_it;
            if (handleIOModeTask(ready_tasks, std::move(task)))
                task_it = local_blocked_tasks.erase(task_it);
            else
                ++task_it;
        }

        if (ready_tasks.empty())
        {
            spin_count += 1;
        }
        else
        {
            spin_count = 0;
            pool.submitCPU(ready_tasks);
            ready_tasks.clear();
        }

        if (spin_count != 0 && spin_count % 64 == 0)
        {
#ifdef __x86_64__
            _mm_pause();
#else
            // TODO: Maybe there's a better intrinsic like _mm_pause on non-x86_64 architecture.
            sched_yield();
#endif
        }
        if (spin_count == 640)
        {
            spin_count = 0;
            sched_yield();
        }
    }
    LOG_INFO(logger, "io event loop finished");
}
} // namespace DB
