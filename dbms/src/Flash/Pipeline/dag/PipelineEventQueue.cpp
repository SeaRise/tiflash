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

#include <Flash/Pipeline/dag/PipelineEventQueue.h>

namespace DB
{
void PipelineEventQueue::submit(PipelineEvent && event)
{
    if (status.load(std::memory_order_acquire) != PipelineEventQueueStatus::running)
        return;
    {
        std::lock_guard lock(mutex);
        queue.emplace_back(std::move(event));
    }
    cond.notify_one();
}

void PipelineEventQueue::submitFirst(PipelineEvent && event)
{
    if (status.load(std::memory_order_acquire) != PipelineEventQueueStatus::running)
        return;
    {
        std::lock_guard lock(mutex);
        queue.emplace_front(std::move(event));
    }
    cond.notify_one();
}

PipelineEventQueueStatus PipelineEventQueue::pop(PipelineEvent & event)
{
    auto get_status = status.load(std::memory_order_acquire);
    if (get_status != PipelineEventQueueStatus::running)
        return get_status;
    {
        std::unique_lock lock(mutex);
        while (queue.empty())
        {
            get_status = status.load(std::memory_order_acquire);
            if (get_status != PipelineEventQueueStatus::running)
                return get_status;
            cond.wait(lock);
        }

        assert(!queue.empty());
        event = std::move(queue.front());
        queue.pop_front();
    }
    return PipelineEventQueueStatus::running;
}

void PipelineEventQueue::finish()
{
    auto expect_cur_status = PipelineEventQueueStatus::running;
    RUNTIME_ASSERT(status.compare_exchange_strong(expect_cur_status, PipelineEventQueueStatus::finished));
    cond.notify_one();
}

void PipelineEventQueue::cancel()
{
    auto expect_cur_status = PipelineEventQueueStatus::running;
    RUNTIME_ASSERT(status.compare_exchange_strong(expect_cur_status, PipelineEventQueueStatus::cancelled));
    cond.notify_one();
}

} // namespace DB
