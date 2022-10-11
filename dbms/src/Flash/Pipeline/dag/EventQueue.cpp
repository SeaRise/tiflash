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

#include <Flash/Pipeline/dag/EventQueue.h>

namespace DB
{
void EventQueue::submit(PipelineEvent && event)
{
    {
        std::unique_lock lock(mutex);
        if (status != EventQueueStatus::running)
            return;
        queue.emplace_back(std::move(event));
    }
    cond.notify_one();
}

EventQueueStatus EventQueue::pop(PipelineEvent & event)
{
    {
        std::unique_lock lock(mutex);
        while (queue.empty() && status == EventQueueStatus::running)
            cond.wait(lock);
        if (status != EventQueueStatus::running)
            return status;
        assert(!queue.empty());
        event = std::move(queue.front());
        queue.pop_front();
    }
    return EventQueueStatus::running;
}

void EventQueue::finish()
{
    {
        std::lock_guard lock(mutex);
        assert(status == EventQueueStatus::running);
        status = EventQueueStatus::finished;
    }
    cond.notify_all();
}

void EventQueue::cancel()
{
    {
        std::lock_guard lock(mutex);
        assert(status == EventQueueStatus::running);
        status = EventQueueStatus::cancelled;
    }
    cond.notify_all();
}

} // namespace DB
