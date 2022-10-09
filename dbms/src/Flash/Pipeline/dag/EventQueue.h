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

#include <Flash/Pipeline/dag/Event.h>

#include <mutex>
#include <list>

namespace DB
{
enum class EventQueueStatus
{
    running,
    cancelled,
    finished,
};

class EventQueue
{
public:
    void submit(PipelineEvent && event);

    EventQueueStatus pop(PipelineEvent & event);

    void finish();

    void cancel();

private:
    mutable std::mutex mutex;
    std::condition_variable cond;
    std::list<PipelineEvent> queue;

    EventQueueStatus status = EventQueueStatus::running;
};
} // namespace DB
