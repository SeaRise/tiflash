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

#include <common/types.h>
#include <Transforms/Transforms.h>

#include <atomic>
#include <vector>

namespace DB
{
class PipelineEventQueue;
using PipelineEventQueuePtr = std::shared_ptr<PipelineEventQueue>;

class PipelineSignal
{
public:
    PipelineSignal(
        UInt32 pipeline_id_, 
        const PipelineEventQueuePtr & event_queue_)
        : pipeline_id(pipeline_id_)
        , event_queue(event_queue_)
    {}

    void init(UInt16 active_task_num_);

    void finish();

    void error(const String & err_msg);

    void cancel(bool is_kill);

    bool isCancelled();
    bool isKilled();

private:
    UInt32 pipeline_id;
    PipelineEventQueuePtr event_queue;

    std::atomic_uint16_t active_task_num;
    std::atomic<bool> is_cancelled{false};
    std::atomic<bool> is_killed{false};
};

using PipelineSignalPtr = std::shared_ptr<PipelineSignal>;
} // namespace DB
