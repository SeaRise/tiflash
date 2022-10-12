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
#include <Flash/Planner/PhysicalPlanNode.h>
#include <Transforms/Transforms.h>

#include <unordered_set>

namespace DB
{
class PipelineEventQueue;
using PipelineEventQueuePtr = std::shared_ptr<PipelineEventQueue>;

class Pipeline
{
public:
    Pipeline(
        const PhysicalPlanNodePtr & plan_node_,
        const MPPTaskId & mpp_task_id_,
        UInt32 id_,
        const std::unordered_set<UInt32> & parent_ids_,
        const PipelineEventQueuePtr & event_queue_,
        const String & req_id);

    std::vector<PipelineTaskPtr> transform(Context & context, size_t concurrency);

    UInt32 getId() const { return id; }
    const std::unordered_set<UInt32> & getParentIds() const { return parent_ids; }

    void cancel(bool is_kill);
    void finish();

    void finish(size_t task_id);

    PhysicalPlanNodePtr getPlanNode() const { return plan_node; }

    String toString() const;

public:
    size_t active_task_num = 0;

private:
    PhysicalPlanNodePtr plan_node;

    MPPTaskId mpp_task_id;

    UInt32 id;

    std::unordered_set<UInt32> parent_ids;

    PipelineEventQueuePtr event_queue;

    std::vector<TransformsPtr> task_transforms_vec;

    LoggerPtr log;
};

using PipelinePtr = std::shared_ptr<Pipeline>;
} // namespace DB
