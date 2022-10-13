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
class PipelineSignal;
using PipelineSignalPtr = std::shared_ptr<PipelineSignal>;

class PipelineTrigger;
using PipelineTriggerPtr = std::shared_ptr<PipelineTrigger>;

class Pipeline
{
public:
    Pipeline(
        const PhysicalPlanNodePtr & plan_node_,
        const MPPTaskId & mpp_task_id_,
        UInt32 id_,
        const String & req_id);

    std::vector<PipelineTaskPtr> transform(Context & context, size_t concurrency);

    UInt32 getId() const { return id; }

    PhysicalPlanNodePtr getPlanNode() const 
    {
        assert(plan_node);
        return plan_node; 
    }

    String toString() const;

    void setSignal(const PipelineSignalPtr & signal_);
    PipelineSignalPtr getSignal() const;
    void addNextPipelineTrigger(const PipelineTriggerPtr & next_trigger);

private:
    PhysicalPlanNodePtr plan_node;

    MPPTaskId mpp_task_id;

    UInt32 id;

    PipelineSignalPtr signal;
    std::vector<PipelineTriggerPtr> next_triggers;

    LoggerPtr log;
};

using PipelinePtr = std::shared_ptr<Pipeline>;
} // namespace DB
