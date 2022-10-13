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
#include <Flash/Executor/ResultHandler.h>
#include <Flash/Mpp/MPPTaskId.h>
#include <Flash/Pipeline/dag/Pipeline.h>
#include <Flash/Planner/PhysicalPlanNode.h>

namespace DB
{
class PipelineIDGenerator
{
    UInt32 current_id = 0;

public:
    UInt32 nextID()
    {
        return ++current_id;
    }
};

struct PipelineEvent;
class PipelineEventQueue;
using PipelineEventQueuePtr = std::shared_ptr<PipelineEventQueue>;

class PipelineSignal;
using PipelineSignalPtr = std::shared_ptr<PipelineSignal>;

class TaskScheduler;
class DAGScheduler
{
public:
    DAGScheduler(
        Context & context_,
        const MPPTaskId & mpp_task_id_,
        const String & req_id);

    // return <is_success, err_msg>
    std::pair<bool, String> run(
        const PhysicalPlanNodePtr & plan_node,
        ResultHandler result_handler);

    void cancel(bool is_kill);

    const MPPTaskId & getMPPTaskId() const { return mpp_task_id; }

private:
    PipelinePtr genPipeline(const PhysicalPlanNodePtr & plan_node, PipelineIDGenerator & id_generator);

    std::vector<PipelinePtr> createParentPipelines(const PhysicalPlanNodePtr & plan_node, PipelineIDGenerator & id_generator);

    PipelinePtr createNonJoinedPipelines(const PipelinePtr & pipeline, PipelineIDGenerator & id_generator);

    void submitInitPipeline();

    void handlePipelineFinish(const PipelineEvent & event);

    String handlePipelineFail(const PipelineEvent & event);

    void handlePipelineCancel(const PipelineEvent & event);

    void cancelPipelines(bool is_kill);

    PhysicalPlanNodePtr handleResultHandler(
        const PhysicalPlanNodePtr & plan_node,
        ResultHandler result_handler);

    void addPipeline(const PipelinePtr & pipeline);

private:
    std::vector<PipelineSignalPtr> pipeline_signals;
    std::vector<PipelinePtr> init_pipelines;

    Context & context;

    PipelineEventQueuePtr event_queue;

    MPPTaskId mpp_task_id;

    LoggerPtr log;

    TaskScheduler & task_scheduler;
};
} // namespace DB
