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
#include <Flash/Pipeline/dag/Event.h>
#include <Flash/Pipeline/dag/EventQueue.h>
#include <Flash/Pipeline/dag/Pipeline.h>
#include <Flash/Pipeline/dag/PipelineStatusMachine.h>
#include <Flash/Planner/PhysicalPlanNode.h>

#include <memory>

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

    void submit(PipelineEventPtr && event);

private:
    PipelinePtr genPipeline(const PhysicalPlanNodePtr & plan_node);

    std::unordered_set<UInt32> createParentPipelines(const PhysicalPlanNodePtr & plan_node);

    PipelinePtr createNonJoinedPipelines(const PipelinePtr & pipeline);

    void submitPipeline(const PipelinePtr & pipeline);

    void submitNext(const PipelinePtr & pipeline);

    void handlePipelineFinish(const PipelineEventPtr & event);

    String handlePipelineFail(const PipelineEventPtr & event);

    void handlePipelineCancel(const PipelineEventPtr & event);

    void cancelRunningPipelines(bool is_kill);

    PhysicalPlanNodePtr handleResultHandler(
        const PhysicalPlanNodePtr & plan_node,
        ResultHandler result_handler);

    String pipelineDAGToString(UInt32 pipeline_id) const;

private:
    UInt32 final_pipeline_id;

    PipelineStatusMachine status_machine;

    PipelineIDGenerator id_generator;

    EventQueue event_queue;

    Context & context;

    MPPTaskId mpp_task_id;

    LoggerPtr log;

    TaskScheduler & task_scheduler;
};

using DAGSchedulerPtr = std::shared_ptr<DAGScheduler>;
} // namespace DB
