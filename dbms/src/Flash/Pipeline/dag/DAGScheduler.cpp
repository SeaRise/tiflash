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

#include <Flash/Mpp/MPPTaskManager.h>
#include <Flash/Pipeline/dag/DAGScheduler.h>
#include <Flash/Pipeline/task/TaskScheduler.h>
#include <Flash/Planner/PhysicalPlanVisitor.h>
#include <Flash/Planner/plans/PhysicalJoinProbe.h>
#include <Flash/Planner/plans/PhysicalPipelineBreaker.h>
#include <Flash/Planner/plans/PhysicalResultHandler.h>
#include <Interpreters/Context.h>
#include <Storages/Transaction/TMTContext.h>
#include <Flash/Pipeline/dag/PipelineEventQueue.h>
#include <Flash/Pipeline/dag/PipelineEvent.h>
#include <Flash/Pipeline/dag/PipelineTrigger.h>
#include <Flash/Pipeline/dag/PipelineSignal.h>

#include <magic_enum.hpp>

namespace DB
{
DAGScheduler::DAGScheduler(
    Context & context_,
    const MPPTaskId & mpp_task_id_,
    const String & req_id)
    : context(context_)
    , event_queue(std::make_shared<PipelineEventQueue>())
    , mpp_task_id(mpp_task_id_)
    , log(Logger::get("DAGScheduler", req_id))
    , task_scheduler(context.getTMTContext().getMPPTaskManager()->getPipelineTaskScheduler())
{}

std::pair<bool, String> DAGScheduler::run(
    const PhysicalPlanNodePtr & plan_node,
    ResultHandler result_handler)
{
    LOG_DEBUG(log, "start run mpp task {} with pipeline model", mpp_task_id.toString());
    assert(plan_node);
    PipelineIDGenerator id_generator;
    auto final_pipeline = genPipeline(handleResultHandler(plan_node, result_handler), id_generator);
    final_pipeline->getSignal()->setIsFinal();

    submitInitPipeline();

    PipelineEvent event;
    String err_msg;
    PipelineEventQueueStatus event_queue_status;
    while (true)
    {
        event_queue_status = event_queue->pop(event);
        if (unlikely(event_queue_status != PipelineEventQueueStatus::running))
            break;
    
        switch (event.type)
        {
        case PipelineEventType::finish:
            handlePipelineFinish(event);
            break;
        case PipelineEventType::fail:
            err_msg = handlePipelineFail(event);
            break;
        case PipelineEventType::cancel:
            handlePipelineCancel(event);
            break;
        }
    }
    LOG_DEBUG(log, "finish pipeline model mpp task {} with status {}", mpp_task_id.toString(), magic_enum::enum_name(event_queue_status));
    return {event_queue_status == PipelineEventQueueStatus::finished, err_msg};
}

PhysicalPlanNodePtr DAGScheduler::handleResultHandler(
    const PhysicalPlanNodePtr & plan_node,
    ResultHandler result_handler)
{
    return PhysicalResultHandler::build(result_handler, log->identifier(), plan_node);
}

void DAGScheduler::cancel(bool is_kill)
{
    event_queue->submitFirst(PipelineEvent::cancel(is_kill));
}

void DAGScheduler::handlePipelineCancel(const PipelineEvent & event)
{
    assert(event.type == PipelineEventType::cancel);
    event_queue->cancel();
    cancelPipelines(event.is_kill);
    pipeline_signals.clear();
}

void DAGScheduler::cancelPipelines(bool is_kill)
{
    for (const auto & signal : pipeline_signals)
        signal->cancel(is_kill);
}

String DAGScheduler::handlePipelineFail(const PipelineEvent & event)
{
    assert(event.type == PipelineEventType::fail);
    event_queue->cancel();
    cancelPipelines(false);
    pipeline_signals.clear();
    return event.err_msg;
}

void DAGScheduler::handlePipelineFinish(const PipelineEvent & event)
{
    assert(event.type == PipelineEventType::finish);
    event_queue->finish();
    pipeline_signals.clear();
}

PipelinePtr DAGScheduler::genPipeline(const PhysicalPlanNodePtr & plan_node, PipelineIDGenerator & id_generator)
{
    auto id = id_generator.nextID();
    auto pipeline = std::make_shared<Pipeline>(plan_node, mpp_task_id, id, log->identifier());
    addPipeline(pipeline);
    auto trigger = std::make_shared<PipelineTrigger>(pipeline, task_scheduler, context);
    const auto & parent_pipelines = createParentPipelines(plan_node, id_generator);
    for (const auto & parent_pipeline : parent_pipelines)
        parent_pipeline->addNextPipelineTrigger(trigger);
    if (parent_pipelines.empty())
        init_pipelines.emplace_back(pipeline);
    return createNonJoinedPipelines(pipeline, id_generator);
}

void DAGScheduler::addPipeline(const PipelinePtr & pipeline)
{
    auto signal = std::make_shared<PipelineSignal>(event_queue);
    pipeline->setSignal(signal);
    pipeline_signals.emplace_back(signal);
}

PipelinePtr DAGScheduler::createNonJoinedPipelines(const PipelinePtr & pipeline, PipelineIDGenerator & id_generator)
{
    std::vector<std::pair<size_t, PhysicalPlanNodePtr>> non_joined;
    size_t index = 0;
    PhysicalPlanVisitor::visit(pipeline->getPlanNode(), [&](const PhysicalPlanNodePtr & plan) {
        assert(plan);
        if (plan->tp() == PlanType::JoinProbe)
        {
            auto physical_join_probe = std::static_pointer_cast<PhysicalJoinProbe>(plan);
            if (auto ret = physical_join_probe->splitNonJoinedPlanNode(); ret.has_value())
                non_joined.emplace_back(index, *ret);
        }
        ++index;
        return true;
    });

    auto gen_plan_tree = [&](PhysicalPlanNodePtr root, size_t index, const PhysicalPlanNodePtr & leaf) -> PhysicalPlanNodePtr {
        assert(root && leaf);
        if (index == 0)
            return leaf;
        root = root->cloneOne();
        root->notTiDBOperator();
        PhysicalPlanNodePtr parent = root;
        assert(parent->childrenSize() == 1);
        for (size_t i = 0; i < index - 1; ++i)
        {
            auto pre = parent;
            parent = pre->children(0);
            assert(parent->childrenSize() == 1);
            parent = parent->cloneOne();
            parent->notTiDBOperator();
            pre->setChild(0, parent);
        }
        parent->setChild(0, leaf);
        return root;
    };

    std::vector<PipelinePtr> parent_pipelines;
    parent_pipelines.emplace_back(pipeline);
    PipelinePtr return_pipeline = pipeline;
    for (int i = non_joined.size() - 1; i >= 0; --i)
    {
        auto [index, non_joined_plan] = non_joined[i];
        auto id = id_generator.nextID();
        auto non_joined_root = gen_plan_tree(pipeline->getPlanNode(), index, non_joined_plan);
        auto non_joined_pipeline = std::make_shared<Pipeline>(non_joined_root, mpp_task_id, id, log->identifier());
        addPipeline(non_joined_pipeline);

        auto trigger = std::make_shared<PipelineTrigger>(non_joined_pipeline, task_scheduler, context);
        for (const auto & parent_pipeline : parent_pipelines)
            parent_pipeline->addNextPipelineTrigger(trigger);
        parent_pipelines.emplace_back(non_joined_pipeline);

        return_pipeline = non_joined_pipeline;
    }
    return return_pipeline;
}

std::vector<PipelinePtr> DAGScheduler::createParentPipelines(const PhysicalPlanNodePtr & plan_node, PipelineIDGenerator & id_generator)
{
    std::vector<PipelinePtr> parent_pipelines;
    for (size_t i = 0; i < plan_node->childrenSize(); ++i)
    {
        const auto & child = plan_node->children(i);
        switch (child->tp())
        {
        case PlanType::PipelineBreaker:
        {
            // PhysicalPipelineBreaker cannot be the root node.
            auto physical_breaker = std::static_pointer_cast<PhysicalPipelineBreaker>(child);
            parent_pipelines.emplace_back(genPipeline(physical_breaker->before(), id_generator));

            // remove PhysicalAggregation
            plan_node->setChild(0, physical_breaker->after());
            const auto & pipelines = createParentPipelines(physical_breaker->after(), id_generator);
            parent_pipelines.insert(parent_pipelines.end(), pipelines.cbegin(), pipelines.cend());
            break;
        }
        default:
        {
            const auto & pipelines = createParentPipelines(child, id_generator);
            parent_pipelines.insert(parent_pipelines.end(), pipelines.cbegin(), pipelines.cend());
            break;
        }
        }
    }
    return parent_pipelines;
}

void DAGScheduler::submitInitPipeline()
{
    for (const auto & init_pipeline : init_pipelines)
        task_scheduler.submit(init_pipeline, context);
    init_pipelines.clear();
}
} // namespace DB
