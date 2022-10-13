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

#include <Common/Logger.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Planner/FinalizeHelper.h>
#include <Flash/Planner/PhysicalPlanHelper.h>
#include <Flash/Planner/plans/PhysicalFinalTopN.h>
#include <Flash/Planner/plans/PhysicalPartialTopN.h>
#include <Flash/Planner/plans/PhysicalPipelineBreaker.h>
#include <Flash/Planner/plans/PhysicalTopN.h>
#include <Interpreters/Context.h>
#include <Transforms/TopNBreaker.h>

namespace DB
{
PhysicalPlanNodePtr PhysicalTopN::build(
    const Context & context,
    const String & executor_id,
    const LoggerPtr & log,
    const tipb::TopN & top_n,
    const PhysicalPlanNodePtr & child)
{
    assert(child);

    if (unlikely(top_n.order_by_size() == 0))
    {
        //should not reach here
        throw TiFlashException("TopN executor without order by exprs", Errors::Planner::BadRequest);
    }

    DAGExpressionAnalyzer analyzer{child->getSchema(), context};
    ExpressionActionsPtr before_sort_actions = PhysicalPlanHelper::newActions(child->getSampleBlock(), context);

    auto order_columns = analyzer.buildOrderColumns(before_sort_actions, top_n.order_by());
    SortDescription order_descr = getSortDescription(order_columns, top_n.order_by());

    if (!context.getDAGContext()->is_pipeline_mode)
    {
        auto physical_top_n = std::make_shared<PhysicalTopN>(
            executor_id,
            child->getSchema(),
            log->identifier(),
            child,
            order_descr,
            before_sort_actions,
            top_n.limit());
        return physical_top_n;
    }
    else
    {
        const auto & settings = context.getSettingsRef();
        TopNBreakerPtr topn_breaker = std::make_shared<TopNBreaker>(
            order_descr,
            log->identifier(),
            settings.max_block_size,
            top_n.limit(),
            before_sort_actions->getSampleBlock());

        auto physical_partial_topn = std::make_shared<PhysicalPartialTopN>(
            executor_id,
            child->getSchema(),
            log->identifier(),
            child,
            order_descr,
            before_sort_actions,
            top_n.limit(),
            topn_breaker);
        physical_partial_topn->notTiDBOperator();

        auto physical_final_topn = std::make_shared<PhysicalFinalTopN>(
            executor_id,
            child->getSchema(),
            log->identifier(),
            before_sort_actions->getSampleBlock(),
            topn_breaker);

        auto physical_breaker = std::make_shared<PhysicalPipelineBreaker>(
            executor_id,
            child->getSchema(),
            log->identifier(),
            physical_partial_topn,
            physical_final_topn);
        physical_breaker->notTiDBOperator();
        return physical_breaker;
    }
}

void PhysicalTopN::transformImpl(DAGPipeline & pipeline, Context & context, size_t max_streams)
{
    child->transform(pipeline, context, max_streams);

    executeExpression(pipeline, before_sort_actions, log, "before TopN");

    orderStreams(pipeline, max_streams, order_descr, limit, false, context, log);
}

void PhysicalTopN::finalize(const Names & parent_require)
{
    Names required_output = parent_require;
    required_output.reserve(required_output.size() + order_descr.size());
    for (const auto & desc : order_descr)
        required_output.emplace_back(desc.column_name);
    before_sort_actions->finalize(required_output);

    child->finalize(before_sort_actions->getRequiredColumns());
    FinalizeHelper::prependProjectInputIfNeed(before_sort_actions, child->getSampleBlock().columns());

    FinalizeHelper::checkSampleBlockContainsParentRequire(getSampleBlock(), parent_require);
}

const Block & PhysicalTopN::getSampleBlock() const
{
    return before_sort_actions->getSampleBlock();
}
} // namespace DB
