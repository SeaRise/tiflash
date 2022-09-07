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
#include <DataStreams/LimitBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Planner/plans/PhysicalLimit.h>
#include <Flash/Planner/plans/PhysicalPartialLimit.h>
#include <Flash/Planner/plans/PhysicalFinalLimit.h>
#include <Flash/Planner/plans/PhysicalPipelineBreaker.h>
#include <Interpreters/Context.h>

namespace DB
{
PhysicalPlanNodePtr PhysicalLimit::build(
    const Context & context,
    const String & executor_id,
    const LoggerPtr & log,
    const tipb::Limit & limit,
    const PhysicalPlanNodePtr & child)
{
    assert(child);
    if (!context.getDAGContext()->is_pipeline_mode)
    {
        auto physical_limit = std::make_shared<PhysicalLimit>(
            executor_id,
            child->getSchema(),
            log->identifier(),
            child,
            limit.limit());
        return physical_limit;
    }
    else
    {
        LimitBreakerPtr limit_breaker = std::make_shared<LimitBreaker>(limit.limit());

        auto physical_partial_limit = std::make_shared<PhysicalPartialLimit>(
            executor_id,
            child->getSchema(),
            log->identifier(),
            child,
            limit_breaker);
        physical_partial_limit->notTiDBOperator();

        auto physical_final_limit = std::make_shared<PhysicalFinalLimit>(
            executor_id,
            child->getSchema(),
            log->identifier(),
            child->getSampleBlock(),
            limit_breaker);

        auto physical_breaker = std::make_shared<PhysicalPipelineBreaker>(
            executor_id,
            child->getSchema(),
            log->identifier(),
            physical_partial_limit,
            physical_final_limit);
        physical_breaker->notTiDBOperator();
        return physical_breaker;
    }
}

void PhysicalLimit::transformImpl(DAGPipeline & pipeline, Context & context, size_t max_streams)
{
    child->transform(pipeline, context, max_streams);

    pipeline.transform([&](auto & stream) { stream = std::make_shared<LimitBlockInputStream>(stream, limit, 0, log->identifier(), false); });
    if (pipeline.hasMoreThanOneStream())
    {
        executeUnion(pipeline, max_streams, log, false, "for partial limit");
        pipeline.transform([&](auto & stream) { stream = std::make_shared<LimitBlockInputStream>(stream, limit, 0, log->identifier(), false); });
    }
}

void PhysicalLimit::finalize(const Names & parent_require)
{
    child->finalize(parent_require);
}

const Block & PhysicalLimit::getSampleBlock() const
{
    return child->getSampleBlock();
}
} // namespace DB
