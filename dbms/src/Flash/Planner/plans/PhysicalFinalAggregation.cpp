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

#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Planner/FinalizeHelper.h>
#include <Flash/Planner/plans/PhysicalFinalAggregation.h>
#include <Interpreters/Context.h>
#include <Transforms/AggregateSource.h>
#include <Transforms/ExpressionTransform.h>
#include <Transforms/TransformsPipeline.h>

namespace DB
{
void PhysicalFinalAggregation::finalize(const Names & parent_require)
{
    FinalizeHelper::checkSchemaContainsParentRequire(schema, parent_require);
    expr_after_agg->finalize(DB::toNames(schema));
}

const Block & PhysicalFinalAggregation::getSampleBlock() const
{
    return expr_after_agg->getSampleBlock();
}

void PhysicalFinalAggregation::transformImpl(DAGPipeline &, Context &, size_t)
{
    throw Exception("Unsupport");
}

void PhysicalFinalAggregation::transform(TransformsPipeline & pipeline, Context & /*context*/, size_t concurrency)
{
    aggregate_store->initForMerge();
    size_t max_threads = aggregate_store->isTwoLevel()
        ? std::min(concurrency, aggregate_store->maxThreads())
        : 1;
    pipeline.init(max_threads);
    pipeline.transform([&](auto & transforms) {
        transforms->setSource(std::make_shared<AggregateSource>(aggregate_store));
        transforms->append(std::make_shared<ExpressionTransform>(expr_after_agg));
    });
}

} // namespace DB
