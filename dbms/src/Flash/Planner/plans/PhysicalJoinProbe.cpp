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

#include <Common/FailPoint.h>
#include <Common/Logger.h>
#include <Common/TiFlashException.h>
#include <DataStreams/HashJoinProbeBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Planner/FinalizeHelper.h>
#include <Flash/Planner/PhysicalPlanHelper.h>
#include <Flash/Planner/plans/PhysicalJoinProbe.h>
#include <Flash/Planner/plans/PhysicalNonJoinProbe.h>
#include <Interpreters/Context.h>
#include <Transforms/ExpressionTransform.h>
#include <Transforms/HashJoinProbeTransform.h>
#include <Transforms/TransformsPipeline.h>
#include <common/logger_useful.h>
#include <fmt/format.h>

namespace DB
{
void PhysicalJoinProbe::transform(TransformsPipeline & pipeline, Context & context, size_t concurrency)
{
    child->transform(pipeline, context, concurrency);

    pipeline.transform([&](auto & transforms) {
        transforms->append(std::make_shared<ExpressionTransform>(probe_side_prepare_actions));
    });

    assert(!has_non_joined);
    auto join_probe_actions = PhysicalPlanHelper::newActions(probe_side_prepare_actions->getSampleBlock(), context);
    join_probe_actions->add(ExpressionAction::ordinaryJoin(join_ptr, columns_added_by_join));
    pipeline.transform([&](auto & transforms) {
        transforms->append(std::make_shared<HashJoinProbeTransform>(join_probe_actions));
    });

    ExpressionActionsPtr schema_actions = PhysicalPlanHelper::newActions(pipeline.getHeader(), context);
    PhysicalPlanHelper::addSchemaProjectAction(schema_actions, schema);
    pipeline.transform([&](auto & transforms) {
        transforms->append(std::make_shared<ExpressionTransform>(schema_actions));
    });
}

void PhysicalJoinProbe::transformImpl(DAGPipeline &, Context &, size_t)
{
    throw Exception("Unsupport");
}

void PhysicalJoinProbe::finalize(const Names & parent_require)
{
    // schema.size() >= parent_require.size()
    FinalizeHelper::checkSchemaContainsParentRequire(schema, parent_require);
}

const Block & PhysicalJoinProbe::getSampleBlock() const
{
    return sample_block;
}

std::optional<PhysicalPlanNodePtr> PhysicalJoinProbe::splitNonJoinedPlanNode()
{
    if (!has_non_joined)
        return {};

    has_non_joined = false;
    auto non_joined_plan = std::make_shared<PhysicalNonJoinProbe>(
        executor_id,
        schema,
        log->identifier(),
        join_ptr,
        probe_side_prepare_actions->getSampleBlock(),
        sample_block);
    non_joined_plan->notTiDBOperator();
    return {non_joined_plan};
}
} // namespace DB
