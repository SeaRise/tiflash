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
#include <DataStreams/HashJoinBuildBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Coprocessor/JoinInterpreterHelper.h>
#include <Flash/Planner/FinalizeHelper.h>
#include <Flash/Planner/plans/PhysicalJoinBuild.h>
#include <Interpreters/Context.h>
#include <Transforms/ExpressionTransform.h>
#include <Transforms/HashJoinBuildSink.h>
#include <Transforms/TransformsPipeline.h>
#include <common/logger_useful.h>
#include <fmt/format.h>

namespace DB
{
void PhysicalJoinBuild::transformImpl(DAGPipeline &, Context &, size_t)
{
    throw Exception("Unsupport");
}

void PhysicalJoinBuild::transform(TransformsPipeline & pipeline, Context & context, size_t concurrency)
{
    child->transform(pipeline, context, concurrency);

    size_t join_build_concurrency = context.getSettingsRef().join_concurrent_build ? std::min(concurrency, pipeline.concurrency()) : 1;
    auto get_concurrency_build_index = JoinInterpreterHelper::concurrencyBuildIndexGenerator(join_build_concurrency);
    pipeline.transform([&](auto & transforms) {
        transforms->append(std::make_shared<ExpressionTransform>(build_side_prepare_actions));
        transforms->setSink(std::make_shared<HashJoinBuildSink>(join_ptr, get_concurrency_build_index()));
    });
    join_ptr->init(pipeline.getHeader(), join_build_concurrency);
}

void PhysicalJoinBuild::finalize(const Names & parent_require)
{
    // schema.size() >= parent_require.size()
    FinalizeHelper::checkSchemaContainsParentRequire(schema, parent_require);
}

const Block & PhysicalJoinBuild::getSampleBlock() const
{
    return build_side_prepare_actions->getSampleBlock();
}
} // namespace DB
