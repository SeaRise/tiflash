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
#include <Transforms/JoinPreBuildSink.h>
#include <Transforms/JoinPreBuildSource.h>
#include <Transforms/JoinPreBuilder.h>
#include <Flash/Planner/plans/PhysicalLeaf.h>
#include <Flash/Planner/plans/PhysicalPipelineBreaker.h>

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

class PhysicalJoinPreBuild : public PhysicalUnary
{
public:
    PhysicalJoinPreBuild(
        const String & executor_id_,
        const NamesAndTypes & schema_,
        const String & req_id,
        const PhysicalPlanNodePtr & child_,
        const ExpressionActionsPtr & build_side_prepare_actions_,
        const JoinPreBuilderPtr & join_pre_builder_)
        : PhysicalUnary(executor_id_, PlanType::JoinPreBuild, schema_, req_id, child_)
        , build_side_prepare_actions(build_side_prepare_actions_)
        , join_pre_builder(join_pre_builder_)
    {}

    void finalize(const Names & parent_require) override
    {
        FinalizeHelper::checkSchemaContainsParentRequire(schema, parent_require);
    }

    const Block & getSampleBlock() const override
    {
        return join_pre_builder->getHeader();
    }

    PhysicalPlanNodePtr cloneOne() const override
    {
        auto clone_one = std::make_shared<PhysicalJoinPreBuild>(*this);
        return clone_one;
    }

    void transform(TransformsPipeline & pipeline, Context & context, size_t concurrency) override
    {
        child->transform(pipeline, context, concurrency);

        join_pre_builder->initForWrite(pipeline.concurrency());
        auto get_concurrency_build_index = JoinInterpreterHelper::concurrencyBuildIndexGenerator(join_pre_builder->max_threads);
        pipeline.transform([&](auto & transforms) {
            transforms->append(std::make_shared<ExpressionTransform>(build_side_prepare_actions));
            transforms->setSink(std::make_shared<JoinPreBuildSink>(join_pre_builder, get_concurrency_build_index()));
        });
    }

private:
    void transformImpl(DAGPipeline &, Context &, size_t) override
    {
        throw Exception("Unsupport");
    }

private:
    ExpressionActionsPtr build_side_prepare_actions;
    JoinPreBuilderPtr join_pre_builder;
};

class PhysicalJoinFinalBuild : public PhysicalLeaf
{
public:
    PhysicalJoinFinalBuild(
        const String & executor_id_,
        const NamesAndTypes & schema_,
        const String & req_id,
        const JoinPtr & join_,
        const JoinPreBuilderPtr & join_pre_builder_)
        : PhysicalLeaf(executor_id_, PlanType::JoinFinalBuild, schema_, req_id)
        , join(join_)
        , join_pre_builder(join_pre_builder_)
    {}

    void finalize(const Names & parent_require) override
    {
        FinalizeHelper::checkSchemaContainsParentRequire(schema, parent_require);
    }

    const Block & getSampleBlock() const override
    {
        return join_pre_builder->getHeader();
    }

    PhysicalPlanNodePtr cloneOne() const override
    {
        auto clone_one = std::make_shared<PhysicalJoinFinalBuild>(*this);
        return clone_one;
    }

    void transform(TransformsPipeline & pipeline, Context & context, size_t concurrency) override
    {
        join_pre_builder->doReserve(join);

        size_t join_build_concurrency = context.getSettingsRef().join_concurrent_build ? std::min(concurrency, join_pre_builder->max_threads) : 1;
        auto get_concurrency_build_index = JoinInterpreterHelper::concurrencyBuildIndexGenerator(join_build_concurrency);

        pipeline.init(join_pre_builder->max_threads);
        size_t source_index = 0;
        pipeline.transform([&](auto & transforms) {
            transforms->setSource(std::make_shared<JoinPreBuildSource>(join_pre_builder, source_index++));
            transforms->setSink(std::make_shared<HashJoinBuildSink>(join, get_concurrency_build_index()));
        });
        join->init(pipeline.getHeader(), join_build_concurrency);
    }

private:
    void transformImpl(DAGPipeline &, Context &, size_t) override
    {
        throw Exception("Unsupport");
    }

private:
    JoinPtr join;
    JoinPreBuilderPtr join_pre_builder;
};

PhysicalPlanNodePtr PhysicalJoinBuild::splitPreAndFinal()
{
    auto join_pre_builder = std::make_shared<JoinPreBuilder>(getSampleBlock());
    auto physical_join_pre_build = std::make_shared<PhysicalJoinPreBuild>(
        executor_id,
        schema,
        log->identifier(),
        child,
        build_side_prepare_actions,
        join_pre_builder);
    physical_join_pre_build->notTiDBOperator();

    auto physical_join_final_build = std::make_shared<PhysicalJoinFinalBuild>(
        executor_id,
        schema,
        log->identifier(),
        join_ptr,
        join_pre_builder);

    auto physical_breaker = std::make_shared<PhysicalPipelineBreaker>(
        executor_id,
        schema,
        log->identifier(),
        physical_join_pre_build,
        physical_join_final_build);
    physical_breaker->notTiDBOperator();
    return physical_breaker;
}
} // namespace DB
