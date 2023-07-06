// Copyright 2023 PingCAP, Ltd.
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

#include <Flash/Coprocessor/ChunkCodec.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/DAGStorageInterpreter.h>
#include <Flash/Coprocessor/GenSchemaAndColumn.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Coprocessor/StorageDisaggregatedInterpreter.h>
#include <Flash/Pipeline/Exec/PipelineExecBuilder.h>
#include <Flash/Planner/FinalizeHelper.h>
#include <Flash/Planner/PhysicalPlanHelper.h>
#include <Flash/Planner/Plans/PhysicalTableScan.h>
#include <Interpreters/Context.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Operators/ExpressionTransformOp.h>

namespace DB
{
namespace
{
ExpressionActionsPtr getSchemaExprActions(
    Context & context,
    const LoggerPtr & log,
    const NamesAndTypes & storage_schema,
    const NamesAndTypes & plan_schema)
{
    RUNTIME_CHECK_MSG(
        storage_schema.size() == plan_schema.size(),
        "the size({}) of storge schema is not equals to the size({}) of table scan schema",
        storage_schema.size(),
        plan_schema.size());

    /// In order to keep TransformOp's schema consistent with PhysicalPlan's schema.
    /// It is worth noting that the column uses the name as the unique identifier in the Block, so the column name must also be consistent.

    ExpressionActionsPtr schema_actions = PhysicalPlanHelper::newActions(storage_schema);

    // perform pre-cast.
    // Because sometimes the table scan schema sent by tidb does not correspond to the actual tiflash storage schema, so add cast here to handle this case.
    // See https://github.com/pingcap/tiflash/issues/6806
    DAGExpressionAnalyzer analyzer{storage_schema, context};
    Names input_names;
    for (size_t i = 0; i < storage_schema.size(); ++i)
    {
        const auto & storage_col = storage_schema[i];
        const auto & plan_col = plan_schema[i];
        auto input_name = storage_col.name;
        if (!storage_col.type->equals(*plan_col.type))
        {
            LOG_WARNING(log, "");
            input_name = analyzer.appendCast(plan_col.type, schema_actions, storage_col.name);
        }
        input_names.push_back(input_name);
    }

    // add project action.
    NamesWithAliases schema_project_cols;
    for (size_t i = 0; i < plan_schema.size(); ++i)
        schema_project_cols.emplace_back(input_names[i], plan_schema[i].name);
    schema_actions->add(ExpressionAction::project(schema_project_cols));

    return schema_actions;
}
} // namespace

PhysicalTableScan::PhysicalTableScan(
    const String & executor_id_,
    const NamesAndTypes & schema_,
    const String & req_id,
    const TiDBTableScan & tidb_table_scan_,
    const Block & sample_block_)
    : PhysicalLeaf(executor_id_, PlanType::TableScan, schema_, FineGrainedShuffle{}, req_id)
    , tidb_table_scan(tidb_table_scan_)
    , sample_block(sample_block_)
{}

PhysicalPlanNodePtr PhysicalTableScan::build(
    const String & executor_id,
    const LoggerPtr & log,
    const TiDBTableScan & table_scan)
{
    auto schema = genNamesAndTypesForTableScan(table_scan);
    auto physical_table_scan = std::make_shared<PhysicalTableScan>(
        executor_id,
        schema,
        log->identifier(),
        table_scan,
        Block(schema));
    return physical_table_scan;
}

void PhysicalTableScan::buildBlockInputStreamImpl(DAGPipeline & pipeline, Context & context, size_t max_streams)
{
    RUNTIME_CHECK(pipeline.streams.empty());

    if (context.getSharedContextDisagg()->isDisaggregatedComputeMode())
    {
        StorageDisaggregatedInterpreter disaggregated_tiflash_interpreter(context, tidb_table_scan, filter_conditions, max_streams);
        disaggregated_tiflash_interpreter.execute(pipeline);
        buildProjection(pipeline, context, disaggregated_tiflash_interpreter.analyzer->getCurrentInputColumns());
    }
    else
    {
        DAGStorageInterpreter storage_interpreter(context, tidb_table_scan, filter_conditions, max_streams);
        storage_interpreter.execute(pipeline);
        buildProjection(pipeline, context, storage_interpreter.analyzer->getCurrentInputColumns());
    }
}

void PhysicalTableScan::buildPipeline(
    PipelineBuilder & builder,
    Context & context,
    PipelineExecutorStatus & exec_status)
{
    // For building PipelineExec in compile time.
    if (context.getSharedContextDisagg()->isDisaggregatedComputeMode())
    {
        StorageDisaggregatedInterpreter disaggregated_tiflash_interpreter(context, tidb_table_scan, filter_conditions, context.getMaxStreams());
        disaggregated_tiflash_interpreter.execute(exec_status, pipeline_exec_builder);
        buildProjection(exec_status, pipeline_exec_builder, context, disaggregated_tiflash_interpreter.analyzer->getCurrentInputColumns());
    }
    else
    {
        DAGStorageInterpreter storage_interpreter(context, tidb_table_scan, filter_conditions, context.getMaxStreams());
        storage_interpreter.execute(exec_status, pipeline_exec_builder);
        buildProjection(exec_status, pipeline_exec_builder, context, storage_interpreter.analyzer->getCurrentInputColumns());
    }

    PhysicalPlanNode::buildPipeline(builder, context, exec_status);
}

void PhysicalTableScan::buildPipelineExecGroupImpl(
    PipelineExecutorStatus & /*exec_status*/,
    PipelineExecGroupBuilder & group_builder,
    Context & /*context*/,
    size_t /*concurrency*/)
{
    assert(group_builder.empty());
    group_builder = std::move(pipeline_exec_builder);
}

void PhysicalTableScan::buildProjection(DAGPipeline & pipeline, Context & context, const NamesAndTypes & storage_schema)
{
    auto schema_actions = getSchemaExprActions(context, log, storage_schema, schema);
    executeExpression(pipeline, schema_actions, log, "table scan schema projection");
}

void PhysicalTableScan::buildProjection(
    PipelineExecutorStatus & exec_status,
    PipelineExecGroupBuilder & group_builder,
    Context & context,
    const NamesAndTypes & storage_schema)
{
    auto schema_actions = getSchemaExprActions(context, log, storage_schema, schema);
    executeExpression(exec_status, group_builder, schema_actions, log);
}

void PhysicalTableScan::finalize(const Names & parent_require)
{
    FinalizeHelper::checkSchemaContainsParentRequire(schema, parent_require);
}

const Block & PhysicalTableScan::getSampleBlock() const
{
    return sample_block;
}

bool PhysicalTableScan::setFilterConditions(const String & filter_executor_id, const tipb::Selection & selection)
{
    /// Since there is at most one selection on the table scan, setFilterConditions() will only be called at most once.
    /// So in this case hasFilterConditions() is always false.
    if (unlikely(hasFilterConditions()))
    {
        return false;
    }

    filter_conditions = FilterConditions::filterConditionsFrom(filter_executor_id, selection);
    return true;
}

bool PhysicalTableScan::hasFilterConditions() const
{
    return filter_conditions.hasValue();
}

const String & PhysicalTableScan::getFilterConditionsId() const
{
    RUNTIME_CHECK(hasFilterConditions());
    return filter_conditions.executor_id;
}
} // namespace DB
