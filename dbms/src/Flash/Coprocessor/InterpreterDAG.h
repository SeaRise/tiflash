#pragma once

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <kvproto/coprocessor.pb.h>
#include <tipb/select.pb.h>
#pragma GCC diagnostic pop

#include <DataStreams/BlockIO.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGQueryBlockInterpreter.h>
#include <Flash/Coprocessor/DAGQuerySource.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Interpreters/AggregateDescription.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/IInterpreter.h>
#include <Storages/RegionQueryInfo.h>
#include <Storages/Transaction/RegionException.h>
#include <Storages/Transaction/TMTStorages.h>

namespace DB
{

class Context;
class Region;
using RegionPtr = std::shared_ptr<Region>;

/** build ch plan from dag request: dag executors -> ch plan
  */
class InterpreterDAG : public IInterpreter
{
public:
    InterpreterDAG(Context & context_, const DAGQuerySource & dag_);

    ~InterpreterDAG() = default;

    BlockIO execute();

private:
    BlockInputStreams executeQueryBlock(DAGQueryBlock & query_block, const std::vector<RegionInfo> & region_infos);
    void executeUnion(Pipeline & pipeline);
    /*
    void executeImpl(Pipeline & pipeline);
    void executeTS(const tipb::TableScan & ts, Pipeline & pipeline);
    void executeWhere(Pipeline & pipeline, const ExpressionActionsPtr & expressionActionsPtr, String & filter_column);
    void executeExpression(Pipeline & pipeline, const ExpressionActionsPtr & expressionActionsPtr);
    void executeOrder(Pipeline & pipeline, Strings & order_column_names);
    void executeUnion(Pipeline & pipeline);
    void executeLimit(Pipeline & pipeline);
    void executeAggregation(Pipeline & pipeline, const ExpressionActionsPtr & expressionActionsPtr, Names & aggregation_keys,
        AggregateDescriptions & aggregate_descriptions);
    void executeFinalProject(Pipeline & pipeline);
    void getAndLockStorageWithSchemaVersion(TableID table_id, Int64 schema_version);
    SortDescription getSortDescription(Strings & order_column_names);
    AnalysisResult analyzeExpressions();
    void recordProfileStreams(Pipeline & pipeline, Int32 index);
    bool addTimeZoneCastAfterTS(std::vector<bool> & is_ts_column, Pipeline & pipeline);
    RegionException::RegionReadStatus getRegionReadStatus(const RegionPtr & current_region);
     */

private:
    Context & context;

    const DAGQuerySource & dag;

    /// How many streams we ask for storage to produce, and in how many threads we will do further processing.
    size_t max_streams = 1;
    /*
    NamesWithAliases final_project;

    /// How many streams we ask for storage to produce, and in how many threads we will do further processing.
    size_t max_streams = 1;

    /// Table from where to read data, if not subquery.
    ManageableStoragePtr storage;
    TableStructureReadLockPtr table_lock;

    std::unique_ptr<DAGExpressionAnalyzer> analyzer;
     */

    const bool keep_session_timezone_info;

    /*
    bool filter_on_handle = false;
    tipb::Expr handle_filter_expr;
    Int32 handle_col_id = -1;
    std::vector<const tipb::Expr *> conditions;
     */

    Poco::Logger * log;
};
} // namespace DB
