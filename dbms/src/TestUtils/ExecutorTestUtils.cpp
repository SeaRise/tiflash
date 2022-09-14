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

#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Common/FmtUtils.h>
#include <Debug/MockComputeServerManager.h>
#include <Flash/Coprocessor/DAGQuerySource.h>
#include <Flash/executeQuery.h>
#include <TestUtils/ExecutorTestUtils.h>
#include <TestUtils/executorSerializer.h>

#include <functional>

namespace DB::tests
{
namespace
{
Block mergeBlocks(Blocks blocks)
{
    if (blocks.empty())
        return {};

    Block sample_block = blocks.back();
    std::vector<MutableColumnPtr> actual_cols;
    for (const auto & column : sample_block.getColumnsWithTypeAndName())
    {
        actual_cols.push_back(column.type->createColumn());
    }
    for (const auto & block : blocks)
    {
        for (size_t i = 0; i < block.columns(); ++i)
        {
            for (size_t j = 0; j < block.rows(); ++j)
            {
                actual_cols[i]->insert((*(block.getColumnsWithTypeAndName())[i].column)[j]);
            }
        }
    }

    ColumnsWithTypeAndName actual_columns;
    for (size_t i = 0; i < actual_cols.size(); ++i)
        actual_columns.push_back({std::move(actual_cols[i]), sample_block.getColumnsWithTypeAndName()[i].type, sample_block.getColumnsWithTypeAndName()[i].name, sample_block.getColumnsWithTypeAndName()[i].column_id});
    return Block(actual_columns);
}
} // namespace

TiDB::TP dataTypeToTP(const DataTypePtr & type)
{
    // TODO support more types.
    switch (removeNullable(type)->getTypeId())
    {
    case TypeIndex::UInt8:
    case TypeIndex::Int8:
        return TiDB::TP::TypeTiny;
    case TypeIndex::UInt16:
    case TypeIndex::Int16:
        return TiDB::TP::TypeShort;
    case TypeIndex::UInt32:
    case TypeIndex::Int32:
        return TiDB::TP::TypeLong;
    case TypeIndex::UInt64:
    case TypeIndex::Int64:
        return TiDB::TP::TypeLongLong;
    case TypeIndex::String:
        return TiDB::TP::TypeString;
    case TypeIndex::Float32:
        return TiDB::TP::TypeFloat;
    case TypeIndex::Float64:
        return TiDB::TP::TypeDouble;
    default:
        throw Exception("Unsupport type");
    }
}

DAGContext & ExecutorTest::getDAGContext()
{
    assert(dag_context_ptr != nullptr);
    return *dag_context_ptr;
}

void ExecutorTest::initializeContext()
{
    dag_context_ptr = std::make_unique<DAGContext>(1024);
    context = MockDAGRequestContext(TiFlashTestEnv::getContext());
    dag_context_ptr->log = Logger::get("executorTest");
    TiFlashTestEnv::getGlobalContext().setExecutorTest();
}

void ExecutorTest::SetUpTestCase()
{
    auto register_func = [](std::function<void()> func) {
        try
        {
            func();
        }
        catch (DB::Exception &)
        {
            // Maybe another test has already registered, ignore exception here.
        }
    };

    register_func(DB::registerFunctions);
    register_func(DB::registerAggregateFunctions);
    register_func(DB::registerWindowFunctions);
}

void ExecutorTest::initializeClientInfo()
{
    context.context.setCurrentQueryId("test");
    ClientInfo & client_info = context.context.getClientInfo();
    client_info.query_kind = ClientInfo::QueryKind::INITIAL_QUERY;
    client_info.interface = ClientInfo::Interface::GRPC;
}

void ExecutorTest::executeInterpreter(const String & expected_string, const std::shared_ptr<tipb::DAGRequest> & request, size_t concurrency)
{
    DAGContext dag_context(*request, "interpreter_test", concurrency);
    context.context.setDAGContext(&dag_context);
    context.context.setExecutorTest();
    // Currently, don't care about regions information in interpreter tests.
    auto res = executeQuery(context.context);
    FmtBuffer fb;
    res.in->dumpTree(fb);
    ASSERT_EQ(Poco::trim(expected_string), Poco::trim(fb.toString()));
}

void ExecutorTest::executeExecutor(
    const std::shared_ptr<tipb::DAGRequest> & request,
    std::function<void(const ColumnsWithTypeAndName &)> assert_func)
{
    WRAP_FOR_DIS_ENABLE_PLANNER_BEGIN
    std::vector<size_t> concurrencies{1, 2, 10};
    for (auto concurrency : concurrencies)
    {
        std::vector<size_t> block_sizes{1, 2, DEFAULT_BLOCK_SIZE};
        for (auto block_size : block_sizes)
        {
            context.context.setSetting("max_block_size", Field(static_cast<UInt64>(block_size)));
            assert_func(executeStreams(request, concurrency));
        }
    }
    WRAP_FOR_DIS_ENABLE_PLANNER_END
}

void ExecutorTest::executeAndAssertColumnsEqual(const std::shared_ptr<tipb::DAGRequest> & request, const ColumnsWithTypeAndName & expect_columns)
{
    executeExecutor(request, [&](const ColumnsWithTypeAndName & res) {
        ASSERT_COLUMNS_EQ_UR(expect_columns, res);
    });
}

void ExecutorTest::executeAndAssertRowsEqual(const std::shared_ptr<tipb::DAGRequest> & request, size_t expect_rows)
{
    executeExecutor(request, [&](const ColumnsWithTypeAndName & res) {
        ASSERT_EQ(expect_rows, Block(res).rows());
    });
}

void readStream(Blocks & blocks, BlockInputStreamPtr stream)
{
    stream->readPrefix();
    while (auto block = stream->read())
    {
        blocks.push_back(block);
    }
    stream->readSuffix();
}

DB::ColumnsWithTypeAndName readBlock(BlockInputStreamPtr stream)
{
    return readBlocks({stream});
}

DB::ColumnsWithTypeAndName readBlocks(std::vector<BlockInputStreamPtr> streams)
{
    Blocks actual_blocks;
    for (const auto & stream : streams)
        readStream(actual_blocks, stream);
    return mergeBlocks(actual_blocks).getColumnsWithTypeAndName();
}

void ExecutorTest::enablePlanner(bool is_enable)
{
    context.context.setSetting("enable_planner", is_enable ? "true" : "false");
}

DB::ColumnsWithTypeAndName ExecutorTest::executeStreams(const std::shared_ptr<tipb::DAGRequest> & request, size_t concurrency)
{
    DAGContext dag_context(*request, "executor_test", concurrency);
    context.context.setExecutorTest();
    context.context.setMockStorage(context.mockStorage());
    context.context.setDAGContext(&dag_context);
    // Currently, don't care about regions information in tests.
    return readBlock(executeQuery(context.context).in);
}

void ExecutorTest::dagRequestEqual(const String & expected_string, const std::shared_ptr<tipb::DAGRequest> & actual)
{
    ASSERT_EQ(Poco::trim(expected_string), Poco::trim(ExecutorSerializer().serialize(actual.get())));
}

} // namespace DB::tests
