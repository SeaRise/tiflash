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

#include <Debug/MockStorage.h>
#include <Interpreters/Context.h>
#include <TestUtils/ExecutorTestUtils.h>
#include <TestUtils/InputStreamTestUtils.h>
#include <TestUtils/mockExecutor.h>
#include "Common/TiFlashMetrics.h"

namespace DB
{
namespace tests
{
class TestExecsWithRU : public DB::tests::ExecutorTest
{
public:
    void initializeContext() override
    {
        ExecutorTest::initializeContext();
        context.mockStorage()->setUseDeltaMerge(true);
        context.context->getSettingsRef().dt_enable_read_thread = true;
        context.context->getSettingsRef().dt_segment_stable_pack_rows = 1;
        context.context->getSettingsRef().dt_segment_limit_rows = 1;
        context.context->getSettingsRef().dt_segment_delta_cache_limit_rows = 1;

        size_t rows = 50000;
        std::vector<TypeTraits<Int64>::FieldType> key(rows);
        std::vector<std::optional<String>> value(rows);
        for (size_t i = 0; i < rows; ++i)
        {
            key[i] = i % 15;
            value[i] = {fmt::format("val_{}", i)};
        }
        context.addMockDeltaMerge(
            {"test_db", "big_table"},
            {{"key", TiDB::TP::TypeLongLong},
             {"value", TiDB::TP::TypeString}},
            {toVec<Int64>("key", key), toNullableVec<String>("value", value)});
    }
};

TEST_F(TestExecsWithRU, Basic)
try
{
    enablePipeline(true);
    auto request = context
                       .scan("test_db", "big_table")
                       .build(context);
    executeStreams(request, 10);
}
CATCH

} // namespace tests
} // namespace DB
