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

#pragma once

#include <TestUtils/AggregateFunctionTestUtils.h>

namespace DB
{
namespace tests
{
namespace
{
constexpr std::string_view test_db = "test_db";
constexpr std::string_view test_table = "test_table";
}
ColumnWithTypeAndName AggregateFunctionTest::executeAggregateFunction(
    const String & func_name,
    const ColumnsWithTypeAndName & columns)
{

}

std::shared_ptr<tipb::DAGRequest> AggregateFunctionTest::buildDAGRequest(MockAstVec agg_funcs)
{
    context.scan(test_db, test_table).aggregation({}, {});
}
}
}
