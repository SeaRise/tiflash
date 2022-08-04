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

#include <TestUtils/ExecutorTestUtils.h>
#include <TestUtils/FunctionTestUtils.h>

namespace DB
{
namespace tests
{
class AggregateFunctionTest : public ExecutorTest
{
public:
    AggregateFunctionTest(): ExecutorTest() {}

    ColumnWithTypeAndName executeAggregateFunction(
        const String & func_name,
        const ColumnsWithTypeAndName & columns);

    template <typename... Args>
    ColumnWithTypeAndName executeAggregateFunction(const String & func_name, const ColumnWithTypeAndName & first_column, const Args &... columns)
    {
        ColumnsWithTypeAndName vec({first_column, columns...});
        return executeAggregateFunction(func_name, vec);
    }

protected:
    std::shared_ptr<tipb::DAGRequest> buildDAGRequest();
};
}
}
