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

#include <TestUtils/InterpreterTestUtils.h>
#include <Poco/JSON/Parser.h>

namespace DB::tests
{
class InterpretTestKit
{
public:
    InterpretTestKit(
        const String & test_case_name_,
        size_t concurrency_)
        : test_case_name(test_case_name_)
        , concurrency(concurrency_)
    {

    }

    void interpret(const std::shared_ptr<tipb::DAGRequest> & request)
    {

    }

private:
    const String test_case_name;
    const size_t concurrency;
    size_t case_index;
};
} // namespace DB::tests
