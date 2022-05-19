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

#include <common/types.h>
#include <Poco/JSON/Object.h>
#include <tipb/select.pb.h>
#include <unordered_map>

#include <memory>
#include <vector>

namespace DB::tests
{

/**
 *
 * format of test.out
 *
 * [
 *   {
 *    "Name": "${cases_name}",
 *     "Cases": [
 *       {
 *         "Result": [
 *           "xx",
 *           "yy"
 *         ]
 *       },
 *       {
 *         "Result": [
 *           "xx",
 *           "yy"
 *         ]
 *       }
 *     ]
 *   },
 *   {
 *     "Name": "${cases_name}",
 *     "Cases": [
 *       {
 *         "Result": [
 *           "xx",
 *           "yy"
 *         ]
 *       },
 *       {
 *         "Result": [
 *           "xx",
 *           "yy"
 *         ]
 *       }
 *     ]
 *   }
 * ]
 */

class Cases
{
public:
    explicit Cases(const Poco::JSON::Object::Ptr & obj);

    explicit Cases(const String & name_);

    bool hasNext() const
    {
        return index < outputs.size();
    }

    const String & next()
    {
        return outputs[index++];
    }

    void pushBack(const String & output)
    {
        outputs.push_back(output);
    }

    const String & getName() const { return name; }

    Poco::JSON::Object::Ptr toJson() const;

private:
    String name;
    std::vector<String> outputs;
    size_t index = 0;
};

class TestCaseOut
{
public:
    explicit TestCaseOut(const String & test_name_);

    void assertWithCases(const String & cases_name, const String & expect_result);

    Poco::JSON::Array::Ptr toJson() const;

    ~TestCaseOut();

private:
     String test_name;
     std::unordered_map<String, Cases> cases_map;
};
} // namespace DB::tests
