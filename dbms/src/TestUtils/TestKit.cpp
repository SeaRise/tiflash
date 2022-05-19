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

#include <Common/Exception.h>
#include <TestUtils/InterpreterTestUtils.h>
#include <TestUtils/mockExecutor.h>
#include <TestUtils/TestKit.h>
#include <Poco/JSON/Parser.h>
#include <Poco/FileStream.h>

#include <fmt/format.h>

#include <unordered_map>

namespace DB::tests
{
namespace
{
/// If you want to override the result of test.out, change it to true.
constexpr bool is_record = true;
//constexpr bool is_record = false;

String getTestOutputFile(const String & test_name)
{
    assert(std::filesystem::is_directory(std::filesystem::current_path()));
    String cur_path = std::filesystem::current_path();
    String test_output_file = fmt::format("{}{}{}.out", cur_path, std::filesystem::path::preferred_separator, test_name);
    Poco::File poco_file {test_output_file};
    if (!poco_file.exists())
    {
        if constexpr (is_record)
        {
            assert(poco_file.createFile());
        }
        else
        {
            throw Exception(fmt::format("{} don't exists", test_output_file));
        }
    }

    return test_output_file;
}
}

Cases::Cases(const Poco::JSON::Object::Ptr & obj)
{
    Poco::DynamicAny name_from_json = obj->get("Name");
    assert(!name_from_json.isEmpty());
    name = name_from_json.toString();

    Poco::JSON::Array::Ptr cases_from_json = obj->getArray("Cases");
    for (const auto & it : *cases_from_json)
    {
        FmtBuffer buffer;
        Poco::JSON::Array::Ptr case_lines = it.extract<Poco::JSON::Object::Ptr>()->getArray("Result");
        buffer.joinStr(
            case_lines->begin(),
            case_lines->end(),
            [](const auto & line, FmtBuffer & buf) { buf.append(line.toString()); },
            "\n");
        outputs.push_back(buffer.toString());
    }
}

Cases::Cases(const String & name_) : name(name_) {}

Poco::JSON::Object::Ptr Cases::toJson() const
{
    Poco::JSON::Object::Ptr json(new Poco::JSON::Object());
    json->set("Name", name);
    Poco::JSON::Array::Ptr outputs_json(new Poco::JSON::Array());
    for (const String & output : outputs)
    {
        Poco::JSON::Array::Ptr lines_json(new Poco::JSON::Array());
        std::istringstream lines(output);
        String line;
        while (getline(lines, line)) {
            lines_json->add(line);
        }
        Poco::JSON::Object::Ptr case_json(new Poco::JSON::Object());
        case_json->set("Result", lines_json);
    }
    json->set("Cases", outputs_json);
    return json;
}

TestCaseOut::TestCaseOut(const String & test_name_): test_name(test_name_)
{
    if constexpr (!is_record)
    {
        Poco::FileInputStream fis(getTestOutputFile(test_name));

        Poco::JSON::Parser parser;
        parser.parse(fis);
        Poco::DynamicAny result = parser.result();
        assert(result.type() == typeid(Poco::JSON::Array::Ptr));
        Poco::JSON::Array::Ptr array = result.extract<Poco::JSON::Array::Ptr>();
        for (const auto & it : *array)
        {
            Cases cases{it.extract<Poco::JSON::Object::Ptr>()};
            cases_map[cases.getName()] = std::move(cases);
        }
    }
}

void TestCaseOut::assertWithCases(const String & cases_name, const String & expect_result)
{
    if constexpr (is_record)
    {
        auto it = cases_map.find(cases_name);
        if (it == cases_map.end())
        {
            Cases cases{cases_name};
            cases.pushBack(Poco::trim(expect_result));
            cases_map.emplace(cases_name, std::move(cases));
        }
        else
        {
            it->second.pushBack(Poco::trim(expect_result));
        }
    }
    else
    {
        auto & cases = cases_map[expect_result];
        assert(cases.hasNext());
        ASSERT_EQ(Poco::trim(expect_result), Poco::trim(cases.next()));
    }
}

Poco::JSON::Array::Ptr TestCaseOut::toJson() const
{
    Poco::JSON::Array::Ptr json(new Poco::JSON::Array());

    for (const auto & cases : cases_map)
    {
        json->add(cases.second.toJson());
    }

    return json;
}

TestCaseOut::~TestCaseOut()
{
    if constexpr (is_record)
    {
        auto test_output_file = getTestOutputFile(test_name);
        Poco::File(test_output_file).setSize(0);
        Poco::FileOutputStream fos(test_output_file);

        Poco::JSON::Stringifier::stringify(toJson(), fos, 4, -1);

        fos.flush();
    }
}
} // namespace DB::tests
