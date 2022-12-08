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

#include <Core/Block.h>
#include <DataTypes/DataTypesNumber.h>
#include <Flash/Pipeline/PStatus.h>
#include <TestUtils/ColumnGenerator.h>
#include <common/types.h>

#include <memory>

namespace DB
{
namespace
{
Block prepareRandomBlock()
{
    Block block;
    for (size_t i = 0; i < 10; ++i)
    {
        DataTypePtr int64_data_type = std::make_shared<DataTypeInt64>();
        auto int64_column = tests::ColumnGenerator::instance().generate({1, "Int64", tests::RANDOM}).column;
        block.insert(ColumnWithTypeAndName{
            std::move(int64_column),
            int64_data_type,
            String("col") + std::to_string(i)});
    }
    return block;
}
} // namespace

class Source
{
public:
    virtual ~Source() = default;

    virtual Block read() = 0;

    virtual TaskResult isBlocked() = 0;
};
using SourcePtr = std::unique_ptr<Source>;

class CPUSource : public Source
{
public:
    Block read() override
    {
        if (block_count > 0)
        {
            doCpuPart();
            --block_count;
            return prepareRandomBlock();
        }
        return {};
    }

    TaskResult isBlocked() override
    {
        return TaskResult::needMore();
    }

private:
    int block_count = 5;
};

class AsyncIOSource : public Source
{
public:
    Block read() override
    {
        assert(!should_io);
        if (block_count > 0)
        {
            --block_count;
            should_io = block_count > 0;
            return prepareRandomBlock();
        }
        assert(0 == io_count);
        return {};
    }

    TaskResult isBlocked() override
    {
        if (should_io)
        {
            should_io = false;
            --io_count;
            return TaskResult::blocked([]() { doIOPart(); });
        }
        else
        {
            return TaskResult::needMore();
        }
    }

private:
    bool should_io = true;
    int io_count = 5;
    int block_count = 5;
};

class SyncIOSource : public Source
{
public:
    Block read() override
    {
        if (block_count > 0)
        {
            doIOPart();
            --io_count;
            --block_count;
            return prepareRandomBlock();
        }
        assert(0 == io_count);
        return {};
    }

    TaskResult isBlocked() override
    {
        return TaskResult::needMore();
    }

private:
    int io_count = 5;
    int block_count = 5;
};
} // namespace DB
