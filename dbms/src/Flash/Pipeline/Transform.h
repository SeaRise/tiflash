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
#include <Flash/Pipeline/PStatus.h>
#include <Flash/Pipeline/Utils.h>
#include <common/types.h>

#include <memory>

namespace DB
{
class Transform
{
public:
    virtual ~Transform() = default;

    virtual TaskResult transform(Block & block) = 0;

    virtual Block fetchBlock() = 0;

    virtual TaskResult isBlocked() = 0;
};
using TransformPtr = std::unique_ptr<Transform>;

class CPUTransform : public Transform
{
public:
    TaskResult transform(Block & block) override
    {
        if (!block)
            return TaskResult::needMore();

        doCpuPart();
        return TaskResult::needMore();
    }

    Block fetchBlock() override
    {
        return {};
    }

    TaskResult isBlocked() override
    {
        return TaskResult::needMore();
    }
};

class AsyncIOTransform : public Transform
{
public:
    TaskResult transform(Block & block) override
    {
        if (!block)
            return TaskResult::needMore();

        assert(!io_block);
        return TaskResult::blocked([&, move_block = std::move(block)]() {
            doIOPart();
            io_block.emplace(std::move(move_block));
        });
    }

    Block fetchBlock() override
    {
        assert(io_block);
        Block block = std::move(io_block.value());
        io_block.reset();
        return block;
    }

    TaskResult isBlocked() override
    {
        return TaskResult::needMore();
    }

private:
    std::optional<Block> io_block;
};

class SyncIOTransform : public Transform
{
public:
    TaskResult transform(Block & block) override
    {
        if (!block)
            return TaskResult::needMore();
        doIOPart();
        return TaskResult::needMore();
    }

    Block fetchBlock() override
    {
        return {};
    }

    TaskResult isBlocked() override
    {
        return TaskResult::needMore();
    }
};
} // namespace DB
