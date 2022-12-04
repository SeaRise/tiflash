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

#include <Common/DynamicThreadPool.h>
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

    virtual PStatus transform(Block & block) = 0;

    virtual Block fetchBlock() = 0;

    virtual bool isBlocked() = 0;
};
using TransformPtr = std::unique_ptr<Transform>;

class CPUTransform : public Transform
{
public:
    PStatus transform(Block & block) override
    {
        if (!block)
            return PStatus::NEED_MORE;

        doCpuPart();
        return PStatus::NEED_MORE;
    }

    Block fetchBlock() override
    {
        return {};
    }

    bool isBlocked() override
    {
        return false;
    }
};

class AsyncIOTransform : public Transform
{
public:
    PStatus transform(Block & block) override
    {
        if (!block)
            return PStatus::NEED_MORE;

        assert(!io_future);
        io_future.emplace(DynamicThreadPool::global_instance->schedule(true, [&, move_block = std::move(block)]() {
            assert(!io_block);
            doIOPart();
            io_block.emplace(std::move(move_block));
        }));
        return PStatus::BLOCKED;
    }

    Block fetchBlock() override
    {
        if (io_block)
        {
            doCpuPart();

            Block block = std::move(io_block.value());
            io_block.reset();
            return block;
        }
        return {};
    }

    bool isBlocked() override
    {
        if (!io_future)
            return false;
        if (io_future->wait_for(std::chrono::seconds(0)) == std::future_status::ready)
        {
            io_future.reset();
            return false;
        }
        return true;
    }

private:
    std::optional<std::future<void>> io_future;
    std::optional<Block> io_block;
};

class SyncIOTransform : public Transform
{
public:
    PStatus transform(Block & block) override
    {
        if (!block)
            return PStatus::NEED_MORE;

        doIOPart();
        doCpuPart();

        return PStatus::NEED_MORE;
    }

    Block fetchBlock() override
    {
        return {};
    }

    bool isBlocked() override
    {
        return false;
    }
};
} // namespace DB
