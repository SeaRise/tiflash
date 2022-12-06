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
class Sink
{
public:
    virtual ~Sink() = default;

    virtual TaskResult write(Block & block) = 0;

    virtual TaskResult isBlocked() = 0;
};
using SinkPtr = std::unique_ptr<Sink>;

class CPUSink : public Sink
{
public:
    TaskResult write(Block & block) override
    {
        if (!block)
            return TaskResult::finish();

        block.clear();
        doCpuPart();
        return TaskResult::needMore();
    }

    TaskResult isBlocked() override
    {
        return TaskResult::needMore();
    }
};

class AsyncIOSink : public Sink
{
public:
    TaskResult write(Block & block) override
    {
        if (!block)
            return TaskResult::finish();
        block.clear();

        return TaskResult::blocked([]() { doIOPart(); });
    }

    TaskResult isBlocked() override
    {
        return TaskResult::needMore();
    }
};

class SyncIOSink : public Sink
{
public:
    TaskResult write(Block & block) override
    {
        if (!block)
            return TaskResult::finish();
        block.clear();
        doIOPart();
        return TaskResult::needMore();
    }

    TaskResult isBlocked() override
    {
        return TaskResult::needMore();
    }
};
} // namespace DB
