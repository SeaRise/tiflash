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

#include <Flash/Pipeline/Sink.h>
#include <Flash/Pipeline/Source.h>
#include <Flash/Pipeline/Transform.h>

namespace DB
{
class Task
{
public:
    Task(
        SourcePtr && source_,
        std::vector<TransformPtr> && transforms_,
        SinkPtr && sink_)
        : source(std::move(source_))
        , transforms(std::move(transforms_))
        , sink(std::move(sink_))
    {}

    TaskResult execute()
    {
        try
        {
            assert(!blocking_job);

            auto is_blocked = isBlocked();
            if (is_blocked.status == PStatus::BLOCKED)
            {
                blocking_job.emplace(std::move(is_blocked.blocking_job));
                return is_blocked;
            }
            else if (is_blocked.status != PStatus::NEED_MORE)
                return is_blocked;

            auto [block, op_index] = fetchBlock();
            RUNTIME_CHECK(!blocked_op_index);
            for (; op_index < transforms.size(); ++op_index)
            {
                auto res = transforms[op_index]->transform(block);
                if (res.status == PStatus::NEED_MORE)
                    continue;
                else if (res.status == PStatus::BLOCKED)
                {
                    blocking_job.emplace(std::move(res.blocking_job));
                    blocked_op_index.emplace(op_index);
                }
                return res;
            }
            auto res = sink->write(block);
            if (res.status == PStatus::BLOCKED)
                blocking_job.emplace(std::move(res.blocking_job));
            return res;
        }
        catch (...)
        {
            return TaskResult::fail(getCurrentExceptionMessage(true, true));
        }
    }

    void doBlockingJob()
    {
        assert(blocking_job);
        blocking_job.value()();
        blocking_job.reset();
    }

private:
    TaskResult isBlocked()
    {
        TaskResult res = sink->isBlocked();
        if (res.status != PStatus::NEED_MORE)
            return res;

        if (blocked_op_index)
            return transforms[*blocked_op_index]->isBlocked();

        return source->isBlocked();
    }

    // Block, next_op_index
    std::tuple<Block, size_t> fetchBlock()
    {
        if (blocked_op_index)
        {
            auto op_index = *blocked_op_index;
            blocked_op_index.reset();
            return {transforms[op_index]->fetchBlock(), op_index + 1};
        }
        return {source->read(), 0};
    }

private:
    SourcePtr source;
    std::vector<TransformPtr> transforms;
    SinkPtr sink;

    // block
    std::optional<int> blocked_op_index;
    std::optional<BlockingJob> blocking_job;
};
using TaskPtr = std::unique_ptr<Task>;
} // namespace DB
