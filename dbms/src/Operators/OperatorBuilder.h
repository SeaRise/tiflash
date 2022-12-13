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

#include <Operators/OperatorExecutor.h>

#include <memory>

namespace DB
{
struct OperatorBuilder
{
    SourcePtr source;
    std::vector<TransformPtr> transforms;
    SinkPtr sink;

    Block header;

    void setSource(SourcePtr && source_)
    {
        assert(!source && source_);
        source = std::move(source_);
        header = source->readHeader();
    }
    void appendTransform(TransformPtr && transform)
    {
        assert(transform);
        transforms.push_back(std::move(transform));
        transforms.back()->transformHeader(header);
    }
    void setSink(SinkPtr && sink_)
    {
        assert(!sink && sink_);
        sink = std::move(sink_);
    }

    OperatorExecutorPtr build()
    {
        assert(source && sink);
        return std::make_unique<OperatorExecutor>(
            std::move(source),
            std::move(transforms),
            std::move(sink));
    }
};

struct OperatorsBuilder
{
    using BuilderGroup = std::vector<OperatorBuilder>;
    std::vector<BuilderGroup> groups;

    size_t max_concurrency_in_groups = 0;

    void addGroup(size_t concurrency)
    {
        assert(concurrency > 0);
        max_concurrency_in_groups = std::max(max_concurrency_in_groups, concurrency);
        BuilderGroup group;
        group.resize(concurrency);
        groups.push_back(std::move(group));
    }

    /// ff: [](OperatorBuilder & builder) {}
    template <typename FF>
    void transform(FF && ff)
    {
        assert(max_concurrency_in_groups > 0);
        for (auto & group : groups)
        {
            for (auto & builder : group)
            {
                ff(builder);
            }
        }
    }

    OperatorExecutorGroups build()
    {
        assert(max_concurrency_in_groups > 0);
        OperatorExecutorGroups op_groups;
        for (auto & group : groups)
        {
            OperatorExecutorGroup op_group;
            for (auto & builder : group)
                op_group.push_back(builder.build());
            op_groups.push_back(std::move(op_group));
        }
        return op_groups;
    }

    Block getHeader()
    {
        assert(max_concurrency_in_groups > 0);
        assert(groups.back().back().header);
        return groups.back().back().header;
    }
};
} // namespace DB
