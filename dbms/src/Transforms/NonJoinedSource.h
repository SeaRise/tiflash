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

#include <DataStreams/IBlockInputStream.h>
#include <Interpreters/Join.h>
#include <Transforms/Source.h>

namespace DB
{
class NonJoinedSource : public Source
{
public:
    NonJoinedSource(
        const JoinPtr & join_,
        const BlockInputStreamPtr & impl_)
        : join(join_)
        , impl(impl_)
    {}

    Block read() override
    {
        return impl->read();
    }

    Block getHeader() const override
    {
        return impl->getHeader();
    }

    void prepare() override
    {
        impl->readPrefix();
    }

    void finish() override
    {
        impl->readSuffix();
    }

private:
    JoinPtr join;
    BlockInputStreamPtr impl;
};
} // namespace DB
