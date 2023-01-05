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

#include <DataStreams/IBlockInputStream.h>
#include <Operators/BlockInputStreamSource.h>

namespace DB
{
BlockInputStreamSource::BlockInputStreamSource(
    const BlockInputStreamPtr & impl_)
    : impl(impl_)
{
    impl->readPrefix();
}

OperatorStatus BlockInputStreamSource::read(Block & block)
{
    if (unlikely(finished))
        return OperatorStatus::PASS;

    block = impl->read();
    if (unlikely(!block))
    {
        impl->readSuffix();
        finished = true;
    }
    return OperatorStatus::PASS;
}

Block BlockInputStreamSource::readHeader()
{
    return impl->getHeader();
}
} // namespace DB