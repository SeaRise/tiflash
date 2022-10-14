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

#include <DataStreams/MergeSortingBlockInputStream.h>
#include <Transforms/TopNBreaker.h>

namespace DB
{
void TopNBreaker::add(Blocks && local_blocks)
{
    std::lock_guard<std::mutex> lock(mu);
    pre_store_blocks_vec.emplace_back(std::move(local_blocks));
}

Block TopNBreaker::read()
{
    std::lock_guard<std::mutex> lock(mu);
    assert(impl);
    return impl->read();
}

void TopNBreaker::initForWrite(size_t concurrency)
{
    std::lock_guard<std::mutex> lock(mu);
    pre_store_blocks_vec.reserve(concurrency);
}

void TopNBreaker::initForRead()
{
    std::lock_guard<std::mutex> lock(mu);
    size_t reserve_size = 0;
    for (auto & pre_store_blocks : pre_store_blocks_vec)
        reserve_size += pre_store_blocks.size();
    blocks.reserve(std::max(1, reserve_size));

    for (auto & pre_store_blocks : pre_store_blocks_vec)
    {
        for (auto & local_block : pre_store_blocks)
            blocks.emplace_back(std::move(local_block));
        pre_store_blocks = {};
    }
    pre_store_blocks_vec = {};

    assert(header.rows() == 0);
    if (blocks.empty())
        blocks.push_back(header);

    // don't need to call readPrefix/readSuffix for MergeSortingBlocksBlockInputStream.
    impl = std::make_unique<MergeSortingBlocksBlockInputStream>(
        blocks,
        description,
        req_id,
        max_merged_block_size,
        limit);
}

Block TopNBreaker::getHeader()
{
    return header;
}
} // namespace DB
