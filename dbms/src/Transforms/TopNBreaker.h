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
#include <Core/SortDescription.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/materializeBlock.h>

#include <memory>

namespace DB
{
// todo use removeConstantsFromBlock, etc to improve sort by literal like MergeSortingBlockInputStream.
class TopNBreaker
{
public:
    TopNBreaker(
        const SortDescription & description_,
        const String & req_id_,
        size_t max_merged_block_size_,
        size_t limit_,
        const Block & sample_block)
        : description(description_)
        , req_id(req_id_)
        , max_merged_block_size(max_merged_block_size_)
        , limit(limit_)
        , header(materializeBlock(sample_block))
    {}

    void initForWrite(size_t concurrency);

    void add(Blocks && local_blocks);

    Block read();

    void initForRead();

    Block getHeader();

private:
    SortDescription description;
    String req_id;
    size_t max_merged_block_size;
    size_t limit;
    Block header;

    std::mutex mu;
    std::vector<Blocks> pre_store_blocks_vec;
    Blocks blocks;
    std::unique_ptr<IBlockInputStream> impl;
};
using TopNBreakerPtr = std::shared_ptr<TopNBreaker>;
} // namespace DB
