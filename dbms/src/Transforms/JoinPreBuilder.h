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
#include <DataStreams/materializeBlock.h>
#include <Interpreters/Join.h>

#include <memory>

namespace DB
{
class JoinPreBuilder
{
public:
    explicit JoinPreBuilder(const Block & sample_block): header(materializeBlock(sample_block)) {}

    void initForWrite(size_t concurrency)
    {
        std::unique_lock init_lock(init_mutex);
        if (inited)
            return;
        inited = true;

        max_threads = concurrency;
        assert(max_threads > 0);
        pre_build_blocks_vec.resize(max_threads);
        rows_vec.reserve(max_threads);
        for (size_t i = 0; i < max_threads; ++i)
            rows_vec.emplace_back(0);
        mutexes = std::make_unique<std::vector<std::mutex>>(max_threads);
    }

    void insert(size_t index, Block && block)
    {
        std::shared_lock init_lock(init_mutex);
        assert(inited);
        assert(index < max_threads);
        std::lock_guard lock((*mutexes)[index]);
        rows_vec[index] += block.rows();
        pre_build_blocks_vec[index].emplace_back(std::move(block));
    }

    Block popBack(size_t index)
    {
        std::shared_lock init_lock(init_mutex);
        assert(inited);
        assert(index < max_threads);
        std::lock_guard lock((*mutexes)[index]);
        auto & pre_build_blocks = pre_build_blocks_vec[index];
        if (pre_build_blocks.empty())
            return {};
        Block block = std::move(pre_build_blocks.back());
        pre_build_blocks.pop_back();
        return block;
    }

    void doReserve(const JoinPtr & join)
    {
        size_t rows = 0;
        std::shared_lock init_lock(init_mutex);
        assert(inited);
        for (size_t i = 0; i < max_threads; ++i)
        {
            std::lock_guard lock((*mutexes)[i]);
            rows += rows_vec[i];
        }
        join->reserve(rows);
    }

    const Block & getHeader() const
    {
        return header;
    }

public:
    size_t max_threads;

private:
    Block header;

    mutable std::shared_mutex init_mutex;
    bool inited = false;
    std::unique_ptr<std::vector<std::mutex>> mutexes;
    std::vector<Blocks> pre_build_blocks_vec;
    std::vector<size_t> rows_vec;
};
using JoinPreBuilderPtr = std::shared_ptr<JoinPreBuilder>;
} // namespace DB
