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

#include <DataStreams/ParallelWriter.h>
#include <Interpreters/Join.h>

namespace DB
{
template <bool is_concurrent_build>
class HashJoinBuildParallelWriter : public ParallelWriter
{
public:
    HashJoinBuildParallelWriter(
        const JoinPtr & join_ptr,
        const String & req_id)
        : ParallelWriter("HashJoinBuild", req_id)
        , join(join_ptr)
    {}

    void write(Block & block, size_t thread_num) override
    {
        if constexpr (is_concurrent_build)
        {
            join->insertFromBlock(block, thread_num);
        }
        else
        {
            join->insertFromBlock(block);
        }
    }

private:
    JoinPtr join;
};
} // namespace DB
