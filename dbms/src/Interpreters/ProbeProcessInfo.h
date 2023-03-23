// Copyright 2023 PingCAP, Ltd.
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

#include <Core/Block.h>

#pragma once

namespace DB
{
struct ProbeProcessInfo
{
    Block block;
    size_t partition_index;
    UInt64 max_block_size;
    size_t start_row;
    size_t end_row;
    bool all_rows_joined_finish;

    explicit ProbeProcessInfo(UInt64 max_block_size_)
        : max_block_size(max_block_size_)
        , all_rows_joined_finish(true)
    {}

    void resetBlock(Block && block_, size_t partition_index_ = 0);
    void updateStartRow();
};
} // namespace DB
