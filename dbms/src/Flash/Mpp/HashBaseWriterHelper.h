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
#include <Flash/Mpp/TrackedMppDataPacket.h>
#include <Storages/Transaction/Collator.h>

namespace DB::HashBaseWriterHelper
{
void materializeBlock(Block & input_block);
void materializeBlocks(std::vector<Block> & input_blocks);

std::vector<MutableColumns> createDestColumns(const Block & sample_block, size_t num);

void computeHash(const Block & block,
                 uint32_t num_bucket,
                 const TiDB::TiDBCollators & collators,
                 std::vector<String> & partition_key_containers,
                 const std::vector<Int64> & partition_col_ids,
                 WeakHash32 & hash,
                 IColumn::Selector & selector);

DB::TrackedMppDataPacketPtrs createPackets(size_t partition_num);

void scatterColumns(const Block & input_block,
                    uint32_t bucket_num,
                    const TiDB::TiDBCollators & collators,
                    std::vector<String> & partition_key_containers,
                    const std::vector<Int64> & partition_col_ids,
                    std::vector<std::vector<MutableColumnPtr>> & result_columns);

void scatterColumnsInplace(const Block & block,
                           uint32_t bucket_num,
                           const TiDB::TiDBCollators & collators,
                           std::vector<String> & partition_key_containers,
                           const std::vector<Int64> & partition_col_ids,
                           WeakHash32 & hash,
                           IColumn::Selector & selector,
                           std::vector<IColumn::ScatterColumns> & scattered);

} // namespace DB::HashBaseWriterHelper
