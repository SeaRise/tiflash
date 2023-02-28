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

#include <DataStreams/MergeSortingBlocksBlockInputStream.h>
#include <DataStreams/MergingSortedBlockInputStream.h>
#include <Interpreters/sortBlock.h>
#include <Operators/TopNTransformOp.h>
#include <DataStreams/MergeSortingHelper.h>

namespace DB
{
    TopNTransformOp::TopNTransformOp(
        PipelineExecutorStatus & exec_status_,
        const SortDescription & order_desc_,
        size_t limit_,
        size_t max_block_size_,
        size_t max_bytes_before_external_sort_,
        const SpillConfig & spill_config_,
        const String & req_id_)
        : TransformOp(exec_status_)
        , log(Logger::get(req_id_))
        , order_desc(order_desc_)
        , limit(limit_)
        , max_block_size(max_block_size_)
        , max_bytes_before_external_sort(max_bytes_before_external_sort_)
        , spill_config(spill_config_)
    {
    }

OperatorStatus TopNTransformOp::transformImpl(Block & block)
{
    while (true)
    {
        switch (status)
        {
        case TopNStatus::INIT:
        {
            header_without_constants = header_;
            MergeSortingHelper::removeConstantsFromBlock(header_without_constants);
            MergeSortingHelper::removeConstantsFromSortDescription(header, order_desc);
            spiller = std::make_unique<Spiller>(spill_config, true, 1, header_without_constants, log);
            status = order_desc.empty() ? TopNStatus::ONLY_LIMIT_OUTPUT : TopNStatus::PARTIAL;
            break;
        }
        case TopNStatus::ONLY_LIMIT_OUTPUT:
        {
            sortBlock(block, order_desc, limit);
            return OperatorStatus::HAS_OUTPUT;
        }
        case TopNStatus::PARTIAL:
        {
            if (unlikely(!block))
            {
                    if (!spiller->hasSpilledData())
                    {
                        if (likely(!blocks.empty()))
                            impl = std::make_unique<MergeSortingBlocksBlockInputStream>(
                                blocks,
                                order_desc,
                                log->identifier(),
                                max_block_size,
                                limit);
                        status = TopNStatus::MERGE;
                    }
                    else
                    {
                        /// If spill happens
            
                        LOG_INFO(log, "Begin external merge sort.");
            
                        /// Create sorted streams to merge.
                        spiller->finishSpill();
                        inputs_to_merge = spiller->restoreBlocks(0, 0);
            
                        /// Rest of blocks in memory.
                        if (!blocks.empty())
                            inputs_to_merge.emplace_back(std::make_shared<MergeSortingBlocksBlockInputStream>(
                                blocks,
                                order_desc,
                                log->identifier(),
                                max_block_size,
                                limit));
            
                        /// Will merge that sorted streams.
                        impl = std::make_unique<MergingSortedBlockInputStream>(inputs_to_merge, order_desc, max_block_size, limit);
                        status = TopNStatus::RESTORE;
                    }
                break;
            }
    
            MergeSortingHelper::removeConstantsFromBlock(block);
            sortBlock(block, order_desc, limit);
            sum_bytes_in_blocks += block.bytes();
            blocks.push_back(std::move(block));

            /** If too many of them and if external sorting is enabled,
              *  will merge blocks that we have in memory at this moment and write merged stream to temporary (compressed) file.
              * NOTE. It's possible to check free space in filesystem.
              */
            if (max_bytes_before_external_sort && sum_bytes_in_blocks > max_bytes_before_external_sort)
            {
                status = TopNStatus::SPILL;
                break;
            }

            return OperatorStatus::NEED_INPUT;
        }
        case TopNStatus::SPILL:
        {
            MergeSortingBlocksBlockInputStream block_in(blocks, order_desc, log->identifier(), max_block_size, limit);
            auto is_cancelled_pred = [this]() {
                return this->exec_status.isCancelled();
            };
            spiller->spillBlocksUsingBlockInputStream(block_in, 0, is_cancelled_pred);
            blocks.clear();
            sum_bytes_in_blocks = 0;
            return OperatorStatus::NEED_INPUT;
        }
        case TopNStatus::MERGE:
        case TopNStatus::RESTORE:
        {
            if (likely(impl))
            {
                block = impl->read();
                if (likely(block))
                    MergeSortingHelper::enrichBlockWithConstants(block, header);
            }
        }
        }
    }
}

OperatorStatus TopNTransformOp::tryOutputImpl(Block & block)
{
    switch (status)
    {
    case TopNStatus::INIT:
    case TopNStatus::ONLY_OUTPUT:
    case TopNStatus::PARTIAL:
        return OperatorStatus::NEED_INPUT;
    case TopNStatus::MERGE:
    case TopNStatus::RESTORE:
    {
        if (likely(impl))
        {
            block = impl->read();
            if (likely(block))
                MergeSortingHelper::enrichBlockWithConstants(block, header);
        }
        return OperatorStatus::HAS_OUTPUT;
    }
    }
}

void TopNTransformOp::transformHeaderImpl(Block & header_)
{
}

} // namespace DB
