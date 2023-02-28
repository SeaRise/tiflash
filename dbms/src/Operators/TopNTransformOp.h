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

#pragma once

#include <Common/Logger.h>
#include <Core/SortDescription.h>
#include <DataStreams/IBlockInputStream.h>
#include <Operators/Operator.h>
#include <Core/Spiller.h>

namespace DB
{
class TopNStatus
{
    INIT,
    ONLY_LIMIT_OUTPUT,
    PARTIAL,
    MERGE,
    SPILL,
    RESTORE,
}

class TopNTransformOp : public TransformOp
{
public:
    TopNTransformOp(
        PipelineExecutorStatus & exec_status_,
        const SortDescription & order_desc_,
        size_t limit_,
        size_t max_block_size_,
        size_t max_bytes_before_external_sort_,
        const SpillConfig & spill_config_,
        const String & req_id_);

    String getName() const override
    {
        return "TopNTransformOp";
    }

protected:
    OperatorStatus transformImpl(Block & block) override;
    OperatorStatus tryOutputImpl(Block & block) override;

    void transformHeaderImpl(Block & header_) override;

private:
    const LoggerPtr log;

    SortDescription order_desc;
    size_t limit;
    size_t max_block_size;

    size_t max_bytes_before_external_sort;
    const SpillConfig spill_config;

    Block header_without_constants;

    Blocks blocks;
    size_t sum_bytes_in_blocks = 0;

    /// Everything below is for external sorting.
    std::unique_ptr<Spiller> spiller;

    std::unique_ptr<IBlockInputStream> impl;

    TopNStatus status{INIT};
};
} // namespace DB
