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

#include <Operators/HashJoinProbeTransformOp.h>

#include <magic_enum.hpp>

namespace DB
{
HashJoinProbeTransformOp::HashJoinProbeTransformOp(
    PipelineExecutorStatus & exec_status_,
    const JoinPtr & join_ptr_,
    size_t probe_index_,
    UInt64 max_block_size,
    const Block & input_header,
    const String & req_id)
    : TransformOp(exec_status_)
    , join_ptr(join_ptr_)
    , probe_index(probe_index_)
    , probe_process_info(max_block_size)
    , log(Logger::get(req_id))
{
    RUNTIME_CHECK_MSG(join_ptr != nullptr, "join ptr should not be null.");
    RUNTIME_CHECK_MSG(join_ptr->getProbeConcurrency() > 0, "Join probe concurrency must be greater than 0");
    if (join_ptr->needReturnNonJoinedData())
        non_joined_stream = join_ptr->createStreamWithNonJoinedRows(input_header, probe_index, join_ptr->getProbeConcurrency(), max_block_size);
}

OperatorStatus HashJoinProbeTransformOp::transformImpl(Block & block)
{
    switch (probe_status)
    {
    case ProbeStatus::PROBE:
    {
        assert(probe_process_info.all_rows_joined_finish);
        if (!block)
        {
            join_ptr->finishOneProbe();
            if (join_ptr->needReturnNonJoinedData())
            {
                if (!join_ptr->isAllProbeFinished())
                {
                    probe_status = ProbeStatus::WAIT_FOR_READ_NON_JOINED_DATA;
                    return OperatorStatus::WAITING;
                }
                assert(non_joined_stream);
                probe_status = ProbeStatus::READ_NON_JOINED_DATA;
                non_joined_stream->readPrefix();
            }
            else
            {
                probe_status = ProbeStatus::FINISHED;
                return OperatorStatus::HAS_OUTPUT;
            }
        }
        else
        {
            join_ptr->checkTypes(block);
            probe_process_info.resetBlock(std::move(block));
            block.clear();
        }
        assert(!block);
        return tryOutputImpl(block);
    }
    default:
        // probe status can only be PROBE here.
        RUNTIME_ASSERT(false, "Unexpected probe status: {} in transform", magic_enum::enum_name(probe_status));
    }
}

OperatorStatus HashJoinProbeTransformOp::tryOutputImpl(Block & block)
{
    switch (probe_status)
    {
    case ProbeStatus::PROBE:
    {
        if (probe_process_info.all_rows_joined_finish)
            return OperatorStatus::NEED_INPUT;
        block = join_ptr->joinBlock(probe_process_info);
        return OperatorStatus::HAS_OUTPUT;
    }
    case ProbeStatus::READ_NON_JOINED_DATA:
    {
        assert(non_joined_stream);
        block = non_joined_stream->read();
        if (!block)
        {
            non_joined_stream->readSuffix();
            probe_status = ProbeStatus::FINISHED;
        }
        return OperatorStatus::HAS_OUTPUT;
    }
    case ProbeStatus::FINISHED:
        return OperatorStatus::HAS_OUTPUT;
    default:
        // probe status can not be WAIT_FOR_READ_NON_JOINED_DATA here.
        RUNTIME_ASSERT(false, "Unexpected probe status: {} in tryOutput", magic_enum::enum_name(probe_status));
    }
}

OperatorStatus HashJoinProbeTransformOp::awaitImpl()
{
    switch (probe_status)
    {
    case ProbeStatus::PROBE:
        return OperatorStatus::NEED_INPUT;
    case ProbeStatus::WAIT_FOR_READ_NON_JOINED_DATA:
    {
        if (join_ptr->isAllProbeFinished())
        {
            probe_status = ProbeStatus::READ_NON_JOINED_DATA;
            return OperatorStatus::HAS_OUTPUT;
        }
        return OperatorStatus::WAITING;
    }
    default:
        // probe status can not be READ_NON_JOINED_DATA/FINISHED here.
        RUNTIME_ASSERT(false, "Unexpected probe status: {} in await", magic_enum::enum_name(probe_status));
    }
}

void HashJoinProbeTransformOp::transformHeaderImpl(Block & header_)
{
    assert(header_.rows() == 0);
    ProbeProcessInfo header_probe_process_info(0);
    header_probe_process_info.resetBlock(std::move(header_));
    header_ = join_ptr->joinBlock(header_probe_process_info);
}
} // namespace DB
