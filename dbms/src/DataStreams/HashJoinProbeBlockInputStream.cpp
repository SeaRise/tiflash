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

#include <DataStreams/HashJoinBuildBlockInputStream.h>
#include <DataStreams/HashJoinProbeBlockInputStream.h>
#include <DataStreams/NonJoinedBlockInputStream.h>

namespace DB
{
HashJoinProbeBlockInputStream::HashJoinProbeBlockInputStream(
    const BlockInputStreamPtr & input,
    const JoinPtr & join_,
    size_t non_joined_stream_index,
    const String & req_id,
    UInt64 max_block_size_)
    : log(Logger::get(req_id))
    , original_join(join_)
    , probe_exec(HashJoinProbeExec::build(original_join, input, non_joined_stream_index, max_block_size_))
    , max_block_size(max_block_size_)
    , probe_process_info(max_block_size_)
{
    children.push_back(input);
    RUNTIME_CHECK_MSG(original_join != nullptr, "join ptr should not be null.");
    RUNTIME_CHECK_MSG(original_join->getProbeConcurrency() > 0, "Join probe concurrency must be greater than 0");
}

Block HashJoinProbeBlockInputStream::getHeader() const
{
    Block res = children.back()->getHeader();
    assert(res.rows() == 0);
    ProbeProcessInfo header_probe_process_info(0);
    header_probe_process_info.resetBlock(std::move(res));
    /// use original_join here so we don't need add lock
    return original_join->joinBlock(header_probe_process_info);
}

void HashJoinProbeBlockInputStream::cancel(bool kill)
{
    IProfilingBlockInputStream::cancel(kill);
    /// When the probe stream quits probe by cancelling instead of normal finish, the Join operator might still produce meaningless blocks
    /// and expects these meaningless blocks won't be used to produce meaningful result.

    HashJoinProbeExecPtr current_probe_exec;
    {
        std::lock_guard lock(mutex);
        current_probe_exec = probe_exec;
    }
    /// Cancel join just wake up all the threads waiting in Join::waitUntilAllBuildFinished/Join::waitUntilAllProbeFinished,
    /// the ongoing join process will not be interrupted
    /// There is a little bit hack here because cancel will be called in two cases:
    /// 1. the query is cancelled by the caller or meet error: in this case, wake up all waiting threads is safe
    /// 2. the query is executed normally, and one of the data stream has read an empty block, the the data stream and all its children
    ///    will call `cancel(false)`, in this case, there is two sub-cases
    ///    a. the data stream read an empty block because of EOF, then it means there must be no threads waiting in Join, so cancel the join is safe
    ///    b. the data stream read an empty block because of early exit of some executor(like limit), in this case, just wake the waiting
    ///       threads is not 100% safe because if the probe thread is wake up when build is not finished yet, it may produce wrong results, for now
    ///       it is safe because when any of the data stream read empty block because of early exit, the execution framework ensures that no further
    ///       data will be used.
    current_probe_exec->cancel();
}

Block HashJoinProbeBlockInputStream::readImpl()
{
    try
    {
        Block ret = getOutputBlock();
        return ret;
    }
    catch (...)
    {
        auto error_message = getCurrentExceptionMessage(false, true);
        probe_exec->meetError(error_message);
        throw Exception(error_message);
    }
}

void HashJoinProbeBlockInputStream::readSuffixImpl()
{
    LOG_DEBUG(log, "Finish join probe, total output rows {}, joined rows {}, non joined rows {}", joined_rows + non_joined_rows, joined_rows, non_joined_rows);
}

void HashJoinProbeBlockInputStream::onCurrentProbeDone()
{
    probe_exec->finishProbe();
    if (probe_exec->needOutputNonJoinedData() || probe_exec->isEnableSpill())
    {
        status = ProbeStatus::WAIT_PROBE_FINISH;
    }
    else
    {
        status = ProbeStatus::FINISHED;
    }
}

void HashJoinProbeBlockInputStream::onCurrentReadNonJoinedDataDone()
{
    probe_exec->finishNonJoined();
    status = probe_exec->isEnableSpill() ? ProbeStatus::FINISHED : ProbeStatus::GET_RESTORE_JOIN;
}

void HashJoinProbeBlockInputStream::tryGetRestoreJoin()
{
    /// find restore join in DFS way
    while (true)
    {
        assert(probe_exec->isEnableSpill());
        /// first check if current join has a partition to restore
        if (probe_exec->getJoin()->hasPartitionSpilledWithLock())
        {
            auto restore_info = probe_exec->getJoin()->getOneRestoreStream(max_block_size);
            /// get a restore join
            if (restore_info.join)
            {
                /// restored join should always enable spill
                assert(restore_info.join->isEnableSpill());
                parents.push_back(probe_exec);
                {
                    std::lock_guard lock(mutex);
                    if (isCancelledOrThrowIfKilled())
                    {
                        status = ProbeStatus::FINISHED;
                        return;
                    }
                    probe_exec = HashJoinProbeExec::buildForRestore(restore_info.join, restore_info.build_stream, restore_info.probe_stream, restore_info.non_joined_stream);
                }
                status = ProbeStatus::RESTORE_BUILD;
                return;
            }
            assert(probe_exec->getJoin()->hasPartitionSpilledWithLock() == false);
        }
        /// current join has no more partition to restore, so check if previous join still has partition to restore
        if (!parents.empty())
        {
            /// replace current join with previous join
            std::lock_guard lock(mutex);
            if (isCancelledOrThrowIfKilled())
            {
                status = ProbeStatus::FINISHED;
                return;
            }
            else
            {
                probe_exec = parents.back();
                parents.pop_back();
            }
        }
        else
        {
            /// no previous join, set status to FINISHED
            status = ProbeStatus::FINISHED;
            return;
        }
    }
}

void HashJoinProbeBlockInputStream::onAllProbeDone()
{
    if (probe_exec->needOutputNonJoinedData())
    {
        status = ProbeStatus::READ_NON_JOINED_DATA;
        probe_exec->nonJoinedPrefix();
    }
    else
    {
        status = ProbeStatus::GET_RESTORE_JOIN;
    }
}

Block HashJoinProbeBlockInputStream::getOutputBlock()
{
    try
    {
        while (true)
        {
            switch (status)
            {
            case ProbeStatus::WAIT_BUILD_FINISH:
                probe_exec->waitBuildFinish();
                status = ProbeStatus::PROBE;
                break;
            case ProbeStatus::PROBE:
            {
                if (probe_process_info.all_rows_joined_finish)
                {
                    auto [partition_index, block] = getOneProbeBlock();
                    if (!block)
                    {
                        onCurrentProbeDone();
                        break;
                    }
                    else
                    {
                        probe_exec->getJoin()->checkTypes(block);
                        probe_process_info.resetBlock(std::move(block), partition_index);
                    }
                }
                auto ret = probe_exec->getJoin()->joinBlock(probe_process_info);
                joined_rows += ret.rows();
                return ret;
            }
            case ProbeStatus::WAIT_PROBE_FINISH:
                probe_exec->getJoin()->waitUntilAllProbeFinished();
                onAllProbeDone();
                break;
            case ProbeStatus::READ_NON_JOINED_DATA:
            {
                auto block = probe_exec->fetchNonJoinedBlock();
                non_joined_rows += block.rows();
                if (!block)
                {
                    onCurrentReadNonJoinedDataDone();
                    break;
                }
                return block;
            }
            case ProbeStatus::GET_RESTORE_JOIN:
            {
                tryGetRestoreJoin();
                break;
            }
            case ProbeStatus::RESTORE_BUILD:
            {
                probe_process_info.all_rows_joined_finish = true;
                probe_exec->restoreBuild();
                status = ProbeStatus::WAIT_BUILD_FINISH;
                break;
            }
            case ProbeStatus::FINISHED:
                return {};
            }
        }
    }
    catch (...)
    {
        /// set status to finish if any exception happens
        status = ProbeStatus::FINISHED;
        throw;
    }
}

std::tuple<size_t, Block> HashJoinProbeBlockInputStream::getOneProbeBlock()
{
    size_t partition_index = 0;
    Block block;

    /// Even if spill is enabled, if spill is not triggered during build,
    /// there is no need to dispatch probe block
    if (!probe_exec->getJoin()->isSpilled())
    {
        probe_exec->fetchProbeSideBlock();
    }
    else
    {
        while (true)
        {
            if (!probe_partition_blocks.empty())
            {
                auto partition_block = probe_partition_blocks.front();
                probe_partition_blocks.pop_front();
                partition_index = std::get<0>(partition_block);
                block = std::get<1>(partition_block);
                break;
            }
            else
            {
                auto new_block = probe_exec->fetchProbeSideBlock();
                if (new_block)
                    probe_exec->getJoin()->dispatchProbeBlock(new_block, probe_partition_blocks);
                else
                    break;
            }
        }
    }
    return {partition_index, block};
}

} // namespace DB
