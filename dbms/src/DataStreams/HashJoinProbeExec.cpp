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

#include <DataStreams/NonJoinedBlockInputStream.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/HashJoinProbeExec.h>

namespace DB
{
HashJoinProbeExecPtr HashJoinProbeExec::build(const JoinPtr & join, const BlockInputStreamPtr & probe_stream, size_t non_joined_stream_index, UInt64 max_block_size)
{
    assert(!join->isRestoreJoin());
    assert(probe_stream);
    BlockInputStreamPtr non_joined_stream;
    if (join->needReturnNonJoinedData())
        non_joined_stream = join->createStreamWithNonJoinedRows(probe_stream->getHeader(), non_joined_stream_index, join->getProbeConcurrency(), max_block_size);
    return std::make_shared<HashJoinProbeExec>(join, nullptr, probe_stream, non_joined_stream_index, non_joined_stream);
}

HashJoinProbeExecPtr HashJoinProbeExec::buildForRestore(const JoinPtr & join, const BlockInputStreamPtr & restore_build_stream, const BlockInputStreamPtr & probe_stream, const BlockInputStreamPtr & non_joined_stream)
{
    assert(join);
    assert(join->isRestoreJoin());
    assert(restore_build_stream);
    assert(probe_stream);
    size_t non_joined_stream_index = 0;
    if (non_joined_stream)
        non_joined_stream_index = dynamic_cast<NonJoinedBlockInputStream *>(non_joined_stream.get())->getNonJoinedIndex();
    return std::make_shared<HashJoinProbeExec>(join, restore_build_stream, probe_stream, non_joined_stream_index, non_joined_stream);
}

HashJoinProbeExec::HashJoinProbeExec(
    const JoinPtr & join_,
    const BlockInputStreamPtr & restore_build_stream_,
    const BlockInputStreamPtr & probe_stream_,
    size_t non_joined_stream_index_,
    const BlockInputStreamPtr & non_joined_stream_)
    : join(join_)
    , restore_build_stream(restore_build_stream_)
    , probe_stream(probe_stream_)
    , non_joined_stream_index(non_joined_stream_index_)
    , non_joined_stream(non_joined_stream_)
{
}

void HashJoinProbeExec::cancel()
{
    assert(join);
    join->cancel();
    if (restore_build_stream != nullptr)
    {
        if (auto * p_stream = dynamic_cast<IProfilingBlockInputStream *>(restore_build_stream.get()); p_stream != nullptr)
            p_stream->cancel(false);
    }
    // probe_stream cannot be nullptr.
    assert(probe_stream);
    if (auto * p_stream = dynamic_cast<IProfilingBlockInputStream *>(probe_stream.get()); p_stream != nullptr)
        p_stream->cancel(false);
    if (non_joined_stream != nullptr)
    {
        if (auto * p_stream = dynamic_cast<IProfilingBlockInputStream *>(non_joined_stream.get()); p_stream != nullptr)
            p_stream->cancel(false);
    }
}

void HashJoinProbeExec::restoreBuild()
{
    assert(restore_build_stream);
    restore_build_stream->readPrefix();
    while (restore_build_stream->read()) {};
    restore_build_stream->readSuffix();
}

bool HashJoinProbeExec::needOutputNonJoinedData()
{
    return non_joined_stream != nullptr;
}

Block HashJoinProbeExec::fetchProbeSideBlock()
{
    assert(probe_stream);
    return probe_stream->read();
}

Block HashJoinProbeExec::fetchNonJoinedBlock()
{
    assert(non_joined_stream);
    return non_joined_stream->read();
}

void HashJoinProbeExec::meetError(const String & error_message)
{
    assert(join);
    join->meetError(error_message);
}

bool HashJoinProbeExec::isEnableSpill()
{
    assert(join);
    return join->isEnableSpill();
}

void HashJoinProbeExec::waitBuildFinish()
{
    join->waitUntilAllBuildFinished();
    /// after Build finish, always go to Probe stage
    if (join->isRestoreJoin())
        probe_stream->readSuffix();
}

void HashJoinProbeExec::finishProbe()
{
    if (join->isRestoreJoin())
        probe_stream->readSuffix();
    join->finishOneProbe();
}

void HashJoinProbeExec::nonJoinedPrefix()
{
    assert(non_joined_stream);
    non_joined_stream->readPrefix();
}

void HashJoinProbeExec::finishNonJoined()
{
    assert(non_joined_stream);
    non_joined_stream->readSuffix();
    if (join->isEnableSpill())
        join->finishOneNonJoin(non_joined_stream_index);
}

const JoinPtr & HashJoinProbeExec::getJoin()
{
    assert(join);
    return join;
}
}
