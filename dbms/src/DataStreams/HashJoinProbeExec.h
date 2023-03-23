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
#include <DataStreams/IBlockInputStream.h>
#include <Interpreters/Join.h>

#include <memory>

#pragma once

namespace DB
{
class HashJoinProbeExec;
using HashJoinProbeExecPtr = std::shared_ptr<HashJoinProbeExec>;

class HashJoinProbeExec
{
public:
    static HashJoinProbeExecPtr build(const JoinPtr & join, const BlockInputStreamPtr & probe_stream, size_t non_joined_stream_index, UInt64 max_block_size);
    static HashJoinProbeExecPtr buildForRestore(const JoinPtr & join, const BlockInputStreamPtr & restore_build_stream, const BlockInputStreamPtr & probe_stream, const BlockInputStreamPtr & non_joined_stream);

    HashJoinProbeExec(
        const JoinPtr & join_,
        const BlockInputStreamPtr & restore_build_stream_,
        const BlockInputStreamPtr & probe_stream_,
        size_t non_joined_stream_index_,
        const BlockInputStreamPtr & non_joined_stream_);

    void cancel();

    void restoreBuild();

    bool needOutputNonJoinedData();

    Block fetchProbeSideBlock();

    Block fetchNonJoinedBlock();

    void meetError(const String & error_message);

    bool isEnableSpill();

    void waitBuildFinish();

    void finishProbe();

    void nonJoinedPrefix();

    void finishNonJoined();

    const JoinPtr & getJoin();

private:
    JoinPtr join;

    // only used by restore build.
    BlockInputStreamPtr restore_build_stream;

    BlockInputStreamPtr probe_stream;

    size_t non_joined_stream_index;
    BlockInputStreamPtr non_joined_stream;
};
} // namespace DB
