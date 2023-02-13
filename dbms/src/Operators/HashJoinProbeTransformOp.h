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
#include <DataStreams/ProbeStatus.h>
#include <Interpreters/Join.h>
#include <Operators/Operator.h>

namespace DB
{
class HashJoinProbeTransformOp : public TransformOp
{
public:
    HashJoinProbeTransformOp(
        PipelineExecutorStatus & exec_status_,
        const JoinPtr & join_ptr_,
        size_t probe_index_,
        UInt64 max_block_size,
        const Block & input_header,
        const String & req_id);

    String getName() const override
    {
        return "HashJoinProbeTransformOp";
    }

protected:
    OperatorStatus transformImpl(Block & block) override;

    OperatorStatus tryOutputImpl(Block & block) override;

    OperatorStatus awaitImpl() override;

    void transformHeaderImpl(Block & header_) override;

private:
    void logOnFinish();

private:
    JoinPtr join_ptr;
    size_t probe_index;
    ProbeProcessInfo probe_process_info;
    BlockInputStreamPtr non_joined_stream;
    const LoggerPtr log;

    ProbeStatus probe_status{ProbeStatus::PROBE};
    size_t joined_rows = 0;
    size_t non_joined_rows = 0;
};
} // namespace DB