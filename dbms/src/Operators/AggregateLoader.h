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
#include <DataStreams/IBlockInputStream.h>

#include <memory>

namespace DB
{
class PipelineExecutorStatus;

class AggregateLoader : public std::enable_shared_from_this<AggregateLoader>
{
public:
    class Input
    {
    public:
        explicit Input(const BlockInputStreamPtr & stream_);

        bool hasOutput() const;
        Block moveOutput();
        Int32 getOutputBucketNum() const;

        bool finished() const;

        bool load();

    private:
        BlockInputStreamPtr stream;
        std::optional<Block> output;
        bool is_exhausted = false;
    };
    using InputPtr = std::shared_ptr<Input>;
    using Inputs = std::vector<InputPtr>;

    AggregateLoader(
        const BlockInputStreams & load_streams,
        const Block & header_,
        PipelineExecutorStatus & exec_status_,
        const String & req_id);

    bool tryGet(BlocksList & result);

private:
    void onLoadingDone();

    bool onValid(BlocksList & result);

    void onIdle();

private:
    Block header;

    PipelineExecutorStatus & exec_status;

    LoggerPtr log;

    enum class LoadStatus
    {
        idle,
        loading,
        valid,
        finished,
    };
    std::atomic<LoadStatus> status{LoadStatus::idle};

    std::mutex result_mu;
    std::vector<BlocksList> loaded_result;

    Inputs loading_inputs;

    static constexpr Int32 NUM_BUCKETS = 256;
    std::atomic_int32_t current_bucket_num = 0;
};
using AggregateLoaderPtr = std::shared_ptr<AggregateLoader>;
} // namespace DB
