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

#include <DataStreams/ExpressionBlockInputStream.h>
#include <DataStreams/SharedQueryBlockInputStream.h>
#include <DataStreams/UnionBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/InterpreterUtils.h>

namespace DB
{
namespace
{
using UnionWithBlock = UnionBlockInputStream<>;
using UnionWithoutBlock = UnionBlockInputStream<StreamUnionMode::Basic, /*ignore_block=*/true>;
} // namespace

void restoreConcurrency(
    DAGPipeline & pipeline,
    size_t concurrency,
    const LoggerPtr & log)
{
    if (concurrency > 1 && pipeline.streams.size() == 1 && pipeline.streams_with_non_joined_data.empty())
    {
        BlockInputStreamPtr shared_query_block_input_stream
            = std::make_shared<SharedQueryBlockInputStream>(concurrency * 5, pipeline.firstStream(), log->identifier());
        pipeline.streams.assign(concurrency, shared_query_block_input_stream);
    }
}

BlockInputStreamPtr combinedNonJoinedDataStream(
    DAGPipeline & pipeline,
    size_t max_threads,
    const LoggerPtr & log,
    bool ignore_block)
{
    BlockInputStreamPtr ret = nullptr;
    if (pipeline.streams_with_non_joined_data.size() == 1)
        ret = pipeline.streams_with_non_joined_data.at(0);
    else if (pipeline.streams_with_non_joined_data.size() > 1)
    {
        if (ignore_block)
            ret = std::make_shared<UnionWithoutBlock>(pipeline.streams_with_non_joined_data, nullptr, max_threads, log->identifier());
        else
            ret = std::make_shared<UnionWithBlock>(pipeline.streams_with_non_joined_data, nullptr, max_threads, log->identifier());
    }
    pipeline.streams_with_non_joined_data.clear();
    return ret;
}

void executeUnion(
    DAGPipeline & pipeline,
    size_t max_streams,
    const LoggerPtr & log,
    bool ignore_block)
{
    if (pipeline.streams.size() == 1 && pipeline.streams_with_non_joined_data.empty())
        return;
    auto non_joined_data_stream = combinedNonJoinedDataStream(pipeline, max_streams, log, ignore_block);
    if (!pipeline.streams.empty())
    {
        if (ignore_block)
            pipeline.firstStream() = std::make_shared<UnionWithoutBlock>(pipeline.streams, non_joined_data_stream, max_streams, log->identifier());
        else
            pipeline.firstStream() = std::make_shared<UnionWithBlock>(pipeline.streams, non_joined_data_stream, max_streams, log->identifier());
        pipeline.streams.resize(1);
    }
    else if (non_joined_data_stream != nullptr)
    {
        pipeline.streams.push_back(non_joined_data_stream);
    }
}

void executeExpression(
    DAGPipeline & pipeline,
    const ExpressionActionsPtr & expressionActionsPtr,
    const LoggerPtr & log)
{
    if (!expressionActionsPtr->getActions().empty())
    {
        pipeline.transform([&](auto & stream) { stream = std::make_shared<ExpressionBlockInputStream>(stream, expressionActionsPtr, log->identifier()); });
    }
}

void updateFinalConcurrency(
    DAGContext & dag_context,
    size_t cur_streams_size,
    size_t max_streams)
{
    dag_context.final_concurrency = std::min(std::max(dag_context.final_concurrency, cur_streams_size), max_streams);
}
} // namespace DB
