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

#include <Common/FmtUtils.h>
#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <Flash/Coprocessor/GenSchemaAndColumn.h>
#include <Flash/Mpp/ExchangeReceiver.h>
#include <Flash/Statistics/ConnectionProfileInfo.h>
#include <Interpreters/Context.h>
#include <Transforms/Source.h>
#include <common/logger_useful.h>

#include <chrono>
#include <thread>
#include <utility>

namespace DB
{
// ExchangeReceiverSource is a source that read/receive data from ExchangeReceiver.
class ExchangeReceiverSource : public Source
{
public:
    ExchangeReceiverSource(std::shared_ptr<ExchangeReceiver> remote_reader_, const String & req_id, const String & executor_id, size_t stream_id_)
        : remote_reader(remote_reader_)
        , source_num(remote_reader->getSourceNum())
        , name(fmt::format("TiRemote({})", ExchangeReceiver::name))
        , log(Logger::get(name, req_id, executor_id))
        , stream_id(stream_id_)
    {
        sample_block = Block(getColumnWithTypeAndName(toNamesAndTypes(remote_reader->getOutputSchema())));
    }

    Block getHeader() const override { return sample_block; }

    void cancel(bool kill) override
    {
        if (kill)
            remote_reader->cancel();
    }
    enum class FetchResult
    {
        finished,
        fetched,
        notFetched
    };
    bool isIOReady() override
    {
        if (done || recv_msg || !block_queue.empty() || !error_msg.empty())
            return true;
        auto fetch_result = fetchRemoteResult();
        switch (fetch_result)
        {
        case FetchResult::finished:
        {
            done = true;
            return true;
        }
        case FetchResult::notFetched:
            return false;
        default:
            return true;
        }
    }
    Block read() override
    {
        if (done)
            return {};
        if (block_queue.empty())
        {
            if (unlikely(!error_msg.empty()))
            {
                LOG_WARNING(log, "remote reader meets error: {}", error_msg);
                throw Exception(error_msg);
            }
            auto result = remote_reader->toDecodeResult(block_queue, sample_block, recv_msg);
            if (result.meet_error)
            {
                LOG_WARNING(log, "remote reader meets error: {}", result.error_msg);
                throw Exception(result.error_msg);
            }
            recv_msg = nullptr;
        }
        // todo should merge some blocks to make sure the output block is big enough
        Block block = block_queue.front();
        block_queue.pop();
        return block;
    }

private:
    FetchResult fetchRemoteResult()
    {
        while (true)
        {
            auto result = remote_reader->asyncReceive(stream_id);
            if (result.meet_error)
            {
                error_msg = result.error_msg;
                return FetchResult::fetched;
            }
            if (result.eof)
                return FetchResult::finished;
            if (result.await)
                return FetchResult::notFetched;
            if (!result.recv_msg->chunks.empty())
            {
                recv_msg = result.recv_msg;
                return FetchResult::fetched;
            }
            // else continue
        }
    }

private:
    std::shared_ptr<ExchangeReceiver> remote_reader;
    size_t source_num;

    Block sample_block;

    std::queue<Block> block_queue;

    String name;

    const LoggerPtr log;

    bool done = false;

    // For fine grained shuffle, sender will partition data into muiltiple streams by hashing.
    // ExchangeReceiverBlockInputStream only need to read its own stream, i.e., streams[stream_id].
    // CoprocessorBlockInputStream doesn't take care of this.
    size_t stream_id;

    std::shared_ptr<ReceivedMessage> recv_msg;
    String error_msg;
};
} // namespace DB
