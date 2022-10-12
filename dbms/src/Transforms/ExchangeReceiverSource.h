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
    ExchangeReceiverSource(std::shared_ptr<ExchangeReceiver> remote_reader_, const String & req_id, const String & executor_id)
        : remote_reader(remote_reader_)
        , source_num(remote_reader->getSourceNum())
        , log(Logger::get("ExchangeReceiverSource", req_id, executor_id))
    {
        sample_block = Block(getColumnWithTypeAndName(toNamesAndTypes(remote_reader->getOutputSchema())));
    }

    Block getHeader() const override { return sample_block; }

    void cancel(bool kill) override
    {
        if (kill)
            remote_reader->cancel();
    }

    bool isIOReady() override
    {
        if (done || !block_queue.empty() || recv_msg)
            return true;

        while (true)
        {
            assert(!recv_msg);
            if (!remote_reader->asyncReceive(recv_msg))
            {
                assert(!recv_msg);
                return false;
            }
            else
            {
                if (recv_msg)
                {
                    if (!recv_msg->chunks.empty())
                        return true;
                    else
                    {
                        recv_msg = nullptr;
                        continue;
                    }
                }
                else
                {
                    done = true;
                    return true;
                }
            }
        }
    }

    Block read() override
    {
        if (done)
            return {};
        if (block_queue.empty())
        {
            assert(recv_msg);
            auto result = remote_reader->toDecodeResult(block_queue, sample_block, recv_msg);
            recv_msg = nullptr;
            if (result.meet_error)
            {
                LOG_WARNING(log, "remote reader meets error: {}", result.error_msg);
                throw Exception(result.error_msg);
            }
        }
        // todo should merge some blocks to make sure the output block is big enough
        Block block = std::move(block_queue.front());
        block_queue.pop();
        return block;
    }

private:
    std::shared_ptr<ExchangeReceiver> remote_reader;
    size_t source_num;

    Block sample_block;

    std::queue<Block> block_queue;

    const LoggerPtr log;

    bool done = false;

    std::shared_ptr<ReceivedMessage> recv_msg;
};
} // namespace DB
