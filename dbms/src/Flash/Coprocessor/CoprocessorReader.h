#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Flash/Coprocessor/ArrowChunkCodec.h>
#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <Flash/Coprocessor/DecodeChunksDetail.h>
#include <Flash/Coprocessor/DefaultChunkCodec.h>
#include <Flash/Statistics/CoprocessorReadProfileInfo.h>
#include <Interpreters/Context.h>
#include <Storages/Transaction/TMTContext.h>
#include <common/logger_useful.h>

#include <chrono>
#include <mutex>
#include <thread>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <kvproto/mpp.pb.h>
#include <pingcap/coprocessor/Client.h>
#include <pingcap/kv/Rpc.h>
#include <tipb/executor.pb.h>
#include <tipb/select.pb.h>

#pragma GCC diagnostic pop


namespace DB
{
struct CoprocessorReaderResult
{
    std::shared_ptr<tipb::SelectResponse> resp;
    bool meet_error;
    String error_msg;
    bool eof;
    String req_info = "cop request";
    UInt64 rows;
    UInt64 packet_bytes;

    CoprocessorReaderResult(
        std::shared_ptr<tipb::SelectResponse> resp_,
        bool meet_error_ = false,
        const String & error_msg_ = "",
        bool eof_ = false,
        UInt64 rows_ = 0,
        UInt64 packet_bytes_ = 0)
        : resp(resp_)
        , meet_error(meet_error_)
        , error_msg(error_msg_)
        , eof(eof_)
        , rows(rows_)
        , packet_bytes(packet_bytes_)
    {}
};

/// this is an adapter for pingcap::coprocessor::ResponseIter, so it can be used in TiRemoteBlockInputStream
class CoprocessorReader
{
public:
    static constexpr bool is_streaming_reader = false;

private:
    DAGSchema schema;
    bool has_enforce_encode_type;
    std::vector<CoprocessorTaskInfo> cop_task_infos;
    pingcap::coprocessor::ResponseIter resp_iter;

public:
    CoprocessorReader(
        const DAGSchema & schema_,
        pingcap::kv::Cluster * cluster,
        std::vector<pingcap::coprocessor::copTask> tasks,
        bool has_enforce_encode_type_,
        int concurrency)
        : schema(schema_)
        , has_enforce_encode_type(has_enforce_encode_type_)
        , cop_task_infos(tasks.cbegin(), tasks.cend())
        , resp_iter(std::move(tasks), cluster, concurrency, &Poco::Logger::get("pingcap/coprocessor"))
    {
        resp_iter.open();
    }

    std::vector<ConnectionProfileInfoPtr> createConnectionProfileInfos() const
    {
        return {std::make_shared<CoprocessorReadProfileInfo>(cop_task_infos)};
    }

    const DAGSchema & getOutputSchema() const { return schema; }

    void cancel() { resp_iter.cancel(); }

    static DecodeChunksDetail decodeChunks(std::shared_ptr<tipb::SelectResponse> & resp, std::queue<Block> & block_queue, const DataTypes & expected_types, const DAGSchema & schema)
    {
        DecodeChunksDetail detail;
        int chunk_size = resp->chunks_size();
        if (chunk_size == 0)
            return detail;

        detail.packet_bytes = resp->ByteSizeLong();
        for (int i = 0; i < chunk_size; i++)
        {
            Block block;
            const tipb::Chunk & chunk = resp->chunks(i);
            switch (resp->encode_type())
            {
            case tipb::EncodeType::TypeCHBlock:
                block = CHBlockChunkCodec().decode(chunk.rows_data(), schema);
                break;
            case tipb::EncodeType::TypeChunk:
                block = ArrowChunkCodec().decode(chunk.rows_data(), schema);
                break;
            case tipb::EncodeType::TypeDefault:
                block = DefaultChunkCodec().decode(chunk.rows_data(), schema);
                break;
            default:
                throw Exception("Unsupported encode type", ErrorCodes::LOGICAL_ERROR);
            }

            detail.rows += block.rows();

            if (unlikely(block.rows() == 0))
                continue;
            assertBlockSchema(expected_types, block, "CoprocessorReader decode chunks");
            block_queue.push(std::move(block));
        }
        return detail;
    }
    CoprocessorReaderResult nextResult(std::queue<Block> & block_queue, const DataTypes & expected_types)
    {
        auto && [result, has_next] = resp_iter.next();
        if (!result.error.empty())
        {
            return {nullptr, true, result.error.message(), false};
        }
        if (!has_next)
        {
            return {nullptr, false, "", true};
        }
        const std::string & data = result.data();
        std::shared_ptr<tipb::SelectResponse> resp = std::make_shared<tipb::SelectResponse>();
        if (resp->ParseFromString(data))
        {
            if (has_enforce_encode_type && resp->encode_type() != tipb::EncodeType::TypeCHBlock)
                return {nullptr, true, "Encode type of coprocessor response is not CHBlock, "
                                       "maybe the version of some TiFlash node in the cluster is not match with this one",
                        false};
            auto detail = decodeChunks(resp, block_queue, expected_types, schema);
            return {resp, false, "", false, detail.rows, detail.packet_bytes};
        }
        else
        {
            return {nullptr, true, "Error while decoding coprocessor::Response", false};
        }
    }

    size_t getSourceNum() { return 1; }
    String getName() { return "CoprocessorReader"; }
};
} // namespace DB
