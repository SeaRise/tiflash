#include <DataStreams/IProfilingBlockInputStream.h>
#include <Flash/Coprocessor/ArrowChunkCodec.h>
#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <Flash/Coprocessor/DAGBlockOutputStream.h>
#include <Flash/Coprocessor/DefaultChunkCodec.h>

namespace DB
{

namespace ErrorCodes
{
extern const int UNSUPPORTED_PARAMETER;
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

template <>
DAGBlockOutputStream<false>::DAGBlockOutputStream(tipb::SelectResponse * dag_response_, Int64 records_per_chunk_,
    tipb::EncodeType encode_type_, std::vector<tipb::FieldType> result_field_types_, Block && header_, DAGContext & dag_context_,
    bool collect_execute_summary_)
    : dag_response(dag_response_),
      result_field_types(std::move(result_field_types_)),
      header(std::move(header_)),
      records_per_chunk(records_per_chunk_),
      encode_type(encode_type_),
      current_records_num(0),
      dag_context(dag_context_),
      collect_execute_summary(collect_execute_summary_)
{
    previous_execute_stats.resize(dag_context.profile_streams_map.size(), std::make_tuple(0, 0, 0));
    if (encode_type == tipb::EncodeType::TypeDefault)
    {
        chunk_codec_stream = std::make_unique<DefaultChunkCodec>()->newCodecStream(result_field_types);
    }
    else if (encode_type == tipb::EncodeType::TypeChunk)
    {
        chunk_codec_stream = std::make_unique<ArrowChunkCodec>()->newCodecStream(result_field_types);
    }
    else if (encode_type == tipb::EncodeType::TypeCHBlock)
    {
        chunk_codec_stream = std::make_unique<CHBlockChunkCodec>()->newCodecStream(result_field_types);
        records_per_chunk = -1;
    }
    else
    {
        throw Exception("Only Default and Arrow encode type is supported in DAGBlockOutputStream.", ErrorCodes::UNSUPPORTED_PARAMETER);
    }
    dag_response->set_encode_type(encode_type);
}

template <>
DAGBlockOutputStream<true>::DAGBlockOutputStream(BlockInputStreamPtr input_, StreamWriterPtr writer_, Int64 records_per_chunk_,
    tipb::EncodeType encode_type_, std::vector<tipb::FieldType> result_field_types_, Block && header_, DAGContext & dag_context_,
    bool collect_execute_summary_)
    : finished(false),
      writer(writer_),
      result_field_types(std::move(result_field_types_)),
      header(std::move(header_)),
      records_per_chunk(records_per_chunk_),
      encode_type(encode_type_),
      current_records_num(0),
      dag_context(dag_context_),
      collect_execute_summary(collect_execute_summary_)
{
    previous_execute_stats.resize(dag_context.profile_streams_map.size(), std::make_tuple(0, 0, 0));
    if (encode_type == tipb::EncodeType::TypeDefault)
    {
        chunk_codec_stream = std::make_unique<DefaultChunkCodec>()->newCodecStream(result_field_types);
    }
    else if (encode_type == tipb::EncodeType::TypeChunk)
    {
        chunk_codec_stream = std::make_unique<ArrowChunkCodec>()->newCodecStream(result_field_types);
    }
    else if (encode_type == tipb::EncodeType::TypeCHBlock)
    {
        chunk_codec_stream = std::make_unique<CHBlockChunkCodec>()->newCodecStream(result_field_types);
        records_per_chunk = -1;
    }
    else
    {
        throw Exception(
            "Only Default and Arrow encode type is supported in StreamingDAGBlockOutputStream.", ErrorCodes::UNSUPPORTED_PARAMETER);
    }
    children.push_back(input_);
}

template <bool streaming>
void DAGBlockOutputStream<streaming>::writePrefix()
{
    //something to do here?
}

template <>
void DAGBlockOutputStream<true>::readPrefix()
{
    children.back()->readPrefix();
}

template <>
void DAGBlockOutputStream<false>::encodeChunkToDAGResponse()
{
    auto dag_chunk = dag_response->add_chunks();
    dag_chunk->set_rows_data(chunk_codec_stream->getString());
    chunk_codec_stream->clear();
    current_records_num = 0;
}

template <>
Block DAGBlockOutputStream<true>::readImpl()
{
    if (finished)
        return {};
    while (Block block = children.back()->read())
    {
        if (!block)
        {
            finished = true;
            return {};
        }
        write(block);
    }
    return {};
}

template <bool streaming>
void DAGBlockOutputStream<streaming>::addExecuteSummaries(tipb::SelectResponse * response)
{
    if (!collect_execute_summary)
        return;
    // add ExecutorExecutionSummary info
    for (auto & p : dag_context.profile_streams_map)
    {
        auto * executeSummary = response->add_execution_summaries();
        UInt64 time_processed_ns = 0;
        UInt64 num_produced_rows = 0;
        UInt64 num_iterations = 0;
        for (auto & streamPtr : p.second.input_streams)
        {
            if (auto * p_stream = dynamic_cast<IProfilingBlockInputStream *>(streamPtr.get()))
            {
                time_processed_ns = std::max(time_processed_ns, p_stream->getProfileInfo().execution_time);
                num_produced_rows += p_stream->getProfileInfo().rows;
                num_iterations += p_stream->getProfileInfo().blocks;
            }
        }
        for (auto & join_alias : dag_context.qb_id_to_join_alias_map[p.second.qb_id])
        {
            if (dag_context.profile_streams_map_for_join_build_side.find(join_alias)
                != dag_context.profile_streams_map_for_join_build_side.end())
            {
                UInt64 process_time_for_build = 0;
                for (auto & join_stream : dag_context.profile_streams_map_for_join_build_side[join_alias])
                {
                    if (auto * p_stream = dynamic_cast<IProfilingBlockInputStream *>(join_stream.get()))
                        process_time_for_build = std::max(process_time_for_build, p_stream->getProfileInfo().execution_time);
                }
                time_processed_ns += process_time_for_build;
            }
        }
        executeSummary->set_time_processed_ns(time_processed_ns);
        executeSummary->set_num_produced_rows(num_produced_rows);
        executeSummary->set_num_iterations(num_iterations);
        if constexpr (streaming)
            executeSummary->set_executor_id(p.first);
    }
}

template <>
void DAGBlockOutputStream<true>::encodeChunkToDAGResponse()
{
    ::coprocessor::BatchResponse resp;

    tipb::SelectResponse stream_dag_response;
    stream_dag_response.set_encode_type(encode_type);
    auto dag_chunk = stream_dag_response.add_chunks();
    dag_chunk->set_rows_data(chunk_codec_stream->getString());
    chunk_codec_stream->clear();
    current_records_num = 0;
    addExecuteSummaries(&stream_dag_response);
    std::string dag_data;
    stream_dag_response.SerializeToString(&dag_data);
    resp.set_data(dag_data);

    writer->write(resp);
}

template <bool streaming>
void DAGBlockOutputStream<streaming>::writeSuffix()
{
    // todo error handle
    if (current_records_num > 0)
    {
        encodeChunkToDAGResponse();
    }
    if constexpr (!streaming)
    {
        addExecuteSummaries(dag_response);
    }
}

template <bool streaming>
void DAGBlockOutputStream<streaming>::write(const Block & block)
{
    if (block.columns() != result_field_types.size())
        throw Exception("Output column size mismatch with field type size", ErrorCodes::LOGICAL_ERROR);
    if (records_per_chunk == -1)
    {
        current_records_num = 0;
        if (block.rows() > 0)
        {
            chunk_codec_stream->encode(block, 0, block.rows());
            encodeChunkToDAGResponse();
        }
    }
    else
    {
        size_t rows = block.rows();
        for (size_t row_index = 0; row_index < rows;)
        {
            if (current_records_num >= records_per_chunk)
            {
                encodeChunkToDAGResponse();
            }
            const size_t upper = std::min(row_index + (records_per_chunk - current_records_num), rows);
            chunk_codec_stream->encode(block, row_index, upper);
            current_records_num += (upper - row_index);
            row_index = upper;
        }
    }
}

template <>
void DAGBlockOutputStream<true>::readSuffix()
{
    // todo error handle
    if (current_records_num > 0)
    {
        encodeChunkToDAGResponse();
    }
    children.back()->readSuffix();
}


} // namespace DB
