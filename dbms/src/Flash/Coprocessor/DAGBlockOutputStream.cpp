#include <Flash/Coprocessor/DAGBlockOutputStream.h>

#include <DataTypes/DataTypeNullable.h>
#include <Storages/Transaction/Codec.h>
#include <Storages/Transaction/Datum.h>
#include <Storages/Transaction/TypeMapping.h>

namespace DB
{

namespace ErrorCodes
{
extern const int UNSUPPORTED_PARAMETER;
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

using TiDB::DatumBumpy;
using TiDB::TP;

DAGBlockOutputStream::DAGBlockOutputStream(tipb::SelectResponse & dag_response_, Int64 records_per_chunk_, tipb::EncodeType encodeType_,
    std::vector<tipb::FieldType> && result_field_types_, Block header_)
    : dag_response(dag_response_),
      records_per_chunk(records_per_chunk_),
      encodeType(encodeType_),
      result_field_types(result_field_types_),
      header(header_)
{
    if (encodeType == tipb::EncodeType::TypeArrow)
    {
        throw Exception("Encode type TypeArrow is not supported yet in DAGBlockOutputStream.", ErrorCodes::UNSUPPORTED_PARAMETER);
    }
    current_chunk = nullptr;
    current_records_num = 0;
}


void DAGBlockOutputStream::writePrefix()
{
    //something to do here?
}

void DAGBlockOutputStream::writeSuffix()
{
    // error handle,
    if (current_chunk != nullptr && current_records_num > 0)
    {
        current_chunk->set_rows_data(current_ss.str());
        dag_response.add_output_counts(current_records_num);
    }
}

void DAGBlockOutputStream::write(const Block & block)
{
    if (block.columns() != result_field_types.size())
        throw Exception("Output column size mismatch with field type size", ErrorCodes::LOGICAL_ERROR);

    // TODO: Check compatibility between field_tp_and_flags and block column types.

    // Encode data to chunk
    size_t rows = block.rows();
    for (size_t i = 0; i < rows; i++)
    {
        if (current_chunk == nullptr || current_records_num >= records_per_chunk)
        {
            if (current_chunk)
            {
                // set the current ss to current chunk
                current_chunk->set_rows_data(current_ss.str());
                dag_response.add_output_counts(current_records_num);
            }
            current_chunk = dag_response.add_chunks();
            current_ss.str("");
            current_records_num = 0;
        }
        for (size_t j = 0; j < block.columns(); j++)
        {
            const auto & field = (*block.getByPosition(j).column.get())[i];
            DatumBumpy datum(field, static_cast<TP>(result_field_types[j].tp()));
            EncodeDatum(datum.field(), getCodecFlagByFieldType(result_field_types[j]), current_ss);
        }
        // Encode current row
        current_records_num++;
    }
}

} // namespace DB
