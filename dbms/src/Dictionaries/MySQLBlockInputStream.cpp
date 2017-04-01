#include <DB/Dictionaries/MySQLBlockInputStream.h>
#include <DB/Columns/ColumnsNumber.h>
#include <DB/Columns/ColumnString.h>
#include <ext/range.hpp>
#include <vector>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_COLUMNS_DOESNT_MATCH;
}


MySQLBlockInputStream::MySQLBlockInputStream(
    const mysqlxx::PoolWithFailover::Entry & entry, const std::string & query_str, const Block & sample_block,
    const std::size_t max_block_size)
    : entry{entry}, query{this->entry->query(query_str)}, result{query.use()},
        max_block_size{max_block_size}
{
    if (sample_block.columns() != result.getNumFields())
        throw Exception{
            "mysqlxx::UseQueryResult contains " + toString(result.getNumFields()) + " columns while " +
                toString(sample_block.columns()) + " expected",
            ErrorCodes::NUMBER_OF_COLUMNS_DOESNT_MATCH};

    description.init(sample_block);
}


String MySQLBlockInputStream::getID() const
{
    return "MySQL(" + query.str() + ")";
}


namespace
{
    using ValueType = ExternalResultDescription::ValueType;

    void insertValue(IColumn * const column, const ValueType type, const mysqlxx::Value & value)
    {
        switch (type)
        {
            case ValueType::UInt8: static_cast<ColumnUInt8 *>(column)->insert(value.getUInt()); break;
            case ValueType::UInt16: static_cast<ColumnUInt16 *>(column)->insert(value.getUInt()); break;
            case ValueType::UInt32: static_cast<ColumnUInt32 *>(column)->insert(value.getUInt()); break;
            case ValueType::UInt64: static_cast<ColumnUInt64 *>(column)->insert(value.getUInt()); break;
            case ValueType::Int8: static_cast<ColumnInt8 *>(column)->insert(value.getInt()); break;
            case ValueType::Int16: static_cast<ColumnInt16 *>(column)->insert(value.getInt()); break;
            case ValueType::Int32: static_cast<ColumnInt32 *>(column)->insert(value.getInt()); break;
            case ValueType::Int64: static_cast<ColumnInt64 *>(column)->insert(value.getInt()); break;
            case ValueType::Float32: static_cast<ColumnFloat32 *>(column)->insert(value.getDouble()); break;
            case ValueType::Float64: static_cast<ColumnFloat64 *>(column)->insert(value.getDouble()); break;
            case ValueType::String: static_cast<ColumnString *>(column)->insertData(value.data(), value.size()); break;
            case ValueType::Date: static_cast<ColumnUInt16 *>(column)->insert(UInt16{value.getDate().getDayNum()}); break;
            case ValueType::DateTime: static_cast<ColumnUInt32 *>(column)->insert(time_t{value.getDateTime()}); break;
        }
    }
}


Block MySQLBlockInputStream::readImpl()
{
    auto row = result.fetch();
    if (!row)
        return {};

    auto block = description.sample_block.cloneEmpty();

    /// cache pointers returned by the calls to getByPosition
    std::vector<IColumn *> columns(block.columns());
    for (const auto i : ext::range(0, columns.size()))
        columns[i] = block.safeGetByPosition(i).column.get();

    std::size_t num_rows = 0;
    while (row)
    {
        for (const auto idx : ext::range(0, row.size()))
        {
            const auto value = row[idx];
            if (!value.isNull())
                insertValue(columns[idx], description.types[idx], value);
            else
                insertDefaultValue(columns[idx], *description.sample_columns[idx]);
        }

        ++num_rows;
        if (num_rows == max_block_size)
            break;

        row = result.fetch();
    }

    return block;
}

}
