// Copyright 2023 PingCAP, Inc.
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

#include <Flash/Coprocessor/CHBlockChunkCodecV1.h>
#include <Flash/Coprocessor/ChunkDecodeAndSquash.h>
#include <IO/ReadBufferFromString.h>

namespace DB
{
CHBlockChunkDecodeAndSquash::CHBlockChunkDecodeAndSquash(const Block & header_, size_t rows_limit_)
    : header(header_)
    , codec(header_)
    , rows_limit(rows_limit_)
{}

std::optional<Block> CHBlockChunkDecodeAndSquash::decodeAndSquashV1(std::string_view sv)
{
    if unlikely (sv.empty())
    {
        std::optional<Block> res;
        if (accumulated_block)
            res.swap(accumulated_block);
        return res;
    }

    // read first byte of compression method flag which defined in `CompressionMethodByte`
    if (static_cast<CompressionMethodByte>(sv[0]) == CompressionMethodByte::NONE)
    {
        ReadBufferFromString istr(sv.substr(1, sv.size() - 1));
        return decodeAndSquashV1Impl(istr);
    }

    ReadBufferFromString istr(sv);
    auto && compress_buffer = CompressedCHBlockChunkReadBuffer(istr);
    return decodeAndSquashV1Impl(compress_buffer);
}

std::optional<Block> CHBlockChunkDecodeAndSquash::decodeAndSquashV1Impl(ReadBuffer & istr)
{
    std::optional<Block> res;

    if (!accumulated_block)
    {
        size_t rows{};
        Block block = DecodeHeader(istr, header, rows);
        if (rows)
        {
            DecodeColumns(istr, block, rows, static_cast<size_t>(rows_limit * 1.5));
            accumulated_block.emplace(std::move(block));
        }
    }
    else
    {
        size_t rows{};
        DecodeHeader(istr, header, rows);
        DecodeColumns(istr, *accumulated_block, rows, 0);
    }

    if (accumulated_block && accumulated_block->rows() >= rows_limit)
    {
        /// Return accumulated data and reset accumulated_block
        res.swap(accumulated_block);
        return res;
    }
    return res;
}

std::optional<Block> CHBlockChunkDecodeAndSquash::decodeAndSquash(const String & str)
{
    std::optional<Block> res;
    ReadBufferFromString istr(str);
    if (istr.eof())
    {
        if (accumulated_block)
            res.swap(accumulated_block);
        return res;
    }

    if (!accumulated_block)
    {
        /// hard-code 1.5 here, since final column size will be more than rows_limit in most situations,
        /// so it should be larger than 1.0, just use 1.5 here, no special meaning
        Block block = codec.decodeImpl(istr, static_cast<size_t>(rows_limit * 1.5));
        if (block)
            accumulated_block.emplace(std::move(block));
    }
    else
    {
        /// Dimensions
        size_t columns = 0;
        size_t rows = 0;
        codec.readBlockMeta(istr, columns, rows);

        if (rows)
        {
            auto mutable_columns = accumulated_block->mutateColumns();
            for (size_t i = 0; i < columns; ++i)
            {
                ColumnWithTypeAndName column;
                codec.readColumnMeta(i, istr, column);
                CHBlockChunkCodec::readData(*column.type, *(mutable_columns[i]), istr, rows);
            }
            accumulated_block->setColumns(std::move(mutable_columns));
        }
    }

    if (accumulated_block && accumulated_block->rows() >= rows_limit)
    {
        /// Return accumulated data and reset accumulated_block
        res.swap(accumulated_block);
        return res;
    }
    return res;
}

std::optional<Block> CHBlockChunkDecodeAndSquash::decodeAndSquash(Block && block)
{
    if unlikely (!block)
    {
        std::optional<Block> res;
        if (accumulated_block)
            res.swap(accumulated_block);
        return res;
    }

    if (block.rows() > 0)
    {
        if (!accumulated_block)
        {
            accumulated_block.emplace(header.cloneWithColumns(block.mutateColumns()));
        }
        else
        {
            size_t columns = block.columns();
            size_t rows = block.rows();
            for (size_t i = 0; i < columns; ++i)
            {
                MutableColumnPtr mutable_column = (*std::move(accumulated_block->getByPosition(i).column)).mutate();
                mutable_column->insertRangeFrom(*block.getByPosition(i).column, 0, rows);
                accumulated_block->getByPosition(i).column = std::move(mutable_column);
            }
        }
    }

    if (accumulated_block && accumulated_block->rows() >= rows_limit)
    {
        /// Return accumulated data and reset accumulated_block
        std::optional<Block> res;
        res.swap(accumulated_block);
        return res;
    }
    else
    {
        return {};
    }
}

std::optional<Block> CHBlockChunkDecodeAndSquash::flush()
{
    if (!accumulated_block)
        return accumulated_block;
    std::optional<Block> res;
    accumulated_block.swap(res);
    return res;
}

} // namespace DB
