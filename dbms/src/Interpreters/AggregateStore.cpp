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

#include <DataStreams/MergingAggregatedMemoryEfficientBlockInputStream.h>
#include <Interpreters/AggregateStore.h>

namespace DB
{
AggregateStore::AggregateStore(
    const String & req_id,
    const FileProviderPtr & file_provider_,
    bool is_final_,
    size_t temporary_data_merge_threads_)
    : file_provider(file_provider_)
    , is_final(is_final_)
    , temporary_data_merge_threads(temporary_data_merge_threads_)
    , log(Logger::get("AggregateStore", req_id))
{
    assert(temporary_data_merge_threads > 0);
}

void AggregateStore::init(size_t max_threads_, const Aggregator::Params & params)
{
    assert(!inited);
    inited = true;

    max_threads = max_threads_;
    assert(max_threads > 0);

    many_data.reserve(max_threads);
    for (size_t i = 0; i < max_threads; ++i)
        many_data.emplace_back(std::make_shared<AggregatedDataVariants>());

    threads_data.reserve(max_threads);
    for (size_t i = 0; i < max_threads; ++i)
        threads_data.emplace_back(params.keys_size, params.aggregates_size);

    mutexs = std::make_unique<std::vector<std::mutex>>(max_threads);

    aggregator = std::make_unique<Aggregator>(params, log->identifier());
}

Block AggregateStore::getHeader() const
{
    return aggregator->getHeader(is_final);
}

void AggregateStore::executeOnBlock(size_t index, const Block & block)
{
    assert(index < max_threads);
    std::lock_guard lock((*mutexs)[index]);
    executeOnBlockWithoutLock(index, block);
}

void AggregateStore::executeOnBlockWithoutLock(size_t index, const Block & block)
{
    assert(index < max_threads);
    auto & thread_data = threads_data[index];
    aggregator->executeOnBlock(
        block,
        *many_data[index],
        file_provider,
        thread_data.key_columns,
        thread_data.aggregate_columns,
        thread_data.local_delta_memory,
        thread_data.no_more_keys);

    thread_data.src_rows += block.rows();
    thread_data.src_bytes += block.bytes();
}

void AggregateStore::tryFlush(size_t index)
{
    if (aggregator->hasTemporaryFiles())
    {
        /// Flush data in the RAM to disk. So it's easier to unite them later.
        auto & data = *many_data[index];

        if (data.isConvertibleToTwoLevel())
            data.convertToTwoLevel();

        if (!data.empty())
            aggregator->writeToTemporaryFile(data, file_provider);
    }
}

void AggregateStore::tryFlush()
{
    if (aggregator->hasTemporaryFiles())
    {
        /// It may happen that some data has not yet been flushed,
        ///  because at the time of `onFinishThread` call, no data has been flushed to disk, and then some were.
        for (const auto & data : many_data)
        {
            if (data->isConvertibleToTwoLevel())
                data->convertToTwoLevel();

            if (!data->empty())
                aggregator->writeToTemporaryFile(*data, file_provider);
        }
    }
}

std::unique_ptr<IBlockInputStream> AggregateStore::merge()
{
    RUNTIME_ASSERT(!aggregator->hasTemporaryFiles());
    return aggregator->mergeAndConvertToBlocks(many_data, is_final, max_threads, true);
}

std::pair<size_t, size_t> AggregateStore::mergeSrcRowsAndBytes() const
{
    size_t total_src_rows = 0;
    size_t total_src_bytes = 0;
    for (const auto & thread_data : threads_data)
    {
        total_src_rows += thread_data.src_rows;
        total_src_bytes += thread_data.src_bytes;
    }
    return {total_src_rows, total_src_bytes};
}

bool AggregateStore::isTwoLevel() const
{
    assert(!many_data.empty());
    return many_data[0]->isTwoLevel();
}
} // namespace DB
