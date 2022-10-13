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
#include <Transforms/AggregateStore.h>

namespace DB
{
namespace
{
class AllLock
{
public:
    explicit AllLock(std::vector<std::mutex> & mutexes_): mutexes(mutexes_)
    {
        for (auto & mutex : mutexes)
            mutex.lock();
    }

    ~AllLock()
    {
        for (auto & mutex : mutexes)
            mutex.unlock();
    }
private:
    std::vector<std::mutex> & mutexes;
};
}

AggregateStore::AggregateStore(
    const String & req_id,
    const FileProviderPtr & file_provider_,
    bool is_final_)
    : file_provider(file_provider_)
    , is_final(is_final_)
    , log(Logger::get("AggregateStore", req_id))
{}

void AggregateStore::init(size_t max_threads_, const Aggregator::Params & params)
{
    std::unique_lock init_lock(init_mutex);
    if (inited)
        return;
    inited = true;

    max_threads = max_threads_;
    assert(max_threads > 0);

    many_data.reserve(max_threads);
    for (size_t i = 0; i < max_threads; ++i)
        many_data.emplace_back(std::make_shared<AggregatedDataVariants>());

    threads_data.reserve(max_threads);
    for (size_t i = 0; i < max_threads; ++i)
        threads_data.emplace_back(params.keys_size, params.aggregates_size);

    mutexes = std::make_unique<std::vector<std::mutex>>(max_threads);

    aggregator = std::make_unique<Aggregator>(params, log->identifier());
}

size_t AggregateStore::maxThreads() const
{
    std::shared_lock init_lock(init_mutex);
    assert(inited);
    return max_threads;
}

Block AggregateStore::getHeader() const
{
    std::shared_lock init_lock(init_mutex);
    assert(inited);
    return aggregator->getHeader(is_final);
}

void AggregateStore::executeOnBlock(size_t index, const Block & block)
{
    std::shared_lock init_lock(init_mutex);
    assert(inited);
    assert(index < max_threads);
    std::lock_guard lock((*mutexes)[index]);
    executeOnBlockWithoutLock(index, block);
}

void AggregateStore::executeOnBlockWithoutLock(size_t index, const Block & block)
{
    std::shared_lock init_lock(init_mutex);
    assert(inited);
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

bool AggregateStore::isTwoLevel()
{
    std::shared_lock init_lock(init_mutex);
    assert(inited);
    std::lock_guard lock((*mutexes)[0]);
    assert(impl);
    assert(!many_data.empty());
    return many_data[0]->isTwoLevel();
}

void AggregateStore::initForMerge()
{
    std::shared_lock init_lock(init_mutex);
    assert(inited);
    AllLock lock(*mutexes);
    RUNTIME_ASSERT(!aggregator->hasTemporaryFiles());
    assert(!impl);
    impl = aggregator->mergeAndConvertToBlocks(many_data, is_final, max_threads, true);
}

// don't need to lock here.
Block AggregateStore::readForMerge()
{
    std::shared_lock init_lock(init_mutex);
    assert(inited);
    assert(impl);
    return impl->read();
}
} // namespace DB
