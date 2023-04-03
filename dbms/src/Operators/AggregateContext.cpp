// Copyright 2023 PingCAP, Ltd.
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

#include <Operators/AggregateContext.h>

namespace DB
{
void AggregateContext::initBuild(const Aggregator::Params & params, size_t max_threads_, Aggregator::CancellationHook && hook)
{
    RUNTIME_CHECK(!inited_build && !inited_convergent);
    max_threads = max_threads_;
    empty_result_for_aggregation_by_empty_set = params.empty_result_for_aggregation_by_empty_set;
    keys_size = params.keys_size;
    many_data.reserve(max_threads);
    threads_data.reserve(max_threads);
    for (size_t i = 0; i < max_threads; ++i)
    {
        threads_data.emplace_back(std::make_unique<ThreadData>(params.keys_size, params.aggregates_size));
        many_data.emplace_back(std::make_shared<AggregatedDataVariants>());
    }

    aggregator = std::make_unique<Aggregator>(params, log->identifier());
    aggregator->setCancellationHook(std::move(hook));
    aggregator->initThresholdByAggregatedDataVariantsSize(many_data.size());
    inited_build = true;
    build_watch.emplace();
    LOG_TRACE(log, "Aggregate Context inited");
}

void AggregateContext::buildOnBlock(size_t task_index, const Block & block)
{
    RUNTIME_CHECK(inited_build && !inited_convergent);
    auto & data = *many_data[task_index];
    aggregator->executeOnBlock(block, data, threads_data[task_index]->key_columns, threads_data[task_index]->aggregate_columns);
    threads_data[task_index]->src_bytes += block.bytes();
    threads_data[task_index]->src_rows += block.rows();
}

std::optional<std::function<void()>> AggregateContext::trySpill(size_t task_index, bool try_mark_need_spill)
{
    auto & data = *many_data[task_index];
    if (try_mark_need_spill)
        data.tryMarkNeedSpill();
    if (data.need_spill)
        return [&]() { aggregator->spill(data); };
    return {};
}

bool AggregateContext::hasSpilledData()
{
    return aggregator->hasSpilledData();
}

void AggregateContext::prepareForNonSpilled()
{
    assert(build_watch);
    double elapsed_seconds = build_watch->elapsedSeconds();
    size_t total_src_rows = 0;
    size_t total_src_bytes = 0;
    for (size_t i = 0; i < max_threads; ++i)
    {
        size_t rows = many_data[i]->size();
        LOG_TRACE(
            log,
            "Aggregated. {} to {} rows (from {:.3f} MiB) in {:.3f} sec. ({:.3f} rows/sec., {:.3f} MiB/sec.)",
            threads_data[i]->src_rows,
            rows,
            (threads_data[i]->src_bytes / 1048576.0),
            elapsed_seconds,
            threads_data[i]->src_rows / elapsed_seconds,
            threads_data[i]->src_bytes / elapsed_seconds / 1048576.0);
        total_src_rows += threads_data[i]->src_rows;
        total_src_bytes += threads_data[i]->src_bytes;
    }

    LOG_TRACE(
        log,
        "Total aggregated {} rows (from {:.3f} MiB) in {:.3f} sec. ({:.3f} rows/sec., {:.3f} MiB/sec.)",
        total_src_rows,
        (total_src_bytes / 1048576.0),
        elapsed_seconds,
        total_src_rows / elapsed_seconds,
        total_src_bytes / elapsed_seconds / 1048576.0);

    if (total_src_rows == 0 && keys_size == 0 && !empty_result_for_aggregation_by_empty_set)
        aggregator->executeOnBlock(
            this->getHeader(),
            *many_data[0],
            threads_data[0]->key_columns,
            threads_data[0]->aggregate_columns);
}

void AggregateContext::initConvergent(PipelineExecutorStatus & status)
{
    RUNTIME_CHECK(inited_build && !inited_convergent);

    if (!aggregator->hasSpilledData())
    {
        prepareForNonSpilled();

        merging_buckets = aggregator->mergeAndConvertToBlocks(many_data, true, max_threads);
        inited_convergent = true;
        RUNTIME_CHECK(!merging_buckets || merging_buckets->getConcurrency() > 0);
    }
    else
    {
        aggregator.finishSpill();
        BlockInputStreams input_streams = aggregator.restoreSpilledData();
        RUNTIME_CHECK(input_streams.empty());
        loader = std::make_shared<AggregateLoader>(input_streams, getHeader(), status, log->identifier());
    }
}

size_t AggregateContext::getConvergentConcurrency()
{
    RUNTIME_CHECK(inited_convergent);
    if (merging_buckets)
        return merging_buckets->getConcurrency();
    if (loader)
        return max_threads;
    return 1;
}

Block AggregateContext::getHeader() const
{
    RUNTIME_CHECK(inited_build);
    return aggregator->getHeader(true);
}

bool AggregateContext::useNullSource()
{
    RUNTIME_CHECK(inited_convergent);
    return !merging_buckets && !loader;
}

Block AggregateContext::readForConvergent(size_t index)
{
    RUNTIME_CHECK(inited_convergent);
    return merging_buckets->getData(index);
}
} // namespace DB
