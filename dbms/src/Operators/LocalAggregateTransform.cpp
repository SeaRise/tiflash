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

#include <Flash/Executor/PipelineExecutorStatus.h>
#include <Operators/LocalAggregateTransform.h>

#include <magic_enum.hpp>

namespace DB
{
namespace
{
/// for local agg, the concurrency of build and convert must both be 1.
constexpr size_t local_concurrency = 1;

/// for local agg, the task_index of build and convert must both be 0.
constexpr size_t task_index = 0;
} // namespace

LocalAggregateTransform::LocalAggregateTransform(
    PipelineExecutorStatus & exec_status_,
    const String & req_id,
    const Aggregator::Params & params_)
    : TransformOp(exec_status_, req_id)
    , params(params_)
    , agg_context(req_id)
{
    agg_context.initBuild(params, local_concurrency, /*hook=*/[&]() { return exec_status.isCancelled(); });
}

OperatorStatus LocalAggregateTransform::transformImpl(Block & block)
{
    switch (status)
    {
    case LocalAggStatus::build:
        if (unlikely(!block))
        {
            if (!agg_context.hasSpilledData())
            {
                // status from build to convert.
                status = LocalAggStatus::convert;
                agg_context.initConvergent();
                if likely (!agg_context.useNullSource())
                {
                    RUNTIME_CHECK(agg_context.getConvergentConcurrency() == local_concurrency);
                    block = agg_context.readForConvergent(task_index);
                }
                return OperatorStatus::HAS_OUTPUT;
            }
            else
            {
                // status from build to restore.
                status = LocalAggStatus::restore;
                if (auto func = agg_context.trySpill(task_index, /*mark_need_spill=*/true); func)
                    io_funcs.push_back(std::move(*func));
                // xxxx
                return OperatorStatus::IO;
            }
        }
        agg_context.buildOnBlock(task_index, block);
        block.clear();
        if (auto func = agg_context.trySpill(task_index); func)
        {
            io_funcs.push_back(std::move(*func));
            return OperatorStatus::IO;
        }
        return OperatorStatus::NEED_INPUT;
    default:
        throw Exception(fmt::format("Unexpected status: {}", magic_enum::enum_name(status)));
    }
}

OperatorStatus LocalAggregateTransform::tryOutputImpl(Block & block)
{
    switch (status)
    {
    case LocalAggStatus::build:
        return OperatorStatus::NEED_INPUT;
    case LocalAggStatus::convert:
        if likely (!agg_context.useNullSource())
            block = agg_context.readForConvergent(task_index);
        return OperatorStatus::HAS_OUTPUT;
    case LocalAggStatus::restore:
        if (restore_data.empty())
        {
            io_func.push_back(restore_func)
            return io_status;
        }
        else
        {
            // restore_data --> block
            return OperatorStatus::HAS_OUTPUT;
        }
    }
}

OperatorStatus LocalAggregateTransform::executeIOImpl()
{
    for (auto & io_func : io_funcs)
        io_func();
    io_funcs.clear();
    return status == LocalAggStatus::build
        ? OperatorStatus::NEED_INPUT
        : OperatorStatus::HAS_OUTPUT;
}

void LocalAggregateTransform::transformHeaderImpl(Block & header_)
{
    header_ = agg_context.getHeader();
}
} // namespace DB
