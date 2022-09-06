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

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Executor/ResultHandler.h>
#include <Flash/Planner/plans/PhysicalUnary.h>
#include <Transforms/ResultHandlerSink.h>
#include <Transforms/TransformsPipeline.h>

namespace DB
{
class ResultHandlerBlockInputStream : public IProfilingBlockInputStream
{
private:
    static constexpr auto NAME = "ResultHandler";

public:
    ResultHandlerBlockInputStream(
        const BlockInputStreamPtr & input,
        ResultHandler result_handler_)
        : result_handler(result_handler_)
    {
        children.push_back(input);
    }

    String getName() const override { return NAME; }
    Block getHeader() const override
    {
        return children.back()->getHeader();
    }

protected:
    Block readImpl() override
    {
        Block block = children.back()->read();
        if (block)
        {
            static std::mutex mu;
            std::lock_guard lock(mu);
            result_handler(block);
        }
        return block;
    }

private:
    ResultHandler result_handler;
};

class PhysicalResultHandler : public PhysicalUnary
{
public:
    static PhysicalPlanNodePtr build(
        ResultHandler result_handler,
        const String & req_id,
        const PhysicalPlanNodePtr & child)
    {
        return result_handler.isDefault()
            ? child
            : std::make_shared<PhysicalResultHandler>(
                "ResultHandler",
                child->getSchema(),
                req_id,
                child,
                result_handler);
    }

    PhysicalResultHandler(
        const String & executor_id_,
        const NamesAndTypes & schema_,
        const String & req_id,
        const PhysicalPlanNodePtr & child_,
        ResultHandler result_handler_)
        : PhysicalUnary(executor_id_, PlanType::ResultHandler, schema_, req_id, child_)
        , result_handler(result_handler_)
    {}

    void finalize(const Names & parent_require) override
    {
        return child->finalize(parent_require);
    }

    const Block & getSampleBlock() const override
    {
        return child->getSampleBlock();
    }

    PhysicalPlanNodePtr cloneOne() const override
    {
        auto clone_one = std::make_shared<PhysicalResultHandler>(*this);
        return clone_one;
    }

    void transform(TransformsPipeline & pipeline, Context & context) override
    {
        child->transform(pipeline, context);
        pipeline.transform([&](auto & transforms) {
            transforms->setSink(std::make_shared<ResultHandlerSink>(result_handler));
        });
    }

private:
    void transformImpl(DAGPipeline & pipeline, Context & context, size_t max_streams) override
    {
        child->transform(pipeline, context, max_streams);
        pipeline.transform([&](auto & stream) { stream = std::make_shared<ResultHandlerBlockInputStream>(stream, result_handler); });
    }

private:
    ResultHandler result_handler;
};
} // namespace DB
