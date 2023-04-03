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

#include <Operators/AggregateLoader.h>
#include <Flash/Pipeline/Schedule/Events/Event.h>
#include <Flash/Pipeline/Schedule/Tasks/EventTask.h>

namespace
{
class LoadTask : public EventTask
{
public:
    LoadTask(
        MemoryTrackerPtr mem_tracker_,
        const String & req_id,
        PipelineExecutorStatus & exec_status_,
        const EventPtr & event_,
        AggregateLoader::InputPtr && input_)
        : EventTask(std::move(mem_tracker_), req_id, exec_status_, event_)
        , input(std::move(input_))
    {
        assert(input);
    }

protected:
    ExecTaskStatus doExecuteImpl() override
    {
        return ExecTaskStatus::IO;
    }

    ExecTaskStatus doExecuteIOImpl() override
    {
        input->load();
        return ExecTaskStatus::FINISHED;
    }

    ExecTaskStatus doAwaitImpl() override
    {
        return ExecTaskStatus::IO;
    }

private:
    AggregateLoader::InputPtr input;
};

class LoadEvent : public Event
{
public:
    LoadEvent(
        PipelineExecutorStatus & exec_status_,
        MemoryTrackerPtr mem_tracker_,
        const String & req_id,
        const AggregateLoaderPtr & agg_loader_,
        const AggregateLoader::Inputs & inputs_)
        : Event(exec_status_, std::move(mem_tracker_), req_id)
        , agg_loader(agg_loader_)
        , inputs(inputs_)
    {}

protected:
    std::vector<TaskPtr> scheduleImpl() override
    {
        std::vector<TaskPtr> tasks;
        for (auto & input : inputs)
            tasks.push_back(std::make_shared<LoadTask>(mem_tracker, log->identifier(), exec_status, shared_from_this(), std::move(input)));
        inputs.clear();
        return tasks;
    }

    void finishImpl() override
    {
        agg_loader->onLoadingDone();
    }
};

private:
    AggregateLoaderPtr agg_loader;
    AggregateLoader::Inputs inputs;
};

namespace DB
{
AggregateLoader::AggregateLoader(
    const BlockInputStreams & load_streams,
    const Block & header_,
    PipelineExecutorStatus & exec_status_,
    const String & req_id)
    : header(header_)
    , exec_status(exec_status_)
    , log(Logger::get(req_id))
{
    for (const auto & load_stream : load_streams)
        loading_inputs.emplace_back(load_stream);
    RUNTIME_CHECK(!loading_inputs.empty());
}

void AggregateLoader::onLoadingDone()
{
    assert(status == LoadStatus::loading);

    auto cur_bucket_num = NUM_BUCKETS;
    for (const auto & input : loading_inputs)
    {
        if (input->hasOutput())
            local_bucket_num = std::min(cur_bucket_num, input->getOutputBucketNum());
    }
    current_bucket_num = 1 + cur_bucket_num;
    if (cur_bucket_num == NUM_BUCKETS)
    {
        status = LoadStatus::finished;
        return;
    }

    BlocksList ret;
    for (const auto & input : loading_inputs)
    {
        if (input->hasOutput() && input->getOutputBucketNum() == local_bucket_num)
            ret.push_back(input->moveOutput());
    }
    assert(!ret.empty());
    std::lock_guard lock(result_mu);
    load_result.push_back(std::move(ret));
    status = LoadStatus::valid;
}

bool AggregateLoader::onValid(BlocksList & result)
{
    assert(status == LoadStatus::valid);
    assert(result.empty());
    if (status.compare_exchange_strong(LoadStatus::valid, LoadStatus::idle))
    {
        std::lock_guard lock(result_mu);
        assert(!load_result.empty());
        result = std::move(load_result.back());
        return true;
    }
    return false;
}

void AggregateLoader::onIdle()
{
    assert(status == LoadStatus::idle);
    if (!status.compare_exchange_strong(LoadStatus::idle, LoadStatus::loading))
        return;

    if (current_bucket_num >= bucket_num)
    {
        status = LoadStatus::finished;
        return;
    }

    Inputs schedule_inputs;
    bool all_finished = true;
    for (const auto & input : loading_inputs)
    {
        if (input->finished())
        {
            all_finished = false;
            if (!input->hasOutput())
                schedue_inputs.push_back(input);
        }
    }
    if (all_finished)
    {
        status = LoadStatus::finished;
    }
    else
    {
        auto memory_tracker = current_memory_tracker ? current_memory_tracker->shared_from_this() : nullptr;
        auto load_event = std::make_shared<LoadEvent>(status, memory_tracker, log->identifier(), shared_from_this(), schedule_inputs);
        assert(load_event->withoutInput());
        load_event->schedule();
    }
}

bool AggregateLoader::tryGet(BlocksList & result)
{
    assert(result.empty());
    switch (status)
    {
    case LoadStatus::idle:
        onIdle();
        return false;
    case LoadStatus::loading:
        return false;
    case LoadStatus::valid:
        return onValid(result);
    case LoadStatus::finished:
        return true;
    }
}

AggregateLoader::Input::Input(const BlockInputStreamPtr & stream_)
    : stream(stream_)
{
    stream->readPrefix();
}

bool AggregateLoader::hasOutput() const
{
    return output.has_value();
}

Block AggregateLoader::Input::moveOutput()
{
    assert(hasOutput());
    Block ret = std::move(*output);
    output.reset();
    return ret;
}
Int32 AggregateLoader::Input::getOutputBucketNum() const
{
    assert(hasOutput());
    return output->info.bucket_num;
}

bool AggregateLoader::Input::finished() const
{
    return is_exhausted;
}

bool AggregateLoader::Input::load()
{
    if (finished())
        return false;

    if (output.has_value())
        return true;

    Block ret = stream->read();
    if (!ret)
    {
        is_exhausted = true;
        return false;
    }
    else
    {
        /// Only two level data can be spilled.
        assert(ret.info.bucket_num != -1);
        output.emplace(std::move(ret));
        return true;
    }
}
} // namespace DB
