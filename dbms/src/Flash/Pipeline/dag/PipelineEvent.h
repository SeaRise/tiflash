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

#include <Flash/Pipeline/dag/Pipeline.h>

#include <memory>

namespace DB
{
enum class PipelineEventType
{
    finish,
    fail,
    cancel,
};

struct PipelineEvent
{
    static PipelineEvent finish(UInt32 pipeline_id)
    {
        return {pipeline_id, "", false, PipelineEventType::finish};
    }

    static PipelineEvent fail(UInt32 pipeline_id, const String & err_msg)
    {
        return {pipeline_id, err_msg, false, PipelineEventType::fail};
    }

    static PipelineEvent fail(const String & err_msg)
    {
        return fail(0, err_msg);
    }

    static PipelineEvent cancel(bool is_kill)
    {
        return {0, "", is_kill, PipelineEventType::cancel};
    }

    PipelineEvent() = default;

    PipelineEvent(
        UInt32 pipeline_id_,
        const String & err_msg_,
        bool is_kill_,
        PipelineEventType type_)
        : pipeline_id(pipeline_id_)
        , err_msg(err_msg_)
        , is_kill(is_kill_)
        , type(type_)
    {}

    PipelineEvent(PipelineEvent && event)
        : pipeline_id(std::move(event.pipeline_id))
        , err_msg(std::move(event.err_msg))
        , is_kill(event.is_kill)
        , type(std::move(event.type))
    {}

    PipelineEvent & operator=(PipelineEvent && event)
    {
        if (this != &event)
        {
            pipeline_id = std::move(event.pipeline_id);
            err_msg = std::move(event.err_msg);
            is_kill = event.is_kill;
            type = std::move(event.type);
        }
        return *this;
    }

    UInt32 pipeline_id;
    String err_msg;
    bool is_kill;
    PipelineEventType type;
};
} // namespace DB
