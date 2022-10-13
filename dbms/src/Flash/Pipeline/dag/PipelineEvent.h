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
    static PipelineEvent finish()
    {
        return {"", false, PipelineEventType::finish};
    }

    static PipelineEvent fail(const String & err_msg)
    {
        return {err_msg, false, PipelineEventType::fail};
    }

    static PipelineEvent cancel(bool is_kill)
    {
        return {"", is_kill, PipelineEventType::cancel};
    }

    PipelineEvent() = default;

    PipelineEvent(
        const String & err_msg_,
        bool is_kill_,
        PipelineEventType type_)
        : err_msg(err_msg_)
        , is_kill(is_kill_)
        , type(type_)
    {}

    PipelineEvent(PipelineEvent && event)
        : err_msg(std::move(event.err_msg))
        , is_kill(event.is_kill)
        , type(std::move(event.type))
    {}

    PipelineEvent & operator=(PipelineEvent && event)
    {
        if (this != &event)
        {
            err_msg = std::move(event.err_msg);
            is_kill = event.is_kill;
            type = std::move(event.type);
        }
        return *this;
    }

    String err_msg;
    bool is_kill;
    PipelineEventType type;
};
} // namespace DB
