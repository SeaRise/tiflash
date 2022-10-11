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

struct PipelineEvent;
using PipelineEventPtr = std::unique_ptr<PipelineEvent>;

struct PipelineEvent
{
    static PipelineEventPtr finish(UInt32 task_id, UInt32 pipeline_id)
    {
        return std::make_unique<PipelineEvent>(task_id, pipeline_id, "", false, PipelineEventType::finish);
    }

    static PipelineEventPtr fail(UInt32 task_id, UInt32 pipeline_id, const String & err_msg)
    {
        return std::make_unique<PipelineEvent>(task_id, pipeline_id, err_msg, false, PipelineEventType::fail);
    }

    static PipelineEventPtr fail(const String & err_msg)
    {
        return fail(0, 0, err_msg);
    }

    static PipelineEventPtr cancel(bool is_kill)
    {
        return std::make_unique<PipelineEvent>(0, 0, "", is_kill, PipelineEventType::cancel);
    }

    PipelineEvent(
        UInt32 task_id_,
        UInt32 pipeline_id_,
        const String & err_msg_,
        bool is_kill_,
        PipelineEventType type_)
        : task_id(task_id_)
        , pipeline_id(pipeline_id_)
        , err_msg(err_msg_)
        , is_kill(is_kill_)
        , type(type_)
    {}

    UInt32 task_id;
    UInt32 pipeline_id;
    String err_msg;
    bool is_kill;
    PipelineEventType type;
};
} // namespace DB
