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

#include <Flash/Pipeline/PipelineExecStatus.h>
#include <assert.h>

namespace DB
{
ExecutionResult PipelineExecStatus::toExecutionResult()
{
    auto get_err_msg = getErrMsg();
    return get_err_msg.empty()
        ? ExecutionResult::success()
        : ExecutionResult::fail(get_err_msg);
}

String PipelineExecStatus::getErrMsg()
{
    std::lock_guard lock(mu);
    return err_msg;
}

void PipelineExecStatus::toError(String && err_msg_)
{
    {
        std::lock_guard lock(mu);
        if (!err_msg.empty())
            return;
        err_msg = err_msg_.empty() ? empty_err_msg : std::move(err_msg_);
    }
    cancel();
}

void PipelineExecStatus::wait()
{
    std::unique_lock lock(mu);
    cv.wait(lock, [&] { return 0 == active_event_count; });
}

void PipelineExecStatus::addActiveEvent()
{
    std::lock_guard lock(mu);
    ++active_event_count;
}

void PipelineExecStatus::completeEvent()
{
    bool notify = false;
    {
        std::lock_guard lock(mu);
        notify = (0 == --active_event_count);
    }
    if (notify)
        cv.notify_all();
}

void PipelineExecStatus::cancel()
{
    is_cancelled = true;
}
} // namespace DB