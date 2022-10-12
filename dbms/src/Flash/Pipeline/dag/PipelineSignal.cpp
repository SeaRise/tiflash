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

#include <Flash/Pipeline/dag/PipelineSignal.h>
#include <Flash/Pipeline/dag/PipelineEventQueue.h>

namespace DB
{
void PipelineSignal::finish()
{
    if (1 == active_task_num.fetch_sub(1))
        event_queue->submit(PipelineEvent::finish(pipeline_id));
}

void PipelineSignal::error(const String & err_msg)
{
    event_queue->submitFirst(PipelineEvent::fail(err_msg));
}

} // namespace DB
