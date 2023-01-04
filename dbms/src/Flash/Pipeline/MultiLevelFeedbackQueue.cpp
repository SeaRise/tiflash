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

#include <Flash/Pipeline/MultiLevelFeedbackQueue.h>
#include <assert.h>
#include <common/likely.h>

namespace DB
{
void UnitQueue::take(TaskPtr & task)
{
    assert(!task);
    assert(!empty());
    task = std::move(task_queue.front());
    task_queue.pop_front();
    assert(task);
}

bool UnitQueue::empty()
{
    return task_queue.empty();
}

void UnitQueue::submit(TaskPtr && task)
{
    assert(task);
    task_queue.push_back(std::move(task));
}

double UnitQueue::accuTimeAfterDivisor()
{
    assert(factor_for_normal > 0);
    return accu_consume_time / factor_for_normal;
}
} // namespace DB
