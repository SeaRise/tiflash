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

#include <Common/Exception.h>
#include <Common/MemoryTrackerSetter.h>
#include <Flash/Pipeline/Schedule/Event/Event.h>
#include <Flash/Pipeline/Schedule/TaskScheduler.h>
#include <assert.h>

namespace DB
{
// if any exception throw here, we should pass err msg and cancel the query.
#define CATCH                                            \
    catch (...)                                          \
    {                                                    \
        toError(getCurrentExceptionMessage(true, true)); \
    }

void Event::addDependency(const EventPtr & dependency)
{
    assert(status == EventStatus::INIT);
    dependency->addDependent(shared_from_this());
    ++unfinished_dependencies;
}

bool Event::isNonDependent()
{
    assert(status == EventStatus::INIT);
    return 0 == unfinished_dependencies;
}

void Event::addDependent(const EventPtr & dependent)
{
    assert(status == EventStatus::INIT);
    dependents.push_back(dependent);
}

void Event::onDependencyComplete()
{
    auto cur_value = unfinished_dependencies.fetch_sub(1);
    assert(cur_value >= 1);
    if (1 == cur_value)
        schedule();
}

void Event::schedule() noexcept
{
    switchStatus(EventStatus::INIT, EventStatus::SCHEDULED);
    exec_status.onEventStart();
    MemoryTrackerSetter setter{true, mem_tracker.get()};
    // if err throw here, we should call finish directly.
    bool direct_finish = true;
    try
    {
        // if no task is scheduled here, we can call finish directly.
        direct_finish = scheduleImpl();
    }
    CATCH
    if (direct_finish)
        finish();
}

void Event::finish() noexcept
{
    switchStatus(EventStatus::SCHEDULED, EventStatus::FINISHED);
    MemoryTrackerSetter setter{true, mem_tracker.get()};
    try
    {
        finishImpl();
    }
    CATCH
    // If query has already been cancelled, it will not trigger dependents.
    if (likely(!isCancelled()))
    {
        // finished processing the event, now we can schedule events that depend on this event.
        for (auto & dependent : dependents)
        {
            assert(dependent);
            dependent->onDependencyComplete();
            dependent.reset();
        }
    }
    // Release all dependents, so that the event that did not call `finishImpl`
    // because of `isCancelled()` will be destructured before the end of `exec_status.wait`.
    dependents.clear();
    // In order to ensure that `exec_status.wait()` doesn't finish when there is an active event,
    // we have to call `exec_status.onEventFinish()` here,
    // since `exec_status.onEventStart()` will have been called by dependents.
    // The call order will be `eventA++ ───► eventB++ ───► eventA-- ───► eventB-- ───► exec_status.await finished`.
    exec_status.onEventFinish();
}

void Event::scheduleTasks(std::vector<TaskPtr> & tasks)
{
    assert(!tasks.empty());
    assert(0 == unfinished_tasks);
    unfinished_tasks = tasks.size();
    assert(status != EventStatus::FINISHED);
    TaskScheduler::instance->submit(tasks);
}

void Event::onTaskFinish(const LocalTaskProfileInfo & task_profile_info) noexcept
{
    assert(status != EventStatus::FINISHED);
    exec_status.update(task_profile_info);
    auto cur_value = unfinished_tasks.fetch_sub(1);
    assert(cur_value >= 1);
    if (1 == cur_value)
        finish();
}

void Event::toError(std::string && err_msg)
{
    exec_status.toError(std::move(err_msg));
}

void Event::switchStatus(EventStatus from, EventStatus to)
{
    RUNTIME_CHECK(status.compare_exchange_strong(from, to));
}

#undef CATCH

} // namespace DB
