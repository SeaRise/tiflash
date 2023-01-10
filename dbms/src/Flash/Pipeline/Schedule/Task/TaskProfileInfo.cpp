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

#include <Common/FmtUtils.h>
#include <Flash/Pipeline/Schedule/Task/TaskProfileInfo.h>

namespace DB
{
namespace
{
template <typename ProfileInfo>
String profileInfoToJson(const ProfileInfo & profile_info)
{
    return fmt::format(
        R"({{"execute_time_ns":{},"execute_pending_time_ns":{},"spill_time_ns":{},"spill_pending_time_ns":{},"await_time_ns":{}}})",
        profile_info.execute_time,
        profile_info.execute_pending_time,
        profile_info.spill_time,
        profile_info.spill_pending_time,
        profile_info.await_time);
}
} // namespace

void LocalTaskProfileInfo::startTimer()
{
    stopwatch.start();
}

UInt64 LocalTaskProfileInfo::elapsedFromPrev()
{
    return stopwatch.elapsedFromLastTime();
}

void LocalTaskProfileInfo::addExecuteTime(UInt64 value)
{
    execute_time += value;
}

void LocalTaskProfileInfo::addExecutePendingTime()
{
    execute_pending_time += elapsedFromPrev();
}

void LocalTaskProfileInfo::addSpillTime(UInt64 value)
{
    spill_time += value;
}

void LocalTaskProfileInfo::addSpillPendingTime()
{
    spill_pending_time += elapsedFromPrev();
}

void LocalTaskProfileInfo::addAwaitTime()
{
    await_time += elapsedFromPrev();
}

String LocalTaskProfileInfo::toJson() const
{
    return profileInfoToJson(*this);
}

void GlobalTaskProfileInfo::merge(const LocalTaskProfileInfo & local_one)
{
    execute_time += local_one.execute_time;
    execute_pending_time += local_one.execute_pending_time;
    spill_time += local_one.spill_time;
    spill_pending_time += local_one.spill_pending_time;
    await_time += local_one.await_time;
}

String GlobalTaskProfileInfo::toJson() const
{
    return profileInfoToJson(*this);
}
} // namespace DB
