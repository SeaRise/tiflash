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

#include <Common/FmtUtils.h>
#include <Common/Stopwatch.h>

#include <atomic>

namespace DB
{
template <typename UnitType>
class TaskProfileInfo
{
public:
    UnitType execute_time = 0;
    UnitType execute_pending_time = 0;

    UnitType spill_time = 0;
    UnitType spill_pending_time = 0;

    UnitType await_time = 0;

public:
    void startTimer()
    {
        stopwatch.start();
    }

    UInt64 elapsedFromPrev()
    {
        return stopwatch.elapsedFromLastTime();
    }

    void addExecuteTime(UInt64 value)
    {
        execute_time += value;
    }

    void addExecutePendingTime()
    {
        execute_pending_time += elapsedFromPrev();
    }

    void addSpillTime(UInt64 value)
    {
        spill_time += value;
    }

    void addSpillPendingTime()
    {
        spill_pending_time += elapsedFromPrev();
    }

    void addAwaitTime()
    {
        await_time += elapsedFromPrev();
    }

    template <typename Other>
    void merge(const Other & other)
    {
        execute_time += other.execute_time;
        execute_pending_time += other.execute_pending_time;
        spill_time += other.spill_time;
        spill_pending_time += other.spill_pending_time;
        await_time += other.await_time;
    }

    String toJson() const
    {
        return fmt::format(
            R"({{"execute_time_ns":{},"execute_pending_time_ns":{},"spill_time_ns":{},"spill_pending_time_ns":{},"await_time_ns":{}}})",
            execute_time,
            execute_pending_time,
            spill_time,
            spill_pending_time,
            await_time);
    }

private:
    Stopwatch stopwatch{CLOCK_MONOTONIC_COARSE};
};

using LocalTaskProfileInfo = TaskProfileInfo<UInt64>;
using GlobalTaskProfileInfo = TaskProfileInfo<std::atomic_uint64_t>;

} // namespace DB
