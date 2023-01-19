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

#pragma once

#include <Common/Stopwatch.h>

#include <atomic>

namespace DB
{
#define PROFILE_MEMBER(UnitType)       \
    UnitType execute_time = 0;         \
    UnitType execute_pending_time = 0; \
    UnitType spill_time = 0;           \
    UnitType spill_pending_time = 0;   \
    UnitType await_time = 0;

class LocalTaskProfileInfo
{
public:
    PROFILE_MEMBER(UInt64)

public:
    void startTimer();

    UInt64 elapsedFromPrev();

    void addExecuteTime(UInt64 value);

    void elapsedExecutePendingTime();

    void addSpillTime(UInt64 value);

    void elapsedSpillPendingTime();

    void elapsedAwaitTime();

    String toJson() const;

private:
    Stopwatch stopwatch{CLOCK_MONOTONIC_COARSE};
};

class GlobalTaskProfileInfo
{
public:
    PROFILE_MEMBER(std::atomic_uint64_t)

public:
    void merge(const LocalTaskProfileInfo & local_one);

    String toJson() const;
};

#undef PROFILE_MEMBER

} // namespace DB
