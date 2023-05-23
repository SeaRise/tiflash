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
#include <Core/Block.h>

#include <memory>

namespace DB
{
/// Information for Operator profiling
struct OperatorProfile
{
    Stopwatch total_stopwatch{CLOCK_MONOTONIC_COARSE};

    size_t rows = 0;
    size_t blocks = 0;
    size_t bytes = 0;
    size_t allocated_bytes = 0;
    // execution time is the total time spent on current Operator
    UInt64 execution_time = 0;

    ALWAYS_INLINE void anchor()
    {
        total_stopwatch.start();
    }

    ALWAYS_INLINE void update(const Block & block)
    {
        if likely (block)
        {
            ++blocks;
            rows += block.rows();
            bytes += block.bytes();
            allocated_bytes += block.allocatedBytes();
        }
        execution_time += total_stopwatch.elapsedFromLastTime();
    }

    ALWAYS_INLINE void update()
    {
        execution_time += total_stopwatch.elapsedFromLastTime();
    }
};

using OperatorProfilePtr = std::shared_ptr<OperatorProfile>;

} // namespace DB
