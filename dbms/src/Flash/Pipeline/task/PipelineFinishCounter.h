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

#include <atomic>
#include <memory>

namespace DB
{
class PipelineFinishCounter
{
public:
    explicit PipelineFinishCounter(int32_t init_num): counter(init_num) {}
   
    // return non-finished task count
    int32_t finish()
    {
        return counter.fetch_sub(1) - 1;
    }

    bool isFinished()
    {
        return 0 == counter;
    }

private:
    std::atomic_int32_t counter;
};

using PipelineFinishCounterPtr = std::shared_ptr<PipelineFinishCounter>;
} // namespace DB