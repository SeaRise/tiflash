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

#include <Flash/Pipeline/Utils.h>

#include <thread>

namespace DB
{
size_t doCpuPart()
{
    size_t count = 0;
    size_t seed = 1 + (random() % 100);
    for (size_t i = 0; i < 19999999; ++i)
        count += (i % seed);
    return count;
}

void doIOPart()
{
    std::this_thread::sleep_for(std::chrono::milliseconds(800));
}
} // namespace DB
