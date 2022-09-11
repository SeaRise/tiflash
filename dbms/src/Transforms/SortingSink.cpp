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

#include <Transforms/SortingSink.h>

namespace DB
{
bool SortingSink::write(Block & block, size_t)
{
    if (!block)
        return false;

    sortBlock(block, description, limit);
    local_blocks.emplace_back(std::move(block));
    return true;
}

bool SortingSink::finish()
{
    sort_breaker->add(std::move(local_blocks));
    return true;
}
} // namespace DB
