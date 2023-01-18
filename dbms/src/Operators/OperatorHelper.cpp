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
#include <Operators/OperatorHelper.h>

#include <magic_enum.hpp>

namespace DB
{
void assertOperatorStatus(
    OperatorStatus status,
    std::initializer_list<OperatorStatus> expect_running_statuses)
{
    switch (status)
    {
    case OperatorStatus::FINISHED:
    case OperatorStatus::CANCELLED:
    case OperatorStatus::SPILLING:
    case OperatorStatus::WAITING:
        return;
    default:
    {
        for (auto it = expect_running_statuses.begin(); it != expect_running_statuses.end(); ++it)
        {
            if (*it == status)
                return;
        }
        RUNTIME_ASSERT(false, "Unexpected operator status {}", magic_enum::enum_name(status));
    }
    }
}
} // namespace DB
