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

#include <common/types.h>

namespace DB
{
struct PlanType
{
    enum PlanTypeEnum
    {
        Aggregation = 0x1,
        ExchangeReceiver = 0x2,
        ExchangeSender = 0x3,
        Limit = 0x4,
        Projection = 0x5,
        Selection = 0x6,
        Source = 0x7,
        TopN = 0x8,
    };
    PlanTypeEnum enum_value; // 枚举值

    PlanType(int value = 0) // NOLINT(google-explicit-constructor)
        : enum_value(static_cast<PlanTypeEnum>(value))
    {}

    PlanType & operator=(int value)
    {
        this->enum_value = static_cast<PlanTypeEnum>(value);
        return *this;
    }

    operator int() const
    {
        return this->enum_value;
    }

    String toString() const;
};
} // namespace DB
