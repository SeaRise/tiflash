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

#include <Common/Exception.h>
#include <Flash/Planner/PhysicalPlan.h>
#include <common/logger_useful.h>
#include <fmt/format.h>

namespace DB
{
/**
 * A physical plan node with a left and right child.
 */
class PhysicalBinary : public PhysicalPlan
{
public:
    PhysicalBinary(
        const String & executor_id_,
        const PlanType & type_,
        const NamesAndTypes & schema_,
        const String & req_id,
        const PhysicalPlanPtr & left_,
        const PhysicalPlanPtr & right_)
        : PhysicalPlan(executor_id_, type_, schema_, req_id)
        , left(left_)
        , right(right_)
    {}

    PhysicalPlanPtr children(size_t i) const override
    {
        RUNTIME_ASSERT(i <= 1, log, "child_index({}) should not >= childrenSize({})", i, childrenSize());
        assert(left && right);
        return i == 0 ? left : right;
    }

    void setChild(size_t i, const PhysicalPlanPtr & new_child) override
    {
        RUNTIME_ASSERT(i <= 1, log, "child_index({}) should not >= childrenSize({})", i, childrenSize());
        assert(new_child);
        assert(new_child.get() != this);
        auto & child = i == 0 ? left : right;
        child = new_child;
    }

    size_t childrenSize() const override { return 2; };

protected:
    PhysicalPlanPtr left;
    PhysicalPlanPtr right;
};
} // namespace DB
