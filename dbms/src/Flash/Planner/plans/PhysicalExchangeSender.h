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

#include <Flash/Planner/plans/PhysicalUnary.h>
#include <tipb/executor.pb.h>
#include <tipb/select.pb.h>

namespace DB
{
class PhysicalExchangeSender : public PhysicalUnary
{
public:
    static PhysicalPlanPtr build(
        const String & executor_id,
        const LoggerPtr & log,
        const tipb::ExchangeSender & exchange_sender,
        const PhysicalPlanPtr & child);

    PhysicalExchangeSender(
        const String & executor_id_,
        const NamesAndTypes & schema_,
        const String & req_id,
        const PhysicalPlanPtr & child_,
        const std::vector<Int64> & partition_col_ids_,
        const TiDB::TiDBCollators & collators_,
        const tipb::ExchangeType & exchange_type_)
        : PhysicalUnary(executor_id_, PlanType::ExchangeSender, schema_, req_id, child_)
        , partition_col_ids(partition_col_ids_)
        , partition_col_collators(collators_)
        , exchange_type(exchange_type_)
    {}

    void finalize(const Names & parent_require) override;

    const Block & getSampleBlock() const override;

private:
    void transformImpl(DAGPipeline & pipeline, Context & context, size_t max_streams) override;

    std::vector<Int64> partition_col_ids;
    TiDB::TiDBCollators partition_col_collators;
    tipb::ExchangeType exchange_type;
};
} // namespace DB