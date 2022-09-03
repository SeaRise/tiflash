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

#include <Common/Logger.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Interpreters/AggregateStore.h>

namespace DB
{
class FinalAggregatingBlockInputStream : public IProfilingBlockInputStream
{
    static constexpr auto NAME = "FinalAggregating";

public:
    FinalAggregatingBlockInputStream(
        const AggregateStorePtr & aggregate_store_,
        const String & req_id)
        : aggregate_store(aggregate_store_)
        , log(Logger::get(NAME, req_id))
    {}

    String getName() const override { return NAME; }

    Block getHeader() const override;

protected:
    Block readImpl() override;
    void readPrefixImpl() override;

    AggregateStorePtr aggregate_store;

    const LoggerPtr log;

    /** From here we get the finished blocks after the aggregation.
      */
    std::unique_ptr<IBlockInputStream> impl;
};
} // namespace DB
