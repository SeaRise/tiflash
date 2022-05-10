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

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Interpreters/AggregateStore.h>

namespace DB
{
class FinalAggregatingBlockInputStream : public IProfilingBlockInputStream
{
    static constexpr auto NAME = "FinalAggregating";

public:
    FinalAggregatingBlockInputStream(
        const BlockInputStreamPtr & input,
        const AggregateStorePtr & aggregate_store_,
        const String & req_id);

    String getName() const override { return NAME; }

    Block getHeader() const override { return aggregate_store->getHeader(); }

protected:
    Block readImpl() override;

private:
    void prepare();

private:
    const LoggerPtr log;

    AggregateStorePtr aggregate_store;

    /** From here we get the finished blocks after the aggregation.
      */
    std::unique_ptr<IBlockInputStream> impl;

    std::atomic<bool> prepared{false};
};
} // namespace DB
