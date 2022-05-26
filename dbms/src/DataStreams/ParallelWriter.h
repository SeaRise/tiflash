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
#include <Core/Block.h>
#include <common/types.h>

#include <memory>

namespace DB
{
class ParallelWriter
{
public:
    ParallelWriter(
        const String & name_,
        const String & req_id)
        : name(name_)
        , log(Logger::get(name_, req_id))
    {}

    virtual void write(Block & /*block*/, size_t /*thread_num*/) {}

    /// `onFinishThread` and `onFinish` may be called repeatedly.
    virtual void onFinishThread(size_t /*thread_num*/) {}
    virtual void onFinish() {}

    virtual void appendInfo(FmtBuffer & /*buffer*/) const {}

    String getName() const { return name; }

    virtual ~ParallelWriter() = default;

protected:
    const String name;
    const LoggerPtr log;
};

using ParallelWriterPtr = std::shared_ptr<ParallelWriter>;
} // namespace DB