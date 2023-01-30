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

#include <ext/singleton.h>
#include <mutex>
#include <string>

namespace DB
{
class ClusterIdHolder : public ext::Singleton<ClusterIdHolder>
{
public:
    void init(const std::string & cluster_id_)
    {
        std::lock_guard lock(mu);
        RUNTIME_ASSERT(!initialized, "Cannot be initialized cluster id more than once");
        cluster_id = cluster_id_;
    }

    void initDefault()
    {
        init("default");
    }

    const std::string & get() const
    {
        std::lock_guard lock(mu);
        RUNTIME_ASSERT(initialized, "cluster id not yet initialized");
        return cluster_id;
    }

private:
    mutable std::mutex mu;
    std::string cluster_id;
    bool initialized = false;
};
} // namespace DB
