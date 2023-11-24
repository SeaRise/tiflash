// Copyright 2023 PingCAP, Inc.
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

#include <Flash/Mpp/MPPTaskId.h>
#include <common/likely.h>
#include <fmt/core.h>

namespace DB
{
bool isOldVersion(const MPPQueryId & mpp_query_id)
{
    return mpp_query_id.query_ts == 0 && mpp_query_id.local_query_id == 0 && mpp_query_id.server_id == 0;
}


bool MPPQueryId::operator<(const MPPQueryId & mpp_query_id) const
{
    // compare with MPP query generated by TiDB that version less than v6.6
    bool left_old_version = isOldVersion(*this);
    bool right_old_version = isOldVersion(mpp_query_id);
    if (unlikely(left_old_version && right_old_version))
    {
        return start_ts < mpp_query_id.start_ts;
    }
    if (unlikely(left_old_version))
    {
        return true;
    }
    if (unlikely(right_old_version))
    {
        return false;
    }
    // compare with MPP query generated by TiDB that version after v6.6
    if (query_ts != mpp_query_id.query_ts)
    {
        return query_ts < mpp_query_id.query_ts;
    }
    if (server_id == mpp_query_id.server_id)
    {
        return local_query_id < mpp_query_id.local_query_id;
    }
    // now we can't compare reasonably, just choose one randomly by hash.
    auto lhash = MPPQueryIdHash()(*this);
    auto rhash = MPPQueryIdHash()(mpp_query_id);
    if (lhash != rhash)
    {
        return lhash < rhash;
    }
    // hash values are same, just compare the rest fields.
    if (local_query_id != mpp_query_id.local_query_id)
    {
        return local_query_id < mpp_query_id.local_query_id;
    }
    return server_id < mpp_query_id.server_id;
}
bool MPPQueryId::operator==(const MPPQueryId & rid) const
{
    return query_ts == rid.query_ts && local_query_id == rid.local_query_id && server_id == rid.server_id
        && start_ts == rid.start_ts;
}
bool MPPQueryId::operator!=(const MPPQueryId & rid) const
{
    return !(*this == rid);
}
bool MPPQueryId::operator<=(const MPPQueryId & rid) const
{
    return *this < rid || *this == rid;
}

size_t MPPQueryIdHash::operator()(MPPQueryId const & mpp_query_id) const noexcept
{
    if (unlikely(isOldVersion(mpp_query_id)))
    {
        return std::hash<UInt64>()(mpp_query_id.start_ts);
    }
    return std::hash<UInt64>()(mpp_query_id.query_ts) ^ std::hash<UInt64>()(mpp_query_id.local_query_id)
        ^ std::hash<UInt64>()(mpp_query_id.server_id);
}

bool MPPGatherId::operator==(const MPPGatherId & rid) const
{
    return gather_id == rid.gather_id && query_id == rid.query_id;
}

size_t MPPGatherIdHash::operator()(MPPGatherId const & mpp_gather_id) const noexcept
{
    return MPPQueryIdHash()(mpp_gather_id.query_id) ^ std::hash<UInt64>()(mpp_gather_id.gather_id);
}

String MPPTaskId::toString() const
{
    return isUnknown() ? "MPP<gather_id:N/A,task_id:N/A>"
                       : fmt::format("MPP<{},task_id:{}>", gather_id.toString(), task_id);
}

const MPPTaskId MPPTaskId::unknown_mpp_task_id = MPPTaskId{};

constexpr UInt64 MAX_UINT64 = std::numeric_limits<UInt64>::max();
const MPPQueryId MPPTaskId::Max_Query_Id = MPPQueryId(MAX_UINT64, MAX_UINT64, MAX_UINT64, MAX_UINT64, "", 0, "");

bool operator==(const MPPTaskId & lid, const MPPTaskId & rid)
{
    return lid.gather_id == rid.gather_id && lid.task_id == rid.task_id;
}
} // namespace DB
