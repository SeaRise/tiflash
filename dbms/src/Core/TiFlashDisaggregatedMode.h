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

#include <Poco/Util/LayeredConfiguration.h>

#include <string>

#define DEF_PROXY_LABEL "tiflash"
#define DISAGGREGATED_MODE_COMPUTE_PROXY_LABEL DISAGGREGATED_MODE_COMPUTE
#define DISAGGREGATED_MODE_STORAGE "tiflash_storage"
#define DISAGGREGATED_MODE_COMPUTE "tiflash_compute"

namespace DB
{
enum class DisaggregatedMode
{
    None,
    Compute,
    Storage,
};

DisaggregatedMode getDisaggregatedMode(const Poco::Util::LayeredConfiguration & config);
bool useAutoScaler(const Poco::Util::LayeredConfiguration & config);
std::string getProxyLabelByDisaggregatedMode(DisaggregatedMode mode);
} // namespace DB
