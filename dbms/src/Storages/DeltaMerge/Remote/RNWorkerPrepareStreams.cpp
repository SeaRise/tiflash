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

#include <Storages/DeltaMerge/Remote/RNReadTask.h>
#include <Storages/DeltaMerge/Remote/RNWorkerPrepareStreams.h>

namespace DB::DM::Remote
{

RNReadSegmentTaskPtr RNWorkerPrepareStreams::doWork(const RNReadSegmentTaskPtr & task)
{
    task->initInputStream(
        *columns_to_read,
        read_tso,
        push_down_filter,
        read_mode);

    return task;
}

} // namespace DB::DM::Remote
