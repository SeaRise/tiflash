// Copyright 2023 PingCAP, Ltd.
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

#include <Flash/Coprocessor/RuntimeFilterMgr.h>
#include <Flash/Executor/PipelineExecutorStatus.h>
#include <Flash/Pipeline/Schedule/Tasks/Task.h>
#include <Storages/DeltaMerge/ReadThread/SegmentReadTaskScheduler.h>
#include <Storages/DeltaMerge/SegmentReadTaskPool.h>

namespace DB
{
class RFWaitTask : public Task
{
public:
    RFWaitTask(
        const String & req_id,
        PipelineExecutorStatus & exec_status_,
        const DM::SegmentReadTaskPoolPtr & task_pool_,
        int max_wait_time_ms,
        RuntimeFilteList && waiting_rf_list_,
        RuntimeFilteList && ready_rf_list_)
        : Task(nullptr, req_id)
        , exec_status(exec_status_)
        , task_pool(task_pool_)
        , max_wait_time_ns(max_wait_time_ms < 0 ? 0 : 1000000UL * max_wait_time_ms)
        , waiting_rf_list(std::move(waiting_rf_list_))
        , ready_rf_list(std::move(ready_rf_list_))
    {
        exec_status.incActiveRefCount();
    }

    ~RFWaitTask() override
    {
        // In order to ensure that `PipelineExecutorStatus` will not be destructed before `RFWaitTask` is destructed.
        exec_status.decActiveRefCount();
    }

    static void filterAndMoveReadyRfs(RuntimeFilteList & waiting_rf_list, RuntimeFilteList & ready_rf_list)
    {
        for (auto it = waiting_rf_list.begin(); it != waiting_rf_list.end();)
        {
            if ((*it)->isReady())
            {
                ready_rf_list.push_back(std::move((*it)));
                it = waiting_rf_list.erase(it);
            }
            else if ((*it)->isFailed())
            {
                it = waiting_rf_list.erase(it);
            }
            else
            {
                ++it;
            }
        }
    }

    static void submitReadyRfsAndSegmentTaskPool(const RuntimeFilteList & ready_rf_list, const DM::SegmentReadTaskPoolPtr & task_pool)
    {
        for (const RuntimeFilterPtr & rf : ready_rf_list)
        {
            auto rs_operator = rf->parseToRSOperator(task_pool->getColumnToRead());
            task_pool->appendRSOperator(rs_operator);
        }
        DM::SegmentReadTaskScheduler::instance().add(task_pool);
    }

private:
    ExecTaskStatus executeImpl() override
    {
        return ExecTaskStatus::WAITING;
    }

    ExecTaskStatus awaitImpl() override
    {
        if unlikely (exec_status.isCancelled())
            return ExecTaskStatus::CANCELLED;
        try
        {
            filterAndMoveReadyRfs(waiting_rf_list, ready_rf_list);
            if (waiting_rf_list.empty() || stopwatch.elapsed() >= max_wait_time_ns)
            {
                submitReadyRfsAndSegmentTaskPool(ready_rf_list, task_pool);
                return ExecTaskStatus::FINISHED;
            }
            return ExecTaskStatus::WAITING;
        }
        catch (...)
        {
            exec_status.onErrorOccurred(std::current_exception());
            return ExecTaskStatus::ERROR;
        }
    }

private:
    PipelineExecutorStatus & exec_status;

    DM::SegmentReadTaskPoolPtr task_pool;

    UInt64 max_wait_time_ns;
    RuntimeFilteList waiting_rf_list;
    RuntimeFilteList ready_rf_list;

    Stopwatch stopwatch{CLOCK_MONOTONIC_COARSE};
};
} // namespace DB
