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

#include <Storages/DeltaMerge/SegmentReadTaskPool.h>
#include <Transforms/Source.h>

#include <mutex>

namespace DB
{
// copy from UnorderedInputStream
class UnorderSource : public Source
{
public:
    explicit UnorderSource(
        const DM::SegmentReadTaskPoolPtr & task_pool_,
        const DM::ColumnDefines & columns_to_read_,
        const int extra_table_id_index,
        const TableID physical_table_id,
        const String & req_id)
        : task_pool(task_pool_)
        , header(toEmptyBlock(columns_to_read_))
        , extra_table_id_index(extra_table_id_index)
        , physical_table_id(physical_table_id)
        , log(Logger::get("UnorderSource", req_id))
        , ref_no(0)
        , task_pool_added(false)
    {
        if (extra_table_id_index != InvalidColumnID)
        {
            auto & extra_table_id_col_define = DM::getExtraTableIDColumnDefine();
            ColumnWithTypeAndName col{extra_table_id_col_define.type->createColumn(), extra_table_id_col_define.type, extra_table_id_col_define.name, extra_table_id_col_define.id, extra_table_id_col_define.default_value};
            header.insert(extra_table_id_index, col);
        }
        ref_no = task_pool->increaseUnorderedInputStreamRefCount();
        LOG_FMT_DEBUG(log, "Created, pool_id={} ref_no={}", task_pool->poolId(), ref_no);
    }

    ~UnorderSource()
    {
        task_pool->decreaseUnorderedInputStreamRefCount();
        LOG_FMT_DEBUG(log, "Destroy, pool_id={} ref_no={}", task_pool->poolId(), ref_no);
    }

    std::pair<bool, Block> read() override
    {
        std::lock_guard lock(mutex);
        if (done)
            return {true, {}};
        Block res;
        std::swap(res, io_block);
        if (extra_table_id_index != InvalidColumnID)
        {
            auto & extra_table_id_col_define = DM::getExtraTableIDColumnDefine();
            ColumnWithTypeAndName col{{}, extra_table_id_col_define.type, extra_table_id_col_define.name, extra_table_id_col_define.id};
            size_t row_number = res.rows();
            auto col_data = col.type->createColumnConst(row_number, Field(physical_table_id));
            col.column = std::move(col_data);
            res.insert(extra_table_id_index, std::move(col));
        }
        return {true, std::move(res)};
    }

    bool isIOReady() override
    {
        std::lock_guard lock(mutex);
        if (done || io_block)
            return true;
        addReadTaskPoolToScheduler();
        while (true)
        {
            Block res;
            if (!task_pool->tryPopBlock(res))
                return false;
            if (res)
            {
                if (!res.rows())
                {
                    continue;
                }
                else
                {
                    io_block = std::move(res);
                    return true;
                }
            }
            else
            {
                done = true;
                return true;
            }
        }
    }

    Block getHeader() const override
    {
        return header;
    }

private:
    void addReadTaskPoolToScheduler()
    {
        if (likely(task_pool_added))
        {
            return;
        }
        std::call_once(task_pool->addToSchedulerFlag(), [&]() { DM::SegmentReadTaskScheduler::instance().add(task_pool); });
        task_pool_added = true;
    }

private:
    DM::SegmentReadTaskPoolPtr task_pool;
    Block header;
    // position of the ExtraPhysTblID column in column_names parameter in the StorageDeltaMerge::read function.
    const int extra_table_id_index;
    bool done = false;
    Block io_block;
    TableID physical_table_id;
    LoggerPtr log;
    int64_t ref_no;
    bool task_pool_added;

    std::mutex mutex;
};
} // namespace DB
