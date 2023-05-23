#include <Interpreters/Context.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileDataProvider.h>
#include <Storages/DeltaMerge/Remote/RNDataProvider.h>
#include <Storages/DeltaMerge/Remote/RNLocalPageCache.h>
#include <Storages/DeltaMerge/Remote/RNReadTask.h>
#include <Storages/DeltaMerge/Remote/Serializer.h>
#include <Storages/DeltaMerge/Segment.h>

namespace DB::DM::Remote
{

RNReadSegmentTaskPtr RNReadSegmentTask::buildFromEstablishResp(
    const LoggerPtr & log,
    const Context & db_context,
    const ScanContextPtr & scan_context,
    const RemotePb::RemoteSegment & proto,
    const DisaggTaskId & snapshot_id,
    StoreID store_id,
    const String & store_address,
    KeyspaceID keyspace_id,
    TableID physical_table_id)
{
    RowKeyRange segment_range;
    {
        ReadBufferFromString rb(proto.key_range());
        segment_range = RowKeyRange::deserialize(rb);
    }
    RowKeyRanges read_ranges(proto.read_key_ranges_size());
    for (int i = 0; i < proto.read_key_ranges_size(); ++i)
    {
        ReadBufferFromString rb(proto.read_key_ranges(i));
        read_ranges[i] = RowKeyRange::deserialize(rb);
    }

    auto dm_context = std::make_shared<DMContext>(
        db_context,
        /* path_pool */ nullptr,
        /* storage_pool */ nullptr,
        /* min_version */ 0,
        keyspace_id,
        physical_table_id,
        /* is_common_handle */ segment_range.is_common_handle,
        /* rowkey_column_size */ segment_range.rowkey_column_size,
        db_context.getSettingsRef(),
        scan_context);

    auto segment = std::make_shared<Segment>(
        log,
        /*epoch*/ 0,
        segment_range,
        proto.segment_id(),
        /*next_segment_id*/ 0,
        nullptr,
        nullptr);

    auto segment_snap = Serializer::deserializeSegmentSnapshotFrom(
        *dm_context,
        store_id,
        physical_table_id,
        proto);

    // Note: At this moment, we still cannot read from `task->segment_snap`,
    // because they are constructed using ColumnFileDataProviderNop.

    std::vector<UInt64> delta_tinycf_ids;
    std::vector<size_t> delta_tinycf_sizes;
    {
        auto persisted_cfs = segment_snap->delta->getPersistedFileSetSnapshot();
        delta_tinycf_ids.reserve(persisted_cfs->getColumnFileCount());
        delta_tinycf_sizes.reserve(persisted_cfs->getColumnFileCount());
        for (const auto & cfs : persisted_cfs->getColumnFiles())
        {
            if (auto * tiny = cfs->tryToTinyFile(); tiny)
            {
                delta_tinycf_ids.emplace_back(tiny->getDataPageId());
                delta_tinycf_sizes.emplace_back(tiny->getDataPageSize());
            }
        }
    }

    LOG_DEBUG(
        log,
        "Build RNReadSegmentTask, store_id={} keyspace_id={} table_id={} memtable_cfs={} persisted_cfs={}",
        store_id,
        keyspace_id,
        physical_table_id,
        segment_snap->delta->getMemTableSetSnapshot()->getColumnFileCount(),
        segment_snap->delta->getPersistedFileSetSnapshot()->getColumnFileCount());

    return std::shared_ptr<RNReadSegmentTask>(new RNReadSegmentTask(
        RNReadSegmentMeta{
            .physical_table_id = physical_table_id,
            .segment_id = proto.segment_id(),
            .store_id = store_id,

            .keyspace_id = keyspace_id,
            .delta_tinycf_page_ids = delta_tinycf_ids,
            .delta_tinycf_page_sizes = delta_tinycf_sizes,
            .segment = segment,
            .segment_snap = segment_snap,
            .store_address = store_address,

            .read_ranges = read_ranges,
            .snapshot_id = snapshot_id,
            .dm_context = dm_context,
        }));
}

void RNReadSegmentTask::initColumnFileDataProvider(const RNLocalPageCacheGuardPtr & pages_guard)
{
    auto & data_provider = meta.segment_snap->delta->getPersistedFileSetSnapshot()->data_provider;
    RUNTIME_CHECK(std::dynamic_pointer_cast<ColumnFileDataProviderNop>(data_provider));

    auto page_cache = meta.dm_context->db_context.getSharedContextDisagg()->rn_page_cache;
    data_provider = std::make_shared<ColumnFileDataProviderRNLocalPageCache>(
        page_cache,
        pages_guard,
        meta.store_id,
        KeyspaceTableID{meta.keyspace_id, meta.physical_table_id});
}

void RNReadSegmentTask::initInputStream(
    const ColumnDefines & columns_to_read,
    UInt64 read_tso,
    const PushDownFilterPtr & push_down_filter,
    ReadMode read_mode)
{
    RUNTIME_CHECK(input_stream == nullptr);
    input_stream = meta.segment->getInputStream(
        read_mode,
        *meta.dm_context,
        columns_to_read,
        meta.segment_snap,
        meta.read_ranges,
        push_down_filter,
        read_tso,
        DEFAULT_BLOCK_SIZE);
}

} // namespace DB::DM::Remote
