#pragma once

#include <Common/CurrentMetrics.h>
#include <Common/FailPoint.h>
#include <Common/ProfileEvents.h>
#include <Poco/Ext/ThreadNumber.h>
#include <Storages/Page/Config.h>
#include <Storages/Page/PageDefines.h>
#include <Storages/Page/V2/PageEntries.h>
#include <Storages/Page/V2/VersionSet/PageEntriesBuilder.h>
#include <Storages/Page/V2/VersionSet/PageEntriesEdit.h>
#include <Storages/Page/V2/VersionSet/PageEntriesView.h>

#include <boost/core/noncopyable.hpp>
#include <cassert>
#include <chrono>
#include <list>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_set>
#include <utility>

namespace CurrentMetrics
{
extern const Metric PSMVCCNumSnapshots;
} // namespace CurrentMetrics

namespace DB::PS::V2
{
class DeltaVersionEditAcceptor;

class PageEntriesVersionSetWithDelta
{
public:
    using EditAcceptor = DeltaVersionEditAcceptor;
    using VersionType = PageEntriesForDelta;
    using VersionPtr = std::shared_ptr<VersionType>;

public:
    explicit PageEntriesVersionSetWithDelta(String name_, const MVCC::VersionSetConfig & config_, Poco::Logger * log_)
        : current(VersionType::createBase())
        , snapshots()
        , config(config_)
        , name(std::move(name_))
        , log(log_)
    {
    }

    ~PageEntriesVersionSetWithDelta()
    {
        current.reset();

        removeExpiredSnapshots();

        // snapshot list is empty
        assert(snapshots.empty());
    }

    void apply(PageEntriesEdit & edit);

    size_t size() const;

    size_t sizeUnlocked() const;

    std::tuple<size_t, double, unsigned> getSnapshotsStat() const;

    std::string toDebugString() const
    {
        std::shared_lock lock(read_write_mutex);
        return versionToDebugString(current);
    }

    static std::string versionToDebugString(VersionPtr tail)
    {
        std::string s;
        bool is_first = true;
        std::stack<VersionPtr> deltas;
        for (auto v = tail; v != nullptr; v = std::atomic_load(&v->prev))
        {
            deltas.emplace(v);
        }
        while (!deltas.empty())
        {
            auto v = deltas.top();
            deltas.pop();
            s += is_first ? "" : "<-";
            is_first = false;
            s += "{\"rc\":";
            s += DB::toString(v.use_count() - 1);
            s += ",\"addr\":", s += DB::ptrToString(v.get());
            s += '}';
        }
        return s;
    }

    /// Snapshot.
    /// When snapshot object is freed, it will call `view.release()` to compact VersionList,
    /// and its weak_ptr in the VersionSet's snapshots list will become empty.
    class Snapshot : private boost::noncopyable
    {
    public:
        PageEntriesVersionSetWithDelta * vset;
        PageEntriesView view;

        using TimePoint = std::chrono::time_point<std::chrono::steady_clock>;
        const unsigned t_id;

    private:
        const TimePoint create_time;

    public:
        Snapshot(PageEntriesVersionSetWithDelta * vset_, VersionPtr tail_)
            : vset(vset_)
            , view(std::move(tail_))
            , t_id(Poco::ThreadNumber::get())
            , create_time(std::chrono::steady_clock::now())
        {
            CurrentMetrics::add(CurrentMetrics::PSMVCCNumSnapshots);
        }

        // Releasing a snapshot object may do compaction on vset's versions.
        ~Snapshot()
        {
            vset->compactOnDeltaRelease(view.getSharedTailVersion());
            // Remove snapshot from linked list

            view.release();

            CurrentMetrics::sub(CurrentMetrics::PSMVCCNumSnapshots);
        }

        const PageEntriesView * version() const { return &view; }

        // The time this snapshot living for
        double elapsedSeconds() const
        {
            auto end = std::chrono::steady_clock::now();
            std::chrono::duration<double> diff = end - create_time;
            return diff.count();
        }

        friend class PageEntriesVersionSetWithDelta;
    };

    using SnapshotPtr = std::shared_ptr<Snapshot>;
    using SnapshotWeakPtr = std::weak_ptr<Snapshot>;

    SnapshotPtr getSnapshot();

    std::pair<std::set<PageFileIdAndLevel>, std::set<PageId>> gcApply(PageEntriesEdit & edit, bool need_scan_page_ids = true);

    /// List all PageFile that are used by any version
    std::pair<std::set<PageFileIdAndLevel>, std::set<PageId>> //
    listAllLiveFiles(std::unique_lock<std::shared_mutex> &&, bool need_scan_page_ids = true);

#ifndef DBMS_PUBLIC_GTEST
private:
#endif
    void appendVersion(VersionPtr && v, const std::unique_lock<std::shared_mutex> & lock);

    enum class RebaseResult
    {
        SUCCESS,
        INVALID_VERSION,
    };

    /// Use after do compact on VersionList, rebase all
    /// successor Version of Version{`old_base`} onto Version{`new_base`}.
    /// Specially, if no successor version of Version{`old_base`}, which
    /// means `current`==`old_base`, replace `current` with `new_base`.
    /// Examples:
    /// ┌────────────────────────────────┬───────────────────────────────────┐
    /// │         Before rebase          │           After rebase            │
    /// ├────────────────────────────────┼───────────────────────────────────┤
    /// │ Va    <-   Vb  <-    Vc        │      Vd     <-   Vc               │
    /// │       (old_base)  (current)    │   (new_base)    (current)         │
    /// ├────────────────────────────────┼───────────────────────────────────┤
    /// │ Va    <- Vb    <-    Vc        │           Vd                      │
    /// │             (current,old_base) │     (current, new_base)           │
    /// └────────────────────────────────┴───────────────────────────────────┘
    /// Caller should ensure old_base is in VersionSet's link
    RebaseResult rebase(const VersionPtr & old_base, const VersionPtr & new_base);

    std::unique_lock<std::shared_mutex> acquireForLock();

    // Return true if `tail` is in current version-list
    bool isValidVersion(const VersionPtr tail) const;

    // If `tail` is in the latest versions-list, do compaction on version-list [head, tail].
    // If there some versions after tail, use vset's `rebase` to concat them.
    void compactOnDeltaRelease(VersionPtr tail);

    // Scan over all `snapshots`, remove the invalid snapshots and get some statistics
    // of all living snapshots and the oldest living snapshot.
    // Return < num of snapshots,
    //          living time(seconds) of the oldest snapshot,
    //          created thread id of the oldest snapshot      >
    std::tuple<size_t, double, unsigned> removeExpiredSnapshots() const;

    void collectLiveFilesFromVersionList( //
        const PageEntriesView & view,
        std::set<PageFileIdAndLevel> & live_files,
        std::set<PageId> & live_normal_pages,
        bool need_scan_page_ids) const;

private:
    mutable std::shared_mutex read_write_mutex;
    VersionPtr current;
    mutable std::list<SnapshotWeakPtr> snapshots;
    const MVCC::VersionSetConfig config;
    const String name;
    Poco::Logger * log;
};

/// Read old entries state from `view_` and apply new edit to `view_->tail`
class DeltaVersionEditAcceptor
{
public:
    explicit DeltaVersionEditAcceptor(const PageEntriesView * view_, //
                                      const String & name_,
                                      bool ignore_invalid_ref_ = false,
                                      Poco::Logger * log_ = nullptr);

    ~DeltaVersionEditAcceptor();

    void apply(PageEntriesEdit & edit);

    static void applyInplace(const String & name,
                             const PageEntriesVersionSetWithDelta::VersionPtr & current,
                             const PageEntriesEdit & edit,
                             Poco::Logger * log);

    void gcApply(PageEntriesEdit & edit) { PageEntriesBuilder::gcApplyTemplate(view, edit, current_version); }

    static void gcApplyInplace( //
        const PageEntriesVersionSetWithDelta::VersionPtr & current,
        PageEntriesEdit & edit)
    {
        assert(current->isBase());
        assert(current.use_count() == 1);
        PageEntriesBuilder::gcApplyTemplate(current, edit, current);
    }

private:
    // Read old state from `view` and apply new edit to `current_version`

    void applyPut(PageEntriesEdit::EditRecord & record);
    void applyDel(PageEntriesEdit::EditRecord & record);
    void applyRef(PageEntriesEdit::EditRecord & record);
    void decreasePageRef(PageId page_id);

private:
    PageEntriesView * view;
    PageEntriesVersionSetWithDelta::VersionPtr current_version;
    bool ignore_invalid_ref;

    const String & name;
    Poco::Logger * log;
};

} // namespace DB::PS::V2
