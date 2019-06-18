#pragma once

#include <mutex>
#include <unordered_map>
#include <vector>

#include <boost/noncopyable.hpp>

#include <Poco/File.h>

#include <IO/HashingWriteBuffer.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <common/logger_useful.h>

#include <Storages/Page/Page.h>
#include <Storages/Page/PageDefines.h>
#include <Storages/Page/WriteBatch.h>

namespace DB
{

/// A light-weight object which can be created and copied cheaply.
/// Use createWriter()/createReader() to open write/read system file.
class PageFile : public Allocator<false>
{
public:
    using Version = UInt32;

    static const Version CURRENT_VERSION;

    /// Writer can NOT be used by multi threads.
    class Writer : private boost::noncopyable
    {
        friend class PageFile;

    public:
        Writer(PageFile &, bool sync_on_write);
        ~Writer();

        void write(const WriteBatch & wb, PageCacheMap & page_cache_map);

    private:
        PageFile & page_file;
        bool       sync_on_write;

        std::string data_file_path;
        std::string meta_file_path;

        int data_file_fd;
        int meta_file_fd;
    };

    /// Reader is safe to used by multi threads.
    class Reader : private boost::noncopyable, private Allocator<false>
    {
        friend class PageFile;

    public:
        explicit Reader(PageFile & page_file);
        ~Reader();

        /// Read pages from files.
        /// After return, the items in to_read could be reordered, but won't be removed or added.
        PageMap read(PageIdAndCaches & to_read);

        void read(PageIdAndCaches & to_read, const PageHandler & handler);

    private:
        std::string data_file_path;
        int         data_file_fd;
    };

    struct Comparator
    {
        bool operator()(const PageFile & lhs, const PageFile & rhs) const
        {
            return std::make_pair(lhs.file_id, lhs.level) < std::make_pair(rhs.file_id, rhs.level);
        }
    };

public:
    /// Create an empty page file.
    PageFile() = default;
    /// Recover a page file from disk.
    static std::pair<PageFile, bool> recover(const std::string & parent_path, const std::string & page_file_name, Logger * log);
    /// Create a new page file.
    static PageFile newPageFile(PageFileId file_id, UInt32 level, const std::string & parent_path, bool is_tmp, Logger * log);
    /// Open an existing page file for read.
    static PageFile openPageFileForRead(PageFileId file_id, UInt32 level, const std::string & parent_path, Logger * log);

    /// Get pages' metadata by this method. Will also update file pos.
    /// Call this method after a page file recovered.
    void readAndSetPageMetas(PageCacheMap & page_caches);

    /// Rename this page file into formal style.
    void setFormal();
    /// Destroy underlying system files.
    void destroy();

    /// Return a writer bound with this PageFile object.
    /// Note that the user MUST keep the PageFile object around before this writer being freed.
    std::unique_ptr<Writer> createWriter(bool sync_on_write) { return std::make_unique<Writer>(*this, sync_on_write); }
    /// Return a reader for this file.
    /// The PageFile object can be released any time.
    std::shared_ptr<Reader> createReader() { return std::make_shared<Reader>(*this); }

    UInt64             getFileId() const { return file_id; }
    UInt32             getLevel() const { return level; }
    PageFileIdAndLevel fileIdLevel() const { return std::make_pair(file_id, level); }
    bool               isValid() const { return file_id; }
    UInt64             getDataFileAppendPos() const { return data_file_pos; }
    UInt64             getDataFileSize() const;

private:
    /// Create a new page file.
    PageFile(PageFileId file_id_, UInt32 level_, const std::string & parent_path, bool is_tmp_, bool is_create, Logger * log);

    std::string folderPath() const
    {
        return parent_path + "/" + (is_tmp ? ".tmp.page_" : "page_") + DB::toString(file_id) + "_" + DB::toString(level);
    }
    std::string dataPath() const { return folderPath() + "/page"; }
    std::string metaPath() const { return folderPath() + "/meta"; }

private:
    UInt64      file_id = 0;     // Valid id start from 1.
    UInt32      level   = 0;     // 0: normal, >= 1: generated by GC.
    bool        is_tmp  = false; // true if currently writen by GC thread.
    std::string parent_path{};   // The parent folder of this page file.

    // The append pos.
    UInt64 data_file_pos = 0;
    UInt64 meta_file_pos = 0;

    Logger * log = nullptr;
};

} // namespace DB
