//
// Created by terark on 9/25/16.
//

#ifndef TERARK_FUSE_TFSBUFFER_H
#define TERARK_FUSE_TFSBUFFER_H
#include<tbb/concurrent_unordered_map.h>
#include <tbb/reader_writer_lock.h>
#include <mutex>
#include "tfs.h"
#include <cstdio>

struct FileInfo{

    terark::TFS tfs;
    std::atomic_bool update_flag;
    tbb::reader_writer_lock rw_lock;
    FileInfo():update_flag{false}{
        ref.store(0,std::memory_order_relaxed);
    }
    std::atomic<int32_t > ref;
};
class TfsBuffer {

private:
    tbb::concurrent_unordered_map<std::string, std::shared_ptr<FileInfo>> buf_map;
    std::recursive_mutex ctx_mtx;
    terark::db::DbContextPtr ctx;
    uint32_t path_idx_id;
    size_t file_stat_cg_id;
    size_t file_mode_id;
    size_t file_gid_id;
    size_t file_uid_id;
    size_t file_atime_id;
    size_t file_mtime_id;
    size_t file_ctime_id;
    size_t file_content_id;
    terark::db::CompositeTablePtr tab;
    void getSataFromTfs(terark::TFS&,struct stat &st);
    void getSataFromTfsCg(terark::TFS_Colgroup_file_stat &tfs_fs, struct stat &st);
    const char *terark_state = "/terark-state";
public:
    enum class FILE_TYPE {
        NOF,DIR, REG,
    };
    TfsBuffer(const char *db_path);

    //the only method to create new element to buf
    terark::llong insertToBuf(const std::string &path, mode_t mode,bool update = true);

    //load element from terark to buf
    terark::llong loadToBuf(const std::string &path);

    terark::llong release(const std::string &);

    int truncate(const std::string&,size_t new_size);

    FILE_TYPE exist(const std::string &);

    int read(const std::string &, char *buf, size_t size, size_t off);

    size_t write(const std::string &path, const char *buf, size_t size, size_t offset);

    bool getFileInfo(const std::string &path, struct stat &st);

    void compact();

    bool getNextFile(terark::db::IndexIteratorPtr& iip, const std::string &dir,std::string &file_name);
    terark::db::IndexIteratorPtr getDirIter(const std::string & path);

    bool remove(const std::string &path);

    ~TfsBuffer();

private:
    uint64_t getTime();

    long long getRid(const std::string &path);

    FILE_TYPE existInBuf(const std::string &);

    FILE_TYPE existInTerark(const std::string &);

    void writeToTerarkState(const char *buf,const size_t size);
};
#endif //TERARK_FUSE_TFSBUFFER_H
