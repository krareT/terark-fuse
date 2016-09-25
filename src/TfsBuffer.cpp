//
// Created by terark on 9/25/16.
//

#include "TfsBuffer.h"
const  uint64_t ns_per_sec = 1000000000;
terark::llong TfsBuffer::insertToBuf(const std::string &path, mode_t mode) {

    struct timespec time;
    auto ret = clock_gettime(CLOCK_REALTIME, &time);
    if (ret == -1)
        return -errno;

    uint64_t time_ns = time.tv_sec * ns_per_sec + time.tv_nsec;
    auto uid = getuid();
    auto gid = getgid();
    {
        std::lock_guard<std::mutex> _lock(prepare_insert[path]);
        if (buf_map.find(path) != buf_map.end())
            return -EEXIST;
        {
            tbb::reader_writer_lock::scoped_lock(buf_map[path].rw_lock);
            auto &tfs = buf_map.find(path)->second.tfs;

            tfs.atime = tfs.ctime = tfs.mtime = time_ns;
            tfs.path = path;
            tfs.size = 0;
            tfs.gid = gid;
            tfs.uid = uid;
            tfs.mode = mode;
            buf_map[path].ref++;
        }
    }
    prepare_insert.unsafe_erase(path);
    return 1;
}

terark::llong TfsBuffer::release(const std::string &path,terark::db::DbContextPtr ctx) {

    auto iter = buf_map.find(path);
    if (iter == buf_map.end())
        return -ENOENT;
    auto &fi = iter->second;
    terark::llong rid = 0;
    bool erase_flag = false;
    terark::NativeDataOutput<terark::AutoGrownMemIO> rowBuilder;
    {
        tbb::reader_writer_lock::scoped_lock(fi.rw_lock);

        fi.ref--;
        if ( fi.ref.load() == 0){
            //write to terark
            rid = ctx->upsertRow(fi.tfs.encode(rowBuilder));
            erase_flag = true;
        }
    }

    //BUG HERE!!!!!! can not earse!
    return rid;
}
