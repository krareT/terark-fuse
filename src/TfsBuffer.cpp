//
// Created by terark on 9/25/16.
//

#include "TfsBuffer.h"
const  uint64_t ns_per_sec = 1000000000;
terark::llong TfsBuffer::insertToBuf(const std::string &path, mode_t mode) {

    std::lock_guard<std::mutex> _lock(insert_mtx);
    if (buf_map.find(path) != buf_map.end())
        return -EEXIST;
    struct timespec time;
    auto ret = clock_gettime(CLOCK_REALTIME, &time);
    if (ret == -1)
        return -errno;
    uint64_t time_ns = time.tv_sec * ns_per_sec + time.tv_nsec;
    terark::db::DbContextPtr ctx;

    auto info_ptr = std::shared_ptr<FileInfo>(new FileInfo, [ctx](FileInfo* fi){

        if (fi->update_flag.load(std::memory_order_relaxed)){
            terark::NativeDataOutput<terark::AutoGrownMemIO> row_builder;
            auto rid = ctx->upsertRow(fi->tfs.encode(row_builder));
            //TODO:what should i do if upsertRow failed.
        }
        delete fi;
    });
    buf_map[path] = info_ptr;
}

terark::llong TfsBuffer::release(const std::string &path) {

    buf_map.unsafe_erase(path);
    return 0;

}

bool TfsBuffer::exist(const std::string &path) {

    return buf_map.find(path) != buf_map.end();
}

int TfsBuffer::read(const std::string &path, char *buf, size_t size, size_t offset) {

    assert(exist(path));
    auto ptr = buf_map[path];
    {
        auto &content = ptr->tfs.content;
        tbb::reader_writer_lock::scoped_lock _lock(ptr->rw_lock);
        if (offset < content.size()) {
            if (offset + size > content.size())
                size = content.size() - offset;
            memcpy(buf, content.data() + offset, size);
        } else {
            size = 0;
        }
        ptr->tfs.atime = getTime();
    }
    return 0;
}

uint64_t TfsBuffer::getTime() {

    struct timespec ts;
    clock_gettime(CLOCK_REALTIME,&ts);
    return ts.tv_sec * ns_per_sec + ts.tv_nsec;

}
