//
// Created by terark on 9/25/16.
//

#include <fcntl.h>
#include <thread>
#include "TfsBuffer.h"
const  uint64_t ns_per_sec = 1000000000;

terark::llong TfsBuffer::insertToBuf(const std::string &path, mode_t mode) {
    std::lock_guard<std::recursive_mutex> _lock(ctx_mtx);
    if (buf_map.find(path) != buf_map.end())
        return -EEXIST;
    struct timespec time;

    uint64_t time_ns = getTime();
    auto context = ctx;
    auto info_ptr = std::shared_ptr<FileInfo>(new FileInfo, [context](FileInfo* fi){

        if (fi->update_flag.load(std::memory_order_relaxed)){
            terark::NativeDataOutput<terark::AutoGrownMemIO> row_builder;
            auto rid = context->upsertRow(fi->tfs.encode(row_builder));
            //TODO:what should i do if upsertRow failed.
        }
        delete fi;
        fi = nullptr;
    });
    buf_map[path] = info_ptr;
}

terark::llong TfsBuffer::release(const std::string &path) {
    assert(existInBuf(path));
    std::lock_guard<std::recursive_mutex> _lock(ctx_mtx);
    auto fi_ptr = buf_map[path];
    tbb::reader_writer_lock::scoped_lock __lock(fi_ptr->rw_lock);
    fi_ptr->ref --;

    if ( fi_ptr->ref.load(std::memory_order_relaxed) == 0)
        buf_map.unsafe_erase(path);
    return 0;
}

bool TfsBuffer::existInBuf(const std::string &path) {
    return buf_map.find(path) != buf_map.end();
}

int TfsBuffer::read(const std::string &path, char *buf, size_t size, size_t offset) {
    assert(existInBuf(path));

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

terark::llong TfsBuffer::loadToBuf(const std::string &path) {

    assert(!existInBuf(path));
    std::lock_guard<std::recursive_mutex> _lock(ctx_mtx);
    auto context = ctx;
    auto info_ptr = std::shared_ptr<FileInfo>(new FileInfo, [context](FileInfo* fi){

        if (fi->update_flag.load(std::memory_order_relaxed)){
            terark::NativeDataOutput<terark::AutoGrownMemIO> row_builder;
            auto rid = context->upsertRow(fi->tfs.encode(row_builder));
            //TODO:what should i do if upsertRow failed.
        }
        delete fi;
        fi = nullptr;
    });

    terark::valvec<terark::byte> row;
    auto rid = getRid(path);
    ctx->getValue(rid,&row);
    info_ptr->tfs.decode(row);
    buf_map[path] = info_ptr;
    return 0;
}

long long TfsBuffer::getRid(const std::string &path) {

    std::lock_guard<std::recursive_mutex> _lock(ctx_mtx);
    terark::valvec<terark::llong> ridvec;
    std::string path_str = path;
    ctx->indexSearchExact(path_idx_id, path_str, &ridvec);
    if ( ridvec.size() != 0)
        return *std::max_element(ridvec.begin(),ridvec.end());

    path_str.push_back('/');
    ctx->indexSearchExact(path_idx_id, path_str, &ridvec);
    if ( ridvec.size() != 0)
        return *std::max_element(ridvec.begin(),ridvec.end());
    return -1;
}

TfsBuffer::TfsBuffer(const char *db_path) {
    tab = terark::db::CompositeTable::open(db_path);
    assert(tab != nullptr);
    ctx = tab->createDbContext();
    assert(ctx != nullptr);

    path_idx_id = tab->getIndexId("path");
    file_stat_cg_id = tab->getColgroupId("file_stat");
    file_mode_id = tab->getColumnId("mode");
    file_uid_id = tab->getColumnId("uid");
    file_gid_id = tab->getColumnId("gid");
    file_atime_id = tab->getColumnId("atime");
    file_ctime_id = tab->getColumnId("ctime");
    file_mtime_id = tab->getColumnId("mtime");
    file_content_id = tab->getColumnId("content");

    assert(path_idx_id < tab->getIndexNum());
    assert(file_stat_cg_id < tab->getColgroupNum());
    assert(file_mode_id < tab->getColumnNum());
    assert(file_uid_id < tab->getColumnNum());
    assert(file_gid_id < tab->getColumnNum());
    assert(file_atime_id < tab->getColumnNum());
    assert(file_ctime_id < tab->getColumnNum());
    assert(file_mtime_id < tab->getColumnNum());
    assert(file_content_id < tab->getColumnNum());

    std::lock_guard<std::recursive_mutex>  _lock(ctx_mtx);
    //create root dict : "/"
    if (false == ctx->indexKeyExists(path_idx_id, "/")) {
        auto ret = this->insertToBuf("/", 0666 | S_IFDIR);
        assert(ret == 0);
    }
}

size_t TfsBuffer::write(const std::string &path, const char *buf, size_t size, size_t offset) {
    assert(existInBuf(path));
    tbb::reader_writer_lock::scoped_lock _lock(buf_map[path]->rw_lock);
    terark::TFS &tfs = buf_map[path]->tfs;

    if (offset + size > tfs.content.size()) {
        tfs.content.resize(offset + size);
    }
    tfs.content.replace(offset, size, buf, size);
    tfs.size = tfs.content.size();
    tfs.mtime = getTime();
    return size;
}

void TfsBuffer::compact() {
    std::thread t([](TfsBuffer *tb){
        tb->tab->compact();
    },this);
    t.detach();
}

terark::db::IndexIteratorPtr TfsBuffer::getIter() {
    std::lock_guard<std::recursive_mutex> _lock(ctx_mtx);
    return tab->createIndexIterForward(path_idx_id,ctx.get());
}

uint8_t TfsBuffer::exist(const std::string &path) {
    if (path == "/")
        return DIR;
    if ( existInBuf(path))
        return REG;
    return existInTerark(path);
}

bool TfsBuffer::getFileInfo(const std::string &path,struct stat &st) {

    assert(exist(path));


    auto iter = buf_map.find(path);
    if (iter != buf_map.end()){

        tbb::reader_writer_lock::scoped_lock_read(iter->second->rw_lock);
        auto &tfs = iter->second->tfs;
        st.st_mode = tfs.mode;
        st.st_atim.tv_sec = tfs.atime / ns_per_sec;
        st.st_atim.tv_nsec = tfs.atime % ns_per_sec;
        st.st_ctim.tv_sec = tfs.ctime / ns_per_sec;
        st.st_ctim.tv_nsec = tfs.ctime % ns_per_sec;
        st.st_mtim.tv_sec = tfs.mtime / ns_per_sec;
        st.st_mtim.tv_nsec = tfs.mtime % ns_per_sec;
        st.st_nlink = tfs.nlink;
        st.st_blocks = tfs.blocks;
        st.st_gid = tfs.gid;
        st.st_uid = tfs.uid;
        st.st_size = tfs.size;
        st.st_ino = tfs.ino;
        return true;
    }
    auto rid = getRid(path);
    terark::TFS tfs;
    terark::TFS_Colgroup_file_stat tfs_fs;
    terark::valvec<terark::byte> cg_data;
    {
        std::lock_guard<std::recursive_mutex> _lock(ctx_mtx);
        ctx->selectOneColgroup(rid, file_stat_cg_id, &cg_data);
    }
    if (cg_data.size() == 0) {
        return false;
    }
    tfs_fs.decode(cg_data);
    st.st_mode = tfs.mode;
    st.st_atim.tv_sec = tfs.atime / ns_per_sec;
    st.st_atim.tv_nsec = tfs.atime % ns_per_sec;
    st.st_ctim.tv_sec = tfs.ctime / ns_per_sec;
    st.st_ctim.tv_nsec = tfs.ctime % ns_per_sec;
    st.st_mtim.tv_sec = tfs.mtime / ns_per_sec;
    st.st_mtim.tv_nsec = tfs.mtime % ns_per_sec;
    st.st_nlink = tfs.nlink;
    st.st_blocks = tfs.blocks;
    st.st_gid = tfs.gid;
    st.st_uid = tfs.uid;
    st.st_size = tfs.size;
    st.st_ino = tfs.ino;

    return true;
}

bool TfsBuffer::remove(const std::string &path) {
    if ( !existInTerark(path))
        return false;
    std::lock_guard<std::recursive_mutex> _lock(ctx_mtx);
    ctx->removeRow(getRid(path));
    return true;
}

bool TfsBuffer::existInTerark(const std::string &path) {

    std::lock_guard<std::recursive_mutex> _lock(ctx_mtx);
    if (ctx->indexKeyExists(path_idx_id, path))
        return path.back() == '/' ?DIR:REG;
    if (path.back() == '/') {
        return ctx->indexKeyExists(path_idx_id, path.substr(0, path.size() - 1)) ? REG: NOF;
    } else {
        return ctx->indexKeyExists(path_idx_id, path + "/") ?DIR: NOF;
    }
}
