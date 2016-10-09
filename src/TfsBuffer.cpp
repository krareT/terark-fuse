//
// Created by terark on 9/25/16.
//

#include <fcntl.h>
#include <thread>
#include <iostream>
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
            assert(rid >= 0);
            //TODO:what should i do if upsertRow failed.
        }
        delete fi;
        fi = nullptr;
    });
    info_ptr->tfs.path = path;
    info_ptr->tfs.mode = mode;
    info_ptr->tfs.ctime = info_ptr->tfs.atime = info_ptr->tfs.mtime = time_ns;
    info_ptr->tfs.gid = getgid();
    info_ptr->tfs.uid = getuid();
    info_ptr->tfs.size = 0;
    info_ptr->ref++;
    info_ptr->update_flag.store(true);
    info_ptr->tfs.nlink = 0;
    info_ptr->tfs.blocks = 0;
    info_ptr->tfs.ino = 0;
    
    buf_map[path] = info_ptr;
    std::cout << "Insert to buf:" << path << std::endl;
    return buf_map.find(path) != buf_map.end();
}

terark::llong TfsBuffer::release(const std::string &path) {
    assert(existInBuf(path) != FILE_TYPE::NOF);
    std::lock_guard<std::recursive_mutex> _lock(ctx_mtx);
    auto fi_ptr = buf_map[path];
    tbb::reader_writer_lock::scoped_lock __lock(fi_ptr->rw_lock);
    fi_ptr->ref --;
    std::cout << "release:" << path << " " << fi_ptr->ref.load(std::memory_order_relaxed) << std::endl;
    if ( fi_ptr->ref.load(std::memory_order_relaxed) <= 0) {

            buf_map.unsafe_erase(path);
    }
    return 0;
}

TfsBuffer::FILE_TYPE TfsBuffer::existInBuf(const std::string &path) {
    return buf_map.find(path) != buf_map.end() ? FILE_TYPE::REG:FILE_TYPE::NOF;
}

int TfsBuffer::read(const std::string &path, char *buf, size_t size, size_t offset) {
    assert(existInBuf(path) != FILE_TYPE::NOF);

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

    assert(existInBuf(path) == FILE_TYPE::NOF);
    std::lock_guard<std::recursive_mutex> _lock(ctx_mtx);
    auto context = ctx;
    auto info_ptr = std::shared_ptr<FileInfo>(new FileInfo, [context](FileInfo* fi){

        if (fi->update_flag.load(std::memory_order_relaxed)){
            terark::NativeDataOutput<terark::AutoGrownMemIO> row_builder;
            auto rid = context->upsertRow(fi->tfs.encode(row_builder));
            //TODO:what should i do if upsertRow failed.
            assert(rid >= 0);
        }
        delete fi;
        fi = nullptr;
    });

    terark::valvec<terark::byte> row;
    auto rid = getRid(path);
    ctx->getValue(rid,&row);
    info_ptr->tfs.decode(row);
    info_ptr->ref++;
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
        release("/");
        assert(getRid("/") >= 0);
    }
    terark_state_tfs.path = "/terark-state";
    terark_state_tfs.mode = 0666 | S_IFDIR;
    terark_state_tfs.gid = getgid();
    terark_state_tfs.uid = getuid();
    terark_state_tfs.atime = terark_state_tfs.ctime = terark_state_tfs.mtime = getTime();

}

size_t TfsBuffer::write(const std::string &path, const char *buf, size_t size, size_t offset) {
    assert(existInBuf(path) != FILE_TYPE::NOF);
    tbb::reader_writer_lock::scoped_lock _lock(buf_map[path]->rw_lock);
    terark::TFS &tfs = buf_map[path]->tfs;
    buf_map[path]->update_flag.store(true,std::memory_order_relaxed);
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



TfsBuffer::FILE_TYPE TfsBuffer::exist(const std::string &path) {
    if (path == "/")
        return FILE_TYPE::DIR;
    if (path == terark_state_tfs.path)
        return FILE_TYPE::REG;
    if ( existInBuf(path) == FILE_TYPE::REG) {
        return FILE_TYPE::REG;
    }
    else{
        return existInTerark(path);
    }
}

bool TfsBuffer::getFileInfo(const std::string &path,struct stat &st) {

    assert(exist(path) != TfsBuffer::FILE_TYPE::NOF);
    if (path == terark_state_tfs.path){
        getSataFromTfs(terark_state_tfs,st);
        return true;
    }
    auto iter = buf_map.find(path);
    if (iter != buf_map.end()){

        tbb::reader_writer_lock::scoped_lock_read _lock(iter->second->rw_lock);
        auto &tfs = iter->second->tfs;
        getSataFromTfs(tfs,st);
        return true;
    }
    auto rid = getRid(path);
    if (rid < 0)
        return false;
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
    getSataFromTfsCg(tfs_fs,st);
    return true;
}

bool TfsBuffer::remove(const std::string &path) {

    if ( existInTerark(path) == FILE_TYPE::NOF)
        return false;
    std::lock_guard<std::recursive_mutex> _lock(ctx_mtx);
    ctx->removeRow(getRid(path));
    return true;
}

TfsBuffer::FILE_TYPE TfsBuffer::existInTerark(const std::string &path) {

    std::lock_guard<std::recursive_mutex> _lock(ctx_mtx);
    if (ctx->indexKeyExists(path_idx_id, path)) {
        return path.back() == '/' ? FILE_TYPE::DIR : FILE_TYPE::REG;
    }
    if (path.back() == '/' ) {
        return ctx->indexKeyExists(path_idx_id,path.substr(0,path.size() - 1)) ? FILE_TYPE::REG: FILE_TYPE::NOF;
    } else {
        return ctx->indexKeyExists(path_idx_id,path + "/")? FILE_TYPE::DIR : FILE_TYPE::NOF;
    }
}

void TfsBuffer::getSataFromTfs(terark::TFS &tfs, stat &st) {


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
}
void TfsBuffer::getSataFromTfsCg(terark::TFS_Colgroup_file_stat &tfs_fs, stat &st) {

    st.st_mode = tfs_fs.mode;
    st.st_atim.tv_sec = tfs_fs.atime / ns_per_sec;
    st.st_atim.tv_nsec = tfs_fs.atime % ns_per_sec;
    st.st_ctim.tv_sec = tfs_fs.ctime / ns_per_sec;
    st.st_ctim.tv_nsec = tfs_fs.ctime % ns_per_sec;
    st.st_mtim.tv_sec = tfs_fs.mtime / ns_per_sec;
    st.st_mtim.tv_nsec = tfs_fs.mtime % ns_per_sec;
    st.st_nlink = tfs_fs.nlink;
    st.st_blocks = tfs_fs.blocks;
    st.st_gid = tfs_fs.gid;
    st.st_uid = tfs_fs.uid;
    st.st_size = tfs_fs.size;
    st.st_ino = tfs_fs.ino;
}

int TfsBuffer::truncate(const std::string &path,size_t new_size) {

    assert(exist(path) == FILE_TYPE::REG);
    auto iter = buf_map.find(path);
    if (iter != buf_map.end()){
        //path in buf
        tbb::reader_writer_lock::scoped_lock _lock(iter->second->rw_lock);
        auto &tfs = iter->second->tfs;
        tfs.content.resize(new_size,'\0');
        tfs.size = new_size;
    }else{
        //path in terark
        terark::valvec<terark::byte> row;
        terark::NativeDataOutput<terark::AutoGrownMemIO> row_builder;
        {
            std::lock_guard<std::recursive_mutex> _lock(ctx_mtx);
            auto rid = getRid(path);
            if ( rid < 0)
                return -ENOENT;
            ctx->getValue(rid,&row);
        }
        terark::TFS tfs;
        tfs.decode(row);
        tfs.content.resize(new_size,'\0');
        {
            std::lock_guard<std::recursive_mutex> _lock(ctx_mtx);
            ctx->upsertRow(tfs.encode(row_builder));
        }
    }
    return 0;
}

bool TfsBuffer::getNextFile(terark::db::IndexIteratorPtr& iip, const std::string &dir,std::string &file_name) {

    assert(iip != nullptr);//

    assert(dir.back() == '/');//
    terark::llong rid;
    terark::valvec<terark::byte> ret_path;
    auto ret = iip->increment(&rid, &ret_path);
    if ( ret && dir.compare(0,dir.size(), reinterpret_cast<char*>(ret_path.data()),dir.size()) == 0){

        //find '/'
        auto iter = std::find(ret_path.begin() + dir.size(),ret_path.end(),'/');
        if (iter != ret_path.end()){
            (*iter)++;
            terark::valvec<terark::byte> _temp_vec;
            iip->seekUpperBound(ret_path, &rid, &_temp_vec);
            ret_path.resize(iter - ret_path.begin());
        }
        file_name = std::string(ret_path.begin() + dir.size(),ret_path.end());
        std::cout << "getNextFile:" << file_name << std::endl;
        return true;
    }
    return false;
}

terark::db::IndexIteratorPtr TfsBuffer::getDirIter(const std::string &path) {

    terark::db::IndexIteratorPtr path_iter = tab->createIndexIterForward(path_idx_id,tab->createDbContext());
    terark::valvec<terark::byte> ret_path;
    terark::llong rid;
    std::string path_str = path;
    if (path_str.back() != '/')
        path_str.push_back('/');

    int exact = path_iter->seekLowerBound( path_str, &rid,&ret_path);
    assert(exact >= 0);
    return path_iter;
}
