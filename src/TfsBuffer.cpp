//
// Created by terark on 9/25/16.
//

#include <fcntl.h>
#include <thread>
#include <iostream>
#include "TfsBuffer.h"
const  uint64_t ns_per_sec = 1000000000;
tbb::enumerable_thread_specific<terark::db::DbContextPtr> TfsBuffer::thread_specific_context;

terark::llong TfsBuffer::insertToBuf(const std::string &path, mode_t mode,bool update) {

    struct timespec time;

    uint64_t time_ns = getTime();
    auto info_ptr = std::shared_ptr<FileInfo>(new FileInfo, [this](FileInfo* fi){

        if (fi->update_flag.load(std::memory_order_relaxed)){

            terark::NativeDataOutput<terark::AutoGrownMemIO> row_builder;
            auto context = this->getThreadSafeContext();
            auto rid = context->upsertRow(fi->tfs.encode(row_builder));
            assert(rid >= 0);
            //std::cout << "write to terark:" << fi->tfs.path << std::endl;
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
    info_ptr->ref.store(1,std::memory_order_relaxed);
    info_ptr->update_flag.store(update);
    info_ptr->tfs.nlink = 0;
    info_ptr->tfs.blocks = 0;
    info_ptr->tfs.ino = 0;
    {
        //TODO: here is a data race if 2 or more threads create the same file.
        tbb::spin_rw_mutex::scoped_lock scoped_lock(buf_map_rw_lock, false);//reader lock
        buf_map_modify[path] = info_ptr;
    }
    return 0;
}
terark::llong TfsBuffer::release(const std::string &path) {

    std::shared_ptr<FileInfo> fi_ptr;
    {
        tbb::spin_rw_mutex::scoped_lock scoped_lock(buf_map_rw_lock, false);//reader lock
        auto iter = buf_map_modify.find(path);
        if ( iter == buf_map_modify.end())
            return -ENOENT;
        fi_ptr = iter->second;
    }
    {
        tbb::reader_writer_lock::scoped_lock __lock(fi_ptr->rw_lock);
        fi_ptr->ref --;
        std::cerr << "release : " << path << " " << fi_ptr->ref.load(std::memory_order_relaxed) << std::endl;
        if ( fi_ptr->ref.load(std::memory_order_relaxed) <= 0) {
            {
                //Must be carefule, maybe deadlock!
                tbb::spin_rw_mutex::scoped_lock scoped_lock(buf_map_rw_lock);//writer lock
                buf_map_modify.unsafe_erase(path);
            }
        }
    }
    return 0;
}
terark::llong TfsBuffer::loadToBuf(const std::string &path) {

    std::shared_ptr<FileInfo> fi_ptr;
    {
        tbb::spin_rw_mutex::scoped_lock scoped_lock(buf_map_rw_lock, false);//reader
        auto iter = buf_map_modify.find(path);
        if (iter != buf_map_modify.end()){
            fi_ptr = iter->second;
        }else{
            fi_ptr = nullptr;
        }
    }
    if (fi_ptr != nullptr){
        {
            tbb::reader_writer_lock::scoped_lock lock(fi_ptr->rw_lock);
            if (fi_ptr->ref.load(std::memory_order_relaxed) != 0){
                fi_ptr->ref++;
                std::cerr << "exist in buf:" << path << " " << fi_ptr->ref.load(std::memory_order_relaxed) << std::endl;
                return 0;
            }
        }
    }
    auto info_ptr = std::shared_ptr<FileInfo>(new FileInfo, [this](FileInfo* fi){

        if (fi->update_flag.load(std::memory_order_relaxed)){
            terark::NativeDataOutput<terark::AutoGrownMemIO> row_builder;
            auto context = this->getThreadSafeContext();
            auto rid = context->upsertRow(fi->tfs.encode(row_builder));
            //TODO:what should i do if upsertRow failed.
            assert(rid >= 0);
        }
        delete fi;
        fi = nullptr;
    });

    terark::valvec<terark::byte> row;
    terark::llong rid = getRid(path);
    if ( rid < 0)
        return -ENOENT;

//    int times = 0;
//    terark::llong rid;
//    int times = 0;
//    do{
//        rid = getRid(path);
//        std::cout << "getRid :" << times++ << std::endl;
//        sleep()
//    }while( rid < 0);

    auto ctx = getThreadSafeContext();
    ctx->getValue(rid,&row);
    info_ptr->tfs.decode(row);
    info_ptr->ref++;
    std::cerr << "exist in terark:" << path << " " << info_ptr->ref.load(std::memory_order_relaxed) << std::endl;
    {
        tbb::spin_rw_mutex::scoped_lock scoped_lock(buf_map_rw_lock, false);//reader
        buf_map_modify[path] = info_ptr;
    }

    return 0;
}
int TfsBuffer::read(const std::string &path, char *buf, size_t size, size_t offset) {
    std::shared_ptr<FileInfo> fi_ptr;
    {
        tbb::spin_rw_mutex::scoped_lock scoped_lock(buf_map_rw_lock, false);//reader lock
        auto iter = buf_map_modify.find(path);
        if (iter == buf_map_modify.end())
            return -ENOENT;
        fi_ptr = iter->second;
    }
    {
        auto &content = fi_ptr->tfs.content;
        tbb::reader_writer_lock::scoped_lock _lock(fi_ptr->rw_lock);
        if (offset < content.size()) {
            if (offset + size > content.size())
                size = content.size() - offset;
            memcpy(buf, content.data() + offset, size);
        } else {
            size = 0;
        }
        fi_ptr->tfs.atime = getTime();
    }
    return size;
}

uint64_t TfsBuffer::getTime() {

    struct timespec ts;
    clock_gettime(CLOCK_REALTIME,&ts);
    return ts.tv_sec * ns_per_sec + ts.tv_nsec;
}



long long TfsBuffer::getRid(const std::string &path) {

    auto ctx = getThreadSafeContext();
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
    assert(std::cout << "Assert is open!" << std::endl);
    tab = terark::TFS::openTable(db_path, true);
    assert(tab != nullptr);


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

    auto ctx = getThreadSafeContext();
    //create root dict : "/"
    if (false == ctx->indexKeyExists(path_idx_id, "/")) {
        auto ret = this->insertToBuf("/", 0666 | S_IFDIR);
        release("/");
        assert(getRid("/") >= 0);
    }

    //release it in the destructor func;
    if (false == ctx->indexKeyExists(path_idx_id,terark_state)) {
        insertToBuf(terark_state, 0666 | S_IFREG);
        release(terark_state);
    }
    loadToBuf(terark_state);
    buf_map_modify[terark_state]->tfs.content = "Hello Terark!\n";
    buf_map_modify[terark_state]->tfs.size = buf_map_modify[terark_state]->tfs.content.size();

}

int32_t TfsBuffer::write(const std::string &path, const char *buf, size_t size, size_t offset) {

    std::shared_ptr<FileInfo> fi_ptr;
    {
        tbb::spin_rw_mutex::scoped_lock scoped_lock(buf_map_rw_lock, false);//reader lock
        auto iter = buf_map_modify.find(path);
        if (iter == buf_map_modify.end())
            return -ENOENT;
        fi_ptr = iter->second;
    }
    if ( strcmp(path.c_str(),terark_state) == 0){
        writeToTerarkState(buf,size);
        return size;
    }
    {
        terark::TFS &tfs = fi_ptr->tfs;
        tbb::reader_writer_lock::scoped_lock _lock(fi_ptr->rw_lock);
        fi_ptr->update_flag.store(true,std::memory_order_relaxed);
        if (offset + size > tfs.content.size()) {
            tfs.content.resize(offset + size);
        }
        tfs.content.replace(offset, size, buf, size);
        tfs.size = tfs.content.size();
        tfs.mtime = getTime();
    }
    return size;

}

void TfsBuffer::compact() {
    std::thread t([](TfsBuffer *tb){
        tb->tab->compact();
    },this);
    t.detach();
}



TfsBuffer::FILE_TYPE TfsBuffer::exist(const std::string &path) {
    if (path == "/"){
        return FILE_TYPE::DIR;
    }
    else{
        return existInTerark(path);
    }
}

bool TfsBuffer::getFileInfo(const std::string &path,struct stat &st) {

    std::shared_ptr<FileInfo> fi_ptr;
    {
        tbb::spin_rw_mutex::scoped_lock scoped_lock(buf_map_rw_lock, false);//reader lock
        auto iter = buf_map_modify.find(path);
        if (iter == buf_map_modify.end())
            fi_ptr = nullptr;
        else
            fi_ptr = iter->second;
    }
    if (fi_ptr != nullptr){

        auto &tfs = fi_ptr->tfs;
        tbb::reader_writer_lock::scoped_lock_read _lock(fi_ptr->rw_lock);
        getSataFromTfs(tfs,st);
        return true;
    }
    //get stat from terark
    auto rid = getRid(path);
    if (rid < 0)
        return false;
    terark::TFS_Colgroup_file_stat tfs_fs;
    terark::valvec<terark::byte> cg_data;
//    {
//        std::lock_guard<std::recursive_mutex> _lock(ctx_mtx);
//        ctx->selectOneColgroup(rid, file_stat_cg_id, &cg_data);
//    }
    getThreadSafeContext()->selectOneColgroup(rid,file_stat_cg_id,&cg_data);
    if (cg_data.size() == 0) {
        return false;
    }
    tfs_fs.decode(cg_data);
    getSataFromTfsCg(tfs_fs,st);
    return true;
}

bool TfsBuffer::remove(const std::string &path) {

    auto rid = getRid(path);
    if (rid < 0)
        return false;
    getThreadSafeContext()->removeRow(rid);
    return true;
}

TfsBuffer::FILE_TYPE TfsBuffer::existInTerark(const std::string &path) {

    auto ctx = getThreadSafeContext();
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
    std::shared_ptr<FileInfo> fi_ptr;
    {
        tbb::spin_rw_mutex::scoped_lock lock(buf_map_rw_lock,false);//reader
        auto iter = buf_map_modify.find(path);
        if (iter == buf_map_modify.end())
            fi_ptr = nullptr;
        else
            fi_ptr = iter->second;
    }

    if (fi_ptr != nullptr){
        //path in buf
        auto &tfs = fi_ptr->tfs;
        tbb::reader_writer_lock::scoped_lock _lock(fi_ptr->rw_lock);
        tfs.content.resize(new_size,'\0');
        tfs.size = new_size;
    }else{
        //path in terark
        terark::valvec<terark::byte> row;
        terark::NativeDataOutput<terark::AutoGrownMemIO> row_builder;
        auto rid = getRid(path);
        if ( rid < 0)
            return -ENOENT;
        auto ctx = getThreadSafeContext();
        ctx->getValue(rid,&row);
        terark::TFS tfs;
        tfs.decode(row);
        tfs.content.resize(new_size,'\0');
        ctx->upsertRow(tfs.encode(row_builder));
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

TfsBuffer::~TfsBuffer() {
    //TODO:do we need to release it manually?
    release(terark_state);
    tab->safeStopAndWaitForCompress();
    tab = NULL;

}

void TfsBuffer::writeToTerarkState(const char *buf, const size_t size) {
    std::cout << "Write To Terark State : " << buf << std::endl;
    if ( strncmp(buf,"compact",7) == 0){
        std::cout << "Compact!" << std::endl;
        compact();
    }
}

terark::db::DbContextPtr TfsBuffer::getThreadSafeContext() {

    terark::db::DbContextPtr ctx = thread_specific_context.local();
    if ( ctx == nullptr){
        ctx = tab->createDbContext();
        thread_specific_context.local() = ctx;
    }
    assert(ctx != nullptr);
    return ctx;
}

int32_t TfsBuffer::flush(const std::string &path) {

    std::shared_ptr<FileInfo> fi_ptr;
    {
        tbb::spin_rw_mutex::scoped_lock lock(buf_map_rw_lock,false);//reader
        auto iter = buf_map_modify.find(path);
        if (iter == buf_map_modify.end())
            return -ENOENT;
        fi_ptr = iter->second;
    }

    terark::NativeDataOutput<terark::AutoGrownMemIO> row_builder;
    auto context = this->getThreadSafeContext();
    {
        tbb::reader_writer_lock::scoped_lock lock(fi_ptr->rw_lock);
        if (fi_ptr->update_flag.load(std::memory_order_relaxed) == false)
            return 0;

        auto rid = context->upsertRow(fi_ptr->tfs.encode(row_builder));
        //TODO:what should i do if upsertRow failed.
        if (rid < 0)
            return -EIO;
        fi_ptr->update_flag.store(false,std::memory_order_relaxed);
    }
    return 0;
}
