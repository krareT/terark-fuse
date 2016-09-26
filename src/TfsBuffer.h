//
// Created by terark on 9/25/16.
//

#ifndef TERARK_FUSE_TFSBUFFER_H
#define TERARK_FUSE_TFSBUFFER_H
#include<tbb/concurrent_unordered_map.h>
#include <mutex>
#include "tfs.h"

struct FileInfo{

    terark::TFS tfs;
    std::atomic_bool update_flag;
    tbb::reader_writer_lock rw_lock;
    FileInfo():update_flag{false}{
    }
};
class TfsBuffer{

private:
    tbb::concurrent_unordered_map<std::string,std::shared_ptr<FileInfo>> buf_map;
    std::mutex insert_mtx;
    uint64_t getTime();
public:
    //the only method to insert new element to buf
    terark::llong insertToBuf(const std::string &path,mode_t mode);
    terark::llong release(const std::string &);
    bool exist(const std::string &);
    int read(const std::string &,char *buf,size_t size,size_t off);
};
#endif //TERARK_FUSE_TFSBUFFER_H
