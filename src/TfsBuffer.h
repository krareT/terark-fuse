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
    std::atomic<uint64_t > ref;
    tbb::reader_writer_lock rw_lock;
    FileInfo(){
        ref.store(0);
    }
};
class TfsBuffer{

private:
    tbb::concurrent_unordered_map<std::string,FileInfo> buf_map;
    tbb::concurrent_unordered_map<std::string,std::mutex> prepare_insert;
public:
    //the only method to insert new element to buf
    terark::llong insertToBuf(const std::string &path,mode_t mode);
    terark::llong release(const std::string &,terark::db::DbContextPtr);
};
#endif //TERARK_FUSE_TFSBUFFER_H
