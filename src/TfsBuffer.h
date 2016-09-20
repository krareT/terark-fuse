//
// Created by terark on 9/9/16.
//

#ifndef TERARK_FUSE_TFSBUFFER_H
#define TERARK_FUSE_TFSBUFFER_H

#include "tfs.h"
#include <atomic>
#include <tbb/concurrent_unordered_map.h>
#include <iostream>

struct FileObj{
    terark::TFS *tfs;
    std::atomic<int32_t> ref;
    FileObj():tfs(nullptr){

        ref.store(0);
    }
    ~FileObj(){
        delete tfs;
    }
};

class TfsBuffer {

private:
    tbb::concurrent_unordered_map<std::string, FileObj*> buffer_map;
public:

    bool insert(const char *path, const terark::llong &rid, terark::db::DbContextPtr ctx) {
        assert(rid >= 0);
        if (buffer_map.count(path) > 0)
            return true;

        terark::valvec<terark::byte> row;

        terark::TFS *tfs = new terark::TFS;
        ctx->getValue(rid, &row);
        tfs->decode(row);
        buffer_map[path] = new FileObj;
        buffer_map[path]->tfs = tfs;
        return true;
    }

    bool release(const char *path) {

        if ( buffer_map.count(path) == 0)
            return false;
        buffer_map[path]->ref--;
        std::cout << "tfsBuffer:release:" << path << ":" << buffer_map[path]->ref <<std::endl;
        if (buffer_map[path]->ref.load(std::memory_order_relaxed) <= 0) {

            delete buffer_map[path];
            buffer_map.unsafe_erase(path);
            return true;
        } else {
            return false;
        }
    }
    terark::TFS *getTFS(const char *path){
        auto find = buffer_map.find(path);
        if ( find == buffer_map.end())
            return NULL;
        find->second->ref++;
        std::cout << "tfsBuffer:get:" << path << ":" << find->second->ref.load(std::memory_order_relaxed) <<std::endl;
        return find->second->tfs;
    }
    ~TfsBuffer(){

        for(auto &each : buffer_map){
            delete each.second;
        }
        buffer_map.clear();
    }
    TfsBuffer(){
        buffer_map.clear();
    }
};

#endif //TERARK_FUSE_TFSBUFFER_H
