//
// Created by terark on 9/9/16.
//

#ifndef TERARK_FUSE_TFSBUFFER_H
#define TERARK_FUSE_TFSBUFFER_H

#include "tfs.hpp"
#include <atomic>

class TfsBuffer {
private:
    std::unordered_map<std::string, std::pair<terark::TFS *, std::atomic<uint32_t> *>> buffer_map;
public:
    terark::TFS *insert(const char *path, const terark::llong &rid, terark::db::DbContextPtr ctx) {

        assert(rid >= 0);
        if (buffer_map.count(path) > 0)
            return buffer_map[path].first;

        terark::valvec<terark::byte> row;
        ctx->getValue(rid, &row);
        terark::TFS *tfs = new terark::TFS();

        std::atomic<uint32_t> *cnt = new std::atomic<uint32_t>();
        buffer_map[path] = std::make_pair(tfs, cnt);
        buffer_map[path].first->decode(row);
        buffer_map[path].second->store(1, std::memory_order_relaxed);
        return buffer_map[path].first;
    }

    bool erase(const char *path) {

        buffer_map[path].second--;
        if (buffer_map[path].second->load(std::memory_order_relaxed) == 0) {
            delete buffer_map[path].first;
            delete buffer_map[path].second;
            buffer_map.erase(path);
            return true;
        } else {
            return false;
        }
    }
    terark::TFS *getTFS(const char *path){
        if ( buffer_map.count(path) == 0)
            return NULL;

        return buffer_map[path].first;
    }
};

#endif //TERARK_FUSE_TFSBUFFER_H
