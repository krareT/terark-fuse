//
// Created by terark on 8/30/16.
//

#include "TerarkFuseOper.h"
terark::db::DbTablePtr TerarkFuseOper::tab;
std::unordered_map<uint32_t , terark::db::DbContextPtr> TerarkFuseOper::ctx_map;

int TerarkFuseOper::getattr(const char *path, struct stat *stbuf) {
    return 0;
}

int TerarkFuseOper::open(const char *, struct fuse_file_info *) {
    return 0;
}

int TerarkFuseOper::read(const char *, char *, size_t, off_t, struct fuse_file_info *) {
    return 0;
}

int TerarkFuseOper::readlink(const char *, char *, size_t) {
    return 0;
}

