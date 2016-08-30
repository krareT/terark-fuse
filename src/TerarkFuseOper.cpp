//
// Created by terark on 8/30/16.
//

#include "TerarkFuseOper.h"
terark::db::DbTablePtr TerarkFuseOper::tab;
uint32_t TerarkFuseOper::path_idx_id;
terark::db::DbContextPtr TerarkFuseOper::ctx;
const char *hello_path = "/hello";
const char *hello_str = "Hello Terark\n";
using namespace terark;
using namespace db;
int TerarkFuseOper::getattr(const char *path, struct stat *stbuf) {
    int ret = 0;
    std::cout << "TerarkFuseOper::getattr:" << path << std::endl;

    memset(stbuf, 0, sizeof(struct stat));

    llong rid = getRid(path);
    if (rid < 0){
        return -ENOENT;
    }

    return ret;

}

int TerarkFuseOper::open(const char *path, struct fuse_file_info *ffo) {

    std::cout << "TerarkFuseOper::open:" << path << std::endl;
    int ret = 0;
    if (strcmp(path, "/hello") != 0)
        ret = -ENOENT;
    if ((ffo->flags & 3) != O_RDONLY)
        ret = -EACCES;
    return ret;
}

int TerarkFuseOper::read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *ffo) {

    std::cout << "TerarkFuseOper::read:" << path << std::endl;
    size_t len;
    if(strcmp(path, hello_path) != 0)
        return -ENOENT;

    len = strlen(hello_str);
    if (offset < len) {
        if (offset + size > len)
            size = len - offset;
        memcpy(buf, hello_str + offset, size);
    } else
        size = 0;

    return size;
}

int TerarkFuseOper::readlink(const char *path, char *buf, size_t size) {

    std::cout << "TerarkFuseOper::readlink:" << path << std::endl;
    return 0;
}

int TerarkFuseOper::create(const char *path, mode_t mod, struct fuse_file_info * ffi) {

    std::cout << "TerarkFuseOper::create:" << path << std::endl;

    return -EACCES;
}

int TerarkFuseOper::readdir(const char *path, void *buf, fuse_fill_dir_t filler,
                         off_t offset, struct fuse_file_info *fi)
{

    std::cout << "TerarkFuseOper::readdir:" << path << std::endl;
    if (strcmp(path, "/") != 0)
        return -ENOENT;

    filler(buf, ".", NULL, 0);
    filler(buf, "..", NULL, 0);
    filler(buf, hello_path + 1, NULL, 0);

    return 0;
}

int TerarkFuseOper::write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *ffi) {

}

bool TerarkFuseOper::insert(const std::string &key, const std::string &content) {



    return false;
}

long long TerarkFuseOper::getRid(const std::string &path) {

    valvec<llong > ridvec;
    fstring key(path);
    ctx->indexSearchExact(path_idx_id,key,&ridvec);


    return ridvec.size() == 1 ? ridvec[0]: -1;
}

