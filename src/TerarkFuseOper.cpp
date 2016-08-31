//
// Created by terark on 8/30/16.
//

#include "TerarkFuseOper.h"

const char *hello_path = "/hello";
const char *hello_str = "Hello Terark\n";
uint64_t TerarkFuseOper::ns_per_sec = 1000000000;
using namespace terark;
using namespace db;

int TerarkFuseOper::create(const char *path, mode_t mod, struct fuse_file_info *ffi) {

    std::cout << "TerarkFuseOper::create:" << path << std::endl;
    //check if exist

    TFS tfs;
    struct timespec time;
    tfs.mode = mod;
    tfs.path = path;
    auto ret = clock_gettime(CLOCK_REALTIME, &time);
    if (ret == -1)
        return -errno;
    tfs.atime = time.tv_sec * ns_per_sec + time.tv_nsec;
    tfs.ctime = time.tv_sec * ns_per_sec + time.tv_nsec;
    tfs.mtime = time.tv_sec * ns_per_sec + time.tv_nsec;
    tfs.gid = getgid();
    tfs.uid = getuid();
    tfs.nlink = 1;
    tfs.size = 0;
    if (createFile(tfs) < 0)
        return -EACCES;
    return 0;
}

int TerarkFuseOper::getattr(const char *path, struct stat *stbuf) {
    int ret = 0;
    std::cout << "TerarkFuseOper::getattr:" << path << std::endl;

    memset(stbuf, 0, sizeof(struct stat));

    if (strcmp(path, "/") == 0) {
        stbuf->st_mode = S_IFDIR | 0755;
        stbuf->st_nlink = 2;
        return 0;
    }
    llong rid = getRid(path);
    std::cout << "TerarkFuseOper::getattr rid:" << rid << std::endl;
    if (rid < 0) {
        return -ENOENT;
    }
    getFileMetainfo(rid, *stbuf);
    stbuf->st_mode = S_IFREG | 0444;
    return 0;
}

int TerarkFuseOper::open(const char *path, struct fuse_file_info *ffo) {

    std::cout << "TerarkFuseOper::open:" << path << std::endl;

    llong rid;
    if (false == ctx->indexKeyExists(path_idx_id, path)) {

        std::cout << "TerarkFuseOper::open: file not exist" << path << std::endl;

        if (ffo->flags & O_CREAT != 0) {

            std::cout << "TerarkFuseOper::open:create new file" << path << std::endl;

            return this->create(path, ffo->flags, ffo);
        } else {
            return -ENOENT;
        }
    }
    std::cout << "TerarkFuseOper::open: file exist" << path << std::endl;

    //file exist
    return 0;

}

int TerarkFuseOper::read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *ffo) {

    std::cout << "TerarkFuseOper::read:" << path << std::endl;
    if (ctx->indexKeyExists(path_idx_id, path) == false)
        return -ENOENT;
    auto rid = getRid(path);
    valvec<byte> row;
    ctx->selectOneColumn(rid, tab->getColumnId("content"), &row);

    if (offset < row.size()) {
        if (offset + size > row.size())
            size = row.size() - offset;
        memcpy(buf, row.data(), size);
    } else {
        size = 0;
    }

    std::cout << "TerarkFuseOper::read len:" << size << std::endl;
    char str[100];
    snprintf(str, row.size(), "%s", row.data());
    str[row.size()] = 0;
    puts(str);
    return size;
}

int TerarkFuseOper::readlink(const char *path, char *buf, size_t size) {

    std::cout << "TerarkFuseOper::readlink:" << path << std::endl;
    return 0;
}

int TerarkFuseOper::readdir(const char *path, void *buf, fuse_fill_dir_t filler,
                            off_t offset, struct fuse_file_info *fi) {

    std::cout << "TerarkFuseOper::readdir:" << path << std::endl;
    if (strcmp(path, "/") != 0)
        return -ENOENT;

    filler(buf, ".", NULL, 0);
    filler(buf, "..", NULL, 0);
    filler(buf, hello_path + 1, NULL, 0);

    return 0;
}

int TerarkFuseOper::write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *ffi) {

    std::cout << "TerarkFuseOper::write:" << path << std::endl;
    valvec<byte> cgData;
    if (ctx->indexKeyExists(path_idx_id, path) == false)
        return -ENOENT;
    auto rid = getRid(path);
    TFS_Colgroup_file_stat tfs_fs;
    ctx->selectOneColgroup(rid, file_stat_cg_id, &cgData);
    tfs_fs.decode(cgData);
    TFS tfs(tfs_fs);
    tfs.path = path;
    tfs.content.assign(buf, size);
    terark::NativeDataOutput<terark::AutoGrownMemIO> rowBuilder;
    rowBuilder.rewind();
    rowBuilder << tfs;
    if (ctx->upsertRow(rowBuilder.written()) < 0)
        return -EACCES;
    return size;
}

long long TerarkFuseOper::getRid(const std::string &path) {

    valvec<llong> ridvec;
    fstring key(path);

    ctx->indexSearchExact(path_idx_id, key, &ridvec);
    return ridvec.size() == 1 ? ridvec[0] : -1;
}

bool TerarkFuseOper::getFileMetainfo(const terark::llong rid, struct stat &stbuf) {

    assert(rid >= 0);
    TFS_Colgroup_file_stat tfs_fs;
    valvec<byte> cgData;

    ctx->selectOneColgroup(rid, file_stat_cg_id, &cgData);
    if (cgData.size() == 0) {
        return false;
    }
    tfs_fs.decode(cgData);
    stbuf = getStat(tfs_fs, stbuf);
    printStat(stbuf);
    return true;
}

struct stat &TerarkFuseOper::getStat(terark::TFS_Colgroup_file_stat &tfs, struct stat &st) {

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
    return st;
}

terark::llong TerarkFuseOper::createFile(terark::TFS &tfs) {

    terark::NativeDataOutput<terark::AutoGrownMemIO> rowBuilder;

    rowBuilder.rewind();
    rowBuilder << tfs;
    return ctx->insertRow(rowBuilder.written());
}

void TerarkFuseOper::printStat(struct stat &st) {

    printf("print stat:\n");
    std::cout << "gid:" << st.st_gid << std::endl;
    std::cout << "uid:" << st.st_uid << std::endl;
    std::cout << "mod:" << st.st_mode << std::endl;
    std::cout << "nlk:" << st.st_nlink << std::endl;
    std::cout << "ctm:" << ctime(&st.st_ctim.tv_sec) << std::endl;
    std::cout << "mtm:" << ctime(&st.st_mtim.tv_sec) << std::endl;
    std::cout << "atm:" << ctime(&st.st_atim.tv_sec) << std::endl;
}


