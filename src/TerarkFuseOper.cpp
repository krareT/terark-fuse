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
    std::cout << "TerarkFuseOper::create:" << printMode(mod) << std::endl;
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

    if (strcmp(path, "/") == 0 || ifDict(path)) {
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
    return 0;
}

int TerarkFuseOper::open(const char *path, struct fuse_file_info *ffo) {

    std::cout << "TerarkFuseOper::open:" << path << std::endl;
    std::cout << "TerarkFuseOper::open flag:" << printFlag(ffo->flags) << std::endl;
    //O_DIRECTORY
    llong rid;
    if (false == ctx->indexKeyExists(path_idx_id, path)) {
        return -ENOENT;
    }
    std::cout << "TerarkFuseOper::open: file exist" << path << std::endl;
    return 0;
}

int TerarkFuseOper::read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *ffo) {

    std::cout << "TerarkFuseOper::read:" << path << std::endl;
    //check if exist
    if (ctx->indexKeyExists(path_idx_id, path) == false)
        return -ENOENT;

    auto rid = getRid(path);
    if (rid < 0)
        return -ENOENT;
    valvec<byte> row;
    ctx->selectOneColumn(rid, tab->getColumnId("content"), &row);

    if (offset < row.size()) {
        if (offset + size > row.size())
            size = row.size() - offset;
        memcpy(buf, row.data() + offset, size);
    } else {
        size = 0;
    }
    timespec time;
    clock_gettime(CLOCK_REALTIME, &time);
    uint64_t nsec = time.tv_sec * ns_per_sec + time.tv_nsec;
    fstring new_atime = terark::db::Schema::fstringOf(&nsec);
    tab->updateColumn(rid, "atime", new_atime);
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
    std::cout << "TerarkFuseOper::write:offset:" << offset << std::endl;
    valvec<byte> row_data;
    //check if exist
    if (ctx->indexKeyExists(path_idx_id, path) == false)
        return -ENOENT;

    auto rid = getRid(path);
    ctx->getValue(rid, &row_data);

    TFS tfs;
    tfs.decode(row_data);
    tfs.path = path;
    if (offset + size > tfs.content.size()) {
        tfs.content.resize(offset + size);
    }
    tfs.content.replace(offset, size, buf, size);
    tfs.size = tfs.content.size();
    timespec time;
    auto ret = clock_gettime(CLOCK_REALTIME, &time);
    if (ret == -1)
        return -errno;
    tfs.atime = time.tv_sec * ns_per_sec + time.tv_nsec;
    tfs.ctime = time.tv_sec * ns_per_sec + time.tv_nsec;
    tfs.mtime = time.tv_sec * ns_per_sec + time.tv_nsec;

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
    std::cout << "siz:" << st.st_size << std::endl;
    std::cout << "ctm:" << ctime(&st.st_ctim.tv_sec) << std::endl;
    std::cout << "mtm:" << ctime(&st.st_mtim.tv_sec) << std::endl;
    std::cout << "atm:" << ctime(&st.st_atim.tv_sec) << std::endl;
}

std::string TerarkFuseOper::printFlag(int flag) {

    std::stringstream ss;
    ss << "open flag:" << flag << std::endl;

    if (flag & O_APPEND)
        ss << "O_APPEND,";

    if (flag & O_ASYNC)
        ss << "O_ASYNC,";

    if (flag & O_CLOEXEC)
        ss << "O_CLOEXEC";

    if (flag & O_CREAT)
        ss << "O_CREAT,";

    if (flag & O_DIRECT)
        ss << "O_DIRECT,";

    if (flag & O_DIRECTORY)
        ss << " O_DIRECTORY";

    if (flag & O_DSYNC)
        ss << " O_DSYNC";

    if (flag & O_EXCL)
        ss << " O_EXCL,";

    if (flag & O_LARGEFILE)
        ss << " O_LARGEFILE,";
    if (flag & O_NOATIME)
        ss << " O_NOATIME,";

    if (flag & O_NOCTTY)
        ss << " O_NOCTTY,";

    if (flag & O_NOFOLLOW)
        ss << " O_NOFOLLOW,";
    if (flag & O_NONBLOCK)
        ss << " O_NONBLOCK,";
    if (flag & O_NDELAY)
        ss << " O_NDELAY,";

    if (flag & O_PATH)
        ss << "O_PATH,";
    if (flag & O_SYNC)
        ss << " O_SYNC,";
    if (flag & O_TRUNC)
        ss << " O_TRUNC,";
    if (flag & O_RDWR)
        ss << " O_RDWR,";
    if (flag & O_RDONLY)
        ss << " O_RDONLY,";
    if (flag & O_WRONLY)
        ss << " O_WRONLY,";

    ss << std::endl;
    return ss.str();

}

std::string TerarkFuseOper::printMode(mode_t mode) {
    std::stringstream ss;
    ss << "mode:" << mode << std::endl;

    if ((mode & S_IRWXU) == S_IRWXU)
        ss << " S_IRWXU,";
    if ((mode & S_IRUSR) == S_IRUSR)
        ss << " S_IRUSR,";

    if ((mode & S_IWUSR) == S_IWUSR)
        ss << " S_IWUSR,";
    if ((mode & S_IXUSR) == S_IXUSR)
        ss << " S_IXUSR,";

    if ((mode & S_IRWXG) == S_IRWXG)
        ss << " S_IRWXG,";
    if ((mode & S_IRGRP) == S_IRGRP)
        ss << " S_IRGRP,";

    if ((mode & S_IWGRP) == S_IWGRP)
        ss << " S_IWGRP,";
    if ((mode & S_IXGRP) == S_IXGRP)
        ss << " S_IXGRP,";

    if ((mode & S_IRWXO) == S_IRWXO)
        ss << " S_IRWXO,";
    if ((mode & S_IROTH) == S_IROTH)
        ss << " S_IROTH,";

    if ((mode & S_IWOTH) == S_IWOTH)
        ss << " S_IWOTH,";
    if ((mode & S_IXOTH) == S_IXOTH)
        ss << " S_IXOTH,";

    if ((mode & S_ISUID) == S_ISUID)
        ss << " S_ISUID,";
    if ((mode & S_ISGID) == S_ISGID)
        ss << " S_ISGID,";
    if ((mode & S_ISVTX) == S_ISVTX)
        ss << " S_ISVTX,";

    return ss.str();
}

bool TerarkFuseOper::ifDict(const char *path) {

    if (NULL == path)
        return false;
    auto len = strlen(path);
    if (path[len - 1] == '/') {
        return true;
    }
    //search in terark to judge if this is a dict
    std::cout << "TerarkFuseOper::ifDict:" << path << std::endl;

    return !ctx->indexKeyExists(path_idx_id, path);
}


