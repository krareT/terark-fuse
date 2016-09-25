//
// Created by terark on 8/30/16.
//

#include "TerarkFuseOper.h"

uint64_t TerarkFuseOper::ns_per_sec = 1000000000;
using namespace terark;
using namespace db;

TerarkFuseOper::TerarkFuseOper(const char *dbpath) {

    tab = terark::db::CompositeTable::open(dbpath);
    assert(tab != NULL);

    path_idx_id = tab->getIndexId("path");
    assert(path_idx_id < tab->getIndexNum());

    ctx = tab->createDbContext();
    file_stat_cg_id = tab->getColgroupId("file_stat");
    file_mode_id = tab->getColumnId("mode");
    file_uid_id = tab->getColumnId("uid");
    file_gid_id = tab->getColumnId("gid");
    file_atime_id = tab->getColumnId("atime");
    file_ctime_id = tab->getColumnId("ctime");
    file_mtime_id = tab->getColumnId("mtime");
    file_content_id = tab->getColumnId("content");


    assert(file_stat_cg_id < tab->getColgroupNum());
    assert(file_mode_id < tab->getColumnNum());
    assert(file_uid_id < tab->getColumnNum());
    assert(file_gid_id < tab->getColumnNum());
    assert(file_atime_id < tab->getColumnNum());
    assert(file_ctime_id < tab->getColumnNum());
    assert(file_mtime_id < tab->getColumnNum());
    assert(file_content_id < tab->getColumnNum());


    //create root dict : "/"
    if (false == ctx->indexKeyExists(path_idx_id, "/")) {

        auto ret = this->createFile("/", 0666 | S_IFDIR);
        assert(ret == 0);
    }
}

int TerarkFuseOper::create(const char *path, mode_t mod, struct fuse_file_info *ffi) {

    if (ifExist(path))
        return -EEXIST;
    //std::cout << "TerarkFuseOper::create:" << path << std::endl;
    //std::cout << "TerarkFuseOper::create:" << printMode(mod) << std::endl;


    if (createFile(path, mod | S_IFREG) < 0)
        return -EACCES;
    return 0;
}

int TerarkFuseOper::getattr(const char *path, struct stat *stbuf) {

    //std::cout << "TerarkFuseOper::getattr:" << path << std::endl;
    if (strcmp(path,"/terark-compact") == 0){
        tab->compact();
        return -EBADF;
    }
    memset(stbuf, 0, sizeof(struct stat));
    if ( !ifExist(path))
        return -ENOENT;
    auto rid = getRid(path);
    getFileMetainfo(rid, *stbuf);

    return 0;
}

int TerarkFuseOper::open(const char *path, struct fuse_file_info *ffo) {

    //std::cout << "TerarkFuseOper::open:" << path << std::endl;
    //std::cout << "TerarkFuseOper::open flag:" << printFlag(ffo->flags) << std::endl;

    if (false == ifExist(path)) {
        return -ENOENT;
    }

    return 0;
}

int TerarkFuseOper::read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *ffo) {

    //std::cout << "TerarkFuseOper::read:" << path << std::endl;
    //check if exist
    if (!ifExist(path))
        return -ENOENT;

    auto rid = getRid(path);
    assert(rid >= 0);
    valvec<byte> row;
    ctx->selectOneColumn(rid, tab->getColumnId("content"), &row);

    if (offset < row.size()) {
        if (offset + size > row.size())
            size = row.size() - offset;
        memcpy(buf, row.data() + offset, size);
    } else {
        size = 0;
    }
    updateAtime(rid);
    return size;
}

int TerarkFuseOper::readdir(const char *path, void *buf, fuse_fill_dir_t filler,
                            off_t offset, struct fuse_file_info *fi) {

    if ( !ifExist(path))
        return -ENOENT;
    if ( !ifDictExist(path))
        return -ENOTDIR;
    //std::cout << "TerarkFuseOper::readdir:" << path << std::endl;
    filler(buf, ".", NULL, 0);
    filler(buf, "..", NULL, 0);
    IndexIteratorPtr path_iter = tab->createIndexIterForward(path_idx_id,ctx.get());
    valvec<byte> ret_path;
    llong rid;
    std::string path_str = path;
    auto path_len = path_str.size();
    if (path_str.back() != '/')
        path_str.push_back('/');

    int ret = path_iter->seekLowerBound( path_str, &rid,&ret_path);
    assert(ret == 0);

    bool not_increment_flag = false;

    while(  not_increment_flag  || path_iter->increment(&rid, &ret_path)){

        if ( memcmp(ret_path.data(),path,path_len) != 0)
            break;
        //find first '/' after path
        auto pos = std::find(ret_path.begin() + path_str.size(),ret_path.end(),'/');
        not_increment_flag = pos != ret_path.end();
        if ( not_increment_flag) {            //reg file
            *pos = 0;
        }
        else {            //dir file
            ret_path.push_back(0);
        }
        filler(buf, reinterpret_cast<char*>(ret_path.data() + path_str.size()),NULL,0);
        if (not_increment_flag) {
            *pos = '0';
            ret = path_iter->seekLowerBound(fstring(ret_path.data()), &rid, &ret_path);
            if (ret < 0)
                break;
        }
    }

    return 0;
}

int TerarkFuseOper::write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *ffi) {

    //std::cout << "TerarkFuseOper::write:" << path << std::endl;
    //std::cout << "TerarkFuseOper::write:flag:" << printFlag(ffi->flags) << std::endl;

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
    tfs.mtime = getTime();
    terark::NativeDataOutput<terark::AutoGrownMemIO> rowBuilder;
    rowBuilder.rewind();
    rowBuilder << tfs;
    if (ctx->upsertRow(rowBuilder.written()) < 0)
        return -EACCES;
    return size;
}

long long TerarkFuseOper::getRid(const std::string &path) {

    valvec<llong> ridvec;

    std::string path_str = path;
    //std::cout << "TerarkFuseOper::getRid:" << path_str << std::endl;
    ctx->indexSearchExact(path_idx_id, path_str, &ridvec);
    if ( ridvec.size() != 0)
        return *std::max_element(ridvec.begin(),ridvec.end());
    path_str.push_back('/');
    //std::cout << "TerarkFuseOper::getRid:" << path_str << std::endl;
    ctx->indexSearchExact(path_idx_id, path_str, &ridvec);
    if ( ridvec.size() != 0)
        return *std::max_element(ridvec.begin(),ridvec.end());
    return -1;

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

terark::llong TerarkFuseOper::createFile(const std::string &path, const mode_t &mod) {
    struct timespec time;
    auto ret = clock_gettime(CLOCK_REALTIME, &time);
    //std::cout << "createFile:" << path << std::endl;
    if (ret == -1)
        return -errno;
    TFS tfs;
    tfs.path = path;
    tfs.mode = mod;
    tfs.atime = time.tv_sec * ns_per_sec + time.tv_nsec;
    tfs.ctime = time.tv_sec * ns_per_sec + time.tv_nsec;
    tfs.mtime = time.tv_sec * ns_per_sec + time.tv_nsec;
    tfs.gid = getgid();
    tfs.uid = getuid();
    tfs.nlink = 1;
    tfs.size = 0;
    terark::NativeDataOutput<terark::AutoGrownMemIO> rowBuilder;

    rowBuilder.rewind();
    rowBuilder << tfs;
    return ctx->upsertRow(rowBuilder.written());
}

void TerarkFuseOper::printStat(struct stat &st) {

    printf("print stat:\n");
    //std::cout << "gid:" << st.st_gid << std::endl;
    //std::cout << "uid:" << st.st_uid << std::endl;
    //std::cout << "mod:" << printMode(st.st_mode) << std::endl;
    //std::cout << "nlk:" << st.st_nlink << std::endl;
    //std::cout << "siz:" << st.st_size << std::endl;
    //std::cout << "ctm:" << ctime(&st.st_ctim.tv_sec);
    //std::cout << "mtm:" << ctime(&st.st_mtim.tv_sec);
    //std::cout << "atm:" << ctime(&st.st_atim.tv_sec);
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
    if ((S_ISREG(mode)))
        ss << " S_IFREG, ";
    if ((S_ISDIR(mode)))
        ss << " S_IFDIR, ";
    return ss.str();
}

bool TerarkFuseOper::ifExist(const std::string &path) {

    if (path == "/")
        return true;
    if (buf_map.find(path) != buf_map.end())
        return true;
    if (ctx->indexKeyExists(path_idx_id, path))
        return true;

    if (path.back() == '/') {
        return ctx->indexKeyExists(path_idx_id, path.substr(0, path.size() - 1));
    } else {
        return ctx->indexKeyExists(path_idx_id, path + "/");
    }
}

bool TerarkFuseOper::ifDict(const std::string &path) {

    auto rid = getRid(path);
    valvec<byte> row;
    ctx->selectOneColumn(rid, tab->getColumnId("path"), &row);

    fstring p(row.data());

    //judge if the last char in p is '/'
    return p.c_str()[p.size() - 1] == '/';
}

int TerarkFuseOper::mkdir(const char *path, mode_t mod) {

    if (ifExist(path))
        return -EEXIST;
    //std::cout << "TerarkFuseOper::mkdir:" << path << std::endl;


    std::string path_str(path);
    if (path_str.back() != '/') {
        path_str.push_back('/');
    }
    mod |= S_IFDIR;
    if (createFile(path_str, mod) < 0)
        return -EACCES;
    //std::cout << "TerarkFuseOper::mkdir:" << printMode(mod) << std::endl;
    return 0;
}

int TerarkFuseOper::opendir(const char *path, struct fuse_file_info *ffi) {

    if (!ifExist(path))
        return -ENOENT;
    auto rid = getRid(path);
    if (rid < 0)
        return -ENOENT;
    struct stat st;
    getFileMetainfo(rid,st);
    if ( !S_ISDIR(st.st_mode))
        return -ENOTDIR;
    return 0;
}

bool TerarkFuseOper::ifDictExist(const std::string &path) {

    if (path.back() == '/')
        return ctx->indexKeyExists(path_idx_id,path);
    else
        return ctx->indexKeyExists(path_idx_id,path + "/");
}

int TerarkFuseOper::unlink(const char *path) {
    //std::cout << "TerarkFuseOper::unlink:" << path << std::endl;
    if ( path == "/"){
        //remove root is unaccess
        return -EACCES;
    }
    if ( !ifExist(path))
        return -ENOENT;
    if ( ifDictExist(path))
        return -EISDIR;
    auto rid = getRid(path);
    if ( rid < 0)
        return -ENOENT;
    ctx->removeRow(rid);
    return 0;
}

int TerarkFuseOper::rmdir(const char *path) {

    if ( path == "/"){
        //remove root is unaccess
        return -EACCES;
    }
    if ( !ifExist(path)) {
        return -ENOENT;
    }
    if ( !ifDictExist(path)) {
        return -ENOTDIR;
    }
    auto rid = getRid(path);
    if ( rid < 0){
        return -EACCES;
    }
    ctx->removeRow(rid);
    return 0;
}

int TerarkFuseOper::chmod(const char *path, mode_t mod) {
    //has bug!!!
    //std::cout << "TerarkFuseOper::chmod:" << path << std::endl;
    if ( !ifExist(path))
        return -ENOENT;
    auto rid = getRid(path);
    if ( rid < 0)
        return -ENOENT;
    updateMode(rid,mod);
    updateCtime(rid);
    return 0;
}

bool TerarkFuseOper::updateMode(terark::llong rid, const mode_t &mod) {

    assert(rid >= 0);
    tab->updateColumn(rid,file_mode_id,Schema::fstringOf(&mod));
    return true;
}

int TerarkFuseOper::rename(const char *old_path, const char *new_path) {


    if ( !ifExist(old_path))
        return -ENOENT;
    if ( ifExist(new_path))
        return -EEXIST;
    if (strcmp(old_path,new_path) == 0)
        return 0;
    auto rid = getRid(old_path);
    if (rid < 0)
        return -ENOENT;

    TFS tfs;
    valvec<byte> row;
    ctx->getValue(rid,&row);
    tfs.decode(row);
    tfs.path = new_path;
    terark::NativeDataOutput<terark::AutoGrownMemIO> rowBuilder;
    rowBuilder.rewind();
    rowBuilder << tfs;
    auto ret = ctx->upsertRow(rowBuilder.written());
    if (ret < 0 )
        return -EACCES;
    ctx->removeRow(rid);
    return 0;
}

int TerarkFuseOper::chown(const char *path, uint64_t owner,uint64_t group) {
    //std::cout << "TerarkFuseOper::chown:" << path << std::endl;
    if ( !ifExist(path))
        return -ENOENT;
    auto rid = getRid(path);
    if ( rid < 0)
        return -ENOENT;

    tab->updateColumn(rid,file_uid_id,Schema::fstringOf(&owner));
    tab->updateColumn(rid,file_gid_id,Schema::fstringOf(&group));
    updateCtime(rid);
    return 0;
}

int TerarkFuseOper::truncate(const char *path, off_t size) {

    if ( size < 0)
        return -EINVAL;
    if (!ifExist(path))
        return -ENOENT;
    if (ifDictExist(path))
        return -EISDIR;
    auto rid = getRid(path);
    if ( rid < 0)
        return -ENOENT;
    valvec<byte> row_data;
    ctx->getValue(rid, &row_data);

    TFS tfs;
    tfs.decode(row_data);


    tfs.content.resize(size,'\0');
    tfs.size = tfs.content.size();

    terark::NativeDataOutput<terark::AutoGrownMemIO> rowBuilder;
    rowBuilder.rewind();
    rowBuilder << tfs;
    rid = ctx->upsertRow(rowBuilder.written());
    if (rid < 0)
        return -EACCES;
    updateCtime(rid);
    updateMtime(rid);
    return 0;
}

int TerarkFuseOper::utime(const char *path, struct utimbuf *tb) {

    if ( !ifExist(path))
        return -ENOENT;
    auto rid = getRid(path);
    if ( rid < 0)
        return -ENOENT;
    uint64_t temp_atime;
    uint64_t temp_mtime;

    if ( tb == NULL) {
        temp_mtime = temp_atime = time(NULL);
    }
    else{
        temp_atime = tb->actime;
        temp_mtime = tb->modtime;
    }
    temp_atime *= ns_per_sec;
    temp_mtime *= ns_per_sec;
    tab->updateColumn(rid,file_atime_id,Schema::fstringOf(&temp_atime));

    tab->updateColumn(rid,file_mtime_id,Schema::fstringOf(&temp_mtime));
    return 0;
}

int TerarkFuseOper::utimens(const char *path, const timespec tv[2]) {

    if ( !ifExist(path))
        return -ENOENT;
    auto rid = getRid(path);
    if ( rid < 0)
        return -ENOENT;
    uint64_t temp_atime = tv[0].tv_sec * ns_per_sec + tv[0].tv_nsec;
    uint64_t temp_mtime = tv[1].tv_sec * ns_per_sec + tv[1].tv_nsec;
    tab->updateColumn(rid,file_atime_id,Schema::fstringOf(&temp_atime));
    tab->updateColumn(rid,file_mtime_id,Schema::fstringOf(&temp_mtime));
    return 0;
}

bool TerarkFuseOper::updateCtime(terark::llong rid, uint64_t ctime) {

    assert(rid >= 0);
    tab->updateColumn(rid,file_ctime_id,Schema::fstringOf(&ctime));
    return true;
}
bool TerarkFuseOper::updateMtime(terark::llong rid, uint64_t mtime) {

    assert(rid >= 0);
    tab->updateColumn(rid,file_mtime_id,Schema::fstringOf(&mtime));
    return true;
}bool TerarkFuseOper::updateAtime(terark::llong rid, uint64_t atime) {

    assert(rid >= 0);
    tab->updateColumn(rid,file_atime_id,Schema::fstringOf(&atime));
    return true;
}

uint64_t TerarkFuseOper::getTime() {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME,&ts);
    return ts.tv_sec * ns_per_sec + ts.tv_nsec;
}

int TerarkFuseOper::flush(const char *path, struct fuse_file_info *ffi) {
    if ( !ifExist(path))
        return -ENOENT;
    if ( ifDictExist(path))
        return -EISDIR;
    return 0;
}

int TerarkFuseOper::release(const char *path, struct fuse_file_info *ffi) {

    if (false == ifExist(path))
        return -ENOENT;
    if (ifDict(path))
        return -EISDIR;
    return 0;
}

terark::llong TerarkFuseOper::insertToBuf(const std::string &path, mode_t mode) {
    return 0;
}


