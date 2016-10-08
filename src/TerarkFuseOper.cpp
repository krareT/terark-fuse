//
// Created by terark on 8/30/16.
//

#include "TerarkFuseOper.h"

uint64_t TerarkFuseOper::ns_per_sec = 1000000000;
using namespace terark;
using namespace db;

TerarkFuseOper::TerarkFuseOper(const char *dbpath):tb(dbpath){

}

int TerarkFuseOper::create(const char *path, mode_t mod, struct fuse_file_info *ffi) {

    if (tb.exist(path) != TfsBuffer::FILE_TYPE::NOF)
        return -EEXIST;
    if (tb.insertToBuf(path, mod | S_IFREG) < 0)
        return -ENOENT;
    return 0;
}

int TerarkFuseOper::getattr(const char *path, struct stat *stbuf) {

    std::cout << "tfo->getattr:" << path << std::endl;
    if (strcmp(path,"/terark-compact") == 0){
        tb.compact();
        return -ENOENT;
    }
    memset(stbuf, 0, sizeof(struct stat));
    auto ret = tb.exist(path);
    if ( ret == TfsBuffer::FILE_TYPE::NOF)
        return -ENOENT;
    if (false == tb.getFileInfo(path,*stbuf))
        return  -ENOENT;
    return 0;
}

int TerarkFuseOper::open(const char *path, struct fuse_file_info *ffo) {

    auto ret = tb.exist(path);
    if (ret == TfsBuffer::FILE_TYPE::NOF) {
        return -ENOENT;
    }
    if (ret == TfsBuffer::FILE_TYPE::DIR){
        return -EISDIR;
    }
    if (tb.loadToBuf(path) < 0)
        return -EACCES;
    return 0;
}

int TerarkFuseOper::read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *ffo) {

    //std::cout << "TerarkFuseOper::read:" << path << std::endl;
    //check if exist
    if (tb.exist(path) == TfsBuffer::FILE_TYPE::NOF)
        return -ENOENT;

    tb.read(path,buf,size,offset);
    return size;
}

int TerarkFuseOper::readdir(const char *path, void *buf, fuse_fill_dir_t filler,
                            off_t offset, struct fuse_file_info *fi) {

    auto ret = tb.exist(path);
    if ( ret == TfsBuffer::FILE_TYPE::NOF)
        return -ENOENT;
    if ( ret != TfsBuffer::FILE_TYPE::DIR)
        return -ENOTDIR;
    filler(buf, ".", NULL, 0);
    filler(buf, "..", NULL, 0);
    std::string path_str = path;
    if (path_str.back() != '/')
        path_str.push_back('/');

    auto iter = tb.getDirIter(path_str);
    std::string file_name;
    while(tb.getNextFile(iter,path_str,file_name)){
        if (file_name.size() == 0)
            continue;
        filler(buf,file_name.c_str(),NULL,0);
        std::cout << "readdir:" << file_name.c_str() << std::endl;
    }
    return 0;
}

int TerarkFuseOper::write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *ffi) {

    //std::cout << "TerarkFuseOper::write:" << path << std::endl;
    //std::cout << "TerarkFuseOper::write:flag:" << printFlag(ffi->flags) << std::endl;

    valvec<byte> row_data;
    //check if exist
    if (tb.exist(path) == TfsBuffer::FILE_TYPE::NOF)
        return -ENOENT;
    return tb.write(path,buf,size,offset);
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
int TerarkFuseOper::mkdir(const char *path, mode_t mod) {

    if (tb.exist(path) != TfsBuffer::FILE_TYPE::NOF)
        return -EEXIST;
    std::string path_str(path);
    if (path_str.back() != '/') {
        path_str.push_back('/');
    }
    mod |= S_IFDIR;
    if (tb.insertToBuf(path_str,mod) < 0)
        return -EACCES;
    tb.release(path_str);
    return 0;
}

int TerarkFuseOper::opendir(const char *path, struct fuse_file_info *ffi) {

    auto ret = tb.exist(path);
    if (ret == TfsBuffer::FILE_TYPE::NOF)
        return -ENOENT;
    if (ret == TfsBuffer::FILE_TYPE::REG)
        return -ENOTDIR;
    return 0;
}

int TerarkFuseOper::unlink(const char *path) {
    if ( path == "/"){
        //remove root is unaccess
        return -EACCES;
    }
    auto ret = tb.remove(path);
    return ret ?0:-ENOENT;
}

int TerarkFuseOper::rmdir(const char *path) {

    if ( path == "/"){
        //remove root is unaccess
        return -EACCES;
    }
    auto ret = tb.exist(path);
    if (ret == TfsBuffer::FILE_TYPE::REG)
        return -ENOTDIR;
    if (ret == TfsBuffer::FILE_TYPE::NOF){
        return -ENOENT;
    }
    tb.remove(path);
    return 0;
}

int TerarkFuseOper::chmod(const char *path, mode_t mod) {
    //has bug!!!
    //std::cout << "TerarkFuseOper::chmod:" << path << std::endl;
    if ( TfsBuffer::FILE_TYPE::NOF == tb.exist(path))
        return -ENOENT;
    return 0;
}

int TerarkFuseOper::rename(const char *old_path, const char *new_path) {

    return 0;
}

int TerarkFuseOper::chown(const char *path, uint64_t owner,uint64_t group) {
    return 0;
}

int TerarkFuseOper::truncate(const char *path, off_t size) {
    return tb.truncate(path,size);
}

int TerarkFuseOper::utime(const char *path, struct utimbuf *tb) {

    return 0;
}

int TerarkFuseOper::utimens(const char *path, const timespec tv[2]) {

    return 0;
}
int TerarkFuseOper::flush(const char *path, struct fuse_file_info *ffi) {
    return 0;
}

int TerarkFuseOper::release(const char *path, struct fuse_file_info *ffi) {

    if (TfsBuffer::FILE_TYPE::NOF == tb.exist(path))
        return -ENOENT;
    tb.release(path);
    return 0;
}
