//
// Created by terark on 8/30/16.
//

#ifndef TERARK_FUSE_TERARKFUSEOPER_H
#define TERARK_FUSE_TERARKFUSEOPER_H


#include <cstdio>
#include <fuse.h>
#include <terark/db/db_table.hpp>
#include <tbb/concurrent_unordered_map.h>
#include <iostream>
#include <thread>
#include <terark/db/db_conf.hpp>
#include "tfs.h"
#include <algorithm>
#include <tbb/concurrent_unordered_set.h>
#include <mutex>
#include "TfsBuffer.h"


class TerarkFuseOper {
private:

    TfsBuffer tb;

    void printStat(struct stat &st);

    std::string printFlag(int flag);

    std::string printMode(mode_t mode);

    const uint32_t content_max_len = 67108864;//64M

public:
    static uint64_t ns_per_sec;

    TerarkFuseOper(const char *dbpath);

    ~TerarkFuseOper() {
    }

    int getattr(const char *, struct stat *);

    int (*readlink)(const char *, char *, size_t);

    int (*mknod)(const char *, mode_t, dev_t);

    int mkdir(const char *, mode_t);

    int unlink(const char *);

    int rmdir(const char *);

    int (*symlink)(const char *, const char *);

    int rename(const char *, const char *);

    int (*link)(const char *, const char *);

    int chmod(const char *, mode_t);

    int chown(const char *, uint64_t , uint64_t);


    int truncate(const char *, off_t);


    int utime(const char *, struct utimbuf *);

    //not support flag:O_ASYNC,O_CLOEXEC,O_DIRECT
    int open(const char *, struct fuse_file_info *);


    int read(const char *, char *, size_t, off_t,
             struct fuse_file_info *);


    int write(const char *, const char *, size_t, off_t,
              struct fuse_file_info *);


    int (*statfs)(const char *, struct statvfs *);


    int flush(const char *, struct fuse_file_info *);


    int release(const char *, struct fuse_file_info *);

    int (*fsync)(const char *, int, struct fuse_file_info *);


    int (*setxattr)(const char *, const char *, const char *, size_t, int);


    int (*getxattr)(const char *, const char *, char *, size_t);


    int (*listxattr)(const char *, char *, size_t);


    int (*removexattr)(const char *, const char *);

    int opendir(const char *, struct fuse_file_info *);


    int readdir(const char *, void *, fuse_fill_dir_t, off_t,
                struct fuse_file_info *);


    int (*releasedir)(const char *, struct fuse_file_info *);


    int (*fsyncdir)(const char *, int, struct fuse_file_info *);


    void *(*init)(struct fuse_conn_info *conn);


    void (*destroy)(void *);


    int (*access)(const char *, int);


    int create(const char *, mode_t, struct fuse_file_info *);


    int (*ftruncate)(const char *, off_t, struct fuse_file_info *);

    int (*fgetattr)(const char *, struct stat *, struct fuse_file_info *);


    int (*lock)(const char *, struct fuse_file_info *, int cmd,
                struct flock *);


    int utimens(const char *, const struct timespec tv[2]);


    int (*bmap)(const char *, size_t blocksize, uint64_t *idx);


    unsigned int flag_nullpath_ok:1;

    unsigned int flag_nopath:1;

    unsigned int flag_utime_omit_ok:1;

    unsigned int flag_reserved:29;


    int (*ioctl)(const char *, int cmd, void *arg,
                 struct fuse_file_info *, unsigned int flags, void *data);


    int (*poll)(const char *, struct fuse_file_info *,
                struct fuse_pollhandle *ph, unsigned *reventsp);


    int (*write_buf)(const char *, struct fuse_bufvec *buf, off_t off,
                     struct fuse_file_info *);


    int (*read_buf)(const char *, struct fuse_bufvec **bufp,
                    size_t size, off_t off, struct fuse_file_info *);

    int (*flock)(const char *, struct fuse_file_info *, int op);


    int (*fallocate)(const char *, int, off_t, off_t,
                     struct fuse_file_info *);
};

#endif //TERARK_FUSE_TERARKFUSEOPER_H
