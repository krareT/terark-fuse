
#define FUSE_USE_VERSION 29

#include <fuse.h>
#include <iostream>
#include <bits/signum.h>
#include <signal.h>
#include "TerarkFuseOper.h"

std::shared_ptr<TerarkFuseOper> g_TFO;

int terark_getattr(const char *path, struct stat *stbuf) {

    return g_TFO->getattr(path, stbuf);
}

int terark_open(const char *path, struct fuse_file_info *ffo) {

    return g_TFO->open(path, ffo);
}

int terark_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *ffo) {

    return g_TFO->read(path, buf, size, offset, ffo);
}

int terark_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
                   off_t offset, struct fuse_file_info *fi) {

    return g_TFO->readdir(path, buf, filler, offset, fi);
}

void fuse_init(struct fuse_operations &fo, TerarkFuseOper &tfo) {

    memset(&fo, 0, sizeof(fo));
    fo.read = terark_read;
    fo.open = terark_open;
    fo.getattr = terark_getattr;
    fo.readdir = terark_readdir;
}

void sig_fuc(int sig) {

    g_TFO.reset();
    exit(1);
}

int main(int argc, char **argv) {

    signal(SIGINT, sig_fuc);
    const char *dbpath = NULL;
    std::string flag = "-terark_core=";
    std::vector<char *> argvec;

    for (int i = 0; i < argc; i++) {
        if (strncmp(flag.c_str(), argv[i], flag.size()) == 0) {
            dbpath = argv[i] + flag.size();
        } else {
            argvec.push_back(argv[i]);
        }
    }
    if (NULL == dbpath) {
        fprintf(stderr, "terark_core is empty!\n");
        exit(1);
    }
    std::cout << dbpath << std::endl;

    g_TFO = std::make_shared<TerarkFuseOper>(dbpath);

    struct fuse_operations terark_fuse_oper;
    fuse_init(terark_fuse_oper, *g_TFO);

    std::unique_ptr<char *> fuse_argv;
    fuse_argv = std::unique_ptr<char *>(new char *[argvec.size()]);
    for (int i = 0; i < argvec.size(); i++) {
        fuse_argv.get()[i] = argvec[i];
    }

    return fuse_main(argvec.size(), fuse_argv.get(), &terark_fuse_oper, NULL);
}