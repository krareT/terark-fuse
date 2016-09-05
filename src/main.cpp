
#define FUSE_USE_VERSION 29

#include <fuse.h>
#include <iostream>
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

int terark_creat(const char *path, mode_t mod, struct fuse_file_info *ffi) {
    return g_TFO->create(path, mod, ffi);
}

int terark_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *ffi) {

    return g_TFO->write(path, buf, size, offset, ffi);
}

int terark_mkdir(const char *path, mode_t mode) {

    return g_TFO->mkdir(path, mode);
}
int terark_opendir(const char *path, struct fuse_file_info *ffi){

    return g_TFO->opendir(path,ffi);
}
void fuse_init(struct fuse_operations &fo, TerarkFuseOper &tfo) {

    memset(&fo, 0, sizeof(fo));
    fo.read = terark_read;
    fo.open = terark_open;
    fo.getattr = terark_getattr;
    fo.readdir = terark_readdir;
    fo.create = terark_creat;
    fo.write = terark_write;
    fo.mkdir = terark_mkdir;
    fo.opendir = terark_opendir;
}

void sig_fuc(int sig) {

    g_TFO.reset();
    exit(1);
}

int main(int argc, char **argv) {

    const char *dbpath = NULL;
    std::string flag = "-terark_core=";
    std::vector<char *> argvec;
    struct fuse_operations terark_fuse_oper;
    std::unique_ptr<char *> fuse_argv;

    signal(SIGINT, sig_fuc);
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
    fuse_init(terark_fuse_oper, *g_TFO);
    fuse_argv = std::unique_ptr<char *>(new char *[argvec.size()]);

    for (int i = 0; i < argvec.size(); i++) {
        fuse_argv.get()[i] = argvec[i];
    }
    return fuse_main(argvec.size(), fuse_argv.get(), &terark_fuse_oper, NULL);
}