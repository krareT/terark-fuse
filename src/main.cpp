
#define FUSE_USE_VERSION 29
#include <fuse.h>
#include <iostream>
#include "TerarkFuseOper.h"

void fuse_init(struct fuse_operations &fo,TerarkFuseOper &tfo){

    fo.read = tfo.read;
    fo.open = tfo.open;
    fo.getattr = tfo.getattr;
    fo.readlink = tfo.readlink;
}
int main(int argc,char **argv) {

    const char* dbpath = NULL;
    std::string flag = "-terark_core=";
    std::vector<char *> argvec;

    for(int i = 0;i < argc;i++){
        if (strncmp(flag.c_str(),argv[i],flag.size()) == 0){
            dbpath = argv[i] + flag.size();
        }
        else {
            argvec.push_back(argv[i]);
        }
    }
    if (NULL == dbpath){
        fprintf(stderr, "terark_core is empty!\n");
        exit(1);
    }
    std::cout << dbpath << std::endl;
    TerarkFuseOper tfo(dbpath);

    struct fuse_operations terark_fuse_oper;
    fuse_init(terark_fuse_oper,tfo);

    std::unique_ptr<char *> fuse_argv;
    fuse_argv = std::unique_ptr<char *>(new char*[argvec.size()]);
    for(int i = 0; i < argvec.size(); i ++){
        fuse_argv.get()[i] = argvec[i];
    }

    return fuse_main(argvec.size(),fuse_argv.get(), &terark_fuse_oper, NULL);
}