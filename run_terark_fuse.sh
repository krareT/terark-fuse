#!/usr/bin/env bash
#make
mnt_dir=/tmp/terark

rm -rf build
mkdir build
cd build
cmake ..
make
cd ../

#run
fusermount -u $mnt_dir
mkdir $mnt_dir
rm -f terark-fuse-core/run.lock
rm -rf terark-fuse-core/g*
gdb --args ./build/terark_fuse -d -s -f -o default_permissions -o nonempty $mnt_dir -o allow_other -o auto_unmount -terark_core=./terark-fuse-core
