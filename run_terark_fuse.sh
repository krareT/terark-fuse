#!/usr/bin/env bash
#make
mnt_dir=/data/publicdata/wikipedia/experiment/posix_tefuse/
core_dir=/data/terark_fuse_mount/terark-fuse-core/
rm -rf build
mkdir build
cd build
cmake ..
make
cd ../

#run
fusermount -u $mnt_dir
mkdir $mnt_dir
rm -f $core_dir/run.lock
rm -rf $core_dir/g-*
gdb --args ./build/terark_fuse -d -f -s -o nonempty -o allow_other -o auto_unmount $mnt_dir -terark_core=$core_dir
