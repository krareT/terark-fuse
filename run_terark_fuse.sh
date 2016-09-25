#!/usr/bin/env bash
#make
mnt_dir=/data/publicdata/wikipedia/experiment/posix_tefuse/
core_dir=./terark-fuse-core/
echo "------------------- cp ./dbmeta.json $core_dir/"
cp ./dbmeta.json $core_dir/
echo "-------------------rebuild"
rm -rf build
mkdir build
cd build
cmake ..
make
cd ../

#run
echo "-------------------clean"
fusermount -u $mnt_dir
mkdir $mnt_dir
rm -f $core_dir/run.lock
rm -rf $core_dir/g-*
gdb --args ./build/terark_fuse -d -f -s -o big_writes -o nonempty -o allow_other -o auto_unmount $mnt_dir -terark_core=$core_dir
