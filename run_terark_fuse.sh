#!/usr/bin/env bash
#make
mnt_dir=/newssd1/posix_tefuse/
core_dir=/newssd1/tefuse_core/
echo "------------------- cp ./dbmeta.json $core_dir/"
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
mkdir $core_dir
cp ./dbmeta.json $core_dir/
#rm -f $core_dir/run.lock
#rm -rf $core_dir/g-*
./build/terark_fuse -f -o kernel_cache -o big_writes -o nonempty -o allow_other -o auto_unmount $mnt_dir -terark_core=$core_dir
