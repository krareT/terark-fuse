#!/usr/bin/env bash
#make
#mnt_dir,where the terark-fuse mount
mnt_dir=/newssd1/posix_benchmark/tefuse/
#core_dir,where store the terarkdb's data
core_dir=/newssd1/tefuse_core/
#build terark-fuse
echo "-------------------rebuild"
rm -rf build
mkdir build
cd build
cmake ..
make
cd ../
echo "-------------------clean"
#fusermount is the fuse's tools to umount
fusermount -u $mnt_dir

mkdir $mnt_dir
mkdir $core_dir
#copy the dbmeta to core_dir
cp ./dbmeta.json $core_dir/

#rm -f $core_dir/run.lock
#rm -rf $core_dir/g-*
#run terark-fuse
./build/terark_fuse -f -o kernel_cache -o big_writes -o nonempty -o allow_other -o auto_unmount $mnt_dir -terark_core=$core_dir
