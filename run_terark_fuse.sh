#!/usr/bin/env bash
#make
mnt_dir=/data/publicdata/wikipedia/experiment/posix_tefuse/
core_dir=./terark-fuse-core/ 
echo "------------------- cp ./dbmeta.json $core_dir/"
cp ./dbmeta.json $core_dir/
echo "------------------- ~/Documents/pkg/terark-db-Linux-x86_64-g++-5.3-bmi2-1/bin/terark-db-schema-compile.exe ./dbmeta.json terark TFS > ./src/tfs.h"
/home/terark/Documents/pkg/terark-db-Linux-x86_64-g++-5.3-bmi2-1/bin/terark-db-schema-compile.exe ./dbmeta.json terark TFS > ./src/tfs.h
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
echh "-------------------run"
gdb --args ./build/terark_fuse -d -f -s -o nonempty -o allow_other -o auto_unmount $mnt_dir -terark_core=$core_dir
