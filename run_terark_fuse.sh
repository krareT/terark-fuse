#!/usr/bin/env bash
#make
mnt_dir=/temp/terark

sudo rm -rf build
mkdir build
cd build
sudo cmake ..
sudo make
cd ../

#run
sudo fusermount -u $mnt_dir
sudo mkdir /temp/terark
sudo rm -f terark-fuse-core/run.lock
sudo rm -rf terark-fuse-core/g*
sudo gdb --args ./build/terark_fuse -d -s -f -o default_permissions -o nonempty $mnt_dir -terark_core=./terark-fuse-core
