#!/usr/bin/env bash
#make

sudo rm -rf build
mkdir build
cd build
sudo cmake ..
sudo make
cd ../

#run
sudo mkdir /temp/terark
sudo ./build/terark_fuse -d -s -f /temp/terark -terark_core=./terark-fuse-core
