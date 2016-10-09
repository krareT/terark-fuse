cmake_minimum_required(VERSION 2.8)
project(terark_fuse)

set(TERARKDB_PATH /home/terark/Documents/pkg/terark-db-Linux-x86_64-g++-5.3-bmi2-1/)


include_directories(${TERARKDB_PATH}/include)
link_directories(${TERARKDB_PATH}/lib)
link_directories(/usr/local/lib/)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -D_FILE_OFFSET_BITS=64 -g")
set(SOURCE_FILES src/main.cpp src/TerarkFuseOper.cpp src/TerarkFuseOper.h src/TfsBuffer.cpp src/TfsBuffer.h)

add_executable(terark_fuse ${SOURCE_FILES})
target_link_libraries(terark_fuse
                        tbb
                        fuse
                        boost_system
                        boost_filesystem
                        terark-fsa_all-d
                        terark-db-d
                        wiredtiger
                        pthread )