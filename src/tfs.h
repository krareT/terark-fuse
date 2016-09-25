#pragma once
#include <terark/db/db_table.hpp>
#include <terark/io/DataIO.hpp>
#include <terark/io/MemStream.hpp>
#include <terark/io/RangeStream.hpp>
#include <tbb/reader_writer_lock.h>

namespace terark {
  struct TFS {
    std::string path;
    std::string content;
    std::uint32_t mode;
    std::uint64_t ino;
    std::uint64_t nlink;
    std::uint64_t uid;
    std::uint64_t gid;
    std::uint64_t size;
    std::uint64_t blocks;
    std::uint64_t atime;
    std::uint64_t mtime;
    std::uint64_t ctime;

    DATA_IO_LOAD_SAVE(TFS,
      &terark::db::Schema::StrZero(path)
      &content
      &mode
      &ino
      &nlink
      &uid
      &gid
      &size
      &blocks
      &atime
      &mtime
      &ctime
    )

    TFS& decode(terark::fstring row) {
      terark::NativeDataInput<terark::MemIO> dio(row.range());
      dio >> *this;
      return *this;
    }
    terark::fstring
    encode(terark::NativeDataOutput<terark::AutoGrownMemIO>& dio) const {
      dio.rewind();
      dio << *this;
      return dio.written();
    }
  }; // TFS

  struct TFS_Colgroup_file_stat {
    std::uint32_t mode;
    std::uint64_t ino;
    std::uint64_t nlink;
    std::uint64_t uid;
    std::uint64_t gid;
    std::uint64_t size;
    std::uint64_t blocks;
    std::uint64_t atime;
    std::uint64_t mtime;
    std::uint64_t ctime;

    DATA_IO_LOAD_SAVE(TFS_Colgroup_file_stat,
      &mode
      &ino
      &nlink
      &uid
      &gid
      &size
      &blocks
      &atime
      &mtime
      &ctime
    )

    TFS_Colgroup_file_stat& decode(terark::fstring row) {
      terark::NativeDataInput<terark::MemIO> dio(row.range());
      dio >> *this;
      return *this;
    }
    terark::fstring
    encode(terark::NativeDataOutput<terark::AutoGrownMemIO>& dio) const {
      dio.rewind();
      dio << *this;
      return dio.written();
    }
  }; // TFS_Colgroup_file_stat

} // namespace terark
