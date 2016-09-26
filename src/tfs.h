#pragma once
#include <terark/db/db_table.hpp>
#include <terark/io/DataIO.hpp>
#include <terark/io/MemStream.hpp>
#include <terark/io/RangeStream.hpp>
#include <boost/static_assert.hpp>


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

    TFS& decode(terark::fstring ___row) {
      terark::NativeDataInput<terark::MemIO> ___dio(___row.range());
      ___dio >> *this;
      return *this;
    }
    terark::fstring
    encode(terark::NativeDataOutput<terark::AutoGrownMemIO>& ___dio) const {
      ___dio.rewind();
      ___dio << *this;
      return ___dio.written();
    }

    // DbTablePtr use none-const ref is just for ensure application code:
    // var 'tab' must be a 'DbTablePtr', can not be a 'DbTable*'
    static bool
    checkTableSchema(terark::db::DbTablePtr& tab, bool checkColname = false);

    static terark::db::DbTablePtr
    openTable(const boost::filesystem::path& dbdir, bool checkColname = false) {
      using namespace terark::db;
      DbTablePtr tab = DbTable::open(dbdir);
      if (!checkTableSchema(tab, checkColname)) {
        THROW_STD(invalid_argument,
          "database schema is inconsistence with compiled c++ code, dbdir: %s",
          dbdir.string().c_str());
      }
      return tab;
    }

    static bool
    checkSchema(const terark::db::Schema& schema, bool checkColname = false) {
      using namespace terark;
      using namespace terark::db;
      if (schema.columnNum() != 12) {
        return false;
      }
      {
        const fstring     colname = schema.getColumnName(0);
        const ColumnMeta& colmeta = schema.getColumnMeta(0);
        if (checkColname && colname != "path") {
          return false;
        }
        if (colmeta.type != ColumnType::StrZero) {
          return false;
        }
      }
      {
        const fstring     colname = schema.getColumnName(1);
        const ColumnMeta& colmeta = schema.getColumnMeta(1);
        if (checkColname && colname != "content") {
          return false;
        }
        if (colmeta.type != ColumnType::Binary) {
          return false;
        }
      }
      {
        const fstring     colname = schema.getColumnName(2);
        const ColumnMeta& colmeta = schema.getColumnMeta(2);
        if (checkColname && colname != "mode") {
          return false;
        }
        if (colmeta.type != ColumnType::Uint32) {
          return false;
        }
      }
      {
        const fstring     colname = schema.getColumnName(3);
        const ColumnMeta& colmeta = schema.getColumnMeta(3);
        if (checkColname && colname != "ino") {
          return false;
        }
        if (colmeta.type != ColumnType::Uint64) {
          return false;
        }
      }
      {
        const fstring     colname = schema.getColumnName(4);
        const ColumnMeta& colmeta = schema.getColumnMeta(4);
        if (checkColname && colname != "nlink") {
          return false;
        }
        if (colmeta.type != ColumnType::Uint64) {
          return false;
        }
      }
      {
        const fstring     colname = schema.getColumnName(5);
        const ColumnMeta& colmeta = schema.getColumnMeta(5);
        if (checkColname && colname != "uid") {
          return false;
        }
        if (colmeta.type != ColumnType::Uint64) {
          return false;
        }
      }
      {
        const fstring     colname = schema.getColumnName(6);
        const ColumnMeta& colmeta = schema.getColumnMeta(6);
        if (checkColname && colname != "gid") {
          return false;
        }
        if (colmeta.type != ColumnType::Uint64) {
          return false;
        }
      }
      {
        const fstring     colname = schema.getColumnName(7);
        const ColumnMeta& colmeta = schema.getColumnMeta(7);
        if (checkColname && colname != "size") {
          return false;
        }
        if (colmeta.type != ColumnType::Uint64) {
          return false;
        }
      }
      {
        const fstring     colname = schema.getColumnName(8);
        const ColumnMeta& colmeta = schema.getColumnMeta(8);
        if (checkColname && colname != "blocks") {
          return false;
        }
        if (colmeta.type != ColumnType::Uint64) {
          return false;
        }
      }
      {
        const fstring     colname = schema.getColumnName(9);
        const ColumnMeta& colmeta = schema.getColumnMeta(9);
        if (checkColname && colname != "atime") {
          return false;
        }
        if (colmeta.type != ColumnType::Uint64) {
          return false;
        }
      }
      {
        const fstring     colname = schema.getColumnName(10);
        const ColumnMeta& colmeta = schema.getColumnMeta(10);
        if (checkColname && colname != "mtime") {
          return false;
        }
        if (colmeta.type != ColumnType::Uint64) {
          return false;
        }
      }
      {
        const fstring     colname = schema.getColumnName(11);
        const ColumnMeta& colmeta = schema.getColumnMeta(11);
        if (checkColname && colname != "ctime") {
          return false;
        }
        if (colmeta.type != ColumnType::Uint64) {
          return false;
        }
      }
      return true;
    }
  }; // TFS

  struct TFS_Colgroup_path {
    std::string path;

    DATA_IO_LOAD_SAVE(TFS_Colgroup_path,
      &terark::RestAll(path)
    )

    TFS_Colgroup_path& decode(terark::fstring ___row) {
      terark::NativeDataInput<terark::MemIO> ___dio(___row.range());
      ___dio >> *this;
      return *this;
    }
    terark::fstring
    encode(terark::NativeDataOutput<terark::AutoGrownMemIO>& ___dio) const {
      ___dio.rewind();
      ___dio << *this;
      return ___dio.written();
    }
    TFS_Colgroup_path& select(const TFS& ___row) {
      path = ___row.path;
      return *this;
    }
    void assign_to(TFS& ___row) const {
      ___row.path = path;
    }

    static bool
    checkSchema(const terark::db::Schema& schema, bool checkColname = false) {
      using namespace terark;
      using namespace terark::db;
      if (schema.columnNum() != 1) {
        return false;
      }
      {
        const fstring     colname = schema.getColumnName(0);
        const ColumnMeta& colmeta = schema.getColumnMeta(0);
        if (checkColname && colname != "path") {
          return false;
        }
        if (colmeta.type != ColumnType::StrZero) {
          return false;
        }
      }
      return true;
    }
  }; // TFS_Colgroup_path
  typedef TFS_Colgroup_path TFS_Index_path;


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

    TFS_Colgroup_file_stat& decode(terark::fstring ___row) {
      terark::NativeDataInput<terark::MemIO> ___dio(___row.range());
      ___dio >> *this;
      return *this;
    }
    terark::fstring
    encode(terark::NativeDataOutput<terark::AutoGrownMemIO>& ___dio) const {
      ___dio.rewind();
      ___dio << *this;
      return ___dio.written();
    }
    TFS_Colgroup_file_stat& select(const TFS& ___row) {
      mode   = ___row.mode;
      ino    = ___row.ino;
      nlink  = ___row.nlink;
      uid    = ___row.uid;
      gid    = ___row.gid;
      size   = ___row.size;
      blocks = ___row.blocks;
      atime  = ___row.atime;
      mtime  = ___row.mtime;
      ctime  = ___row.ctime;
      return *this;
    }
    void assign_to(TFS& ___row) const {
      ___row.mode   = mode;
      ___row.ino    = ino;
      ___row.nlink  = nlink;
      ___row.uid    = uid;
      ___row.gid    = gid;
      ___row.size   = size;
      ___row.blocks = blocks;
      ___row.atime  = atime;
      ___row.mtime  = mtime;
      ___row.ctime  = ctime;
    }

    static bool
    checkSchema(const terark::db::Schema& schema, bool checkColname = false) {
      using namespace terark;
      using namespace terark::db;
      if (schema.columnNum() != 10) {
        return false;
      }
      {
        const fstring     colname = schema.getColumnName(0);
        const ColumnMeta& colmeta = schema.getColumnMeta(0);
        if (checkColname && colname != "mode") {
          return false;
        }
        if (colmeta.type != ColumnType::Uint32) {
          return false;
        }
      }
      {
        const fstring     colname = schema.getColumnName(1);
        const ColumnMeta& colmeta = schema.getColumnMeta(1);
        if (checkColname && colname != "ino") {
          return false;
        }
        if (colmeta.type != ColumnType::Uint64) {
          return false;
        }
      }
      {
        const fstring     colname = schema.getColumnName(2);
        const ColumnMeta& colmeta = schema.getColumnMeta(2);
        if (checkColname && colname != "nlink") {
          return false;
        }
        if (colmeta.type != ColumnType::Uint64) {
          return false;
        }
      }
      {
        const fstring     colname = schema.getColumnName(3);
        const ColumnMeta& colmeta = schema.getColumnMeta(3);
        if (checkColname && colname != "uid") {
          return false;
        }
        if (colmeta.type != ColumnType::Uint64) {
          return false;
        }
      }
      {
        const fstring     colname = schema.getColumnName(4);
        const ColumnMeta& colmeta = schema.getColumnMeta(4);
        if (checkColname && colname != "gid") {
          return false;
        }
        if (colmeta.type != ColumnType::Uint64) {
          return false;
        }
      }
      {
        const fstring     colname = schema.getColumnName(5);
        const ColumnMeta& colmeta = schema.getColumnMeta(5);
        if (checkColname && colname != "size") {
          return false;
        }
        if (colmeta.type != ColumnType::Uint64) {
          return false;
        }
      }
      {
        const fstring     colname = schema.getColumnName(6);
        const ColumnMeta& colmeta = schema.getColumnMeta(6);
        if (checkColname && colname != "blocks") {
          return false;
        }
        if (colmeta.type != ColumnType::Uint64) {
          return false;
        }
      }
      {
        const fstring     colname = schema.getColumnName(7);
        const ColumnMeta& colmeta = schema.getColumnMeta(7);
        if (checkColname && colname != "atime") {
          return false;
        }
        if (colmeta.type != ColumnType::Uint64) {
          return false;
        }
      }
      {
        const fstring     colname = schema.getColumnName(8);
        const ColumnMeta& colmeta = schema.getColumnMeta(8);
        if (checkColname && colname != "mtime") {
          return false;
        }
        if (colmeta.type != ColumnType::Uint64) {
          return false;
        }
      }
      {
        const fstring     colname = schema.getColumnName(9);
        const ColumnMeta& colmeta = schema.getColumnMeta(9);
        if (checkColname && colname != "ctime") {
          return false;
        }
        if (colmeta.type != ColumnType::Uint64) {
          return false;
        }
      }
      return true;
    }
  }; // TFS_Colgroup_file_stat

  struct TFS_Colgroup_content {
    std::string content;

    DATA_IO_LOAD_SAVE(TFS_Colgroup_content,
      &terark::RestAll(content)
    )

    TFS_Colgroup_content& decode(terark::fstring ___row) {
      terark::NativeDataInput<terark::MemIO> ___dio(___row.range());
      ___dio >> *this;
      return *this;
    }
    terark::fstring
    encode(terark::NativeDataOutput<terark::AutoGrownMemIO>& ___dio) const {
      ___dio.rewind();
      ___dio << *this;
      return ___dio.written();
    }
    TFS_Colgroup_content& select(const TFS& ___row) {
      content = ___row.content;
      return *this;
    }
    void assign_to(TFS& ___row) const {
      ___row.content = content;
    }

    static bool
    checkSchema(const terark::db::Schema& schema, bool checkColname = false) {
      using namespace terark;
      using namespace terark::db;
      if (schema.columnNum() != 1) {
        return false;
      }
      {
        const fstring     colname = schema.getColumnName(0);
        const ColumnMeta& colmeta = schema.getColumnMeta(0);
        if (checkColname && colname != "content") {
          return false;
        }
        if (colmeta.type != ColumnType::Binary) {
          return false;
        }
      }
      return true;
    }
  }; // TFS_Colgroup_content

  // DbTablePtr use none-const ref is just for ensure application code:
  // var 'tab' must be a 'DbTablePtr', can not be a 'DbTable*'
  inline bool TFS::checkTableSchema(terark::db::DbTablePtr& tab, bool checkColname) {
    using namespace terark::db;
    assert(tab.get() != nullptr);
    const SchemaConfig& sconf = tab->getSchemaConfig();
    if (!TFS::checkSchema(*sconf.m_rowSchema)) {
      return false;
    }
    if (!TFS_Colgroup_path::checkSchema(
          sconf.getColgroupSchema(0), checkColname)) {
      return false;
    }
    if (!TFS_Colgroup_file_stat::checkSchema(
          sconf.getColgroupSchema(1), checkColname)) {
      return false;
    }
    if (!TFS_Colgroup_content::checkSchema(
          sconf.getColgroupSchema(2), checkColname)) {
      return false;
    }
    return true;
  } // TFS

} // namespace terark
