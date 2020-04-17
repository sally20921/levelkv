/******* levelkv *******/
/* db_impl.cc
* 
* 
*/
#include <mutex>
#include <condition_variable>
#include "levelkv/db.h"
#include "levelkv/iterator.h"
#include "db_impl.h"
#include "db_iter.h"

namespace levelkv {

static void on_io_complete(void *args) {
    Monitor *mon = (Monitor *)args;
    mon->notify();
}

DBImpl::DBImpl(const Options& options, const std::string& dbname) 
: options_(options) {
  kvd_ = new kvssd::KVSSD(dbname.c_str());
  if (options.indexType == LSM)
	  key_idx_ = NewLSMIndex(options, kvd_);
  else if (options.indexType == BTREE)
    key_idx_ = NewBTreeIndex(options, kvd_);
  else if (options.indexType == BASE) {
    key_idx_ = NewBaseIndex(options, kvd_);
  }
  else if (options.indexType == INMEM) {
    key_idx_ = NewInMemIndex(options, kvd_);
  }
  else {
    printf("WRONG KV INDEX TYPE\n");
    exit(-1);
  }
}

DBImpl::~DBImpl() {
  delete key_idx_;
	delete kvd_;
}

Status DBImpl::Put(const WriteOptions& options,
                     const Slice& key,
                     const Slice& value) {
  kvssd::Slice put_key(key.data(), key.size());
  kvssd::Slice put_val(value.data(), value.size());
  Monitor mon;
  kvd_->kv_store_async(&put_key, &put_val, on_io_complete, &mon);
  key_idx_->Put(key);

  mon.wait(); // wait data I/O done
  return Status();
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
  kvssd::Slice del_key(key.data(), key.size());
	kvd_->kv_delete(&del_key);
  key_idx_->Delete(key);
  return Status();
}

Status DBImpl::Write(const WriteOptions& options, WriteBatch* updates) {

  return Status();
}

Status DBImpl::Get(const ReadOptions& options,
                     const Slice& key,
                     std::string* value) {
  kvssd::Slice get_key(key.data(), key.size());
  char *vbuf;
	int vlen;
	kvd_->kv_get(&get_key, vbuf, vlen);
	value->append(vbuf, vlen);
  free(vbuf);

  return Status();
}

Iterator* DBImpl::NewIterator(const ReadOptions& options) {
  return NewDBIterator(this, options);
}

Status DB::Open(const Options& options, const std::string& dbname,
                DB** dbptr) {

  *dbptr = NULL;

  DB *db = new DBImpl(options, dbname);
  *dbptr = db;
  return Status(Status::OK());
}

}  // namespace levelkv
