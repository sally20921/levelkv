/******* levelkv *******/
/* db_impl.h
* 
* 
*/

#ifndef _db_impl_h_
#define _db_impl_h_

#include <stdint.h>
#include <stdio.h>
#include <string>
#include "levelkv/db.h"
#include "kvssd/kvssd.h"
#include "kv_index.h"

namespace levelkv {

// Monitor for async I/O
class Monitor {
public:
  std::mutex mtx_;
  std::condition_variable cv_;
  bool ready_ ;
  Monitor() : ready_(false) {}
  ~Monitor(){}
  void reset() {ready_ = false;};
  void notify() {
    std::unique_lock<std::mutex> lck(mtx_);
    ready_ = true;
    cv_.notify_one();
  }
  void wait() {
    std::unique_lock<std::mutex> lck(mtx_);
    while (!ready_) cv_.wait(lck);
  }
};

class DBImpl : public DB{
friend class DBIterator;
public:
  DBImpl(const Options& options, const std::string& dbname);
  ~DBImpl();

  // Implementations of the DB interface
  Status Put(const WriteOptions&, const Slice& key, const Slice& value);
  Status Delete(const WriteOptions&, const Slice& key);
  Status Write(const WriteOptions& options, WriteBatch* updates);
  Status Get(const ReadOptions& options,
                     const Slice& key,
                     std::string* value);
  Iterator* NewIterator(const ReadOptions&);

  kvssd::KVSSD* GetKVSSD() {return kvd_;}
  KVIndex* GetKVIndex() {return key_idx_;}

private:
  kvssd::KVSSD *kvd_;
  KVIndex *key_idx_;

  const Options options_;

public:
  // DEBUG ONLY
  void close_idx () {
    delete key_idx_;
  }
  void open_idx() {
    if (options_.indexType == LSM)
      key_idx_ = NewLSMIndex(options_, kvd_);
    else if (options_.indexType == BTREE)
      key_idx_ = NewBTreeIndex(options_, kvd_);
    else if (options_.indexType == BASE) {
      key_idx_ = NewBaseIndex(options_, kvd_);
    }
    else if (options_.indexType == INMEM) {
      key_idx_ = NewInMemIndex(options_, kvd_);
    }
    else {
      printf("WRONG KV INDEX TYPE\n");
      exit(-1);
    }
  }
};


}  // namespace levelkv



#endif