/******* levelkv *******/
/* kv_index.h
* 
* 
*/

#include "kv_index.h"
#include "levelkv/comparator.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "leveldb/write_batch.h"
#include "leveldb/comparator.h"

namespace levelkv {


class ComparatorLSM : public leveldb::Comparator {
public:
  ComparatorLSM(const levelkv::Comparator* cmp) : cmp_(cmp) {};
  //~ComparatorLSM() {};
  int Compare(const leveldb::Slice& a, const leveldb::Slice& b) const {
    Slice aa(a.data(), a.size());
    Slice bb(b.data(), b.size());
    return cmp_->Compare(aa, bb);
  }
  const char* Name() const { return "lsm.comparator"; }
  void FindShortestSeparator(
      std::string* start,
      const leveldb::Slice& limit) const {
    // from leveldb bytewise comparator
    // Find length of common prefix
    size_t min_length = std::min(start->size(), limit.size());
    size_t diff_index = 0;
    while ((diff_index < min_length) &&
          ((*start)[diff_index] == limit[diff_index])) {
      diff_index++;
    }

    if (diff_index >= min_length) {
      // Do not shorten if one string is a prefix of the other
    } else {
      uint8_t diff_byte = static_cast<uint8_t>((*start)[diff_index]);
      if (diff_byte < static_cast<uint8_t>(0xff) &&
          diff_byte + 1 < static_cast<uint8_t>(limit[diff_index])) {
        (*start)[diff_index]++;
        start->resize(diff_index + 1);
      }
    }
  }
  void FindShortSuccessor(std::string* key) const {
    // Find first character that can be incremented
    size_t n = key->size();
    for (size_t i = 0; i < n; i++) {
      const uint8_t byte = (*key)[i];
      if (byte != static_cast<uint8_t>(0xff)) {
        (*key)[i] = byte + 1;
        key->resize(i+1);
        return;
      }
    }
    // *key is a run of 0xffs.  Leave it alone.
  }
private:
  const levelkv::Comparator* cmp_;
};

class IDXWriteBatchLSM : public IDXWriteBatch {
public: 
  IDXWriteBatchLSM () {};
  ~IDXWriteBatchLSM () {};
  void Put(const Slice& key) {
    leveldb::Slice put_key(key.data(), key.size());
    leveldb::Slice put_val; //don't care;
    batch_.Put(put_key, put_val);
  }
  void Delete(const Slice& key) {
    leveldb::Slice put_key(key.data(), key.size());
    batch_.Delete(put_key);
  }
  void Clear() {batch_.Clear();}
  void *InternalBatch() {return &batch_;}

public:
  leveldb::WriteBatch batch_;
};

class IDXIteratorLSM : public IDXIterator {
public:
  IDXIteratorLSM(leveldb::DB *db_, const ReadOptions& options) {
    leveldb::ReadOptions rd_option;
    it_ = db_->NewIterator(rd_option);
  }
  ~IDXIteratorLSM() { delete it_; }

  bool Valid() const {it_->Valid();}
  void SeekToFirst() {it_->SeekToFirst();}
  void SeekToLast() {it_->SeekToLast();}
  void Seek(const Slice& target) {
    leveldb::Slice seek_key(target.data(), target.size());
    it_->Seek(seek_key);
  }
  void Next() {it_->Next();}
  void Prev() {it_->Prev();}
  Slice key() const {
    leveldb::Slice ret_key = it_->key();
    return Slice(ret_key.data(), ret_key.size());
  }
private:
  leveldb::Iterator *it_;
};

class KVIndexLSM : public KVIndex {
public:
  KVIndexLSM (const Options& options, kvssd::KVSSD* kvd);
  ~KVIndexLSM ();

  // implmentations
  bool Put(const Slice& key);
  bool Delete(const Slice& key);
  bool Write(IDXWriteBatch* updates);
  IDXIterator* NewIterator(const ReadOptions& options);
 
private:
  leveldb::DB* db_;
  leveldb::WriteOptions write_options_;
  kvssd::KVSSD* kvd_;
  ComparatorLSM* cmp_;
};

KVIndexLSM::KVIndexLSM(const Options& db_options, kvssd::KVSSD* kvd) : kvd_(kvd) {
  leveldb::Options options;
  options.create_if_missing = true;
  options.max_open_files = 1000;
  options.filter_policy = NULL;
  options.compression = leveldb::kNoCompression;
  options.reuse_logs = false;
  options.write_buffer_size = 1 << 20;
  options.max_file_size = 1 << 20;

  // apply db options
  cmp_ = new ComparatorLSM(db_options.comparator);
  options.comparator = cmp_;

  options.env = leveldb::NewKVEnv(leveldb::Env::Default(), kvd);
  leveldb::Status status = leveldb::DB::Open(options, "", &db_);
}

KVIndexLSM::~KVIndexLSM() {
  delete db_;
  delete cmp_;
}

KVIndex* NewLSMIndex(const Options& options, kvssd::KVSSD* kvd) {
  return new KVIndexLSM(options, kvd);
}

bool KVIndexLSM::Put(const Slice &key) {
  leveldb::Slice put_key(key.data(), key.size());
  leveldb::Slice put_val; // don't care
  return (db_->Put(write_options_, put_key, put_val)).ok();
}

bool KVIndexLSM::Delete(const Slice &key) {
  leveldb::Slice put_key(key.data(), key.size());
  return (db_->Delete(write_options_, put_key)).ok();
}

bool KVIndexLSM::Write(IDXWriteBatch* updates) {
  return (db_->Write(write_options_, (leveldb::WriteBatch*)(updates->InternalBatch()))).ok();
}

IDXIterator* KVIndexLSM::NewIterator(const ReadOptions& options) {
  return new IDXIteratorLSM(db_, options);
}

}  // namespace levelkv
