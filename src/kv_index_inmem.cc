/******* levelkv *******/
/* kv_index_inmem.cc
* 08/21/2019
* 
*/

#include "kv_index.h"
#include "levelkv/comparator.h"
#include "inmem/inmem.h"

namespace levelkv {


class ComparatorInMem : public inmem::Comparator {
public:
  ComparatorInMem(const levelkv::Comparator* cmp) : cmp_(cmp) {};
  ~ComparatorInMem() {};
  int Compare(const inmem::Slice& a, const inmem::Slice& b) const {
    Slice aa(a.data(), a.size());
    Slice bb(b.data(), b.size());
    return cmp_->Compare(aa, bb);
  }
private:
  const levelkv::Comparator* cmp_;
};

class IDXWriteBatchBase : public IDXWriteBatch {
public: 
  IDXWriteBatchBase () {};
  ~IDXWriteBatchBase () {};
  void Put(const Slice& key) {
    // do nothing
  }
  void Delete(const Slice& key) {
    // do nothing
  }
  void Clear() {}
};

class IDXIteratorInMem: public IDXIterator {
public:
  IDXIteratorInMem(inmem::InMem *inmem_, const ReadOptions& options) {
    it_ = inmem_->NewIterator();
  }
  ~IDXIteratorInMem() { delete it_; }

  bool Valid() const {it_->Valid();}
  void SeekToFirst() {it_->SeekToFirst();}
  void SeekToLast() {
    // NOT IMPLEMENT
  }
  void Seek(const Slice& target) {
    inmem::Slice seek_key(target.data(), target.size());
    it_->Seek(&seek_key);
  }
  void Next() {it_->Next();}
  void Prev() {
    // NOT IMPLEMENT
  }
  Slice key() const {
    inmem::Slice ret_key = it_->key();
    return Slice(ret_key.data(), ret_key.size());
  }
private:
  inmem::InMem::Iterator *it_;
};

class KVIndexInMem : public KVIndex {
public:
  KVIndexInMem (const Options& options, kvssd::KVSSD* kvd);
  ~KVIndexInMem ();

  // implmentations
  bool Put(const Slice& key);
  bool Delete(const Slice& key);
  bool Write(IDXWriteBatch* updates);
  IDXIterator* NewIterator(const ReadOptions& options);
 
private:
  inmem::InMem* inmem_;
  kvssd::KVSSD* kvd_;
  ComparatorInMem* cmp_;
};

KVIndexInMem::KVIndexInMem(const Options& db_options, kvssd::KVSSD* kvd) : kvd_(kvd) {
  // apply db options
  cmp_ = new ComparatorInMem(db_options.comparator);
  inmem_ = new inmem::InMem(cmp_, kvd);
}

KVIndexInMem::~KVIndexInMem() {
  delete cmp_;
  delete inmem_;
}

KVIndex* NewInMemIndex(const Options& options, kvssd::KVSSD* kvd) {
  return new KVIndexInMem(options, kvd);
}

bool KVIndexInMem::Put(const Slice &key) {
  inmem::Slice put_key(key.data(), key.size());
  inmem_->Insert(&put_key);
  return true;
}

bool KVIndexInMem::Delete(const Slice &key) {
  inmem::Slice del_key(key.data(), key.size());
  inmem_->Delete(&del_key);
  return true;
}

bool KVIndexInMem::Write(IDXWriteBatch* updates) {
  // do nothing
  return true;
}

IDXIterator* KVIndexInMem::NewIterator(const ReadOptions& options) {
  return new IDXIteratorInMem(inmem_, options);
}

}  // namespace levelkv
