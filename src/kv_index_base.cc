/******* levelkv *******/
/* kv_index.h
* 08/04/2019
* 
*/

#include "kv_index.h"
#include "levelkv/comparator.h"
#include "base/base.h"

namespace levelkv {


class ComparatorBase : public base::Comparator {
public:
  ComparatorBase(const levelkv::Comparator* cmp) : cmp_(cmp) {};
  ~ComparatorBase() {};
  int Compare(const base::Slice& a, const base::Slice& b) const {
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

class IDXIteratorBase: public IDXIterator {
public:
  IDXIteratorBase(base::BaseOrder *base_, const ReadOptions& options) {
    it_ = base_->NewIterator();
  }
  ~IDXIteratorBase() { delete it_; }

  bool Valid() const {it_->Valid();}
  void SeekToFirst() {it_->SeekToFirst();}
  void SeekToLast() {
    // NOT IMPLEMENT
  }
  void Seek(const Slice& target) {
    base::Slice seek_key(target.data(), target.size());
    it_->Seek(&seek_key);
  }
  void Next() {it_->Next();}
  void Prev() {
    // NOT IMPLEMENT
  }
  Slice key() const {
    base::Slice ret_key = it_->key();
    return Slice(ret_key.data(), ret_key.size());
  }
private:
  base::BaseOrder::Iterator *it_;
};

class KVIndexBase : public KVIndex {
public:
  KVIndexBase (const Options& options, kvssd::KVSSD* kvd);
  ~KVIndexBase ();

  // implmentations
  bool Put(const Slice& key);
  bool Delete(const Slice& key);
  bool Write(IDXWriteBatch* updates);
  IDXIterator* NewIterator(const ReadOptions& options);
 
private:
  base::BaseOrder* base_;
  kvssd::KVSSD* kvd_;
  ComparatorBase* cmp_;
};

KVIndexBase::KVIndexBase(const Options& db_options, kvssd::KVSSD* kvd) : kvd_(kvd) {
  // apply db options
  cmp_ = new ComparatorBase(db_options.comparator);
  base_ = new base::BaseOrder(cmp_, kvd);
}

KVIndexBase::~KVIndexBase() {
  delete cmp_;
  delete base_;
}

KVIndex* NewBaseIndex(const Options& options, kvssd::KVSSD* kvd) {
  return new KVIndexBase(options, kvd);
}

bool KVIndexBase::Put(const Slice &key) {
  // do nothing
  return true;
}

bool KVIndexBase::Delete(const Slice &key) {
  // do nothing
  return true;
}

bool KVIndexBase::Write(IDXWriteBatch* updates) {
  // do nothing
  return true;
}

IDXIterator* KVIndexBase::NewIterator(const ReadOptions& options) {
  return new IDXIteratorBase(base_, options);
}

}  // namespace levelkv
