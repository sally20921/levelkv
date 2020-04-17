/******* levelkv *******/
/* kv_index_btree.cc
* 07/29/2019
* 
*/

#include "kv_index.h"
#include "levelkv/comparator.h"
#include "kvbtree/bplustree.h"
#include "kvbtree/write_batch.h"

namespace levelkv {

class ComparatorBTree : public kvbtree::Comparator {
public:
  ComparatorBTree(const levelkv::Comparator* cmp) : cmp_(cmp) {};
  ~ComparatorBTree() {};
  int Compare(const kvbtree::Slice& a, const kvbtree::Slice& b) const {
    Slice aa(a.data(), a.size());
    Slice bb(b.data(), b.size());
    return cmp_->Compare(aa, bb);
  }
  
private:
  const levelkv::Comparator* cmp_;
};

class IDXWriteBatchBTree : public IDXWriteBatch {
public: 
  IDXWriteBatchBTree () {};
  ~IDXWriteBatchBTree () {};
  void Put(const Slice& key) {
    kvbtree::Slice put_key(key.data(), key.size());
    batch_.Put(put_key);
  }
  void Delete(const Slice& key) {
    kvbtree::Slice del_key(key.data(), key.size());
    batch_.Delete(del_key);
  }
  void Clear() {batch_.Clear();}
  void *InternalBatch() {return &batch_;}

public:
  kvbtree::WriteBatch batch_;
};

class IDXIteratorBTree : public IDXIterator {
public:
  IDXIteratorBTree(kvbtree::KVBplusTree* db, const ReadOptions& options) {
    // TODO apply read options to kvbtree iterator
    it_ = db->NewIterator();
  }
  ~IDXIteratorBTree() { delete it_; }

  bool Valid() const {it_->Valid();}
  void SeekToFirst() {it_->SeekToFirst();}
  void SeekToLast() { }
  void Seek(const Slice& target) {
    kvbtree::Slice seek_key(target.data(), target.size());
    it_->Seek(&seek_key);
  }
  void Next() {it_->Next();}
  void Prev() { }
  Slice key() const {
    kvbtree::Slice ret_key = it_->key();
    return Slice(ret_key.data(), ret_key.size());
  }
private:
  kvbtree::KVBplusTree::Iterator *it_;
};

class KVIndexBTree : public KVIndex {
public:
  KVIndexBTree (const Options& options, kvssd::KVSSD* kvd);
  ~KVIndexBTree ();

  // implmentations
  bool Put(const Slice& key);
  bool Delete(const Slice& key);
  bool Write(IDXWriteBatch* updates);
  IDXIterator* NewIterator(const ReadOptions& options);
 
private:
  kvbtree::KVBplusTree *db_;
  kvssd::KVSSD *kvd_;
  kvbtree::Comparator *cmp_;
};

KVIndexBTree::KVIndexBTree(const Options& db_options, kvssd::KVSSD* kvd) : kvd_(kvd) {
  cmp_ = new ComparatorBTree(db_options.comparator);
  db_ = new kvbtree::KVBplusTree(cmp_, kvd, 1024, 4096);
}

KVIndexBTree::~KVIndexBTree() {
  delete db_;
  delete cmp_;
}

KVIndex* NewBTreeIndex(const Options& options, kvssd::KVSSD* kvd) {
  return new KVIndexBTree(options, kvd);
}

bool KVIndexBTree::Put(const Slice &key) {
  kvbtree::Slice put_key(key.data(), key.size());
  return db_->Insert(&put_key);
}

bool KVIndexBTree::Delete(const Slice &key) {
  // NOT IMPLEMENT
}

bool KVIndexBTree::Write(IDXWriteBatch* updates) {
  db_->Write((kvbtree::WriteBatch*)updates->InternalBatch());
}

IDXIterator* KVIndexBTree::NewIterator(const ReadOptions& options) {
  return new IDXIteratorBTree(db_, options);
}

}  // namespace levelkv
