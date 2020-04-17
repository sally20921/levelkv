/******* levelkv *******/
/* kv_index.h
* 
* 
*/

#ifndef _kv_index_h_
#define _kv_index_h_

#include <stdint.h>
#include "levelkv/slice.h"
#include "kvssd/kvssd.h"
#include "levelkv/options.h"

namespace levelkv {

class IDXWriteBatch {
 public:
  IDXWriteBatch() {};
  virtual ~IDXWriteBatch() {};

  // Store the key index
  virtual void Put(const Slice& key) = 0;

  // Delete the key index
  virtual void Delete(const Slice& key) = 0;

  // Clear all updates buffered in this batch.
  virtual void Clear() = 0;

  // return the pointer for specific implementation class batch
  virtual void *InternalBatch() = 0;
};

class IDXIterator {
 public:
  IDXIterator() {};
  virtual ~IDXIterator() {};

  // An iterator is either positioned at a key/value pair, or
  // not valid.  This method returns true iff the iterator is valid.
  virtual bool Valid() const = 0;

  // Position at the first key in the source.  The iterator is Valid()
  // after this call iff the source is not empty.
  virtual void SeekToFirst() = 0;

  // Position at the last key in the source.  The iterator is
  // Valid() after this call iff the source is not empty.
  virtual void SeekToLast() = 0;

  // Position at the first key in the source that is at or past target.
  // The iterator is Valid() after this call iff the source contains
  // an entry that comes at or past target.
  virtual void Seek(const Slice& target) = 0;

  // Moves to the next entry in the source.  After this call, Valid() is
  // true iff the iterator was not positioned at the last entry in the source.
  // REQUIRES: Valid()
  virtual void Next() = 0;

  // Moves to the previous entry in the source.  After this call, Valid() is
  // true iff the iterator was not positioned at the first entry in source.
  // REQUIRES: Valid()
  virtual void Prev() = 0;

  // Return the key for the current entry.  The underlying storage for
  // the returned slice is valid only until the next modification of
  // the iterator.
  // REQUIRES: Valid()
  virtual Slice key() const = 0;

 private:
  // No copying allowed
  IDXIterator(const IDXIterator&);
  void operator=(const IDXIterator&);
};


// abstract class for order secondary index implementation
// LSM-Tree, KVB+Tree
class KVIndex {
 public:
  KVIndex() {};
  virtual ~KVIndex() {};

  virtual bool Put(const Slice& key) = 0;

  virtual bool Delete(const Slice& key) = 0;

  virtual bool Write(IDXWriteBatch* updates) = 0;

  virtual IDXIterator* NewIterator(const ReadOptions& options) = 0;
 private:
  // No copying allowed
  KVIndex(const KVIndex&);
  void operator=(const KVIndex&);
};

KVIndex* NewLSMIndex(const Options& options, kvssd::KVSSD* kvd);
KVIndex* NewBTreeIndex(const Options& options, kvssd::KVSSD* kvd);
KVIndex* NewBaseIndex(const Options& options, kvssd::KVSSD* kvd);
KVIndex* NewInMemIndex(const Options& options, kvssd::KVSSD* kvd);

}  // namespace levelkv

#endif