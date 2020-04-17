/* base.cc
* 08/05/2019
* 
*/

#include "base.h"

namespace base {

BaseOrder::Iterator::Iterator (BaseOrder *base) : base_(base), ordered_keys_(custom_cmp(base->cmp_)) {
  // read all keys from device
  kvssd::KVSSD *kvd = base->kvd_;
  kvd->kv_scan_keys(keys_);

  // sort keys based on customized comparator
  for (auto it = keys_.begin(); it != keys_.end(); ++it) {
    Slice new_key(it->data(), it->size());
    ordered_keys_.insert(new_key);
  }
}

void BaseOrder::Iterator::SeekToFirst() {
  Slice key_target(NULL, 0);
  Seek(&key_target);
}

void BaseOrder::Iterator::Seek(Slice *key) {
  it_ = ordered_keys_.lower_bound(*key);
}

void BaseOrder::Iterator::Next() {
  ++it_;
}

bool BaseOrder::Iterator::Valid() {
  return it_ != ordered_keys_.end();
}

Slice BaseOrder::Iterator::key() {
    return *it_;
}

BaseOrder::Iterator* BaseOrder::NewIterator() {
    return new Iterator(this);
}

} // end namespace base