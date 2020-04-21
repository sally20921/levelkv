/* write_batch.cc
* 07/31/2019
* 
*/

#include "write_batch.h"

namespace kvatree{

WriteBatch::WriteBatch() {}
WriteBatch::~WriteBatch() {}

void WriteBatch::Put(Slice &key) {
  char *key_buf = (char *)malloc(key.size());
  memcpy(key_buf, key.data(), key.size());
  Slice key_in_batch(key_buf, key.size());
  list_.push_back(key_in_batch);
}

void WriteBatch::Delete(Slice &key) {
  // NOT IMPLEMENT
}

void WriteBatch::Clear() {
  for (auto it = list_.begin(); it != list_.end(); ++it) {
    free((void *)it->data());
  }
  list_.clear();
}

std::vector<Slice>::iterator WriteBatch::Iterator() {
  return list_.begin();
}

bool WriteBatch::End(std::vector<Slice>::iterator &it) {
  return it == list_.end();
}

}  // namespace kvbtree
