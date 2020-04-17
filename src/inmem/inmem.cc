/* inmem.cc
* 08/21/2019
* 
*/

#include "inmem.h"
#include <fstream>

namespace inmem {

uint64_t InMem::serializedSize() {
    uint64_t size = 0;
    for (auto it = ordered_keys_.begin(); it != ordered_keys_.end(); ++it) {
        // log_key str, len(u8), phy_key(u64)
        size += it->size() + sizeof(uint8_t) ;
    }
    return size + sizeof(uint64_t); // first u64, blob size;
}

void InMem::serialize(char *filename) {
    // save data to archive
    uint64_t size = serializedSize();
    char *buf = (char *)malloc(size);
    char *data = buf;
    *(uint64_t *)data = size - sizeof(uint64_t);
    data += sizeof(uint64_t);
    for (auto it = ordered_keys_.begin(); it != ordered_keys_.end(); ++it) {
        uint8_t key_size = (uint8_t)it->size();
        // key len (u8)
        *(uint8_t *)data = key_size;
        data += sizeof(uint8_t);
        // key str 
        memcpy(data, it->data(), key_size);
        data += key_size;
    }
    // write to file
    std::ofstream ofs(filename, std::ofstream::out|std::ios::binary);
    ofs.write(buf, size);

    // clean up
    free(buf);
}

void InMem::deserialize(char *filename) {
    std::ifstream ifs(filename, std::ifstream::in|std::ios::binary);
    // create and open an archive for input
    uint64_t blob_size;
    ifs.read((char*)&blob_size, sizeof(uint64_t));
    char *data = (char *)malloc(blob_size);
    ifs.read(data, blob_size);
    // read from archive to data structure
    char *p = data;
    while (blob_size > 0) {
        // key len (u8)
        uint8_t key_size = *(uint8_t *)p;
        p += sizeof(uint8_t);
        blob_size -= sizeof(uint8_t);
        // key str
        Slice key(p, key_size);
        p += key_size;
        blob_size -= key_size;
      
        ordered_keys_.insert(key);
    }
}

bool InMem::Insert (Slice *key) {
  char *buf = (char *)malloc(key->size());
  memcpy(buf, key->data(), key->size());
  Slice new_key(buf, key->size());
  {
    std::unique_lock<std::mutex> lck(m_);
    ordered_keys_.insert(new_key);
  }
  
}

bool InMem::Delete (Slice *key) {
  std::unique_lock<std::mutex> lck(m_);
  auto it = ordered_keys_.find(*key);
  assert(it != ordered_keys_.end());
  ordered_keys_.erase(it);
}

// read-only iterator (thread safe guaranteed)
InMem::Iterator::Iterator (InMem *mem) : inmem_(mem) {}

void InMem::Iterator::SeekToFirst() {
  Slice key_target(NULL, 0);
  Seek(&key_target);
}

void InMem::Iterator::Seek(Slice *key) {
  it_ = inmem_->ordered_keys_.lower_bound(*key);
}

void InMem::Iterator::Next() {
  ++it_;
}

bool InMem::Iterator::Valid() {
  return it_ != inmem_->ordered_keys_.end();
}

Slice InMem::Iterator::key() {
    return *it_;
}

InMem::Iterator* InMem::NewIterator() {
    return new Iterator(this);
}

} // end namespace base