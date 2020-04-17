/* base.h
* 08/05/2019
* 
*/

#ifndef _inmem_h
#define _inmem_h

#include <stdlib.h>
#include <set>
#include <mutex>
#include <fstream>

#include "slice.h"
#include "comparator.h"
#include "kvssd/kvssd.h"

namespace inmem {

struct custom_cmp {
  explicit custom_cmp(Comparator *cmp) : cmp_(cmp) {}
  bool operator() (Slice l, Slice r) const {
    return (cmp_->Compare(l, r) < 0) ;
  }
  Comparator *cmp_;
};

class InMem {
public:
  InMem(Comparator *cmp, kvssd::KVSSD *kvd) : cmp_(cmp), kvd_(kvd), ordered_keys_(custom_cmp(cmp_)) {
    std::ifstream f("mapping_table.log",std::ifstream::in|std::ios::binary);
      if (f) {
        deserialize("mapping_table.log");
      }
  };
  ~InMem() {
    serialize("mapping_table.log");
  }
  kvssd::KVSSD* GetDev() {return kvd_;}

  bool Insert(Slice *key);
  bool Delete(Slice *key);

  class Iterator {
  private: 
    InMem *inmem_;
    std::vector<std::string> keys_;
    std::set<Slice, custom_cmp>::iterator it_;
  public:
    Iterator (InMem *inmem);
    ~Iterator () {}
    void Seek(Slice *key);
    void SeekToFirst();
    void Next();
    bool Valid();
    Slice key();
  };
  Iterator* NewIterator();

private:
    Comparator *cmp_;
    kvssd::KVSSD* kvd_;
    std::set<Slice, custom_cmp> ordered_keys_;
    std::mutex m_;

    uint64_t serializedSize();
    void serialize(char *filename);
    void deserialize(char *filename);
};

} // end namespace base


#endif