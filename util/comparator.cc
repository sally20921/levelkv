/******* levelkv *******/
/* comparator.cc
* 
* 
*/

#include <algorithm>
#include <stdint.h>
#include <pthread.h>
#include "levelkv/comparator.h"
#include "levelkv/slice.h"

namespace levelkv {

Comparator::~Comparator() { }

class BytewiseComparatorImpl : public Comparator {
 public:
  BytewiseComparatorImpl() { }

  virtual int Compare(const Slice& a, const Slice& b) const {
    return a.compare(b);
  }
};

static pthread_once_t once = PTHREAD_ONCE_INIT;
static const Comparator* bytewise;

static void InitModule() {
  bytewise = new BytewiseComparatorImpl;
}

const Comparator* BytewiseComparator() {
  pthread_once(&once, InitModule);
  return bytewise;
}

}  // namespace levelkv