/******* levelkv *******/
/* comparator.h
* 
* 
*/

#ifndef _comparator_h_
#define _comparator_h_

#include <string>

namespace levelkv {

class Slice;

// A Comparator object provides a total order across slices that are
// used as keys in an sstable or a database.  A Comparator implementation
// must be thread-safe since levelkv may invoke its methods concurrently
// from multiple threads.
class Comparator {
 public:
  virtual ~Comparator();

  // Three-way comparison.  Returns value:
  //   < 0 iff "a" < "b",
  //   == 0 iff "a" == "b",
  //   > 0 iff "a" > "b"
  virtual int Compare(const Slice& a, const Slice& b) const = 0;

};

// Return a builtin comparator that uses lexicographic byte-wise
// ordering.  The result remains the property of this module and
// must not be deleted.
extern const Comparator* BytewiseComparator();

}  // namespace levelkv


#endif