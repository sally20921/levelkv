/******* levelkv *******/
/* write_batch.h
* 
* 
*/

#ifndef _write_batch_h_
#define _write_batch_h_


#include <string>
#include "levelkv/status.h"

namespace levelkv {

class Slice;

class WriteBatch {
 public:
  WriteBatch();
  ~WriteBatch();

  // Store the mapping "key->value" in the database.
  void Put(const Slice& key, const Slice& value);

  // If the database contains a mapping for "key", erase it.  Else do nothing.
  void Delete(const Slice& key);

  // Clear all updates buffered in this batch.
  void Clear();

};

}  // namespace levelkv

#endif