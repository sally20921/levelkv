/* write_batch.h
* 07/31/2019
* 
*/

#ifndef _kvbtre_write_batch_h_
#define _kvbtre_write_batch_h_

#include <vector>
#include "slice.h"
namespace kvbtree{

class WriteBatch {
public:
  WriteBatch();
  ~WriteBatch();

  void Put(Slice &key);
  void Delete(Slice &key);
  void Clear();

  std::vector<Slice>::iterator Iterator();
  bool End(std::vector<Slice>::iterator &it);
private:
  std::vector<Slice> list_;
};



}  // namespace kvbtree


#endif