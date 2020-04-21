/* write_batch.h*/

#include <vector>
#include "slice.h"
namespace kvatree{

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
