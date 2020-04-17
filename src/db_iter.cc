/******* levelkv *******/
/* db_iter.cc
* 08/06/2019
* 
*/
#include <mutex>
#include <condition_variable>
#include "levelkv/iterator.h"
#include "db_impl.h"
#include "db_iter.h"

namespace levelkv {

typedef struct {
  std::mutex *m;
  int prefetch_cnt;
  int prefetch_num;
  Monitor *mon;
} Prefetch_context;

void on_prefetch_complete(void* args) {
  Prefetch_context *prefetch_ctx = (Prefetch_context *)args;
  std::mutex *m = prefetch_ctx->m;
  {
    std::unique_lock<std::mutex> lck(*m);
    if (prefetch_ctx->prefetch_cnt++ == prefetch_ctx->prefetch_num-1)
      prefetch_ctx->mon->notify();
  }
}

class DBIterator : public Iterator {
public:
  DBIterator(DBImpl *db, const ReadOptions &options);
  ~DBIterator();

  bool Valid() const {
    return valid_;
  }
  void SeekToFirst();
  void SeekToLast() { /* NOT IMPLEMENT */ }
  void Seek(const Slice& target);
  void Next();
  void Prev() { /* NOT IMPLEMENT */ }
  Slice key() const;
  Slice value();
private:
  DBImpl *db_;
  const ReadOptions &options_;
  IDXIterator *it_;
  kvssd::KVSSD *kvd_;
  std::string value_;
  bool valid_;

  // for value prefetch
  std::string *key_queue_;
  Slice *val_queue_;
  bool *valid_queue_;
  int prefetch_depth_;
  int queue_cur_;

  void prefetch_value(std::vector<Slice>& key_list, std::vector<Slice>& val_list);
};

DBIterator::DBIterator(DBImpl *db, const ReadOptions &options) 
: db_(db), options_(options), kvd_(db->GetKVSSD()), valid_(false),
  prefetch_depth_(1), queue_cur_(0) {
  if (db_->options_.prefetchEnabled) {
    int prefetch_depth = db_->options_.prefetchDepth;
    key_queue_ = new std::string[prefetch_depth];
    val_queue_ = new Slice[prefetch_depth];
    for (int i = 0 ; i < prefetch_depth; i++) val_queue_[i].clear();
    valid_queue_ = new bool[prefetch_depth];
  }
  it_ = db->GetKVIndex()->NewIterator(options);
}

DBIterator::~DBIterator() { 
  delete it_; 

  if (db_->options_.prefetchEnabled) {
    delete [] key_queue_;
    for (int i = 0 ; i < db_->options_.prefetchDepth; i++) {
      if (val_queue_[i].size() != 0) free((void *)val_queue_[i].data());
    }
    delete [] val_queue_;
    delete [] valid_queue_;
  }
}

void DBIterator::prefetch_value(std::vector<Slice>& key_list, std::vector<Slice>& val_list) {
  int prefetch_num = key_list.size();
  char **vbuf_list = new char*[prefetch_num];
  uint32_t *actual_vlen_list = new uint32_t[prefetch_num];
  Monitor mon;
  std::mutex m;
  Prefetch_context *ctx = new Prefetch_context {&m, 0, prefetch_num, &mon};

  std::vector<kvssd::Slice> prefetch_key_list;
  for (int i = 0 ; i < prefetch_num; i++) {
    prefetch_key_list.push_back(kvssd::Slice(key_list[i].data(), key_list[i].size()));
    //kvssd::Async_get_context *io_ctx = new kvssd::Async_get_context {vbuf_list[i], actual_vlen_list[i], (void *)ctx};
    kvssd::Async_get_context *io_ctx = new kvssd::Async_get_context (vbuf_list[i], actual_vlen_list[i], (void *)ctx);
    kvd_->kv_get_async(&prefetch_key_list[i], on_prefetch_complete, (void*) io_ctx);
  }

  mon.wait();
  // save the vbuf
  for (int i = 0; i < prefetch_num; i++)
    val_list.push_back(Slice(vbuf_list[i], actual_vlen_list[i]));
  
  // de-allocate resources
  delete [] vbuf_list;
  delete [] actual_vlen_list;
  delete ctx;
}

void DBIterator::SeekToFirst() { 
  it_->SeekToFirst();
  if (db_->options_.prefetchEnabled) {
    valid_ = valid_queue_[0] = it_->Valid();
    if (it_->Valid())
      key_queue_[0] = it_->key().ToString();
  }
  else {
    valid_ = it_->Valid();
  }
}

void DBIterator::Seek(const Slice& target) { 
  it_->Seek(target); 
  if (db_->options_.prefetchEnabled) {
    valid_ = valid_queue_[0] = it_->Valid();
    if (it_->Valid())
      key_queue_[0] = it_->key().ToString();
  }
  else {
    valid_ = it_->Valid();
  }
}

void DBIterator::Next() {
  assert(valid_);
  if (db_->options_.prefetchEnabled) {
    if (queue_cur_ == prefetch_depth_-1) {
      queue_cur_ = 0; //reset cursor
      // release allocated memory vbuf
      for (int i = 0; i < prefetch_depth_; i++) {
        free ((void *)val_queue_[i].data());
        val_queue_[i].clear();
      }
      // calculate prefetch depth 
      if (prefetch_depth_ < db_->options_.prefetchDepth) {
        prefetch_depth_ = prefetch_depth_ == 0 ? 1 : prefetch_depth_ << 1;
      }

      for (int i = 0; i < prefetch_depth_; i++) {
        it_->Next();
        valid_ = it_->Valid();
        if(valid_) {
          key_queue_[i] = (it_->key()).ToString();
          valid_queue_[i] = true;
        }
        else {
          valid_queue_[i] = false;
          break;
        }
      }
    }
    else
      queue_cur_++;
    
    valid_ = valid_queue_[queue_cur_];
  }
  else {
    it_->Next();
    valid_ = it_->Valid();
  }
}

Slice DBIterator::key() const {
  assert(valid_);
  if (db_->options_.prefetchEnabled)
    return Slice(key_queue_[queue_cur_]);
  else
    return it_->key();
}

Slice DBIterator::value() {
  assert(valid_);
  if (db_->options_.prefetchEnabled) {
    if (queue_cur_ == 0) {// do prefetch_value
      std::vector<Slice> key_list;
      std::vector<Slice> val_list;

      Slice upper_key;
      if (options_.upper_key != NULL) {
        upper_key = *(options_.upper_key);
      }
      for (int i = 0; i < prefetch_depth_; i++) {
        if(valid_queue_[i]) {
          if (upper_key.size() > 0 && db_->options_.comparator->Compare(key_queue_[i], upper_key) < 0) {
            key_list.push_back(Slice(key_queue_[i]));
          }
          else if (upper_key.size() == 0)
            key_list.push_back(Slice(key_queue_[i]));
          else {} // do nothing
        }
        else break;
      }
      prefetch_value(key_list, val_list);
      for (int i = 0; i < val_list.size(); i++) {
        val_queue_[i] = val_list[i];
      }

    }
    return val_queue_[queue_cur_];
  }
  else {
    Slice curr_key = key();
    kvssd::Slice get_key(curr_key.data(), curr_key.size());
    char *vbuf;
    int vlen;
    kvd_->kv_get(&get_key, vbuf, vlen);
    value_.clear();
    value_.append(vbuf, vlen);
    free(vbuf);
    return Slice(value_);
  }

}

Iterator* NewDBIterator(DBImpl *db, const ReadOptions &options) {
  return new DBIterator(db, options);
}

} // end namespace levelkv