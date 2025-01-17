/* bplustree.h
* 07/12/2019
* 
*/
//Things that have changed
//index number is kept inside the tree as private variable
#ifndef _bplustree_h
#define _bplustree_h

#include <stdlib.h>
#include <set>
#include <map>
#include <mutex>
#include <condition_variable>
#include <thread>

#include "slice.h"
#include "cache.h"
#include "comparator.h"
#include "write_batch.h"
#include "kvssd/kvssd.h"
#include "thread-safe-lru/string-key.h"
#include "thread-safe-lru/lru-cache.h"

#define IDEAL_KV_SIZE 32768
#define MAX_MEM_SIZE 65536
#define DEF_FANOUT 1024
#define DEF_CACHE_ITEMS 4096

namespace kvbtree {

struct custom_cmp {
  explicit custom_cmp(Comparator *cmp) : cmp_(cmp) {}
  bool operator() (Slice l, Slice r) const {
    return (cmp_->Compare(l, r) < 0) ;
  }
  Comparator *cmp_;
};

// condition variable
class CondVar {
public:
  std::mutex mtx_;
  std::condition_variable cv_;
  bool ready_ ;
  CondVar() : ready_(false) {}
  ~CondVar(){}
  void reset() {ready_ = false;};
  void notify_one() {
    std::unique_lock<std::mutex> lck(mtx_);
    ready_ = true;
    cv_.notify_one();
  }
  void notify_all() {
    std::unique_lock<std::mutex> lck(mtx_);
    ready_ = true;
    cv_.notify_all();
  }
  void wait() {
    std::unique_lock<std::mutex> lck(mtx_);
    while (!ready_) cv_.wait(lck);
  }
};

class Inmem {
        public:
        //memory to keep nodes
        std::map<Slice, Slice, custom_cmp> internal_run_;
        std::map<Slice, Slice, cutsom_cmp> leaf_run_;
	Inmem(Comparator *cmp) : size_(0), internal_run_(custom_cmp(cmp), leaf_run_(custom_cmp(cmp)) {
        internal_run_.insert(Slice("leaf", 0), Slice(NULL, 0)); // add a dummy key (minimum)
        //leaf_run_.insert(Slice(NULL,0), Slice(NULL, 0));
    }
    ~Inmem() {}
};

class MemNode {
private:
    std::mutex m_;
    std::set<Slice, custom_cmp> sorted_run_; // keep internal node sorted for fast search
    uint32_t size_; // not accurate
public:
    MemNode(Comparator *cmp) : size_(0), sorted_run_(custom_cmp(cmp)) {
        sorted_run_.insert(Slice(NULL, 0)); // add a dummy key (minimum)
    }
    ~MemNode() {}

    class Iterator {
    private: 
        MemNode *node_;
        std::set<Slice, custom_cmp>::iterator it_;
    public:
        Iterator (MemNode *node) : node_(node) {}
        ~Iterator ();
        void Seek(Slice *key);
        void SeekToFirst();
        void SeekToLast();
        void Next();
        void Prev();
        bool Valid();
        Slice Key();
    };
    uint32_t NumEntries() {return sorted_run_.size();}
    uint32_t Size() {return size_;}

    void Insert(Slice *key);
    Slice FirstKey();
    void BulkKeys(Slice *hk, uint32_t size, char *&buf, int &buf_entries, int &buf_size);
    

};

// In memory format
/*  For write: sorted structure sorted_run_ (skiplist, rb tree)
    For read: Slice array (sorted)
 */
// In storage format
/*  key: level[x][min_key]
    4B: number of entries
    xB: [child_entries, key_size, key1], [child_entries, key_size, key2] ...
 */
class InternalNode {
private:
    Comparator *cmp_;
    kvssd::KVSSD *kvd_;
    InternalNode *parent_;
    int level_;
    Slice* key_array_;
    bool dirty_;
    std::map<Slice, uint32_t, custom_cmp> sorted_run_; // keep internal node sorted for fast search
    
    Inmem* inmem_;
    int fanout_;
public:
    InternalNode(Comparator *cmp, kvssd::KVSSD *kvd, InternalNode *p, int fanout, int level);
    InternalNode(Comparator *cmp, kvssd::KVSSD *kvd, InternalNode *p, int fanout, int level, char *buf, uint32_t size);
    ~InternalNode();
    InternalNode* Parent() {return parent_;}
    void UpdateParent(InternalNode *p) {parent_ = p;}
    int GetEntries() {return sorted_run_.size();}

    void Insert(Slice *key);
    void SearchKey(Slice *key, Slice &min, Slice &max);
    int GetChildEntries(Slice *key);
    InternalNode* InsertEntry(Slice *key, int entries, int level);
    Slice FirstKey();
    void Flush();
    void UpdateChildEntries(Slice *key, int entries);
};

// In memory format
/*  Sorted structure sorted_run_ (skiplist, rb tree)
 */
// In storage format
/*  xB: [key_size, next_leaf_key]
    xB: [key_size, key1], [key_size, key2] ...
 */
class LeafNode {
private:
    Comparator *cmp_;
    kvssd::KVSSD *kvd_;
    InternalNode *parent_;
    char *buf_;
    uint32_t entries_;
    std::set<Slice, custom_cmp> sorted_run_;
    Inmem *inmem_;
public:
    // node pointer
    std::string next_leaf_;
    LeafNode(Comparator *cmp, kvssd::KVSSD *kvd, InternalNode *p);
    LeafNode(Comparator *cmp, kvssd::KVSSD *kvd, InternalNode *p, char *buf, uint32_t size);
    ~LeafNode();
    int GetEntries() {return sorted_run_.size();}

    void Add(Slice *key);
    void Append(char *buf, uint32_t size);
    void UpdateNextLeaf(Slice *key);
    LeafNode* Split();
    Slice FirstKey();
    void Flush();
    std::set<Slice, custom_cmp>::iterator SeekToFirst();
    std::set<Slice, custom_cmp>::iterator Seek(Slice *key);
    bool Valid(std::set<Slice, custom_cmp>::iterator it);

};

typedef tstarling::ThreadSafeStringKey String;
typedef String::HashCompare HashCompare;
typedef tstarling::ThreadSafeLRUCache<String, InternalNode*, HashCompare> AtomicCache;
class KVBplusTree {
private:
    Comparator *cmp_;
    kvssd::KVSSD* kvd_;
    std::mutex mutex_;  

    InternalNode *root_;
    Cache *innode_cache_;
    std::unique_ptr<AtomicCache> concurr_cache_; // concurrent cache (for iterator only)
    int cache_size_;
    uint32_t level_; // Non-leaf levels
    int fanout_;

    uint32_t index_num_;
    Inmem* inmem_;

    // BG thread
    CondVar bg_start;
    CondVar bg_end;
    pthread_t t_BG;
    void bg_worker(KVBplusTree *tree);

public:
    KVBplusTree(Comparator *cmp, kvssd::KVSSD *kvd, int fanout = DEF_FANOUT, int cache_size = DEF_CACHE_ITEMS);
    ~KVBplusTree();
    int GetLevel() {return level_;}
    InternalNode* GetRoot() {return root_;}
    kvssd::KVSSD* GetDev() {return kvd_;}

    void BulkAppend(MemNode *mem, int MemEntriesWaterMark);
    bool Insert(Slice *key);
    bool Write(WriteBatch *batch);

    class Iterator {
    private: 
        KVBplusTree *tree_;
        LeafNode *leaf_;
        std::set<Slice, custom_cmp>::iterator it_;
    public:
        Iterator (KVBplusTree *tree) : tree_(tree) {
        }
        ~Iterator () {
            delete leaf_;
        }
        void Seek(Slice *key);
        void SeekToFirst();
        void Next();
        bool Valid();
        Slice key();
    };
    Iterator* NewIterator();

};

}

#endif
