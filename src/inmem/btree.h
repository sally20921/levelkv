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
#include <string>
#include <vector>
#include <stack>
#include <algorithm>

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

namespace inmem {

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

//generic Node 
class Node {
	private:
		bool isLeaf_;
    		bool dirty_;
	
		Comparator *cmp_;
		kvssd::KVSSD *kvd_;
		std::vector<std::string> keys_;
}
// In memory format
/*  For write: sorted structure sorted_run_ (skiplist, rb tree)
    For read: Slice array (sorted)
 */
// In storage format
/*  key: level[x][min_key]
    4B: number of entries
    xB: [child_entries, key_size, key1], [child_entries, key_size, key2] ...
 */
class InternalNode : public Node {
private:
    
    int level_;
     std::vector<Node*> children_;

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
class LeafNode : public Node {
private:
   
    char *buf_;
    uint32_t entries_;
   // std::set<Slice, custom_cmp> sorted_run_;
public:
    // node pointer
    LeafNode* prev;
    LeafNode* next;
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
    Node *root_;
    //Cache *innode_cache_;
    //std::unique_ptr<AtomicCache> concurr_cache_; // concurrent cache (for iterator only)
    int cache_size_;
    uint32_t level_; // Non-leaf levels
    int fanout_;

    uint32_t index_num_;

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
