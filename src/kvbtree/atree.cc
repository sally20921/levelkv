/* bplustree.cpp
* 
*/
//there is no delete so just mark dirty 


//Things that have changed 
//index number is kept inside the tree as private variable
//index_num initialized to zero when frist created  
//uint32 index_num_
//Things to add
//checkpoint in the background 
//for every insert or delete, updated nodes are logged (or flushed) with index number 
#include <stack>
#include "bplustree.h"

namespace kvbtree {
/********** MemNode **********/
void MemNode::Insert(Slice *key) {
	//allocate space for key first in the heap
    char *key_buf = (char *)malloc(key->size());
    memcpy(key_buf, key->data(), key->size());
    {	//then insert in the memnode 
        std::unique_lock<std::mutex> lock(m_);
        sorted_run_.insert(Slice(key_buf, key->size()));
        size_ += key->size();
    }
}

void MemNode::BulkKeys(Slice *hk, uint32_t size, char *&buf, int &buf_entries, int &buf_size){
    // 1, determin size first
    buf_size = 0;
    auto it = sorted_run_.begin();
    buf_entries = 0;
    if (hk->data() == NULL) {
        do {
            buf_size += it->size() + sizeof(uint32_t);
            ++it;
            buf_entries++;
        } while (buf_entries < size && it != sorted_run_.end());
    }
    else {
        do {
            buf_size += it->size() + sizeof(uint32_t);
            ++it;
            buf_entries++;
        } while (it != sorted_run_.end() && buf_entries < size && it->compare(*hk) < 0);
    }
    
    //printf("bulk size %d\n", buf_entries);
    // 2, assembly the bulk keys
    buf = (char *)malloc(buf_size);
    char *p = buf;
    for (int i = 0; i < buf_entries; i++) {
        it = sorted_run_.begin();
        uint32_t size = it->size();
        *(uint32_t *)p = size;
        p += sizeof(uint32_t);
        memcpy(p, it->data(), size);
        p += size;
        free((void *)it->data());
        sorted_run_.erase(it); // iterator will advance
    }
    size_ -= buf_size - buf_entries*sizeof(uint32_t);
}

Slice MemNode::FirstKey() {
    auto it = sorted_run_.begin();
    return *it;
}

//memory 
//sorted run 
//<Slice, uint32_t, custom_cmp> map 

//storage 
//key: level[x][min_key]
//4B: number of entries 
//xB: [child_entries, key_size, key1]  
/********** InternalNode **********/
//key_array- is not used in InternalNode 

//dirty bit true 
//creation of a new empty node  
InternalNode::InternalNode(Comparator *cmp, kvssd::KVSSD *kvd, InternalNode *p, int fanout, int level) 
: cmp_(cmp), kvd_(kvd), parent_(p), level_(level), key_array_(NULL), dirty_(true), fanout_(fanout),
  sorted_run_(custom_cmp(cmp)) { }

//convert SSD internal node to memory internal node 
//dirty bit false 
//buf layout: number of entries + child_entries + keysize + key 
//buf == value of SSD internal node 
InternalNode::InternalNode(Comparator *cmp, kvssd::KVSSD *kvd, InternalNode *p, int fanout, int level, char *buf, uint32_t size)
: cmp_(cmp), kvd_(kvd), parent_(p), level_(level), key_array_(NULL), dirty_(false), fanout_(fanout),
  sorted_run_(custom_cmp(cmp)) {
   //4B: number of entries 
    int num_entries = *(uint32_t *)buf;
    buf += sizeof(uint32_t);
	
   //[child_entries, key_size, key]
    for (int i = 0; i < num_entries; i++) {
        int child_entries = *(uint32_t *)buf;
        buf += sizeof(uint32_t);
        int size = *(uint32_t *)buf;
        buf += sizeof(uint32_t);
        char *key_buf = (char *)malloc(size);
        memcpy(key_buf, buf, size);
        Slice key(size==0?NULL:key_buf, size);
        buf += size;
        sorted_run_[key] = child_entries;
    }

}

InternalNode::~InternalNode() {
    // Write back to storage
    if(dirty_) Flush();
    // clean up internal key buffers
    for (auto it = sorted_run_.begin(); it != sorted_run_.end(); ++it) {
        free((void *)(it->first.data()));
    }
}

//return min and max 
void InternalNode::SearchKey(Slice *key, Slice &min, Slice &max) {

    //returns an iterator to  (key+1) 
    auto it = sorted_run_.upper_bound(*key);

    if (it == sorted_run_.end()) {
        max = Slice(NULL, 0);
    }
    else {
        max = it->first;
    }
    assert(it != sorted_run_.begin());
    it--; 
    min = it->first;
}

//notice here that child entries is number  of child entries 
int InternalNode::GetChildEntries(Slice *key) {
    auto it = sorted_run_.find(*key);
    return it->second;
}

InternalNode* InternalNode::InsertEntry(Slice *key, int entries, int level) {
    
   dirty_ = true; //mark dirty
   
   //entries here refer to number of child  entries 

   //insert key into this internal node 
   //first allocate space for key in heap 
    char *key_buf = (char *)malloc(key->size());
    memcpy(key_buf, key->data(), key->size());

   //by inserting key in this internal node, it might have to split 
    InternalNode *split_node = NULL;
    //insert it in sorted run 
    sorted_run_[Slice(key_buf, key->size())] = entries;

    //split 
    if (sorted_run_.size() >= fanout_) {
        int split_size = sorted_run_.size()/2;
        //new internal node 
        split_node = new InternalNode(cmp_, kvd_, parent_, fanout_, level);
        for (int i = 0; i < split_size; i++) {
            auto it = sorted_run_.begin();
            split_node->InsertEntry((Slice *)&(it->first), it->second, level); // willnot overflow
            sorted_run_.erase(it);
        }
    }
    return split_node;//return NULL or newly created internal node if there was split
}

Slice InternalNode::FirstKey() {
    auto it = sorted_run_.begin();
    if (it->second == 0) { //begin could be dummy node  (NULL, 0) 
        ++it;
    }
    return it->first;
}


//in memory 
//sorted run : map <Slice, int32_t, custom_cmp>
//in storage :
//level[x][min_key]
//4B: number of entries, xB: child entries(uint32_t)+keysize+key
void InternalNode::Flush() {

    //buf variable is for inner SSD value 
    // 1, determin size first
    int buf_size = sizeof(uint32_t); // initial bytes for num of entries
    for (auto it = sorted_run_.begin(); it != sorted_run_.end(); ++it) {
        buf_size += it->first.size() + sizeof(uint32_t) * 2;
    }

    // 2, assembly the bulk keys
    char *buf = (char *)malloc(buf_size);
    char *p = buf;
    *(uint32_t *)p = sorted_run_.size();//number of entries in the node 
    p += sizeof(uint32_t);

    for (auto it = sorted_run_.begin(); it != sorted_run_.end(); ++it) {
        *(uint32_t *)p = it->second;//number of child entries
        p += sizeof(uint32_t);
        uint32_t size = it->first.size();//key_size
        *(uint32_t *)p = size;
        p += sizeof(uint32_t);
        memcpy(p, it->first.data(), size);//key 
        p += size;
    }

    // 3, write to kvssd
    std::string inner_key_str = "level"+std::to_string(level_)+FirstKey().ToString();
    kvssd::Slice inner_key(inner_key_str);
    kvssd::Slice inner_val(buf, buf_size);
    kvd_->kv_store(&inner_key, &inner_val);

    // 4, clean up
    free(buf);
}

//entries here is the number of child entries for key
void InternalNode::UpdateChildEntries(Slice *key, int entries) {
    dirty_ = true;
    sorted_run_[*key] = entries;
}

//memory 
//sorted run
//<Slice, custom_cmp> set 

//storage 
//key: leaf[min_key]
//xB: [key_size, nex_leaf_key]
//xB: [key_size, key1], [key_size, key2] 

/********** LeafNode **********/ 

//create new leaf node in memory 
LeafNode::LeafNode(Comparator *cmp, kvssd::KVSSD *kvd, InternalNode *p) 
: cmp_(cmp), kvd_(kvd), parent_(p), buf_(NULL), entries_(0), sorted_run_(custom_cmp(cmp)) {}

//buf is the same as value format in SSD  
//buf to in memory leaf node 
LeafNode::LeafNode(Comparator *cmp, kvssd::KVSSD *kvd, InternalNode *p, char *buf, uint32_t size) 
: cmp_(cmp), kvd_(kvd), parent_(p), buf_(buf), entries_(0), sorted_run_(custom_cmp(cmp)) {
    
// decode next leaf pointer
    int next_key_size = *(uint32_t *)buf;
    buf += sizeof(uint32_t);
    if (next_key_size != 0) {//there is next leaf node 
        //next_leaf_ is string 
        next_leaf_.append(buf, next_key_size);
        buf += next_key_size;
    }
    //append buf as key in sorted run (or in this leaf node)
   //buf format : key_size_1, key_1, key_size_2, key_size_2
    Append(buf, size-sizeof(uint32_t)-next_key_size);
}

//notice leaf node does not have dirty bit, so we do not flush
LeafNode::~LeafNode() {
    if (buf_!=NULL) free(buf_);
}

//add key in memory to leaf node in memory 
void LeafNode::Add(Slice *key) {
    sorted_run_.insert(*key);
}

//do not confuse this with add 
//There is append and Append 
//buf is in format of value in SSD
//appends buf in form of sorted run in this leaf node 
void LeafNode::Append(char *buf, uint32_t size) {
    while (size > 0) {
        int key_size = *(uint32_t *)buf;
        Slice key(key_size==0?NULL:(buf+sizeof(uint32_t)), key_size);
        sorted_run_.insert(key);
        buf += sizeof(uint32_t) + key_size;
        size -= sizeof(uint32_t) + key_size;
    }
}

//next_leaf_ is std::string
void LeafNode::UpdateNextLeaf(Slice *key) {
    next_leaf_ = key->ToString();
}

//split this leaf node 
LeafNode* LeafNode::Split() {

    LeafNode *split_node = new LeafNode(cmp_, kvd_, parent_);

    int size = sorted_run_.size()/2;

    //add it in split_node and erase it in sorted run 
    for (int i = 0; i < size; i++) {
        auto it = sorted_run_.begin();
        Slice key = *it;
        split_node->Add(&key);
        sorted_run_.erase(it);
    }

    // update next leaf pointer
   //update split_node next_leaf to this leaf
    auto it = sorted_run_.begin();
    Slice next_leaf(it->data(), it->size());
    split_node->UpdateNextLeaf(&next_leaf);
	
    //return the newly created node 
    return split_node;
}

Slice LeafNode::FirstKey() {
    auto it = sorted_run_.begin();
    return *it;
}


//memory: sorted_run_ std::set<Slice, custom_cmp>
//storage
//key: leaf+[min_key}
//value: key_size, next_leaf_key, key_size, key1, key_size, key2 
void LeafNode::Flush() {
    //variable buf is for making value in key-value leaf node 

    // 1, determin size first
    int buf_size = sizeof(uint32_t) + next_leaf_.size(); // initial next leaf pointer
    //key_size+key
    for (auto it = sorted_run_.begin(); it != sorted_run_.end(); ++it) {
        buf_size += it->size() + sizeof(uint32_t);
    }

    // 2, assembly the bulk keys
    char *buf = (char *)malloc(buf_size);
    char *p = buf;
    
    // next leaf pointer
    *(uint32_t *)p = next_leaf_.size();
    p += sizeof(uint32_t);
    memcpy(p, next_leaf_.data(), next_leaf_.size());
    p += next_leaf_.size();
    //keysize+key
    for (auto it = sorted_run_.begin(); it != sorted_run_.end(); ++it) {
        uint32_t size = it->size();
        *(uint32_t *)p = size;
        p += sizeof(uint32_t);
        memcpy(p, it->data(), size);
        p += size;
    }

    // 3, write to kvssd
    std::string leaf_key_str = "leaf"+FirstKey().ToString();//key     
    kvssd::Slice leaf_key(leaf_key_str);//notice here we have to store key-value tuple
    //in terms of kvssd::Slice 
    kvssd::Slice leaf_val(buf, buf_size);
    kvd_->kv_store(&leaf_key, &leaf_val);

    // 4, clean up
    free(buf);
}

std::set<Slice, custom_cmp>::iterator LeafNode::SeekToFirst() {
    return sorted_run_.begin();
}

std::set<Slice, custom_cmp>::iterator LeafNode::Seek(Slice *key) {
    auto it = sorted_run_.lower_bound(*key);
   //this returns iterator that starts from key 
    return it;
}

bool LeafNode::Valid(std::set<Slice, custom_cmp>::iterator it) {
    return it!=sorted_run_.end();//iterator it doesn not start from the end of sorted_run_
}

//checkpoint
//key: checkpoint#[index_num_]
//value: [key_size][key]
/********** KVBplusTree **********/


} // end of kvbtree namespace
