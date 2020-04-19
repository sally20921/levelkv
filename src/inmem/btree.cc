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
#include "btree.h"

namespace inmem {
	bool Node::Get_IsLeaf() { return isLeaf_;}
	vector<std::string> Node::Get_keys() { return keys_;}

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
InternalNode::InternalNode(Comparator *cmp, kvssd::KVSSD *kvd) 
: cmp_(cmp), kvd_(kvd), parent_(p), dirty_(false), isLeaf(false) ,
  sorted_run_() { }

//notice here that child entries is number  of child entries 
vector<Node*> InternalNode::GetChildren() {
    return children_;
}

InternalNode* InternalNode::InsertEntry(std::string key, Node* rightChild) {
    
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
static void DeleteCacheItem (const Slice& key, void* value) {
    delete (InternalNode *)value;
}

void KVBplusTree::WriteMetaKV() {
    kvssd::Slice metaKV_key("KVBTREE_CURRENT");
    kvssd::Slice metaKV_val((char *)&level_, sizeof(level_));
    // write level_ to metaKV
    kvd_->kv_store(&metaKV_key, &metaKV_val);
}

//const char* d  or const std::string& s
 void KVBplusTree::WriteCheckpoint() {
	
	
	//organize key
	//std::string prefix = "checkpoint#"
	//uint32_t size = prefix.size()+sizeof(uint32_t);
	//char *key_buf = (char *)malloc(size);
	//char *p = key_buf;
	//memcpy(p, prefix, prefix.size());
	//p+=prefix.size();
	//*(uint32_t *)p = index_num_;
	std::string key_buf = "checkpoint#"+(char *)index_num_;
	kvssd::Slice check_key(key_buf);
	
	//organize value 
 	//bulk append MemNode
	BulkAppend(mem_, 0);
       //use iterator and get the first leaf node 
       //find leaf node 
	//InternalNode *node = root_;
	auto it = new Iterator(this);
	it->SeekToFirst();
       kvssd::Slice check_val(val_buf);

	//checkpoint tree to kvsssd 
	kvd_->kv_store(&check_key, &check_val);       
 }

bool KVBplusTree::ReadMetaKV() {
    bool newDB;
    kvssd::Slice metaKV_key("KVBTREE_CURRENT");
    char *vbuf; int vlen;
    kvs_result ret = kvd_->kv_get(&metaKV_key, vbuf, vlen);
    if (ret == KVS_ERR_KEY_NOT_EXIST) {
        newDB = true;
    }
    else if (ret == KVS_SUCCESS) {
        newDB = false;
        // parse metaKV
        level_ = *((uint32_t *)vbuf);
        free (vbuf);
    }
    return newDB;
}

bool KVBplusTree::ReadCheckpoint() {
	bool newDB =  true;

	uint32_t index = 0;
	std::string prefix = "checkpoint#"+(char *)index;
	kvssd::Slice check_key(prefix);
	char *vbuf; int vlen;
	kvs_result ret = kvd_->kv_get(&check_key, vbuf, vlen);
	while (ret == KVS_SUCCESS){
		index+=10000;
		prefix = "checkpoint#"+(char *)index;
		ret = kvd_->kv_get(&Slice(prefix), vbuf, vlen);
		newDB = false;
	}
	
	//parse vbuf and rebuild tree 
	//just put it insert it in MemNode 


	return newDB;
	
}

KVBplusTree::KVBplusTree (Comparator *cmp, kvssd::KVSSD *kvd, int fanout, int cache_size) 
: cmp_(cmp), kvd_(kvd), cache_size_(cache_size), level_(1), fanout_(fanout) {
    // New index or not ? Read KVBTREE_CURRENT Record
    bool newDB = ReadMetaKV();

    Slice dummy(NULL, 0);
    mem_ = new MemNode(cmp_);
    imm_ = NULL;
    
    index_num_ = 0;

    innode_cache_ = NewLRUCache(cache_size);
    concurr_cache_ = std::unique_ptr<AtomicCache>(new AtomicCache(cache_size));
    if (newDB) {
        root_ = new InternalNode(cmp_, kvd_, NULL, fanout_, 0);
        root_->InsertEntry(&dummy, 0, 0); // add dummy head

        // write a dummy leaf node
        kvssd::Slice dummy_leaf("leaf");
        char zero_buf[sizeof(uint32_t)] = {0};
        kvssd::Slice dummy_val(zero_buf, sizeof(uint32_t));
        kvd_->kv_store(&dummy_leaf, &dummy_val);
    }
    else {
        std::string root_key = "level"+std::to_string(level_-1);
        kvssd::Slice root_key_(root_key);
        char *vbuf; int vlen;
        kvd_->kv_get(&root_key_, vbuf, vlen);
        root_ = new InternalNode(cmp_, kvd_, NULL, fanout_, level_-1, vbuf, vlen);
    }

    // background thread
    std::thread thrd = std::thread(&KVBplusTree::bg_worker, this, this);
    t_BG = thrd.native_handle();
    thrd.detach();
}

KVBplusTree::~KVBplusTree () {
    if (imm_ != NULL) bg_end.wait();
    BulkAppend(mem_, 0);
    delete mem_;
    delete root_;
    delete innode_cache_;
    WriteMetaKV(); // write metaKV
}

void KVBplusTree::bg_worker(KVBplusTree *tree) {
    CondVar *cv_start = &(tree->bg_start);
    CondVar *cv_end = &(tree->bg_end);
    while (true) {
        cv_start->wait(); // wait for bg job trigger
        tree->BulkAppend(imm_, 0); // bulk append imm_

	tree->CheckpointTree();

        delete imm_;
        imm_ = NULL;
        cv_end->notify_all();
        cv_start->reset();
    }
}

void KVBplusTree::CheckpointTree() {
	if (index %  10000 == 0 ){
		WriteCheckpoint();
	}
}

void KVBplusTree::BulkAppend(MemNode *mem, int MemEntriesWaterMark) {
    while (mem->NumEntries() > MemEntriesWaterMark || mem->Size() > MAX_MEM_SIZE) {
        // 1, get smallest key in MemNode
	//key_target key to insert 
        Slice key_target = mem->FirstKey();
        std::string key_target_str = key_target.ToString();

        // 2, find leaf node
        InternalNode *node = root_;
	//I don't think we use lower_key here. only use upper_key.
        Slice lower_key, upper_key;
        std::stack<Slice> upper_key_stack; // use to determine key upper bound
        
	//from root get to the leaf node 
        for (int i = 0; i < level_; i++) {
            node->SearchKey(&key_target, lower_key, upper_key);
            if (i == level_ - 1) {
                while (upper_key.size() == 0 && !upper_key_stack.empty()) {
                    upper_key = upper_key_stack.top();
                    upper_key_stack.pop();
                }
                break;
            }
            else {
                upper_key_stack.push(upper_key);
            }
            
            // get next internal node
            std::string next_node_key = "level"+std::to_string(level_-i-2)+lower_key.ToString();
            Slice next_key(next_node_key);
            Cache::Handle *cache_handle = innode_cache_->Lookup(next_key);
            if (cache_handle == NULL) {
                char *v_buf; int v_size;
                kvssd::Slice io_key(next_node_key);
                kvd_->kv_get(&io_key, v_buf, v_size);
                node = new InternalNode(cmp_, kvd_, node, fanout_, level_-i-2, v_buf, v_size);
                free(v_buf);
                cache_handle = innode_cache_->Insert(next_key, node, 1, DeleteCacheItem);
            }
            else {
                InternalNode *parent = node;
                node = (InternalNode *)innode_cache_->Value(cache_handle);
                node->UpdateParent(parent);
            }
            innode_cache_->Release(cache_handle);
        }

        // 3, update leaf node
        bool leaf_split = false;
        Slice leaf_right;
        int leaf_right_entries;
        char *mem_buf; int mem_buf_entries; int mem_buf_size;
        mem->BulkKeys(&upper_key, fanout_/2, mem_buf, mem_buf_entries, mem_buf_size);

        int leaf_entries = node->GetChildEntries(&lower_key) + mem_buf_entries;
        LeafNode *leaf_node = NULL;
        if (leaf_entries > fanout_) {
            leaf_split = true;
            std::string leaf_key_str = "leaf"+std::string(lower_key.data(), lower_key.size());
            kvssd::Slice leaf_key(leaf_key_str);
            char *v_buf; int v_size;
            kvd_->kv_get(&leaf_key, v_buf, v_size);
            leaf_node = new LeafNode(cmp_, kvd_, node, v_buf, v_size);
            leaf_node->Append(mem_buf, mem_buf_size);
            LeafNode *leaf_node_left = leaf_node->Split();
            leaf_right = leaf_node->FirstKey();
            leaf_right_entries = leaf_node->GetEntries();
            leaf_node->Flush();
            leaf_entries = leaf_node_left->GetEntries();
            leaf_node_left->Flush();
            delete leaf_node_left;
        }
        else {
            std::string leaf_key_str = "leaf"+std::string(lower_key.data(), lower_key.size());
            kvssd::Slice leaf_key(leaf_key_str);
            kvssd::Slice leaf_val(mem_buf, mem_buf_size);
            kvd_->kv_append(&leaf_key, &leaf_val);

            // std::string leaf_key_str = "leaf"+std::string(lower_key.data(), lower_key.size());
            // char *leaf_key_c_str = (char *)malloc(leaf_key_str.size());
            // memcpy(leaf_key_c_str, leaf_key_str.c_str(), leaf_key_str.size());
            // char *leaf_val_c_str = (char *)malloc(mem_buf_size);
            // memcpy(leaf_val_c_str, mem_buf, mem_buf_size);
            // kvssd::Slice *leaf_key = new kvssd::Slice(leaf_key_c_str, leaf_key_str.size());
            // kvssd::Slice *leaf_val = new kvssd::Slice(leaf_val_c_str, mem_buf_size);
            // kvd_->kv_append_async(leaf_key, leaf_val, NULL, NULL);
        }

        // 4, update internal node (recursive)
        node->UpdateChildEntries(&lower_key, leaf_entries);
        //node->Flush(0); // update level0 node for leaf_entries
        if (leaf_split) {
            InternalNode *internal_split = NULL;
            Slice internal_insert_key = leaf_right;
            int internal_insert_entries = leaf_right_entries;
            int level = 0;
            do {
                internal_split = node->InsertEntry(&internal_insert_key, internal_insert_entries, level);
                internal_insert_key = node->FirstKey();
                internal_insert_entries = node->GetEntries();
                if (internal_split != NULL && node->Parent() == NULL) { // new root
                    assert(root_==node);
                    Cache::Handle *cache_handle;
                    std::string left_node_key = "level"+std::to_string(level);
                    std::string right_node_key = "level"+std::to_string(level)+node->FirstKey().ToString();
                    cache_handle = innode_cache_->Insert(Slice(right_node_key), node, 1, DeleteCacheItem);
                    innode_cache_->Release(cache_handle);
                    cache_handle = innode_cache_->Insert(Slice(left_node_key), internal_split, 1, DeleteCacheItem);
                    innode_cache_->Release(cache_handle);

                    root_ = new InternalNode(cmp_, kvd_, NULL, fanout_, level+1);
                    Slice root_left_key = internal_split->FirstKey();
                    root_->InsertEntry(&root_left_key, internal_split->GetEntries(),level+1);
                    node = root_;
                }
                else if (internal_split != NULL) { // split internal node
                    std::string old_node_key = "level"+std::to_string(level)+internal_split->FirstKey().ToString();
                    std::string new_node_key = "level"+std::to_string(level)+internal_insert_key.ToString();
                    // update cache
                    Cache::Handle *cache_handle;
                    cache_handle = innode_cache_->Lookup(Slice(old_node_key));
                    assert(cache_handle != NULL); // most recent node must be in cache
                    innode_cache_->UpdateValue(cache_handle, internal_split);
                    innode_cache_->Release(cache_handle);
                    cache_handle = innode_cache_->Insert(Slice(new_node_key), node, 1, DeleteCacheItem);
                    innode_cache_->Release(cache_handle);
                    // TODO can parent be evict from cache?
                    // If so, we need to setup a stack (with copyed node) and explict release
                    node = node->Parent(); 
                }
                else { // do nothing if internal node not split
                }
                level++;
            } while (internal_split != NULL);
            if (level > level_) {
                level_++;
            }
        }

        // 5, clean up (TODO)
        free(mem_buf);
        if (leaf_node!=NULL) delete leaf_node;
    }
}

bool KVBplusTree::Insert (Slice *key) {
    WriteBatch batch;
    batch.Put(*key);
    Write(&batch);
    //checkpoint put  
}

bool KVBplusTree::Write (WriteBatch *batch) {
    // check mem_ size
    std::unique_lock<std::mutex> lock(mutex_);
    if (mem_->Size() >= MAX_MEM_SIZE && imm_ == NULL) {
        // signal background thread bulkappend
        imm_ = mem_;
        mem_ = new MemNode(cmp_);
        bg_start.notify_one();
    }
    else if (mem_->Size() >= MAX_MEM_SIZE && imm_ != NULL) {
        // block all writer thread and wait for bg thread finish
        bg_end.wait();
        bg_end.reset();
    }
    else { // mem_->Size() < MAX_MEM_SIZE
        // do nothing, release lock
    }
    // write to mem_ is serialized
    auto it = batch->Iterator();
    for (; !batch->End(it); it++ ) {
        Slice key = *it;
        mem_->Insert(&key);
    }
}

void KVBplusTree::Iterator::SeekToFirst() {
    Slice key_target(NULL, 0);
    Seek(&key_target);
}

void KVBplusTree::Iterator::Seek(Slice *key) {
    // dump MemNode (make sure mem buffer is append in the tree)
    //tree_->BulkAppend(tree_->mem_, 0); // guaranteed when test in device (may affect unit test)

    int level = tree_->GetLevel();
    InternalNode *node = tree_->GetRoot();
    kvssd::KVSSD *kvd = tree_->GetDev();
    //Cache *innode_cache = tree_->innode_cache_;
    AtomicCache::ConstAccessor ac;
    Slice *key_target = key;
    Slice lower_key, upper_key; // don't care upper key
       
    // traverse internal node
    for (int i = 0; i < level; i++) {
        node->SearchKey(key_target, lower_key, upper_key);
        if (i == level - 1) break;
        
        // get next internal node
        std::string next_node_key = "level"+std::to_string(level-i-2)+lower_key.ToString();
        //Slice inner_key = Slice(next_node_key);
        //Cache::Handle *cache_handle = innode_cache->Lookup(inner_key);
        // if (cache_handle == NULL) {
        //     char *v_buf; int v_size;
        //     kvssd::Slice io_key(next_node_key);
        //     kvd->kv_get(&io_key, v_buf, v_size);
        //     node = new InternalNode(tree_->cmp_, kvd, node, tree_->fanout_, level-i-2, v_buf, v_size);
        //     free(v_buf);
        //     cache_handle = innode_cache->Insert(inner_key, node, 1, DeleteCacheItem);
        // }
        // else {
        //     InternalNode *parent = node;
        //     node = (InternalNode *)innode_cache->Value(cache_handle);
        //     node->UpdateParent(parent);
        // }
        // innode_cache->Release(cache_handle);

        String inner_key(next_node_key.data(), next_node_key.size());
        if(!tree_->concurr_cache_->find(ac, inner_key)) {
            char *v_buf; int v_size;
            kvssd::Slice io_key(next_node_key);
            kvd->kv_get(&io_key, v_buf, v_size);
            node = new InternalNode(tree_->cmp_, kvd, node, tree_->fanout_, level-i-2, v_buf, v_size);
            free(v_buf);
            tree_->concurr_cache_->insert(inner_key, node);
        }
        else {
            InternalNode *parent = node;
            node = *(ac.get());
            node->UpdateParent(parent);
        }
    }

    // get leaf node
    std::string leaf_node_key = "leaf"+lower_key.ToString();
    kvssd::Slice leaf_key(leaf_node_key);
    char *v_buf; int v_size;
    kvd->kv_get(&leaf_key, v_buf, v_size);

    leaf_ = new LeafNode(tree_->cmp_, NULL, NULL, v_buf, v_size);
    it_ = leaf_->Seek(key_target);
    if (!leaf_->Valid(it_)) { // go to next leaf node
        std::string next_leaf_key = "leaf"+leaf_->next_leaf_;
        kvssd::Slice next_leaf(next_leaf_key);
        delete leaf_;

        // read next leaf node
        kvd->kv_get(&next_leaf, v_buf, v_size);
        leaf_ = new LeafNode(tree_->cmp_, NULL, NULL, v_buf, v_size);
        it_ = leaf_->SeekToFirst();
    }
    if (key_target->size() == 0 && leaf_->Valid(it_)) ++it_; // skip the dummy head

    // clean up
}

void KVBplusTree::Iterator::Next() {
    ++it_;
    if (!leaf_->Valid(it_)) { // recursively traverse internal node 
        if (leaf_->next_leaf_.size() == 0) return;
        std::string next_leaf_key = "leaf"+leaf_->next_leaf_;
        kvssd::Slice next_leaf(next_leaf_key);
        delete leaf_;

        // read next leaf node
        char *v_buf; int v_size;
        tree_->kvd_->kv_get(&next_leaf, v_buf, v_size);
        leaf_ = new LeafNode(tree_->cmp_, NULL, NULL, v_buf, v_size);
        it_ = leaf_->SeekToFirst();
    }
}

bool KVBplusTree::Iterator::Valid() {
    return leaf_->Valid(it_);
}

Slice KVBplusTree::Iterator::key() {
    return *it_;
}

KVBplusTree::Iterator* KVBplusTree::NewIterator() {
    return new Iterator(this);
}

} // end of kvbtree namespace
