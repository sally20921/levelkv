#include <string.h>
#include <stddef.h>
#include <stdlib.h>
#include <assert.h>
#include <cstdint>
#include <stdio.h>
#include <list>
#include <algorithm>
#inlclude "slice.h"

using std::swap;
using std::binary_search;
using std::lower_bound;
using std::upper_bound;
/*void populate(char **str) {
 // 1. OK
 *str = (char *)malloc(sizeof(char) * 7);
 strcpy(*str, "Memory");
 
 // 2. Not OK if later freeing the memory
 *str = "Memory";
 }*/

namespace bpt {
    /* predefined B+ info */
#define BP_ORDER 1024
#define KEY_SIZE 16
    
    /* offsets */
#define OFFSET_META 0
#define OFFSET_BLOCK OFFSET_META + sizeof(meta_t)
#define SIZE_NO_CHILDREN sizeof(leaf_node_t) - BP_ORDER * sizeof(record_t)
    struct key_t {
        char k[KEY_SIZE];
        
        key_t(const char *str = "")
        {
            bzero(k, sizeof(k));
            strcpy(k, str);
        }
        
        operator bool() const {
            return strcmp(k, "");
        }
    };
    
    inline int keycmp(const key_t &a, const key_t &b) {
        int x = strlen(a.k) - strlen(b.k);
        return x == 0 ? strcmp(a.k, b.k) : x;
    }
    
#define OPERATOR_KEYCMP(type) \
bool operator< (const key_t &l, const type &r) {\
return keycmp(l, r.key) < 0;\
}\
bool operator< (const type &l, const key_t &r) {\
return keycmp(l.key, r) < 0;\
}\
bool operator== (const key_t &l, const type &r) {\
return keycmp(l, r.key) == 0;\
}\
bool operator== (const type &l, const key_t &r) {\
return keycmp(l.key, r) == 0;\
}\


class WriteBatch {
public: 
	WriteBatch();
	~WriteBatch();

	void Put(Slice &key);
	void Delete(Slice &key);
	void Clear();

	std::vector<Slice>::iterator Iterator();
	bool End(std::vector<Slice>::iterator  &it);
private:
	std::vector<Slice> list_;
};

//off_t  signed integer used to represent file sizes
    //this type is transparently replaced by off64_t
    //off_t acts same as the pointer
    /* meta information of B+ tree */
    struct meta_t{
        size_t internal_node_num; /* how many internal nodes */
        size_t leaf_node_num;     /* how many leafs */
        size_t height;            /* height of tree (exclude leafs) */
        off_t slot;        /* where to store new block */
        off_t root_offset; /* where is the root of internal nodes */
        off_t leaf_offset; /* where is the first leaf */
    };
    
    /* internal nodes' index segment */
    struct index_t {
        key_t key;
        off_t child; /* child's offset */
    };
    
    
    /***
     * internal node block
     ***/
    struct internal_node_t {
        typedef index_t * child_t;
        
        off_t parent; /* parent node offset */
        off_t next;
        off_t prev;
        size_t n; /* how many children */
        index_t children[BP_ORDER];
    };
    
    /* the final record of value */
    struct record_t {
        key_t key;
    };
    
    /* leaf node block */
    struct leaf_node_t {
        typedef record_t *child_t;
        
        off_t parent; /* parent node offset */
        off_t next;
        off_t prev;
        size_t n;
        record_t children[BP_ORDER];
    };
    
    class bplus_tree {
    public:
        bplus_tree();
        int insert(const key_t& key);
        meta_t get_meta() {
            return meta;
        };
        
        meta_t meta;
        
        /* init empty tree */
        void init_from_empty();
        
        /* find index */
        off_t search_index(const key_t &key) const;
        
        /* find leaf */
        off_t search_leaf(off_t index, const key_t &key) const;
        off_t search_leaf(const key_t &key) const
        {
            return search_leaf(search_index(key), key);
        }
        
        /* insert into leaf without split */
        void insert_record_no_split(leaf_node_t *leaf,
                                    const key_t &key);
        
        /* add key to the internal node */
        void insert_key_to_index(off_t offset, const key_t &key, off_t old, off_t after);
        void insert_key_to_index_no_split(internal_node_t &node, const key_t &key, off_t value);
        
        /* change children's parent */
        void reset_index_children_parent(index_t *begin, index_t *end,
                                         off_t parent);
        
        template<class T>
        void node_create(off_t offset, T *node, T *next);
        
        template<class T>
        void node_remove(T *prev, T *node);
        
        /* multi-level file open/close */
        mutable FILE *fp;
        mutable int fp_level;
        
        /* alloc from heap */
        off_t alloc(size_t size)
        {
            off_t slot = meta.slot;
            meta.slot += size;
            return slot;
        }
        //pointer-variable = new data-type;
        off_t alloc(leaf_node_t *leaf)
        {
            leaf->n = 0;
            meta.leaf_node_num++;
            off_t temp =(long int) new leaf_node_t;
            return temp;
        }
        off_t alloc(internal_node_t *node)
        {
            node->n = 1;
            meta.internal_node_num++;
            off_t temp =(long int) new internal_node_t;
            return temp;
        }
        
        void unalloc(leaf_node_t *leaf, off_t offset)
        {
            --meta.leaf_node_num;
        }
        
        void unalloc(internal_node_t *node, off_t offset)
        {
            --meta.internal_node_num;
        }
        
        /* read block from heap*/
        int map(void *block, off_t offset, size_t size) const
        {
            //open_file();
            //fseek(fp, offset, SEEK_SET);
            //size_t rd = fread(block, size, 1, fp);
            //close_file();
            
            //return rd - 1;
            return 0;
        }
        
        template<class T>
        int map(T *block, off_t offset) const
        {
            //return map(block, offset, sizeof(T));
            // *block = *(T*)offset;
            memcpy(block, (void*)offset, sizeof(T));
            return 0;
        }
        
        /* write block to disk */
        int unmap(void *block, off_t offset, size_t size) const
        {
            // open_file();
            //fseek(fp, offset, SEEK_SET);
            //size_t wd = fwrite(block, size, 1, fp);
            //close_file();
            
            //return wd - 1;
            return 0;
        }
        
        template<class T>
        int unmap(T *block, off_t offset) const
        {
            //return unmap(block, offset, sizeof(T));
            //offset = new T;
            //*offset = *block;
            offset = (off_t)malloc(sizeof(T));
            memcpy((void*)offset,(void*) block, sizeof(T));
            return 0;
        }
        
        
        
        
    };
    
    
    /* custom compare operator for STL algorithms */
    OPERATOR_KEYCMP(index_t)
    OPERATOR_KEYCMP(record_t)
    
    /* helper iterating function */
    template<class T>
    inline typename T::child_t begin(T &node) {
        return node.children;
    }
    template<class T>
    inline typename T::child_t end(T &node) {
        return node.children + node.n;
    }
    
    /* helper searching function */
    inline index_t *find(internal_node_t &node, const key_t &key) {
        if (key) {
            return upper_bound(begin(node), end(node) - 1, key);
        }
        // because the end of the index range is an empty string, so if we search the empty key(when merge internal nodes), we need to return the second last one
        if (node.n > 1) {
            return node.children + node.n - 2;
        }
        return begin(node);
    }
    inline record_t *find(leaf_node_t &node, const key_t &key) {
        return lower_bound(begin(node), end(node), key);
    }
    
    bplus_tree::bplus_tree()
    : fp(NULL), fp_level(0)
    {
        init_from_empty();
    }
    
    
    int bplus_tree::insert(const key_t& key)
    {
        off_t parent = search_index(key);
        off_t offset = search_leaf(parent, key);
        leaf_node_t leaf;
        map(&leaf, offset);
        
        // check if we have the same key
        if (binary_search(begin(leaf), end(leaf), key))
            return 1;
        
        if (leaf.n == BP_ORDER) {
            // split when full
            
            // new sibling leaf
            leaf_node_t new_leaf;
            node_create(offset, &leaf, &new_leaf);
            
            // find even split point
            size_t point = leaf.n / 2;
            bool place_right = keycmp(key, leaf.children[point].key) > 0;
            if (place_right)
                ++point;
            
            // split
            std::copy(leaf.children + point, leaf.children + leaf.n,
                      new_leaf.children);
            new_leaf.n = leaf.n - point;
            leaf.n = point;
            
            // which part do we put the key
            if (place_right)
                insert_record_no_split(&new_leaf, key);
            else
                insert_record_no_split(&leaf, key);
            
            // save leafs
            unmap(&leaf, offset);
            unmap(&new_leaf, leaf.next);
            
            // insert new index key
            insert_key_to_index(parent, new_leaf.children[0].key,
                                offset, leaf.next);
        } else {
            insert_record_no_split(&leaf, key);
            unmap(&leaf, offset);
        }
        
        return 0;
    }
    
    void bplus_tree::insert_key_to_index(off_t offset, const key_t &key,
                                         off_t old, off_t after)
    {
        if (offset == 0) {
            // create new root node
            internal_node_t root;
            root.next = root.prev = root.parent = 0;
            meta.root_offset = alloc(&root);
            meta.height++;
            
            // insert `old` and `after`
            root.n = 2;
            root.children[0].key = key;
            root.children[0].child = old;
            root.children[1].child = after;
            
            unmap(&meta, OFFSET_META);
            unmap(&root, meta.root_offset);
            
            // update children's parent
            reset_index_children_parent(begin(root), end(root),
                                        meta.root_offset);
            return;
        }
        
        internal_node_t node;
        map(&node, offset);
        
        
        if (node.n == BP_ORDER) {
            // split when full
            
            internal_node_t new_node;
            node_create(offset, &node, &new_node);
            
            // find even split point
            size_t point = (node.n - 1) / 2;
            bool place_right = keycmp(key, node.children[point].key) > 0;
            if (place_right)
                ++point;
            
            // prevent the `key` being the right `middle_key`
            // example: insert 48 into |42|45| 6|  |
            if (place_right && keycmp(key, node.children[point].key) < 0)
                point--;
            
            key_t middle_key = node.children[point].key;
            
            // split
            std::copy(begin(node) + point + 1, end(node), begin(new_node));
            new_node.n = node.n - point - 1;
            node.n = point + 1;
            
            // put the new key
            if (place_right)
                insert_key_to_index_no_split(new_node, key, after);
            else
                insert_key_to_index_no_split(node, key, after);
            
            unmap(&node, offset);
            unmap(&new_node, node.next);
            
            // update children's parent
            reset_index_children_parent(begin(new_node), end(new_node), node.next);
            
            // give the middle key to the parent
            // note: middle key's child is reserved
            insert_key_to_index(node.parent, middle_key, offset, node.next);
        } else {
            insert_key_to_index_no_split(node, key, after);
            unmap(&node, offset);
        }
    }
    
    void bplus_tree::insert_key_to_index_no_split(internal_node_t &node,
                                                  const key_t &key, off_t value)
    {
        index_t *where = upper_bound(begin(node), end(node) - 1, key);
        
        // move later index forward
        std::copy_backward(where, end(node), end(node) + 1);
        
        // insert this key
        where->key = key;
        where->child = (where + 1)->child;
        (where + 1)->child = value;
        
        node.n++;
    }

    void bplus_tree::insert_record_no_split(leaf_node_t *leaf,
                                        const key_t &key)
{
    record_t *where = upper_bound(begin(*leaf), end(*leaf), key);
    std::copy_backward(where, end(*leaf), end(*leaf) + 1);

    where->key = key;
    //where->value = value;
    leaf->n++;
}
    
    void bplus_tree::reset_index_children_parent(index_t *begin, index_t *end,
                                                 off_t parent)
    {
        // this function can change both internal_node_t and leaf_node_t's parent
        // field, but we should ensure that:
        // 1. sizeof(internal_node_t) <= sizeof(leaf_node_t)
        // 2. parent field is placed in the beginning and have same size
        internal_node_t node;
        while (begin != end) {
            map(&node, begin->child);
            node.parent = parent;
            unmap(&node, begin->child, SIZE_NO_CHILDREN);
            ++begin;
        }
    }
    
    off_t bplus_tree::search_index(const key_t &key) const
    {
        off_t org = meta.root_offset;
        int height = meta.height;
        while (height > 1) {
            internal_node_t node;
            map(&node, org);
            
            index_t *i = upper_bound(begin(node), end(node) - 1, key);
            org = i->child;
            --height;
        }
        
        return org;
    }
    
    off_t bplus_tree::search_leaf(off_t index, const key_t &key) const
    {
        internal_node_t node;
        map(&node, index);
        
        index_t *i = upper_bound(begin(node), end(node) - 1, key);
        return i->child;
    }
    
    template<class T>
    void bplus_tree::node_create(off_t offset, T *node, T *next)
    {
        // new sibling node
        next->parent = node->parent;
        next->next = node->next;
        next->prev = offset;
        node->next = alloc(next);
        // update next node's prev
        if (next->next != 0) {
            T old_next;
            map(&old_next, next->next, SIZE_NO_CHILDREN);
            old_next.prev = node->next;
            unmap(&old_next, next->next, SIZE_NO_CHILDREN);
        }
        unmap(&meta, OFFSET_META);
    }
    
    template<class T>
    void bplus_tree::node_remove(T *prev, T *node)
    {
        unalloc(node, prev->next);
        prev->next = node->next;
        if (node->next != 0) {
            T next;
            map(&next, node->next, SIZE_NO_CHILDREN);
            next.prev = node->prev;
            unmap(&next, node->next, SIZE_NO_CHILDREN);
        }
        unmap(&meta, OFFSET_META);
    }
    
    void bplus_tree::init_from_empty()
    {
        // init default meta
        bzero(&meta, sizeof(meta_t));
        // meta.order = BP_ORDER;
        // meta.value_size = sizeof(value_t);
        // meta.key_size = sizeof(key_t);
        meta.height = 1;
        meta.slot = OFFSET_BLOCK;
        
        // init root node
        internal_node_t* root = new internal_node_t;
        root->next = root->prev = root->parent = 0;
        meta.root_offset = (off_t) root;
        
        // init empty leaf
        leaf_node_t* leaf = new leaf_node_t;
        leaf->next = leaf->prev = 0;
        leaf->parent = meta.root_offset;
        meta.leaf_offset = root->children[0].child = (off_t) leaf;
        
        // save
        unmap(&meta, OFFSET_META);
        unmap(&root, meta.root_offset);
        unmap(&leaf, root->children[0].child);
    }
	
	WriteBatch::WriteBatch() {}
	WriteBatch::~WriteBatch() {}

	void WriteBatch::Put(Slice &key){
	}
	
 	void WriteBatch::Delete(Slice &key) {
	}

	void WriteBatch::Clear() {
	
	}
   
}
