#include <string.h>
#include <stddef.h>
#include <stdlib.h>
#include <assert.h>
#include <cstdint>
#include <stdio.h>
#include <list>
#include <algorithm>

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
    void insert_key_to_index_no_split(internal_node_t &node, const key_t &key);

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
}
