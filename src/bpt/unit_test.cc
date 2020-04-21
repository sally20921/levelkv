#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <algorithm>

#define PRINT(a) fprintf(stderr, "\033[33m%s\033[0m \033[32m%s\033[0m\n", a, "Passed")

#include "bpt.cc"
//namespace bpt {
int main()
{
using namespace bpt;
const int size = 128;

bplus_tree* tree = new bplus_tree();
//assert(tree.meta.internal_node_num == 1);
//assert(tree.meta.leaf_node_num == 1);
//assert(tree.meta.height == 1);
//PRINT("EmptyTree");
//assert(tree->meta.internal_node_num == 1);
//assert(tree->meta.leaf_node_num == 1);
//assert(tree->meta.height == 1);
//PRINT("EmptyTree");

//assert(tree.insert("t2") == 0);
//assert(tree.insert("t4") == 0);
//assert(tree.insert("t1") == 0);
//assert(tree.insert("t3") == 0);
  
assert(tree->insert("t2") == 0);
assert(tree->insert("t4") == 0);
assert(tree->insert("t1") == 0);
assert(tree->insert("t3") == 0);

//bpt::leaf_node_t leaf;
//tree.map(&leaf, tree.search_leaf("t1"));
//assert(leaf.n == 4);
//assert(bpt::keycmp(leaf.children[0].key, "t1") == 0);
//assert(bpt::keycmp(leaf.children[1].key, "t2") == 0);
//assert(bpt::keycmp(leaf.children[2].key, "t3") == 0);
//assert(bpt::keycmp(leaf.children[3].key, "t4") == 0);

//PRINT("Insert4Elements");
  
bpt::leaf_node_t leaf;
tree->map(&leaf, tree->search_leaf("t1"));
//assert(leaf.n == 4);
//assert(bpt::keycmp(leaf.children[0].key, "t1") == 0);
//assert(bpt::keycmp(leaf.children[1].key, "t2") == 0);
//assert(bpt::keycmp(leaf.children[2].key, "t3") == 0);
//assert(bpt::keycmp(leaf.children[3].key, "t4") == 0);

PRINT("Insert4Elements");

return 0;
}
//}
