OPT ?= -O2  -DNDEBUG

TARGET=liblevelkv.so

HOME=$(shell pwd)
CC=gcc
INCLUDES=-I$(HOME)/include 
LIBS=-L$(HOME)/libs -Wl,-rpath,$(HOME)/libs -lrt -lpthread -lkvssd -ltbb 
CXXFLAG=-fPIC -w -march=native -std=c++11 $(OPT)

DB_SRCS=$(HOME)/src/kv_index_atree.cc $(HOME)/src/kv_index_btree.cc $(HOME)/src/kv_index_base.cc $(HOME)/src/kv_index_inmem.cc $(HOME)/src/db_impl.cc $(HOME)/src/db_iter.cc

KVATREE_SRCS=$(HOME)/src/atree/atree.cc $(HOME)/src/atree/write_batch.cc
KVBTREE_SRCS=$(HOME)/src/kvbtree/bplustree.cc $(HOME)/src/kvbtree/cache.cc $(HOME)/src/kvbtree/hash.cc $(HOME)/src/kvbtree/write_batch.cc
BASE_SRCS=$(HOME)/src/base/base.cc
INMEM_SRCS=$(HOME)/src/inmem/inmem.cc
UTIL_SRCS=$(HOME)/util/comparator.cc
KVSSD_SRCS=$(HOME)/src/kvssd/kvssd.cc

SRCS=$(DB_SRCS) $(KVATREE_SRCS) $(KVBTREE_SRCS) $(BASE_SRCS) $(INMEM_SRCS) $(UTIL_SRCS)

