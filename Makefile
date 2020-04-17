# (A) Optimized mode
 OPT ?= -O2 -DNDEBUG
# (B) Debug mode
# OPT ?= -g2

TARGET=liblevelkv.so

HOME=$(shell pwd)
CC=gcc
INCLUDES=-I$(HOME)/include
LIBS=-L$(HOME)/libs -Wl,-rpath,$(HOME)/libs -lrt -lpthread -lleveldb -lkvssd -ltbb
CXXFLAG=-fPIC -w -march=native -std=c++11 $(OPT)

DB_SRCS=$(HOME)/src/kv_index_lsm.cc $(HOME)/src/kv_index_btree.cc $(HOME)/src/kv_index_base.cc $(HOME)/src/kv_index_inmem.cc $(HOME)/src/db_impl.cc $(HOME)/src/db_iter.cc
KVBTREE_SRCS=$(HOME)/src/kvbtree/bplustree.cc $(HOME)/src/kvbtree/cache.cc $(HOME)/src/kvbtree/hash.cc $(HOME)/src/kvbtree/write_batch.cc
BASE_SRCS=$(HOME)/src/base/base.cc
INMEM_SRCS=$(HOME)/src/inmem/inmem.cc
KVSSD_SRCS=$(HOME)/src/kvssd/kvssd.cc
UTIL_SRCS=$(HOME)/util/comparator.cc
SRCS=$(DB_SRCS) $(KVBTREE_SRCS) $(BASE_SRCS) $(INMEM_SRCS) $(UTIL_SRCS)

all: kvssd leveldb shared

kvssd:
	$(CC) -shared -o $(HOME)/libs/libkvssd.so $(INCLUDES) $(KVSSD_SRCS) -L$(HOME)/libs -Wl,-rpath,$(HOME)/libs -lkvapi -lnuma $(CXXFLAG)

leveldb:
	make -C $(HOME)/src/leveldb/
	cp $(HOME)/src/leveldb/out-shared/libleveldb.so* $(HOME)/libs

shared:
	$(CC) -shared -o $(HOME)/libs/$(TARGET) $(SRCS) $(INCLUDES) $(LIBS) $(CXXFLAG)

clean:
	make -C $(HOME)/src/leveldb/ clean
	rm -rf $(HOME)/libs/$(TARGET) $(HOME)/libs/libkvssd.so $(HOME)/libs/libleveldb.so*
