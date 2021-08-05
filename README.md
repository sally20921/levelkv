# LevelKV*: Checkpoint Based Persistent Key Value Store for Key-Value SSD 

## Introduction
- recently, another big change has been introduced to storage landscape: Key-Value SSDs.
- Key-Value SSD aims to store key value pairs at a hardware level 
- just replacing block SSD with KV SSD underneath LSM-tree structured key-value store will not work
- LevelKV* is a Key-Value-conscious persistent key-value store with LevelDB as interface
- LevelKV* supports range query and crash consistency

## LevelDB
- widely used key-value store based on LSM-trees
- supports range queries, snapshots, etc. 
- on-disk log file, in-memory memtable, immutable memtable, seven levels of on-disk Sorted String Table (SSTable) files 
- excessive reads and writes that are unnecessary has been one of the major drawbacks of LSM-tree structured key-value store

![image](https://user-images.githubusercontent.com/38284936/128334606-2c694468-d2f1-40be-a38e-f18dbdc64ddd.png)

## Design 
- in-memory B+-Tree, OpQueue (Operation Queue) and on-SSD CheckNodes (Checkpoint Nodes)
- operation in OpQueue is first transformed into an CheckNode then flushed to KV SSD

![image](https://user-images.githubusercontent.com/38284936/128334681-d7e734fc-1008-4a41-9072-ae14660f0cbc.png)

## Operation
- (1) Insertion or deletion operation in OpQueue or 2) Slice (data type in KV SSD interface that is similar to type std::string in c++) type keys that were sorted in the B⁺-Tree have to be transformed into a single key-value pair
- range query: range of queries have to be searched in the B⁺-Tree to get all the indexes; then values are retrieved from KV SSD device by using the internal get function

![image](https://user-images.githubusercontent.com/38284936/128334746-b4a2a306-ed51-47cb-9943-336e9efb253c.png)

## Crash Consistency
- log insert, delete operation in the background thread
- when the system crashes and B⁺-Tree and OpQueue structure are lost in memory, LevelKV* recovers the structure by first calling the last indexed CheckNode that is dividable by NUM
- log insert, delete operation in the background thread

![image](https://user-images.githubusercontent.com/38284936/128334827-7c1f6a6c-bb81-45c2-88b3-46e781c86d1e.png)

![image](https://user-images.githubusercontent.com/38284936/128334866-2d4d8b49-91d3-45cc-bebd-ad7f4f6322e2.png)

## Benchmarks
- experiments are run on a testing machine with two Intel(R) Xeon(R) CPU @ 2.10GHz (24 cores per CPU processors and 64-GB of memory)
- operating system is 64-bit Ubuntu 18.04.03 LTS and the file system used is ext4
- storage device used is 3.84-TB SAMSUNG PM983 SSD, which has both block interface and KV interface



### build emulator (environment without actual device)

```bash
	# build kvapi library
	export PRJ_HOME=$(pwd)
	export KVSSD_HOME=$PRJ_HOME/KVSSD-1.2.0/PDK/core
	$KVSSD_HOME/tools/install_deps.sh # install kvapi dependency
	mkdir $KVSSD_HOME/build
	cd $KVSSD_HOME/build
	cmake -DWITH_EMU=ON $KVSSD_HOME
	make -j4

	# copy libkvapi.so
	mkdir $PRJ_HOME/libs
	cp $KVSSD_HOME/build/libkvapi.so $PRJ_HOME/libs/
```

## build levelkv library

```bash
	make
```

## build test case

```bash
	export PRJ_HOME=$(pwd)
	cd $PRJ_HOME/test
	make lsm # lsm index
	make btree # btree index
```

## run

Configure DB options by environment: (please refer to include/levelkv/options.h)
```bash
	export INDEX_TYPE=LSM # LSM, BTREE, BASE
	export PREFETCH_ENA=TRUE # TRUE, FALSE
	export PREFETCH_PREFETCH_DEPTH=16 # any integer
	export RANGE_FILTER_ENA=TRUE # TRUE, FALSE
```

Note: please keep the kvssd_emul.conf file in the executable file directory. This configuration file override the default configuration by disabling the iops model (run faster).

```bash
	export PRJ_HOME=$(pwd)
	cd $PRJ_HOME/test
	export LD_LIBRARY_PATH=$PRJ_HOME/libs/
	./db_test
```

