CC=g++
#INCLUDES=-I/home/sally20921/temp/levelkv-base/include
#LIBS=-L/home/sally20921/temp/levelkv-base/libs -pthread -llevelkv -std=c++11
CXXFLAG=-O2 -fPIC -w -march=native
CXXFLAG_DBG=-O0 -fPIC -w -march=native -g

all:
        $(CC) kvssd_test.cc -o kvssd_test $(INCLUDES) $(LIBS) $(CXXFLAG_DBG)

clean:
        rm -rf kvssd_test
