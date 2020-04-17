
CC=g++
INCLUDES=-I../include
LIBS=-L../libs -pthread -llevelkv -std=c++11
CXXFLAG=-O2 -fPIC -w -march=native
CXXFLAG_DBG=-O0 -fPIC -w -march=native -g 

all:
	$(CC) db_test.cc -o db_test $(INCLUDES) $(LIBS) $(CXXFLAG_DBG)

clean:
	rm -rf db_test
