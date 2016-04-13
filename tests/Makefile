UNAME=$(shell uname)

CFLAGS=-Wall -Wextra -Wno-unused-parameter -O3 -I./ctest -I../src/
ifdef CTEST_COLOR_OK
CCFLAGS+=-DCOLOR_OK
endif

ifeq ($(UNAME), Darwin)
LDFLAGS=-Wl,-flat_namespace,-undefined,dynamic_lookup
endif

all: test

main.o: main.c ctest/ctest.h
test_buffer.o: test_buffer.c ctest/ctest.h ../src/buffer.h

remake: clean all

objs= main.o test_buffer.o ../src/*.o
test: ctest/ctest.h $(objs) 
	gcc $(LDFLAGS) $(objs)  -o test

clean:
	rm -f test *.o
