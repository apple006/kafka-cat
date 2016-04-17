UNAME=$(shell uname)

CFLAGS=-Wall -Wextra -Wno-unused-parameter -g
PROG_NAME = kafka-cat

ifeq ($(UNAME), Darwin)
LDFLAGS=-Wl,-flat_namespace,-undefined,dynamic_lookup
endif

INSTALL=/usr/bin/install
INSTALLDIR=/usr/local
BINDIR=$(INSTALLDIR)/bin

objs = crc32.o buffer.o conn.o request.o response.o main.o metadata.o util.o cJSON/cJSON.o

$(PROG_NAME): $(objs)	
	gcc -o $(PROG_NAME) $(objs) -lm

buffer.o: buffer.c crc32.h buffer.h
conn.o: conn.c util.h conn.h
crc32.o: crc32.c crc32.h
main.o: main.c conn.h request.h response.h buffer.h main.h metadata.h \
util.h
metadata.o: metadata.c metadata.h
request.o: request.c buffer.h util.h request.h response.h metadata.h \
main.h conn.h
response.o: response.c response.h buffer.h request.h metadata.h main.h \
conn.h util.h error_map.h cJSON/cJSON.h
util.o: util.c util.h

clean:
	rm -f $(PROG_NAME) *.o
	cd cJSON && make clean && cd ..
install:
	mkdir -p $(BINDIR)
	$(INSTALL) $(PROG_NAME) $(BINDIR)
