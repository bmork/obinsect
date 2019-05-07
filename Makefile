VERSION := 0.01

CC=gcc -g -DVERSION='"$(VERSION)"'
LDFLAGS=$(shell pkg-config --libs json-c) $(shell pkg-config --libs libmosquitto)
CFLAGS=-Wall $(shell pkg-config --cflags json-c) $(shell pkg-config --cflags libmosquitto)

BINARIES=obinsectd

all: $(BINARIES)

.PHONY clean:
	rm -f *.o *.so *.lo *~ $(BINARIES) 

obinsectd: obinsectd.c
	$(CC) $(CFLAGS) -DDEBUG -DWITH_TLS $(LDFLAGS) -o $@ $^

