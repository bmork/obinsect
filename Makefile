VERSION := 0.01

CC=gcc -g -DVERSION='"$(VERSION)"'
LDFLAGS=$(shell pkg-config --libs json-c)
CFLAGS=-Wall $(shell pkg-config --cflags json-c)

BINARIES=obinsectd

all: $(BINARIES)

.PHONY clean:
	rm -f *.o *.so *.lo *~ $(BINARIES) 

obinsectd: obinsectd.c
	$(CC) $(CFLAGS) -DDEBUG $(LDFLAGS) -o $@ $^

