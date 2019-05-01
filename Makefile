VERSION := 0.01

CC=gcc -g -DVERSION='"$(VERSION)"'
LDFLAGS=
CFLAGS=-Wall

BINARIES=obinsectd

all: $(BINARIES)

.PHONY clean:
	rm -f *.o *.so *.lo *~ $(BINARIES) 

obinsectd: obinsectd.c
	$(CC) $(CFLAGS) -DDEBUG -Wno-unused-function $(LDFLAGS) -o $@ $^

