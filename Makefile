VERSION := 0.11

LDFLAGS=$(shell pkg-config --libs json-c) $(shell pkg-config --libs libmosquitto)
CFLAGS=-Wall $(shell pkg-config --cflags json-c) $(shell pkg-config --cflags libmosquitto) -DVERSION='"$(VERSION)"' -DWITH_TLS 

BINARIES=obinsectd

all: $(BINARIES)

.PHONY clean:
	rm -f *.o *.so *.lo *~ $(BINARIES) 

obinsectd: obinsectd.c
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

