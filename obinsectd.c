/*
 * SPDX-License-Identifier: GPL-2.0
 *
 * A simple OBIS COSEM to MQTT proxy
 *
 * Copyright 2019 Bjørn Mork <bjorn@mork.no>
 * crc16 and hdlc parts are:  Copyright (C) 2010 Red Hat, Inc.
 *
 * Parsing struct:
 *   https://github.com/roarfred/AmsToMqttBridge/tree/master/Code/Arduino/HanReader/src
 *
 * Data samples and important findings (differences in parity and use of OBIS codes):
 *   https://github.com/roarfred/AmsToMqttBridge/tree/master/Samples
 *
 * HAN data packets are sent inside an HDLC frame. The following COSEM classes are used:
 *  - Data (class_id 1)
 *  - Register (class_id 3)
 *  - Clock (class_id 9)
 */

#include <endian.h>
#include <errno.h>
#include <getopt.h>
#include <fcntl.h>
#include <json.h>
#include <poll.h>
#include <stdbool.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <termios.h>
#include <time.h>
#include <unistd.h>

#ifndef VERSION
  #define VERSION "unknown"
#endif
#define DESCRIPTION "obinsectd (" VERSION ")"

#ifdef DEBUG
static bool debug = true;
#define debug(arg...) fprintf(stderr, arg)
static void print_packet(const char *pfx, void *buf, int len)
{
	int i;
	unsigned char *p = buf;

	fprintf(stderr, "%s", pfx);
	for (i=0; i<len; i++)
		fprintf(stderr, "%02hhx%c", p[i], (i + 1) % 16 ? ' ' : '\n');
	fprintf(stderr, "\n");
}
#else
static bool debug = false;
#define debug(arg...)
#define print_packet(pfx, buf, len)
#endif /* DEBUG */

#define BUFSIZE (1024 * 8)
static char *buf = NULL;

/*
 * crc16 and HDLC escape code borrowed from modemmanager/libqcdm
 * Copyright (C) 2010 Red Hat, Inc.
 */

/* Table of CRCs for each possible byte, with a generator polynomial of 0x8408 */
static const __uint16_t crc_table[256] = {
    0x0000, 0x1189, 0x2312, 0x329b, 0x4624, 0x57ad, 0x6536, 0x74bf,
    0x8c48, 0x9dc1, 0xaf5a, 0xbed3, 0xca6c, 0xdbe5, 0xe97e, 0xf8f7,
    0x1081, 0x0108, 0x3393, 0x221a, 0x56a5, 0x472c, 0x75b7, 0x643e,
    0x9cc9, 0x8d40, 0xbfdb, 0xae52, 0xdaed, 0xcb64, 0xf9ff, 0xe876,
    0x2102, 0x308b, 0x0210, 0x1399, 0x6726, 0x76af, 0x4434, 0x55bd,
    0xad4a, 0xbcc3, 0x8e58, 0x9fd1, 0xeb6e, 0xfae7, 0xc87c, 0xd9f5,
    0x3183, 0x200a, 0x1291, 0x0318, 0x77a7, 0x662e, 0x54b5, 0x453c,
    0xbdcb, 0xac42, 0x9ed9, 0x8f50, 0xfbef, 0xea66, 0xd8fd, 0xc974,
    0x4204, 0x538d, 0x6116, 0x709f, 0x0420, 0x15a9, 0x2732, 0x36bb,
    0xce4c, 0xdfc5, 0xed5e, 0xfcd7, 0x8868, 0x99e1, 0xab7a, 0xbaf3,
    0x5285, 0x430c, 0x7197, 0x601e, 0x14a1, 0x0528, 0x37b3, 0x263a,
    0xdecd, 0xcf44, 0xfddf, 0xec56, 0x98e9, 0x8960, 0xbbfb, 0xaa72,
    0x6306, 0x728f, 0x4014, 0x519d, 0x2522, 0x34ab, 0x0630, 0x17b9,
    0xef4e, 0xfec7, 0xcc5c, 0xddd5, 0xa96a, 0xb8e3, 0x8a78, 0x9bf1,
    0x7387, 0x620e, 0x5095, 0x411c, 0x35a3, 0x242a, 0x16b1, 0x0738,
    0xffcf, 0xee46, 0xdcdd, 0xcd54, 0xb9eb, 0xa862, 0x9af9, 0x8b70,
    0x8408, 0x9581, 0xa71a, 0xb693, 0xc22c, 0xd3a5, 0xe13e, 0xf0b7,
    0x0840, 0x19c9, 0x2b52, 0x3adb, 0x4e64, 0x5fed, 0x6d76, 0x7cff,
    0x9489, 0x8500, 0xb79b, 0xa612, 0xd2ad, 0xc324, 0xf1bf, 0xe036,
    0x18c1, 0x0948, 0x3bd3, 0x2a5a, 0x5ee5, 0x4f6c, 0x7df7, 0x6c7e,
    0xa50a, 0xb483, 0x8618, 0x9791, 0xe32e, 0xf2a7, 0xc03c, 0xd1b5,
    0x2942, 0x38cb, 0x0a50, 0x1bd9, 0x6f66, 0x7eef, 0x4c74, 0x5dfd,
    0xb58b, 0xa402, 0x9699, 0x8710, 0xf3af, 0xe226, 0xd0bd, 0xc134,
    0x39c3, 0x284a, 0x1ad1, 0x0b58, 0x7fe7, 0x6e6e, 0x5cf5, 0x4d7c,
    0xc60c, 0xd785, 0xe51e, 0xf497, 0x8028, 0x91a1, 0xa33a, 0xb2b3,
    0x4a44, 0x5bcd, 0x6956, 0x78df, 0x0c60, 0x1de9, 0x2f72, 0x3efb,
    0xd68d, 0xc704, 0xf59f, 0xe416, 0x90a9, 0x8120, 0xb3bb, 0xa232,
    0x5ac5, 0x4b4c, 0x79d7, 0x685e, 0x1ce1, 0x0d68, 0x3ff3, 0x2e7a,
    0xe70e, 0xf687, 0xc41c, 0xd595, 0xa12a, 0xb0a3, 0x8238, 0x93b1,
    0x6b46, 0x7acf, 0x4854, 0x59dd, 0x2d62, 0x3ceb, 0x0e70, 0x1ff9,
    0xf78f, 0xe606, 0xd49d, 0xc514, 0xb1ab, 0xa022, 0x92b9, 0x8330,
    0x7bc7, 0x6a4e, 0x58d5, 0x495c, 0x3de3, 0x2c6a, 0x1ef1, 0x0f78
};

/* Calculate the CRC for a buffer using a seed of 0xffff */
static __uint16_t crc16(const char *buffer, size_t len)
{
    __uint16_t crc = 0xffff;

    while (len--)
            crc = crc_table[(crc ^ *buffer++) & 0xff] ^ (crc >> 8);
    return ~crc;
}

#define CONTROL 0x7e
#define ESCAPE  0x7d
#define MASK    0x20

static size_t unescape(const char *in, size_t inlen, char *out, size_t outlen)
{
	size_t i, j = 0;
	bool escaping = false;

	for (i = 0; i < inlen; i++) {
		if (j >= outlen) {
			debug("i=%zu, j=%zu, inlen=%zu, outlen=%zu\n", i, j, inlen, outlen);
			return 0;
		}
		if (escaping) {
			out[j++] = in[i] ^ MASK;
			escaping = false;
		} else if (in[i] == ESCAPE) {
			escaping = true;
		} else {
			out[j++] = in[i];
		}
	}
	return j;
}

static size_t hdlc_unframe(const char *in, size_t inlen, char *out, size_t outlen)
{
    __uint16_t crc;
    size_t j, i = inlen;

    /* the first control char is optional */
    if (*in == CONTROL) {
	    in++;
	    i--;
    }
    if (in[i - 1] == CONTROL)
	    i--;

    j = unescape(in, i, out, outlen);
    if (j < 2) {
	    debug("unescape failed: j = %zu\n", j);
	    return 0;
    }
    j -= 2; /* remove the crc */

    /* verify the crc */
    crc = crc16(out, j);
    if (crc != ((unsigned char)out[j] | (unsigned char)out[j + 1] << 8)) {
	    debug("crc failed: 0x%04x != 0x%04x\n", crc, out[j] | out[j + 1] << 8);
	    return 0;
    }
    return j;
}

static int serial_open(const char *dev)
{
	struct termios terminal_data;
	int fd;

	fd = open(dev,  O_RDONLY | O_NOCTTY);
	if (fd > 0) {
		tcgetattr(fd, &terminal_data);
		cfmakeraw(&terminal_data);
		tcsetattr(fd, TCSANOW, &terminal_data);
	}
	if (debug)
		fprintf(stderr, "opened %s\n", dev);
	return fd;
}

struct hdlc_header {
	__uint8_t format;
	__uint16_t len;
	__uint32_t src, dst;
	__uint8_t control;
	__uint16_t hcs;             // check sequence from byte 1-6 (1byte addrs) - inkluderer 0x7E !! */
	__uint8_t ssap, dsap, llcq; /* format identifier, group identifier, group length
				       i følge https://library.iugaza.edu.ps/thesis/121838.pdf
				       Følges av "group length" antall bytes! */


};

struct obistime {
	__uint16_t year;
	__uint8_t month,day, weekday, hour, min, sec;
	__uint8_t unknown;
	__uint8_t deviation;
	__uint16_t status;
};

struct dlms {
	__uint8_t tag;
	__uint32_t id_and_priority;
	__uint8_t timelen;
	struct obistime time;
};

#define MAXPL 20
struct cosemlist {
	struct hdlc_header hdlc;
	struct dlms apdu;
	int n_obis;
//	struct obis payload[MAXPL];
	__uint16_t fcs;
};

/* addr is variable length 1 - 4 bytes, terminated by LSB == 1 */
static int getaddr(const char *p, __uint32_t *addr)
{
	__uint32_t tmp = 0;
	int len = 0;

	while (len < 4 && !(p[len] & 0x01)) {
			tmp <<= 8;
			tmp |= p[len];
			len ++;
	}
	*addr = tmp;
	return len;
}

/*
static inline __uint8_t pull8(char **p)
{
	__uint8_t tmp = *p++[0];
	return tmp;
}

static inline __uint16_t pull16(char **p)
{
	__uint16_t tmp = *p++[0] << 8 | *p++[1];
	return tmp;
}

static inline __uint32_t pull32(char **p)
{
	__uint32_t tmp = *p++[0] << 24 | *p++[1] << 16 |  *p++[2] << 8 | *p++[3];
	return tmp;
}

*/

/* 
 * See /usr/local/src/git/AmsToMqttBridge/Samples/Kaifa/readme.md for docs!
 *
 * http://www.fit.vutbr.cz/units/UIFS/pubs/tr.php?file=%2Fpub%2F11616%2FTR-DLMS.pdf&id=11616
 * (TR-DLMS.pdf) has a Table 3 contains data types usable for attributes of COSEM objects 
 *
 * Better reference:
 *  https://www.dlms.com/files/Blue-Book-Ed-122-Excerpt.pdf,
 *  section 4.1.5 "Common data types"

Important types:

1 struct
2 array
6 double-long-unsigned
9 octet-string
10 visible-string
15 integer (8bit)
16 long (16bit)
18 long-unsigned (16bit)
22 enum (0..255)


both array and struct are followed by an element count.  Structs can be nested


Ref aidon sample:

7e a0d2 41 0883 13 82d6 e6e700
0f 40000000 00
0109 // array of 9 elements
 0202 // struct of 2 elements
  0906 0101000281ff // octet-string of 6 bytes (OBIS code)
  0a0b 4149444f4e5f5630303031 // visible-string of 11 bytes (AIDON_V0001) 
 0202 // struct of 2 elements
  0906 0000600100ff // octet-string of 6 bytes (OBIS code)
  0a10 37333539393932383930393431373432  // visible-string of 16 bytes (7359992890941742)
 0202
  0906 0000600107ff
  0a04 36353135
 0203  // struct of 3 elements
  0906 0100010700ff // octet-string of 6 bytes (OBIS code)
  06 00000552 // double-long-unsigned
  0202         // struct of 2 elements (inner struct!)
   0f00        // 8bit int (0)
   161b        // enum (27)
 0203
  0906 0100020700ff
  06 00000000
  0202
   0f00
   161b
 0203
  0906 0100030700ff
  06 000003e4
  0202
   0f00
   161d
 0203
  0906 0100040700ff
  06 00000000
  0202
   0f00
   161d
 0203
  0906 01001f0700ff
  10 005d
  0202
   0fff
   1621
 0203
  0906 0100200700ff
  12 09c4
  0202
   0fff
   1623
e0c4 7e



ref Kamstrup sample:

7E A0E2 2B 21 13 239A E6E700
0F 00000000 0C07D0010106162100FF800001

0219  // struct of 25 elements
 0A0E 4B616D73747275705F5630303031
 0906 0101000005FF
 0A10 35373036353637303030303030303030
 0906 0101600101FF
 0A12 303030303030303030303030303030303030
 0906 0101010700FF
 0600000000  // double-long-unsigned (0)
 0906 0101020700FF
 0600000000
 0906 0101030700FF
 0600000000
 0906 0101040700FF
 0600000000
 0906 01011F0700FF
 0600000000
 0906 0101330700FF
 0600000000
 0906 0101470700FF
 0600000000
 0906 0101200700FF
 120000 // long-unsigned (0)
 0906 0101340700FF
 120000 // long-unsigned (0)
 0906 0101480700FF
 120000
5BE5 7E



 * HDLC:
 *    no segmentation
 *    1 byte addressing
 *    src: 16
 *    dst(client): 21
 *    frame-type: UI
 *    tx window: 1
 *    max tx len: 1010


 * example:

7E A0E2 2B 21 13 239A E6E700
0F 00000000 0C07D0010106162100FF800001
0219
0A0E 4B616D73747275705F5630303031
0906 0101000005FF 0A10 35373036353637303030303030303030
0906 0101600101FF 0A12 303030303030303030303030303030303030
0906 0101010700FF 0600000000
0906 0101020700FF 0600000000
0906 0101030700FF 0600000000
0906 0101040700FF 0600000000
0906 01011F0700FF 0600000000
0906 0101330700FF 0600000000
0906 0101470700FF 0600000000
0906 0101200700FF 120000
0906 0101340700FF 120000
0906 0101480700FF 120000
5BE57E

 */
/*
struct cosem {
	__uint8_t type;
	__uint8_t len;
	void *data;
};

static int parse_cosem(unsigned char *buf, size_t *len)
{
	__uint8_t type = buf[0];

	switch (type) {
	case 1: // struct
		break;
	case 2: // array
		break;
	default:
	}
}

*/
static unsigned char *parse_hdlc_header(unsigned char *p, size_t *len)
{
	int i, format, hdlclen, src = 0, dst = 0, control, chksum;

	format = (unsigned char)p[0] >> 4;
	hdlclen = (p[0] << 8 | p[1]) & 0x7ff;

	/* src address is always 1 byte? */
	src = p[2];

	/* dst address is 1 - 4 bytes */
	for (i = 0; i < 4; i++) {
		dst <<= 8;
		dst |= p[3 + i];
		if (dst & 1)
			break;
	}

	control = p[4 + i];
	chksum = p[6 + i] << 8 | p[5 + i];
	fprintf(stderr, "HDLC format=%#04x, len=%d (buflen=%zd), src=%d, dst=%d, control=%#04x, chksum=%#06x\n", format, hdlclen, *len, src, dst, control, chksum);
	fprintf(stderr, "calculated chksum=%#06x\n", crc16((char *)p, 5 + i));

	// frame fcs
	chksum = p[*len-1] << 8 | p[*len-2];
	fprintf(stderr, "fcs=%#06x, calc=%#06x\n", chksum, crc16((char *)p, *len-2));

	*len -= 7 + i;
	return &p[7 + i];
}

static int parsebuf(unsigned char *buf, size_t buflen)
{
	unsigned char *p = buf;
	size_t len = buflen;

	/* 1. parse HDLC header */
	p = parse_hdlc_header(p, &len);
	return len;
}
/*
	cosem->hdlc.format = p[0] >> 4;
	cosem->hdlc.len = p[0] & 0xf | p[1];
	&cosem->hdlc.src = p[2];
	p += 3;
	p += getaddr(p, &cosem->hdlc.dst);
	cosem->hdlc.control = *p++;
	cosem->hdlc.hcs = *p++ << 8 | *p++;
	cosem->hdlc.ssap = *p++;
	cosem->hdlc.dsap = *p++;
	cosem->hdlc.llcq = *p++;

	cosem->apdu.tag = *p++;
	cosem->apdu.id_and_priority = pull32(&p);
	cosem->apdu.timelen = *p++;

	// a 09 type byte is sometimes included - the timelen must be either 12 or 0
	if (cosem->apdu.timelen == 0x09)
		cosem->apdu.timelen = *p++;

	if (cosem->apdu.timelen) {
		cosem->apdu.time.year = pull16(&p);
		cosem->apdu.time.month = pull8(&p);
		cosem->apdu.time.day = pull8(&p);
		cosem->apdu.time.weekday = pull8(&p);
		cosem->apdu.time.hour = pull8(&p);
		cosem->apdu.time.min = pull8(&p);
		cosem->apdu.time.sec = pull8(&p);
		cosem->apdu.time.unknown = pull8(&p);
		cosem->apdu.time.deviation = pull8(&p);
		cosem->apdu.time.status = pull16(&p);
	}

	// FIXME: OBIS decoding (variable format!) 
}
*/

/* read states:
 - framelen < 0:  look for CONTROL
 - framelen = 0:  look for first two bytes different from CONTROL
 - framelen > 0:  wait for complete frame
 - repeat
*/

static int hdlc_length(unsigned char *buf)
{
	if ((buf[1] & 0xf8) != 0xa0) {  // HDLC type 3 without segmentation
		fprintf(stderr, "wrong frame type: %0#4x\n", buf[1] >> 4);
		return -1;
	}

	return (buf[1] << 8 | buf[2]) & 0x7ff;
}

static unsigned char *hdlc_start(unsigned char *buf, size_t buflen)
{
	unsigned char *p = memchr(buf, CONTROL, buflen);

	/* skip remaining CONTROL chars */
	while (p && p - buf + 1 < buflen && p[1] == CONTROL)
		p++;
	return p;
}

static const char *hdlc_control(unsigned char control)
{
	static char buf[] = "I-frame (rsn=X, ssn=X)";

	if (control & 3) { /* U-frame */
		switch (control & 0xec) {
		case 0:
			return "UI-frame";
		default:
			return "unknown U-frame";
		}
	} else if (control & 1) { /* S-frame */
		switch (control & 0x0c) {
		default:
			sprintf(buf, "S-frame (rsn=%d)", control >> 5);
		}
	} else { /* I-frame */
		sprintf(buf, "I-frame (rsn=%d, ssn=%d)", control >> 5, (control >> 1) & 7);
	}
	return buf;
}

static unsigned char *hdlc_verify(unsigned char *buf, size_t buflen)
{
	int i, j, format, segmentation, length, src, dst, control, hcs, fcs, check;

	/* check start and stop markers */
	if (buf[0] != CONTROL || buf[buflen -1] != CONTROL)
		return NULL;

	/* verify header */
	format = buf[1] >> 4;
	if (format != 0xa) {
		fprintf(stderr, "DLMS/COSEM requires HDLC frame format \"type 3\" - %#04x is invalid\n", format);
		return NULL;
	}

	segmentation = !!(buf[1] & 0x08);
	if (segmentation) {
		fprintf(stderr, "Segmentation is not supported\n");
		return NULL;
	}

	length = hdlc_length(buf);
	if (length != buflen - 2) {
		fprintf(stderr, "Invalid HDLC frame length: %d != %zd\n", length, buflen - 2);
		return NULL;
	}

	/* src address is always 1 byte */
	src = buf[3];

	/* dst address is 1, 2 or 4 bytes */
	dst = 0;
	for (i = 0; i < 4; i++) {
		dst <<= 8;
		dst |= buf[4 + i];
		if (dst & 1)
			break;
	}

	if (i == 3) {
		fprintf(stderr, "Bogus HDLC destination address - 3 bytes?: %02x %02x %02x\n", buf[4], buf[5], buf[6]);
		return NULL;
	}

	control = buf[5 + i];
	hcs = buf[7 + i] << 8 | buf[6 + i];
	check = crc16((char *)(buf + 1), 5 + i);
	if (hcs != check) {
		fprintf(stderr, "Bogus HDLC header checksum: %#06x != %#06x\n", hcs, check);
		return NULL;
	}

	/* This will be the HCS, and therefore redundant, in case of an empty payload */
	fcs = buf[buflen-2] << 8 | buf[buflen-3];
	check = crc16((char *)(buf + 1), buflen - 4);
	if (fcs != check) {
		fprintf(stderr, "Bogus HDLC frame checksum: %#06x != %#06x\n", fcs, check);
		return NULL;
	}

	/* debug dump */
	fprintf(stderr, "Valid HDLC frame from %d to %d with control=%#04x (%s)\n", src, dst, control, hdlc_control(control) );

	/* dump HDLC header */
	printf("%02x %04x %02x ", CONTROL, 0xa000 | length, src);
	for (j = i; j >= 0; j--)
		printf("%02x", (dst >> (8 * j)) & 0xff);
	printf(" %02x %04x", control, htobe16(hcs));

	/* payload starts after header */
	return &buf[8 + i];
}

/* ref DLMS Blue-Book-Ed-122-Excerpt.pdf section 4.1.6.1 "Date and time formats" */ 
static unsigned char *decode_datetime(unsigned char *buf, struct tm *t)
{
	int deviation, status;

	memset(t, 0, sizeof(*t));
	switch (buf[0]) {
	case 0:
		return buf + 1;
	case 12:
		t->tm_year = (buf[1] << 8 | buf[2]) - 1900; /* POSIX is year - 1900 */
		t->tm_mon = buf[3] - 1; /* COSEM is 1...12, POSIX is 0..11 */
		t->tm_mday = buf[4];
		t->tm_wday = !buf[5] ? 7 : buf[5];  /* COSEM is 1...7, 1 is Monday, POSIX is 0..6, 0 is Sunday */
		t->tm_hour = buf[6];
		t->tm_min = buf[7];
		t->tm_sec = buf[8];
//		hundredths = buf[9];
		deviation = buf[10] << 8 | buf[11]; /* range -720...+720 in minutes of local time to UTC */
		if (deviation & 0x8000)
			fprintf(stderr, "deviation is unspecified\n");
		else
			fprintf(stderr, "deviation is %04d\n", deviation);
		status = buf[12];
		fprintf(stderr, "status is");
		if (status == 0xff)
			fprintf(stderr, " not specified\n");
		else {
			if (status & 0x01)
				fprintf(stderr, " invalid");
			if (status & 0x02)
				fprintf(stderr, " doubtful");
			if (status & 0x04)
				fprintf(stderr, " different");
			if (status & 0x08)
				fprintf(stderr, " invalid clock");
			if (status & 0x80)
				fprintf(stderr, " daylight saving");
			if (!status)
				fprintf(stderr, " OK");
			fprintf(stderr, "\n");
		}
		return buf + 13;
	default:
		fprintf(stderr, "Bogus date-time: %d is not a valid length\n", buf[0]);
		return NULL;
	}
}

static int parse_cosem(unsigned char *buf, size_t buflen, int lvl, json_object **ret)
{
	int i, len = 1;
	unsigned int val;
	unsigned long longval;
	unsigned long long longlongval;
	json_object *myobj;

	*ret = NULL;
	for (i = 0; i < lvl; i++)
		printf("  ");

	/* ref DLMS Blue-Book-Ed-122-Excerpt.pdf section 4.1.5 "Common data types" */
	switch (buf[0]) {
	case 0: // null-data
		break;
	case 1: // array
	case 2: // structure
		*ret = json_object_new_array();
		printf("%02x%02x\n", buf[0], buf[1]);
		fprintf(stderr, "Parsing %s with %d elements\n", buf[0] == 1 ? "array" : "struct", buf[1]);
		len += 1;
		for (i = 0; i < buf[1]; i++) {
			len += parse_cosem(&buf[len], buflen - len, lvl + 1, &myobj);
			if (myobj)
				json_object_array_add(*ret, myobj);
		}
		break;
	case 5: // double-long
		longval = buf[1] << 24 | buf[2] << 16 | buf[3] << 8 | buf[4];
		*ret = json_object_new_int(longval);
		fprintf(stderr, " (%ld)\n", longval);
		printf("%02x %08x\n", buf[0], htobe32(longval));
		len += 4;
		break;
	case 6: // double-long-unsigned
		longval = buf[1] << 24 | buf[2] << 16 | buf[3] << 8 | buf[4];
		*ret = json_object_new_int(longval);
		fprintf(stderr, "double-long-unsigned (%lu)\n", longval);
		printf("%02x %08x\n", buf[0], htobe32(longval));
		len += 4;
		break;
	case 9: // octet-string
		fprintf(stderr, "octet-string (%d bytes):", buf[1]);
		for (i = 0; i < buf[1]; i++)
			fprintf(stderr, " %02x", buf[i + 2]);
		fprintf(stderr, "\n");
		printf("%02x%02x ", buf[0], buf[1]);
		for (i = 0; i < buf[1]; i++)
			printf("%02x", buf[i + 2]);
		printf("\n");
		len += 1 + buf[1];
		break;
	case 10: // visible-string
		fprintf(stderr, "visible-string (%d bytes): ", buf[1]);
		for (i = 0; i < buf[1]; i++)
			fprintf(stderr, "%c", buf[i + 2]);
		fprintf(stderr, "\n");
		printf("%02x%02x ", buf[0], buf[1]);
		for (i = 0; i < buf[1]; i++)
			printf("%02x", buf[i + 2]);
		printf("\n");
		len += 1 + buf[1];
		break;
	case 15: // integer
		*ret = json_object_new_int(buf[1]);
		fprintf(stderr, "integer (%d)\n", (char)buf[1]);
		printf("%02x %02x\n", buf[0], buf[1]);
		len += 1;
		break;
	case 16: // long
		val = buf[1] << 8 | buf[2];
		*ret = json_object_new_int(val);
		fprintf(stderr, "long (%d)\n", (int)val);
		printf("%02x %04x\n", buf[0], htobe16(val));
		len += 2;
		break;
	case 17: // unsigned
		*ret = json_object_new_int(buf[1]);
		fprintf(stderr, "unsigned (%d)\n", buf[1]);
		printf("%02x %02x\n", buf[0], buf[1]);
		len += 1;
		break;
	case 18: // long-unsigned
		val = buf[1] << 8 | buf[2];
		*ret = json_object_new_int(val);
		fprintf(stderr, "long-unsigned (%u)\n", val);
		printf("%02x %04x\n", buf[0], htobe16(val));
		len += 2;
		break;
/*	case 20: // long64
		longlongval = buf[1] << 56 | buf[2] << 48 | buf[3] << 40 | buf[4] << 32 | buf[5] << 24 | buf[6] << 16 | buf[7] << 8 | buf[8];
		fprintf(stderr, "long64 (%lld)\n", longlongval);
		len += 8;
		break;
	case 21: // long64-unsigned
		val = buf[1] << 8 | buf[2];
		longlongval = buf[1] << 56 | buf[2] << 48 | buf[3] << 40 | buf[4] << 32 | buf[5] << 24 | buf[6] << 16 | buf[7] << 8 | buf[8];
		fprintf(stderr, "long64-unsigned (%llu)\n", longlongval);
		len += 8;
		break;
*/
	case 22: // enum
		*ret = json_object_new_int(buf[1]);
		fprintf(stderr, "enum (%u)\n", buf[1]);
		printf("%02x %02x\n", buf[0], buf[1]);
		len += 1;
		break;
	case 25: // date-time
		printf("%02x ", buf[0]);
		for (i = 1; i <= 12; i++)
			printf("%02x", buf[i]);
		printf("\n");
		len += 12;
		break;
	default:
		fprintf(stderr, "Unsupported COSEM data type: %d (%02x)\n", buf[0], buf[0]);
	}
	if (len > buflen)
		fprintf(stderr, "Buggy COSEM data - buffer too short: %zd < %d\n", buflen, len);
	return len;
}

static int parse_payload(unsigned char *buf, size_t buflen)
{
	unsigned long invokeid;
	unsigned char *p;
	struct tm datetime;
	int i;
	json_object *myobj;

	print_packet("*** payload dump: ***\n", buf, buflen);

	/* 
	 * the first 3 bytes of the payload is LLC:
	 *   LSAP | DSAP | Quality, fixed to e6 e7 00
	 */
	if (buf[0] != 0xe6 || buf[1] != 0xe7 || buf[2] != 0x00) {
		fprintf(stderr, "Invalid LLC header: %02x %02x %02x\n", buf[0], buf[1], buf[2]);
		return -1;
	}

	printf(" %02x%02x%02x\n", buf[0], buf[1], buf[2]);
	/*
	 * then follows a xDLMS APDU with:

	 0F            data-notification [15]
	 00 00 00 00   long-invoke-id-and-priority
	 0C 07 E1 0A 14 05 03 3A 1E FF 80 00 00  date-time
	 02 19 ....    notification-body

	*/

	if (buf[3] != 0x0f) {
		fprintf(stderr, "xDLMS APDU must be 'data-notification' [15], not [%d], according to IEC 62056-7-5:2017\n", buf[3]);
		return -1;
	}

	invokeid = buf[4] << 24 | buf[5] << 16 | buf[6] << 8 | buf[7];
	p = &buf[8];
	fprintf(stderr, "long-invoke-id-and-priority: %#010lx\n", invokeid);

	printf("%02x %08x", buf[3], htobe32(invokeid));
	/* some buggy firmwares includes a 0x09 type byte before the date-time - skip it */
	if (p[0] == 0x09)
		p++;

	printf(" %02x ", p[0]);
	for (i = 0; i < p[0]; i++)
		printf("%02x", p[i + 1]);
	printf("\n");

	p = decode_datetime(p, &datetime);
	if (!p)
		return -1;

	fprintf(stderr, "date-time: %s\n", asctime(&datetime));

	parse_cosem(p, buflen + buf - p, 0, &myobj);

	fprintf(stderr, "JSON: %s\n", json_object_get_string(myobj));
	return 0;
}

static int read_and_parse(int fd)
{
	unsigned char *payload, *cur, *hdlc, rbuf[512];
	struct pollfd fds[1];
	int ret, rlen, framelen = -1;

	fds[0].fd = fd;
	fds[0].events = POLLIN;
	cur = rbuf;

	while (1) {
		ret = poll(fds, 1, -1);
		if (ret == -1)
			return -errno;

		if (fds[0].revents & POLLIN)
			rlen = read(fd, cur, sizeof(rbuf) + rbuf - cur);
		else
			rlen = 0;

		if (rlen <= 0)
			return rlen;

		cur += rlen;

nextframe:
		/* looking for a new frame? */
		if (framelen < 0) {
			hdlc = hdlc_start(rbuf, cur - rbuf);

			/* need enough to verify type and length */
			if (!hdlc || cur  - hdlc < 3)
				continue;

			framelen = hdlc_length(hdlc);

			/* drop if invalid */
			if (framelen < 0)
				cur = rbuf;

			/* realign if exceeding buf size */
			if (hdlc + framelen + 2 > rbuf + sizeof(rbuf)) {
				if (framelen + 2 > sizeof(rbuf)) {
					fprintf(stderr, "frame too big: %d > %zd - dropping\n", framelen, sizeof(rbuf) - 2);
					framelen = -1;
					cur = rbuf;
				} else { // realign to start of buffer
					fprintf(stderr, "moving frame to start of buffer to make it fit\n");
					memmove(rbuf, hdlc, cur - hdlc);
					cur -= hdlc - rbuf;
					hdlc = rbuf;
				}
			}
		}

		/* waiting to complete a frame? */
		if (framelen > 0 && (cur - hdlc) < (framelen + 2))
			continue;

		// verify frame
		payload = hdlc_verify(hdlc, framelen + 2);
		if (!payload) {
			print_packet("*** dropping bogus frame: ***\n", hdlc, framelen + 2);

			/* only skip the first CONTROL char in case the real frame starts inside the bogus one */
			memmove(rbuf, hdlc + 1, cur - hdlc - 1);
			cur -= hdlc - rbuf + 1;
			framelen = -1;
			goto nextframe;
		}

		// got a complete frame
		print_packet("*** frame dump: ***\n", hdlc, framelen + 2);
		parse_payload(payload, framelen - (payload - hdlc + 1));

		// print HDLC trailer
		printf("%02x%02x %02x\n\n", hdlc[framelen - 1], hdlc[framelen], CONTROL);

		// keep remaining data for next frame, including the stop marker in case it doubles as start of next frame
		memmove(rbuf, hdlc + framelen + 1, cur - rbuf - framelen + 1);
		cur -= hdlc - rbuf + framelen + 1;
		framelen = -1;
		goto nextframe;
	}
	return 0;
}

static struct option main_options[] = {
	{ "help",	0, 0, 'h' },
	{ "serial",     1, 0, 's' },
	{ "debug",      0, 0, 'd' },
	{ 0, 0, 0, 0 }
};

static void usage(const char *prog)
{
	fprintf(stderr,
		"\n%s: %s [--help] [--debug] --serial <device>\n",
		__func__, prog);
}

int main(int argc, char *argv[])
{
	int opt, serfd = -1, ret = 0;

	fprintf(stderr, "%s\n", DESCRIPTION);
	while ((opt = getopt_long(argc, argv, "s:n:dh", main_options, NULL)) != -1) {
		switch(opt) {
		case 's':
			serfd = !optarg[1] && optarg[0] == '-' ? STDIN_FILENO : serial_open(optarg);
			break;
		case 'd':
			debug = true;
			break;
		case 'h':
			usage(argv[0]);
			return 0;
		}
	}

	buf = malloc(BUFSIZE);
	if (!buf) {
		ret = -ENOMEM;
		goto err;
	}

	read_and_parse(serfd);

err:
	if (serfd > 0 && serfd != STDIN_FILENO)
		close(serfd);
	free(buf);
	return ret;
}
