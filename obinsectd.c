/*
 * SPDX-License-Identifier: GPL-2.0
 *
 * A simple OBIS COSEM to MQTT proxy
 *
 * Copyright 2019 Bjørn Mork <bjorn@mork.no>
 *
 * crc16 and hdlc parts were taken from modemmanager, having:
 *   Copyright (C) 2010 Red Hat, Inc.
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
#include <limits.h>
#include <mosquitto.h>
#include <poll.h>
#include <signal.h>
#include <stdbool.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>    
#include <string.h>
#include <sys/time.h>
#include <sys/types.h>
#include <termios.h>
#include <time.h>
#include <unistd.h>

#ifndef VERSION
  #define VERSION "unknown"
#endif

#define CONFIG_FILE	"/etc/obinsect.conf"
#define MQTT_BROKER	"localhost"
#define MQTT_PORT	1883
#define MQTTS_PORT	8883
#define MQTT_KEEPALIVE	60

/* support config refresh */
static bool reread_cfg = false;
static char *cfgfile = CONFIG_FILE;

/* static metadata */
static char *progname = NULL;
static char hostname[64];
static char *serialport = NULL;

/* parsed configuration */
static json_object *cfg = NULL;

/* current OBIS list */
static json_object *current_list = NULL;

/* OBIS code lookup table - ListId must be first entry, and is fixed */
#define MAX_LIST_LENGTH 32
static const char *obiscode[MAX_LIST_LENGTH] = { "1-1:0.2.129.255" , NULL };

/* a few global settings - OK, this is a mess :-) */
static bool unscaled = false;
static bool units = false;

/* defines size of both the read buffer and the print buffer.- because I'm lazy... */
#define BUFSIZE (1024 * 2)

/* shared buffers */
static unsigned char *buf = NULL;
static char *printbuffer = NULL;

/* mosquitto client */
struct mosquitto *mosq = NULL;

/* logging stuff */
static bool debug = false;

#define _logit_nomqtt(lvl, format, arg...)				\
	do {								\
	switch (lvl) {							\
	case LOG_DEBUG:							\
		if (debug)					       	\
			fprintf(stderr, "DEBUG: " format, arg);		\
		break;							\
	case LOG_INFO:							\
       		fprintf(stderr, "INFO: " format, arg);			\
		break;						       	\
	case LOG_ERROR:							\
       		fprintf(stderr, "ERROR: " format, arg);			\
	}								\
	} while (0)

#define _logit(lvl, format, arg...)					\
	do {								\
	if (!printbuffer) {						\
		_logit_nomqtt(lvl, format, arg);			\
	} else {							\
		size_t __len;						\
		__len = snprintf(printbuffer, BUFSIZE, format, arg);	\
		if (__len >= BUFSIZE)					\
			__len = BUFSIZE - 1;				\
		_logit_nomqtt(lvl, "%s", printbuffer);			\
		while (__len > 1 && printbuffer[__len - 1] == '\n')	\
			__len--;					\
		log2mqtt(lvl, printbuffer, __len);			\
	}								\
	} while (0)

#define debug(format, arg...)	_logit(LOG_DEBUG, format, arg)
#define err(format, arg...)	_logit(LOG_ERROR, format, arg)
#define info(format, arg...)	_logit(LOG_INFO, format, arg)
#define debug_nomqtt(format, arg...)	_logit_nomqtt(LOG_DEBUG, format, arg)
#define err_nomqtt(format, arg...)	_logit_nomqtt(LOG_ERROR, format, arg)
#define info_nomqtt(format, arg...)	_logit_nomqtt(LOG_INFO, format, arg)

enum log_levels {
	LOG_DEBUG = 0,
	LOG_INFO,
	LOG_ERROR,
	_MAX_LOG_LEVELS
};

#define MAX_LOG_TOPICS 4

static const char *log_topic[_MAX_LOG_LEVELS][MAX_LOG_TOPICS] = {};

static void mqtt_publish(const char *topic, const char *msg, size_t msglen)
{
	static int reconnect = 1;
	static int mid = 1;

	if (mosquitto_publish(mosq, &mid, topic, msglen, msg, 0, false)) { /* QoS = 0 */
		info_nomqtt("mqtt broker went away -reconnecting (%d)\n", reconnect++);
		mosquitto_reconnect(mosq);
	}
}

static int log2mqtt(int lvl, const char *msg, size_t msglen)
{
	int i;

	for (i = 0; i < MAX_LOG_TOPICS && log_topic[lvl][i]; i++)
		mqtt_publish(log_topic[lvl][i], msg, msglen);
	return i;
}

static void libmosquitto_log_callback(struct mosquitto *mosq, void *userdata, int level, const char *str)
{
	switch (level) {
	case MOSQ_LOG_ERR:
		err_nomqtt("mqtt: %s\n", str);
		break;
	case MOSQ_LOG_DEBUG:
		debug_nomqtt("mqtt: %s\n", str);
		break;
	default:
		info_nomqtt("mqtt: %s\n", str);
	}
}

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

static int serial_open(const char *dev)
{
	struct termios terminal_data;
	int fd;

	fd = open(dev,  O_RDONLY | O_NOCTTY);
	if (fd > 0) {
		tcgetattr(fd, &terminal_data);
		cfmakeraw(&terminal_data);
	        cfsetspeed(&terminal_data, B2400);  // 2400 8N1
		tcsetattr(fd, TCSANOW, &terminal_data);
	}

	/* MQTT session is not yet ready */
	debug_nomqtt("opened %s\n", dev);
	return fd;
}


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

/* read states:
 - framelen < 0:  look for CONTROL
 - framelen = 0:  look for first two bytes different from CONTROL
 - framelen > 0:  wait for complete frame
 - repeat
*/

static int hdlc_length(unsigned char *buf)
{
	if ((buf[1] & 0xf8) != 0xa0) {  // HDLC type 3 without segmentation
		debug("unsupported frame type/len: %02x%02x\n", buf[1], buf[2]);
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

/* verifies the HDLC header and checksums, and returns the offset to the payload
   side-effect on success:
     creates a new top level json object to hold the parsed frame, adding a hdlc
     struct with format, segmentation, length, src, dst, control, hcs and fcs fields
*/
static unsigned char *hdlc_verify(unsigned char *buf, size_t buflen, json_object *json)
{
	int dstlen, format, segmentation, length, src, dst, control, hcs, fcs, check;
	json_object *tmp;

	/* already failed length check */
	if (buflen < 2)
		return NULL;

	/* check start and stop markers */
	if (buf[0] != CONTROL || buf[buflen -1] != CONTROL) {
		err("HDLC frame is missing start or stop markers (%#04x)\n", CONTROL);
		return NULL;
	}

	/* verify header */
	format = buf[1] >> 4;
	if (format != 0xa) {
		err("DLMS/COSEM requires HDLC frame format \"type 3\" - %#04x is invalid\n", format);
		return NULL;
	}

	segmentation = !!(buf[1] & 0x08);
	if (segmentation) {
		err("HDLC segmentation is unsupported (%02x%02x)\n", buf[1], buf[2]);
		return NULL;
	}

	length = hdlc_length(buf);
	if (length != buflen - 2) {
		err("Invalid HDLC frame length: %d != %zd\n", length, buflen - 2);
		return NULL;
	}

	/* src address is always 1 byte */
	src = buf[3];

	/* dst address is 1, 2 or 4 bytes */
	dst = 0;
	for (dstlen = 0; dstlen < 4; dstlen++) {
		dst <<= 8;
		dst |= buf[4 + dstlen];
		if (dst & 1)
			break;
	}

	if (dstlen == 3) {
		err("Bogus HDLC destination address - 3 bytes?: %02x %02x %02x\n", buf[4], buf[5], buf[6]);
		return NULL;
	}

	control = buf[5 + dstlen];

	hcs = buf[7 + dstlen] << 8 | buf[6 + dstlen];
	check = crc16((char *)(buf + 1), 5 + dstlen);
	if (hcs != check) {
		err("HDLC header checksum: %#06x != %#06x\n", hcs, check);
		return NULL;
	}

	/* This will be the HCS, and therefore redundant, in case of an empty payload */
	fcs = buf[buflen-2] << 8 | buf[buflen-3];
	check = crc16((char *)(buf + 1), buflen - 4);
	if (fcs != check) {
		err("HDLC frame checksum: %#06x != %#06x\n", fcs, check);
		return NULL;
	}

	/* create JSON frame object with HDLC data */
	tmp = json_object_new_object();
	json_object_object_add(tmp, "format",  json_object_new_int(format));
	json_object_object_add(tmp, "segmentation", json_object_new_boolean(segmentation));
	json_object_object_add(tmp, "length", json_object_new_int(length));
	json_object_object_add(tmp, "src", json_object_new_int(src));
	json_object_object_add(tmp, "dst", json_object_new_int(dst));
	json_object_object_add(tmp, "control", json_object_new_int(control));
	json_object_object_add(tmp, "hcs", json_object_new_int(hcs));
	json_object_object_add(tmp, "fcs", json_object_new_int(fcs));

	json_object_object_add(json, "hdlc", tmp);

	/* return offset to payload */
	return &buf[8 + dstlen];
}

/*
 * ref DLMS Blue-Book-Ed-122-Excerpt.pdf section 4.1.6.1 "Date and time formats"
 * Ignoring hundredths, deviation and status which are unspecified in all samples I've seen
 */
static time_t decode_datetime(unsigned char *buf)
{
	struct tm t = {};

	t.tm_year = (buf[0] << 8 | buf[1]) - 1900; /* POSIX is year - 1900 */
	t.tm_mon = buf[2] - 1; /* COSEM is 1...12, POSIX is 0..11 */
	t.tm_mday = buf[3];
	t.tm_wday = !buf[4] ? 7 : buf[4];  /* COSEM is 1...7, 1 is Monday, POSIX is 0..6, 0 is Sunday */
	t.tm_hour = buf[5];
	t.tm_min = buf[6];
	t.tm_sec = buf[7];
	if (buf[8] != 0xff)
		debug("hundredths are valid: %02x\n", buf[8]);
	if (buf[9] != 0x80 || buf[10])
		debug("deviation is valid: %02x %02x\n", buf[9], buf[10]);
	if (buf[11] && buf[11] != 0xff)
		debug("staus is valid and set: %02x\n", buf[12]);
	return mktime(&t);
}

/*
 * Ref BS EN 62056-6-2:2013, page 30, "Table 3 - Enumerated values for physical units".
 * according to "Hårek"
 */
static char *cosem_unit_enum(unsigned char e)
{
	static char ret[17]; /* "unknown-unit-255" is 16 bytes */
	switch (e) {
	case 27: return "W";
	case 28: return "VA";
	case 29: return "VAr";
	case 30: return "Wh"; // guessed based on received values
	case 32: return "VArh"; // guessed based on received values
	case 33: return "A";
	case 35: return "V";
	default:
		sprintf(ret, "unknown-unit-%u", e);
		return ret;
	}
}

static char *cosem_typestr(unsigned char type)
{
	/* ref DLMS Blue-Book-Ed-122-Excerpt.pdf section 4.1.5 "Common data types" */
	switch (type) {
	case 0: return "null-data";
	case 1: return "array";
	case 2: return "structure";
	case 3: return "boolean";
	case 4: return "bit-string";
	case 5: return "double-long";
	case 6: return "double-long-unsigned";
     /* case 7: return “floating-point”; // not usable in DLMS/COSEM. */
	case 9: return "octet-string";
	case 10: return "visible-string";
     /* case 11: return “time”; // not usable in DLMS/COSEM. */
	case 12: return "utf8-string";
	case 13: return "bcd";
	case 15: return "integer";
	case 16: return "long";
	case 17: return "unsigned";
	case 18: return "long-unsigned";
	case 20: return "long64";
	case 21: return "long64-unsigned";
	case 22: return "enum";
	case 23: return "float32";
	case 24: return "float64";
	case 25: return "date-time";
	case 26: return "date";
	case 27: return "time";
	default: return "unknown";
	}
}

// guessing OBIS code if length is 6 and value is a.b.x.x.x.255 where a and b are 0 or 1
static bool is_obis(unsigned char *code)
{
	if (code[0] != 9 || code[1] != 6 || code[2] > 1 || code[3] > 1 || code[7] != 255)
		return false;
	return true;
}

static json_object *cosem_object_new_int(unsigned char *raw, size_t intlen, bool sign)
{
	__uint32_t hi, lo;

	switch (intlen) {
	case 1:
		if (sign)
			lo = (char)raw[1];
		else
			lo = raw[1];
		break;
	case 2:
		if (sign)
			lo = (__int16_t)(raw[1] << 8 | raw[2]);
		else
			lo = raw[1] << 8 | raw[2];
		break;
	case 4:
		if (sign)
			lo = (__int32_t)(raw[1] << 24 | raw[2] << 16 | raw[3] << 8 | raw[4]);
		else
			lo = raw[1] << 24 | raw[2] << 16 | raw[3] << 8 | raw[4];
		break;
	case 8:
		hi = raw[1] << 24 | raw[2] << 16 | raw[3] << 8 | raw[4];
		lo = raw[5] << 24 | raw[6] << 16 | raw[7] << 8 | raw[8];
		return json_object_new_int64(sign ? (__int64_t)hi << 32 | lo : (__uint64_t)hi << 32 | lo);
	}
	return json_object_new_int(lo);
}

/*
 * parse_cosem() returns the number of eaten bytes, as well as a JSON
 * object in ret.
 *
 * type defs: DLMS Blue-Book-Ed-122-Excerpt.pdf section 4.1.5 "Common data types"
 */
static int parse_cosem(unsigned char *buf, size_t buflen, int lvl, json_object **ret)
{
	int i, n, len = 0;
	json_object *myobj;
	char fieldname[32]; /* "double-long-unsigned" is 20 bytes */

	*ret = NULL;

	switch (buf[0]) {
	case 0: // null-data
		len = 1;
		*ret = NULL;
		break;
	case 1: // array
		*ret = json_object_new_array();
		len = 2;
		for (i = 0; i < buf[1]; i++) {
			n = parse_cosem(&buf[len], buflen - len, lvl + 1, &myobj);
			if (n < 0 || len > buflen)
				goto err;
			json_object_array_add(*ret, myobj);
			len += n;
		}
		break;
	case 2: // structure
		*ret = json_object_new_object();
		len = 2;
		for (i = 0; i < buf[1]; i++) {
			n = parse_cosem(&buf[len], buflen - len, lvl + 1, &myobj);
			if (n < 0 || len > buflen)
				goto err;
			sprintf(fieldname, "%s-%u", is_obis(&buf[len]) ? "obis" : cosem_typestr(buf[len]), i);
			json_object_object_add(*ret, fieldname, myobj);
			len += n;
		}
		break;
	case 5: // double-long
		len = 4;
 		*ret = cosem_object_new_int(buf, len++, true);
		break;
	case 6: // double-long-unsigned
		len = 4;
 		*ret = cosem_object_new_int(buf, len++, false);
		break;
	case 9: // octet-string
		/* is this an OBIS code? - special handling make them a bit more readable */
		if (is_obis(buf)) {
			sprintf(fieldname, "%u-%u:%u.%u.%u.%u", buf[2], buf[3], buf[4], buf[5], buf[6], buf[7]);
			*ret = json_object_new_string(fieldname);
		}
		/* fall through */
	case 10: // visible-string
	case 12: // utf8-string
		len = 2 + buf[1];
		if (!*ret)
			*ret = json_object_new_string_len((char *)&buf[2], buf[1]);
		break;
	case 15: // integer
		len = 1;
 		*ret = cosem_object_new_int(buf, len++ , true);
		break;
	case 16: // long
		len = 2;
 		*ret = cosem_object_new_int(buf, len++, true);
		break;
	case 17: // unsigned
		len = 1;
 		*ret = cosem_object_new_int(buf, len++, false);
		break;
	case 18: // long-unsigned
		len = 2;
 		*ret = cosem_object_new_int(buf, len++, false);
		break;
	case 20: // long64
		len = 8;
 		*ret = cosem_object_new_int(buf, len++, true);
		break;
	case 21: // long64-unsigned
		len = 8;
 		*ret = cosem_object_new_int(buf, len++, false);
		break;
	case 22: // enum
		len = 2;
 		*ret = json_object_new_string(cosem_unit_enum(buf[1]));
		break;
	case 25: // date-time
		len = 1 + 12;
		*ret = json_object_new_int(decode_datetime(&buf[1]));
 		break;
	default:
		err("Unsupported COSEM data type: %d (%02x)\n", buf[0], buf[0]);
	}
	if (len > buflen) {
		err("Buggy COSEM data - buffer too short: %zd < %d\n", buflen, len);
		goto err;
	}
	if (*ret)
		return len;

err:
	if (*ret)
		json_object_put(*ret);
	*ret = NULL;
	return -1;
}



/*
 * the first 3 bytes of the payload is LLC:
 *   LSAP | DSAP | Quality, fixed to e6 e7 00
 */

static json_object *parse_llc(unsigned char *buf, size_t buflen)
{
	json_object *tmp;

	if (buf[0] != 0xe6 || buf[1] != 0xe7 || buf[2] != 0x00) {
		err("Invalid LLC header: %02x %02x %02x\n", buf[0], buf[1], buf[2]);
		return NULL;
	}

	tmp = json_object_new_object();
	json_object_object_add(tmp, "lsap",  json_object_new_int(0xe6));
	json_object_object_add(tmp, "dsap",  json_object_new_int(0xe7));
	json_object_object_add(tmp, "quality",  json_object_new_int(0x00));

	return tmp;
}

static bool parse_payload(unsigned char *buf, size_t buflen, json_object *json)
{
	unsigned long invokeid;
	unsigned char *p;
	time_t t = 0;
	json_object *tmp, *body;
	bool datetime_bug = false;

	/* header parsing failed? */
	if (!buf)
		return false;

	/* add LLC field */
	json_object_object_add(json, "llc", parse_llc(buf, buflen));

	/* a xDLMS APDU should follow immediately after the LLC

	 0F            data-notification [15]
	 00 00 00 00   long-invoke-id-and-priority
	 0C 07 E1 0A 14 05 03 3A 1E FF 80 00 00  date-time
	 02 19 ....    notification-body

	*/

	p = &buf[3];
	if (p[0] != 0x0f) {
		err("The xDLMS APDU must be 'data-notification' [15], not [%d], according to IEC 62056-7-5:2017\n", p[0]);
		return false;
	}
	p++;

	/* don't bother decoding the individual bits of long-invoke-id-and-priority */
	invokeid = p[0] << 24 | p[1] << 16 | p[2] << 8 | p[3];
	p += 4;

	/* some buggy firmwares includes a 0x09 type byte before the date-time - skip it */
	if (p[0] == 0x09) {
		datetime_bug = true;
		p++;
	}

	/* The date-time field should by exactly 0 or 12 bytes, */
	if (p[0] == 12)
		t = decode_datetime(&p[1]);
	p += p[0] + 1;


	/* add a data-notification field to the JSON frame */
	tmp = json_object_new_object();
	json_object_object_add(json, "data-notification",  tmp);
	json_object_object_add(tmp, "long-invoke-id-and-priority", json_object_new_int(invokeid));
	if (datetime_bug)
		json_object_object_add(tmp, "date-time-bug", json_object_new_boolean(true));
	if (t)
		json_object_object_add(tmp, "date-time", json_object_new_int(t));

	/* The remaining payload is the notification-body - parse and add to the JSON frame */
	if (parse_cosem(p, buflen + buf - p, 0, &body) > 0)
		json_object_object_add(tmp, "notification-body", body);

	return true;
}


/* take the "raw" JSON data we parsed and convert it to a common vendor-independant struct */

/*
 Samples:

a) Aidon list 2:

  "data-notification":{
    "long-invoke-id-and-priority":1088880655,
    "date-time":0,
    "notification-body":[
      {
        "obis-0":"1-1:0.2.129.255",
        "visible-string-1":"AIDON_V0001"
      },
      {
        "obis-0":"0-0:96.1.0.255",
        "visible-string-1":"7359992890941742"
      },
      {
        "obis-0":"0-0:96.1.7.255",
        "visible-string-1":"6515"
      },
      {
        "obis-0":"1-0:1.7.0.255",
        "double-long-unsigned-1":1362,
        "structure-2":{
          "integer-0":0,
          "enum-1":27
        }
      },
      {
        "obis-0":"1-0:2.7.0.255",
        "double-long-unsigned-1":0,
        "structure-2":{
          "integer-0":0,
          "enum-1":27
        }
      },
      {
        "obis-0":"1-0:3.7.0.255",
        "double-long-unsigned-1":996,
        "structure-2":{
          "integer-0":0,
          "enum-1":29
        }
      },
      {
        "obis-0":"1-0:4.7.0.255",
        "double-long-unsigned-1":0,
        "structure-2":{
          "integer-0":0,
          "enum-1":29
        }
      },
      {
        "obis-0":"1-0:31.7.0.255",
        "long-1":93,
        "structure-2":{
          "integer-0":255,
          "enum-1":33
        }
      },
      {
        "obis-0":"1-0:32.7.0.255",
        "long-unsigned-1":2500,
        "structure-2":{
          "integer-0":255,
          "enum-1":35
        }
      }
    ]
  }

b) Kaifa list 1:

  "data-notification":{
    "long-invoke-id-and-priority":1088880655,
    "date-time-bug":true,
    "date-time":1505254726,
    "notification-body":{
      "double-long-unsigned-0":1333
    }
  }

c) Kaifa list 2:

  "data-notification":{
    "long-invoke-id-and-priority":1088880655,
    "date-time-bug":true,
    "date-time":1505254730,
    "notification-body":{
      "octet-string-0":"KFM_001",
      "octet-string-1":"6970631401753985",
      "octet-string-2":"MA304H3E",
      "double-long-unsigned-3":1316,
      "double-long-unsigned-4":0,
      "double-long-unsigned-5":0,
      "double-long-unsigned-6":129,
      "double-long-unsigned-7":1746,
      "double-long-unsigned-8":4565,
      "double-long-unsigned-9":4712,
      "double-long-unsigned-10":2400,
      "double-long-unsigned-11":0,
      "double-long-unsigned-12":2402
    }
  }

d) Kaifa list 3:

  "data-notification":{
    "long-invoke-id-and-priority":1088880655,
    "date-time-bug":true,
    "date-time":1505415610,
    "notification-body":{
      "octet-string-0":"KFM_001",
      "octet-string-1":"6970631401753985",
      "octet-string-2":"MA304H3E",
      "double-long-unsigned-3":1022,
      "double-long-unsigned-4":0,
      "double-long-unsigned-5":0,
      "double-long-unsigned-6":64,
      "double-long-unsigned-7":1937,
      "double-long-unsigned-8":3229,
      "double-long-unsigned-9":3430,
      "double-long-unsigned-10":2369,
      "double-long-unsigned-11":0,
      "double-long-unsigned-12":2380,
      "octet-string-13":"Thu Sep 14 21:00:10 2017",
      "double-long-unsigned-14":180073,
      "double-long-unsigned-15":0,
      "double-long-unsigned-16":247,
      "double-long-unsigned-17":16380
    }
  }



e) Kamstrup list 1:

  "data-notification":{
    "long-invoke-id-and-priority":15138831,
    "date-time-bug":true,
    "date-time":1508467410,
    "notification-body":{
      "visible-string-0":"Kamstrup_V0001",
      "obis-1":"1-1:0.0.5.255",
      "visible-string-2":"5706567274389702",
      "obis-3":"1-1:96.1.1.255",
      "visible-string-4":"6841121BN243101040",
      "obis-5":"1-1:1.7.0.255",
      "double-long-unsigned-6":1468,
      "obis-7":"1-1:2.7.0.255",
      "double-long-unsigned-8":0,
      "obis-9":"1-1:3.7.0.255",
      "double-long-unsigned-10":0,
      "obis-11":"1-1:4.7.0.255",
      "double-long-unsigned-12":462,
      "obis-13":"1-1:31.7.0.255",
      "double-long-unsigned-14":564,
      "obis-15":"1-1:51.7.0.255",
      "double-long-unsigned-16":202,
      "obis-17":"1-1:71.7.0.255",
      "double-long-unsigned-18":511,
      "obis-19":"1-1:32.7.0.255",
      "long-unsigned-20":232,
      "obis-21":"1-1:52.7.0.255",
      "long-unsigned-22":228,
      "obis-23":"1-1:72.7.0.255",
      "long-unsigned-24":233
    }
  }


f) Kamstrup list 2:

  "data-notification":{
    "long-invoke-id-and-priority":15138831,
    "date-time-bug":true,
    "date-time":1508468405,
    "notification-body":{
      "visible-string-0":"Kamstrup_V0001",
      "obis-1":"1-1:0.0.5.255",
      "visible-string-2":"5706567274389702",
      "obis-3":"1-1:96.1.1.255",
      "visible-string-4":"6841121BN243101040",
      "obis-5":"1-1:1.7.0.255",
      "double-long-unsigned-6":2531,
      "obis-7":"1-1:2.7.0.255",
      "double-long-unsigned-8":0,
      "obis-9":"1-1:3.7.0.255",
      "double-long-unsigned-10":0,
      "obis-11":"1-1:4.7.0.255",
      "double-long-unsigned-12":440,
      "obis-13":"1-1:31.7.0.255",
      "double-long-unsigned-14":996,
      "obis-15":"1-1:51.7.0.255",
      "double-long-unsigned-16":207,
      "obis-17":"1-1:71.7.0.255",
      "double-long-unsigned-18":965,
      "obis-19":"1-1:32.7.0.255",
      "long-unsigned-20":231,
      "obis-21":"1-1:52.7.0.255",
      "long-unsigned-22":226,
      "obis-23":"1-1:72.7.0.255",
      "long-unsigned-24":232,
      "obis-25":"0-1:1.0.0.255",
      "octet-string-26":"Fri Oct 20 05:00:05 2017",
      "obis-27":"1-1:1.8.0.255",
      "double-long-unsigned-28":427244,
      "obis-29":"1-1:2.8.0.255",
      "double-long-unsigned-30":0,
      "obis-31":"1-1:3.8.0.255",
      "double-long-unsigned-32":80,
      "obis-33":"1-1:4.8.0.255",
      "double-long-unsigned-34":61813
    }
  }


 */

static const char *obis_lookup(int idx)
{
	const char *ret = NULL;

	if (idx > 0 && idx < MAX_LIST_LENGTH)
		ret = obiscode[idx - 1];
	if (ret)
		return ret;
	return "unknown";
}

/* some OBIS codes use "wrong" types - fixup for normalization */
static json_object *obis_get_val(const char *key, json_object *val)
{
	/*
	 * Both Aidon and Kaifa use "0-0:1.0.0.255" for the meter time
	 * OBIS code, while Kamstrup use "0-1:1.0.0.255".  Aidon and
	 * Kamstrup get the type wrong, so we have to fixup both cases
	 */
	if (!strcmp(key + 3, ":1.0.0.255") && json_object_get_string_len(val) == 12) // date-time coded as octet-string
		return json_object_new_int(decode_datetime((unsigned char *)json_object_get_string(val)));

	/* default to return val as-is, but with increased ref */
	return json_object_get(val);
}

static const char *get_unit(const char *key)
{
	json_object *unit, *ret;

	if (!units || !current_list)
		return NULL;
	if (!json_object_object_get_ex(current_list, "unit", &unit))
		return NULL;
	if (json_object_object_get_ex(unit, key, &ret))
		return json_object_get_string(ret);
	return NULL;
}

static int get_scaler(const char *key)
{
	json_object *scale, *ret;

	if (unscaled || !current_list)
		return 0;
	if (!json_object_object_get_ex(current_list, "scaler", &scale))
		return 0;
	if (json_object_object_get_ex(scale, key, &ret))
		return json_object_get_int(ret);
	return 0;
}

static const char *get_alias(const char *key)
{
	json_object *alias, *ret;

	if (!current_list)
		return NULL;
	if (!json_object_object_get_ex(current_list, "alias", &alias))
		return NULL;
	if (json_object_object_get_ex(alias, key, &ret))
		return json_object_get_string(ret);
	return NULL;
}

static void add_keyval(json_object *pubcfg, json_object *pub, const char *key, json_object *val, bool createobj)
{
	json_object *tmp, *obj;
	const char *arrayname;
	int i;

	if (!json_object_object_get_ex(pubcfg, key, &tmp))
		return;

	json_object_object_add(pub, key, json_object_get(val));
	for (i = 0; i < json_object_array_length(tmp); i++) {
		arrayname = json_object_get_string(json_object_array_get_idx(tmp, i));

		/* get existing object */
		if (!json_object_object_get_ex(pub, arrayname, &obj)) {
			/* values like "timestamp" will not trigger publication alone */
			if (!createobj) {
				debug("Skipping field '%s' in array '%s' due to non-existing array\n", key, arrayname);
				continue;
			}

			/* publish new array? */
			obj = json_object_new_object();
			json_object_object_add(pub, arrayname, obj);
		}
		json_object_object_add(obj, key, json_object_get(val));

	}
}

static void set_current_list(const char *listname)
{
	json_object *tmp, *list;
	int i = 0;


	/* list config is required.... */
	if (!json_object_object_get_ex(cfg, "obislists", &tmp))
		return;

	/* no need to bother with updates if not found or not changed */
	if (!json_object_object_get_ex(tmp, listname, &list) || list == current_list)
		return;

	/* the "alias" table is required */
	if (!json_object_object_get_ex(list, "alias", &tmp))
		return;

	/* update OBIS lookup table, but leave ListId static */
	json_object_object_foreach(tmp, key, val) {
		debug("adding lookup for '%s' (%s)\n", key, json_object_get_string(val));
		if (!i++)
			continue;
		if (i >= MAX_LIST_LENGTH)
			break;
		obiscode[i] = key;
	}

	/* clear remaining entries */
	for (; i < MAX_LIST_LENGTH; i++)
		obiscode[i] = NULL;

	/* save list pointer */
	current_list = list;
	debug("Current OBIS list set to '%s\n", listname);
}

static json_object *format_value(const char *key, json_object *val)
{
	const char *unit = get_unit(key);
	const int scaler = get_scaler(key);
	double factor;
	char *format;
	int ival, ifactor = 0;

	/* no formatting necessary */
	if (!scaler && !unit)
		return val;

	/* failsafe... */
	if (!json_object_is_type(val, json_type_int))
		return val;

	/* lookup instead of calulate - simpler and faster as long as we only support -3..3 */
	switch (scaler) {
	case -3:
		factor = 0.001;
		format = "%0.3f %s";
		break;
	case -2:
		factor = 0.01;
		format = "%0.2f %s";
		break;
	case -1:
		factor = 0.1;
		format = "%0.1f %s";
		break;
	case 0:
		if (!unit)
			return val;
		ifactor = 1;
		break;
	case 1:
		ifactor = 10;
		break;
	case 2:
		ifactor = 100;
		break;
	case 3:
		ifactor = 1000;
		break;
	default:
		debug("unsupported scaler for %s: %d\n", key, scaler);
		return val;
	}

	ival = json_object_get_int(val);
	if (ifactor) {
		if (!unit)
			return json_object_new_int(ival * ifactor);
		sprintf(printbuffer, "%d %s", ival * ifactor, unit);
	} else {
		if (!unit)
			return json_object_new_double(ival * factor);
		sprintf(printbuffer, format, ival * factor, unit);
	}
	return json_object_new_string(printbuffer);
}

static void add_obis(json_object *pubcfg, json_object *pub, const char *key, json_object *val)
{
	const char *alias = get_alias(key);
	json_object *tmp, *newval = val;

	newval = format_value(key, val);
	if (json_object_object_get_ex(pub, "normal", &tmp))
		json_object_object_add(tmp, key, newval);
	add_keyval(pubcfg, pub, key, newval, true);
	if (alias) {
		if (json_object_object_get_ex(pub, "alias", &tmp))
			json_object_object_add(tmp, alias, newval);
		add_keyval(pubcfg, pub, alias, newval, true);
	}
}

/*
 * post process the parsed packet, converting the data to simple key => value pairs
 */
static json_object *normalize(json_object *pubcfg, json_object *json)
{
	json_object *ret, *tmp, *notification, *body;

	/* 1. figure out list format */

	/* create a new object for the results */
	ret = json_object_new_object();

	if (!json)
		return ret;
	if (!json_object_object_get_ex(json, "data-notification", &notification))
		return ret;
	if (!json_object_object_get_ex(notification, "notification-body", &body))
		return ret;

	/* create a "normal" result list with all OBIS codes as keys? */
	if (json_object_object_get_ex(pubcfg, "normal", NULL))
		add_keyval(pubcfg, ret, "normal", json_object_new_object(), true);

	/* create an "alias" result list with all OBIS aliases as keys? */
	if (json_object_object_get_ex(pubcfg, "alias", NULL))
		add_keyval(pubcfg, ret, "alias", json_object_new_object(), true);

	/* overall formatting differs between the 3:

	   Aidon sends an array of records wrapped in a two- or three-field struct (is this so for list type 1 too?)
              obis code
	      value
	      (when value is number) two-field struct with integer + enum (scale? unit? validity?)

	   Kaifa sends a struct with values only, no codes

	   Kamstrup sends a struct with an odd number of fields, where the first record is the list name and the remaining records are pairs of
              obis code
	      value

	   One starting point could be to flatten or expand all the formats into a common struct of 
	     obis code => value

	   records, where the obis code would be the field name and
	   the value type implicit either integer or string

	   Then we could use external (confihuration file in JSON?) lists of
	      obis code => full name
	      obis code => shortname
	      obis code => unit

	   to further map the records into an exportable common format.


	   Generic flattening logic:

	    - ignore whether the outer envelope is array or struct, simply go through it record for record
	    - if type of record is struct, then look into the struct to get obis code and value
	    - if vendor is Kaifa then look at preconfigured lst of codes to get the obis code for each value
	    - if vendor is Kamstrup, read records in pairs after the first one, getting obis code and then value

	    Note that the "OBIS List version identifier" should
	    uniquely define the format.  We might want to make it an
	    external config though...


	*/

	if (json_object_is_type(body, json_type_array)) { /* Must be Aidon? */
		int i;
		const char *mykey;
		json_object *myval;

		for (i = 0; i < json_object_array_length(body); i++) {
			tmp = json_object_array_get_idx(body, i);
			mykey = NULL;
			myval = NULL;

			/* each element of an Aidon array is either a
			 * two-field struct:
      {
        "obis-0":"0-0:96.1.7.255",
        "visible-string-1":"6515"
      },
			 * or three-field struct:
      {
        "obis-0":"1-0:1.7.0.255",
        "double-long-unsigned-1":1362,
        "structure-2":{
          "integer-0":0,
          "enum-1":27
        }
      },

			 * the first field is always (except maybe for
			 * list 1?) the obis code, and will be named
			 * "obis-0"
			 *
			 * the second field is the value, which can
			 * different integer or string types
			 *
			 * the third field is present for (all?)
			 * integer values, and is a two-element struct
			 * with a one-byte signed integer and an enum
			 * maybe scale and unit?
			 */
			json_object_object_foreach(tmp, key, val) {
				if (!strncmp(key, "obis", 4))
					mykey = json_object_get_string(val);
				else if (!myval && !json_object_is_type(val, json_type_object))
					myval = val;
			}
			if (mykey && myval) {
				/* set current list id? */
				if (!i && !strcmp(mykey, "1-1:0.2.129.255"))
					set_current_list(json_object_get_string(myval));
				add_obis(pubcfg, ret, mykey, obis_get_val(mykey, myval));
			}
		}
	} else {
		const char *mykey = NULL;
		size_t n = json_object_object_length(body);
		int i = 0;

		json_object_object_foreach(body, key, val) {
			i++;
			/* single value lists are always: Active power+ (Q1+Q4) in kW */
			if (n == 1)
				add_obis(pubcfg, ret, "1-0:1.7.0.255", json_object_get(val));
			else if (!strncmp(key, "obis", 4))
				mykey = json_object_get_string(val);
			else {
				/* set current list id? */
				if (i == 1)
					set_current_list(json_object_get_string(val));

				/*
				 * "mykey" is the obis code from the previous element for Kamstrup lists.
				 * Looking up code by index for Kaifa lists, and the first element (ListId)
				 * of Kamstrup lists
				 */
				if (!mykey)
					mykey = obis_lookup(i);
				add_obis(pubcfg, ret,  mykey, obis_get_val(mykey, val));
				mykey = NULL;
			}
		}
	}

	/* include the message time-stamp if available and not 0 */
	if (json_object_object_get_ex(notification, "date-time", &tmp) && json_object_get_int(tmp))
		add_keyval(pubcfg, ret, "date-time", json_object_get(tmp), false);

	return ret;
}

static json_object *get_value(json_object *val, json_object *values)
{
	json_object *ret;

	if (json_object_object_get_ex(values, json_object_get_string(val), &ret))
		return ret;
	return NULL;
}

/* FIMXE: qos should be configurable? */
static int publish(json_object *pubdata)
{
	json_object *topics, *val;
	const char *pub;

	/* nothing published unless configured */
	if (!cfg || !json_object_object_get_ex(cfg, "topicmap", &topics))
		return 0;

	json_object_object_foreach(topics, t, tmp) {
		val = get_value(tmp, pubdata);
		if (!val)
			continue;
		pub = json_object_to_json_string_ext(val, JSON_C_TO_STRING_PLAIN);
		mqtt_publish(t, pub, strlen(pub));
	}

	return 0;
}

static void add_hexdump(json_object *pubcfg, json_object *pubdata, const char *type, unsigned char *p, size_t plen)
{
	int i, pos = 0;
	bool do_pub = printbuffer && json_object_object_get_ex(pubcfg, type, NULL);

	/* printing debug header on stderr - don't want to publish this... */
	debug_nomqtt( "*** %s ***\n", type);

	/* need separate formatting because of the line breaks... */
	for (i=0; i<plen; i++) {
		if (debug)
			fprintf(stderr, "%02hhx%c", p[i], (i + 1) % 16 ? ' ' : '\n');
		if (do_pub)
			pos += snprintf(printbuffer + pos, BUFSIZE - pos, "%02hhx%s", p[i], (i + 1) % 16 ? " " : "  ");

		/* readjust length for the json object below in case of overflow */
		if (pos >= BUFSIZE) {
			pos = BUFSIZE - 1;
			break;
		}
	}
	if (debug)
		fprintf(stderr, "\n");
	if (do_pub)
		add_keyval(pubcfg, pubdata, type, json_object_new_string_len(printbuffer, pos), true);
}

static json_object *save_metadata(size_t framelen, struct timeval *tv)
{
	static int framecount = 0;
	json_object *tmp, *ret;

	/* save time before any potetionally time consuming calls to libjson-c */
	gettimeofday(tv, NULL);

	tmp = json_object_new_object();
	json_object_object_add(tmp, "timestamp", json_object_new_int(tv->tv_sec));
	json_object_object_add(tmp, "framelength", json_object_new_int(framelen));
	json_object_object_add(tmp, "framenumber", json_object_new_int(++framecount));
	json_object_object_add(tmp, "srcprog", json_object_new_string(progname));
	json_object_object_add(tmp, "srchost", json_object_new_string(hostname));
	json_object_object_add(tmp, "version", json_object_new_string(VERSION));
	json_object_object_add(tmp, "serialport", json_object_new_string(serialport));
	ret = json_object_new_object();
	json_object_object_add(ret, "metadata", tmp);
	return ret;
}

static void add_metadata(json_object *pubcfg, json_object *pubdata, json_object *json, struct timeval *start)
{
	struct timeval stop;
	json_object *meta;

	if (!json || !json_object_object_get_ex(json, "metadata", &meta))
		return;

	/* calculate parsing time */
	if (!gettimeofday(&stop, NULL))
		json_object_object_add(meta, "parsetime", json_object_new_int(stop.tv_sec == start->tv_sec ? stop.tv_usec - start->tv_usec : UINT_MAX));

	/* add separate metadata items */
	json_object_object_foreach(meta, mkey, mval)
		add_keyval(pubcfg, pubdata, mkey, mval, false);

	/* add complete metadata blob */
	add_keyval(pubcfg, pubdata, "metadata", meta, false);
}

static void read_config();

static int read_and_parse(int fd, unsigned char *rbuf, size_t rbuflen)
{
	unsigned char *payload, *cur, *hdlc;
	struct pollfd fds[1];
	int ret, rlen, framelen = -1;
	json_object *json, *pubcfg, *pubdata;
	struct timeval tv;

	/* get the publish config */
	if (!json_object_object_get_ex(cfg, "publish", &pubcfg))
		pubcfg = NULL;

	fds[0].fd = fd;
	fds[0].events = POLLIN;
	cur = rbuf;

	while (1) {
		ret = poll(fds, 1, -1);
		if (ret == -1 && !reread_cfg)
			return -errno;

		/* we might have been requested to read the config file again */
		if (reread_cfg) {
			reread_cfg = false;
			info("reading new configuration from '%s'\n", cfgfile);
			if (cfg)
				json_object_put(cfg);
			cfg = NULL;
			read_config();

			/* the publish config must be renewed too */
			if (!json_object_object_get_ex(cfg, "publish", &pubcfg))
				pubcfg = NULL;

			continue;
		}

		if (fds[0].revents & POLLIN)
			rlen = read(fd, cur, rbuflen + rbuf - cur);
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

			/* verify frame type and get the expectedlength */
			framelen = hdlc_length(hdlc);

			/* realign if exceeding buf size */
			if (hdlc + framelen + 2 > rbuf + rbuflen) {
				if (framelen + 2 > rbuflen) {
					debug("frame too big: %d > %zd - dropping\n", framelen, rbuflen - 2);
					framelen = -1;
					cur = rbuf;
				} else { // realign to start of buffer
					debug("moving frame from pos %zu to start of buffer to make it fit\n", hdlc - rbuf);
					memmove(rbuf, hdlc, cur - hdlc);
					cur -= hdlc - rbuf;
					hdlc = rbuf;
				}
			}
		}

		/* still waiting for the complete frame? */
		if (framelen > 0 && (cur - hdlc) < (framelen + 2))
			continue;

		/* Yay! We got a complete frame - let's save some metadata now */
		json = save_metadata(framelen, &tv);

                /* strip off HDLC framing, verify checksums and save to our parse object */
		payload = hdlc_verify(hdlc, framelen + 2, json);
		parse_payload(payload, framelen - (payload - hdlc + 1), json);
		pubdata = normalize(pubcfg, json);

		/* internally generated data */
		add_hexdump(pubcfg, pubdata, "rawpacket", hdlc, framelen > 0 ? framelen + 2 : 64);
		add_metadata(pubcfg, pubdata, json, &tv);
		add_keyval(pubcfg, pubdata, "parserdata", json, true);

		/* don't want these published on the MQTT debug channel */
		debug_nomqtt("*** parser data ***\n%s\n", json_object_to_json_string_ext(json, JSON_C_TO_STRING_PRETTY));
		debug_nomqtt("*** publish data ***\n%s\n", json_object_to_json_string_ext(pubdata, JSON_C_TO_STRING_PRETTY));

		/* publish everything */
		publish(pubdata);

		/* drop all per-frame objects */
		json_object_put(json);
		json_object_put(pubdata);

		/* skip frame marker only in case of hdlc parse failure */
		if (!payload)
			framelen = 0;

		/* keep remaining data for next frame, including the stop marker in case it doubles as start of next frame */
		memmove(rbuf, hdlc + framelen + 1, cur - rbuf - framelen + 1);
		cur -= hdlc - rbuf + framelen + 1;
		framelen = -1;
		goto nextframe;
	}
	return 0;
}


/* parse configuration and build some helper structures

"publish" is a set of keys which are be considered for publishing. The
value is a, possibly empty, list of arrays containing this key.

   "publish": {
      "key1" : [ "__array_2" ],
      "key2" : [],
      "key3" : [ "__array_1" ],
    }


The original topic value is replaced by the array name,

When processing parsed data, matching keys are included in the master
publish set as well as in any listed array publish set.

When publishing, we can simply loop through the list of topics and
publish the matching objects in the master publishset, without
considering the type of object.

 */
static void set_publish(json_object *pub, const char *key, json_object *arrayname)
{
	json_object *tmp;

	if (!json_object_object_get_ex(pub, key, &tmp)) {
		tmp = json_object_new_array();
		json_object_object_add(pub, key, tmp);
	}
	if (arrayname)
		json_object_array_add(tmp, arrayname);
}

static bool is_logging(const char *lvl, const char *topic)
{
	int i, level;

	if (!strcmp(lvl, "debug"))
		level = LOG_DEBUG;
	else if (!strcmp(lvl, "info"))
		level = LOG_INFO;
	else if (!strcmp(lvl, "error"))
		level = LOG_ERROR;
	else
		return false;

	for (i = 0; i < MAX_LOG_TOPICS; i++)
		if (!log_topic[level][i])
			break;
	if (i >= MAX_LOG_TOPICS)
		err("Too many log topics for log level '%s'\n", lvl);
	else
		log_topic[level][i] = topic;
	return true;
}

static void process_cfg(json_object *cfg, char *buf, size_t bufsize)
{
	json_object *topics, *publish, *arrayname;
	int i, idx = 0;
	const char *name;

	/* nothing published unless configured */
	if (!cfg || !json_object_object_get_ex(cfg, "topicmap", &topics))
		return;

	/* unconditionally add parent nodes */
	publish = json_object_new_object();
	json_object_object_add(cfg, "publish", publish);

	/* create a flat set of all keys to be published */
	json_object_object_foreach(topics, t, tmp) {
		/* tmp is one of three different types: OBIS code, symbolic alias, or array */
		if (json_object_is_type(tmp, json_type_array)) {
			/* 1. move to arrays */
			idx++;
			snprintf(buf, bufsize, "__array_%u", idx);
			arrayname = json_object_new_string(buf);

			/* 2. add keys to array */
			for (i = 0; i < json_object_array_length(tmp); i++) {
				name = json_object_get_string(json_object_array_get_idx(tmp, i));
				/* prevent morons from nesting */
				if (!strncmp(name, "__array",7))
					continue;
				set_publish(publish, name, arrayname);
			}

			/* 3. replace the original topic value with the array name */
			json_object_object_add(topics, t,  arrayname);
		} else {
			name = json_object_get_string(tmp);

			/* logging topics are special */
			if (is_logging(name, t))
				continue;

			set_publish(publish, name, NULL);

			/* include "timestamp" in the special "normal" and "alias" objects */
			if (!strcmp(name, "normal"))
				set_publish(publish, "timestamp", tmp);
			if (!strcmp(name, "alias"))
				set_publish(publish, "timestamp", tmp);

		}
	}
}

static json_object *read_json_file(const char *fname, char *buf, size_t bufsize)
{
	int fd;
	ssize_t len;

	fd = open(fname, O_RDONLY);
	if (fd < 0)
		return NULL;

	len = read(fd, buf, bufsize);
	close(fd);
	if (len <= 0)
		return NULL;

	return json_tokener_parse(buf);
}

/* we create indexed lookup arrays for each list */

static json_object *parse_obisfile(json_object *lists, const char *fname, char *buf, size_t bufsize)
{
	json_object *tmp;

	tmp = read_json_file(fname, buf, bufsize);
	if (!tmp)
		return NULL;

	json_object_object_foreach(tmp, key, val) {
		/* FIXME: ignoring for now... */
		if (!strcmp("_metadata", key))
			continue;
		json_object_object_add(lists, key, val);

		/* simply use the first list for lookup until we have something better */
		if (!current_list)
			set_current_list(key);
	}

	return tmp;
}

static void read_config()
{
	char *rbuf = (char *)buf;
	size_t bufsize = BUFSIZE;
	json_object *tmp, *list;
	const char *name;
	int i           ;

	/* reset before re-reading */
	current_list = NULL;
	memset(log_topic, 0, sizeof(log_topic));

	if (cfg)
		json_object_put(cfg);

	cfg = read_json_file(cfgfile, rbuf, bufsize);
	if (!cfg)
		return;

	/* read all the OBIS definitions so we can look up aliases */
	if (json_object_object_get_ex(cfg, "obisdefs", &tmp) && json_object_is_type(tmp, json_type_array)) {
		list = json_object_new_object();
		json_object_object_add(cfg, "obislists", list);

		for (i = 0; i < json_object_array_length(tmp); i++) {
			name = json_object_get_string(json_object_array_get_idx(tmp, i));
			if (!parse_obisfile(list, name, rbuf, bufsize))
				debug("failed to parse '%s'\n", name);
		}
	}

	/* post process config - adding helper structures */
	process_cfg(cfg, (char *)rbuf, bufsize);
}

static struct option main_options[] = {
	{ "help",	0, 0, 'h' },

	{ "config",     0, 0, 'c' },
	{ "debug",      0, 0, 'd' },

	{ "broker",     1, 0, 'b' },
	{ "id",         1, 0, 'i' },
	{ "keepalive",  1, 0, 'k' },
	{ "port",       1, 0, 'p' },
	{ "password",   1, 0, 'P' },
	{ "serial",     1, 0, 's' },
	{ "user",       1, 0, 'u' },

#ifdef WITH_TLS
#ifdef WITH_TLS
	{ "cafile",     1, 0, 1 },
	{ "capath",     1, 0, 2 },
	{ "cert",       1, 0, 3 },
	{ "key",        1, 0, 4 },
	{ "insecure",   0, 0, 5 },
#endif
#endif
	{ "unscaled",   0, 0, 6 },
	{ "units",      0, 0, 7 },
	{ 0, 0, 0, 0 }
};

static void usage()
{
	int maj, min, rev;

	mosquitto_lib_version(&maj, &min, &rev);
	printf("%s version %s, using libmosquitto %u.%u.%u and libjson-c %s\n\n", progname, VERSION, maj, min, rev, json_c_version());
	printf("Usage: %s [-d] [-c configfile] -s device\n", progname);
	printf("                     [-b hostname] [-p port] [-u username [-P password]]\n");
#ifdef WITH_TLS
	printf("                     [-i id] [-k keepalive] [--insecure]\n");
	printf("                     [{--cafile file | --capath dir} [--cert file] [--key file]\n");
#else
	printf("                     [-i id] [-k keepalive]\n");
#endif
	printf("                     [--unscaled | --units ]\n\n");

	printf(" -c : Configuration file.  Default: %s\n", CONFIG_FILE);
	printf(" -d : Enable debugging\n");
	printf(" -s : Serial device connected to M-Bus. E.g /dev/ttyUSB0. Use '-' to read from stdin\n");

	printf("\nMQTT client options:\n");
	printf(" -b : Broker hostname or IP address. Default: %s\n", MQTT_BROKER);
	printf(" -i : Client Id. A random id is generated by default\n");
	printf(" -k : Keepalive in seconds. Default: %u\n", MQTT_KEEPALIVE);
	printf(" -p : Broker TCP port. Default: %u (%u when using MQTTS)\n", MQTT_PORT, MQTTS_PORT);
	printf(" -P : Password.  Default is no authentication\n");
	printf(" -u : Username\n");

#ifdef WITH_TLS
	printf("\nMQTT over TLS (MQTTS):\n");
	printf(" --insecure : Do not validate brokername against certificate\n");
	printf(" --cafile   : Trusted CA certificates PEM file\n");
	printf(" --capath   : Trusted CA certificates directory\n");
	printf(" --cert     : Client certificate\n");
	printf(" --key      : Client private key\n");
#endif

	printf("\nPublished value format:\n");
	printf(" --unscaled : Do not scale numbers. Default: false (scaling enabled)\n");
	printf(" --units    : Include units. Implies value scaling. Default: false\n");

	printf("\nExample: %s -s /dev/ttyUSB0 -b broker.example.com\n", progname);

}

static void sig_handler(int sig)
{
	debug("handling signal %d\n", sig);
	switch (sig) {
	case SIGHUP:
		reread_cfg = true;
		break;
	}
}

int main(int argc, char *argv[])
{
	int opt, serfd = -1, ret = 0;
	// mosquitto opts
	char *mqttid = NULL;
	char *broker = MQTT_BROKER;
	int port = 0;
	int keepalive = MQTT_KEEPALIVE;
	bool clean_session = true;
	// mosquitto auth opts
	char *mqttuser = NULL, *mqttpw = NULL;
#ifdef WITH_TLS
	// mosquitto tls opts
	const char *cafile = NULL;
  	const char *capath = NULL;
  	const char *certfile = NULL;
  	const char *keyfile = NULL;
	bool insecure = false;
#endif

	/* initialize global vars */
	progname = argv[0];
	gethostname(hostname, sizeof(hostname));

	/* deal with signals */
	signal(SIGHUP, sig_handler);

	while ((opt = getopt_long(argc, argv, "?hdb:c:i:k:p:P:s:u", main_options, NULL)) != -1) {
		switch(opt) {
		case '?':
		case 'h':
			usage();
			return 0;
		case 'd':
			debug = true;
			break;
		case 'b':
			broker = optarg;
			break;
		case 'c':
			cfgfile = optarg;
			break;
		case 'i':
			mqttid = optarg;
			break;
		case 'k':
			keepalive = atoi(optarg);
			break;
		case 'p':
			port = atoi(optarg);
			break;
		case 'P':
			mqttpw = optarg;
			break;
		case 's':
			if (!optarg[1] && optarg[0] == '-') {
				serialport = "stdin";
				serfd = STDIN_FILENO;
			} else {
				serialport = optarg;
				serfd = serial_open(optarg);
			}
			break;
		case 'u':
			mqttuser = optarg;
			break;
#ifdef WITH_TLS
		case 1:
			cafile = optarg;
			break;
		case 2:
			capath = optarg;
			break;
		case 3:
			certfile = optarg;
			break;
		case 4:
			keyfile = optarg;
			break;
		case 5:
			insecure = true;
			break;
		case 6:
			unscaled = true;
			break;
		case 7:
			units = true;
			break;
#endif
		}
	}

	if (units)
		unscaled = false;

	if (serfd < 0) {
		usage(argv[0]);
		return 0;
	}

	/* print banner */
			info("%s version %s running on %s\n", progname, VERSION, hostname);

        /* initialize mqtt client and read buffer */
	mosquitto_lib_init();
	mosq = mosquitto_new(mqttid, clean_session, NULL);
	mosquitto_log_callback_set(mosq, libmosquitto_log_callback);
	buf = malloc(BUFSIZE);
	printbuffer = malloc(BUFSIZE);
	if (!buf || !printbuffer || !mosq) {
		fprintf(stderr, "Error: Out of memory.\n");
		ret = -ENOMEM;
		goto err;
	}

	/* read config file */
	read_config();
	if (!cfg)
		err_nomqtt("Failed to parse '%s' - will not publish anything to MQTT broker '%s'\n", cfgfile, broker);
	else
		debug_nomqtt("*** configuration ***\n%s\n", json_object_to_json_string_ext(cfg, JSON_C_TO_STRING_PRETTY));

	/* configure broker connection */
	if (mqttuser)
		mosquitto_username_pw_set(mosq, mqttuser, mqttpw);

#ifdef WITH_TLS
	if (certfile || keyfile) {
		if (!certfile || !keyfile) {
			err_nomqtt("Option '%s' is required for MQTT over TLS\n", certfile ? "--key" : "--cert");
			goto err;
		}
		/* set default port for MQTTS if not set by command line option */
		if (!port)
			port = MQTTS_PORT;
		mosquitto_tls_set(mosq, cafile, capath, certfile, keyfile, NULL); // FIXME: callback?
		// mosquitto_tls_opts_set()
		mosquitto_tls_insecure_set(mosq, insecure);
		// mosquitto_tls_psk_set()
	}
#endif
	/* set default port if not set by command line or TLS defaults */
	if (!port)
		port = MQTT_PORT;

	/* connect to broker */
	mosquitto_connect(mosq, broker, port, keepalive);

	/* loop forever */
	read_and_parse(serfd, buf, BUFSIZE);

err:

	if (serfd > 0 && serfd != STDIN_FILENO)
		close(serfd);

	sleep(1);	/* give the mosquitto lib some time to flush remaining messages - FIXME: there gotta be a better way? */
	free(buf);
	free(printbuffer);
	if (cfg)
		json_object_put(cfg);
	mosquitto_disconnect(mosq);
	mosquitto_destroy(mosq);
	mosquitto_lib_cleanup();
	return ret;
}
