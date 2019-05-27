# obinsect - simple COSEM OBIS to MQTT proxy

## plan

IEC 62056-7-5:2017 annex D.1 "M-Bus with the HDLC based data link
layer" provides this protocol stack:

* COSEM Application process (IEC 62056-6-1, IEC 62056-6-2)
* DLMS/COSEM Application layer (IEC 62056-5-3)
* HLDC based Data Link layer (IEC 62056-46)
* M-Bus Physical Layer (IEC 13757-2)


### M-BUS connection

 * by USB serial device.
 * details out of scope.
 * Assumed to be working and available on /dev/ttyUSBx.
 * configurable device name, supporting symlinks

### HDLC decoding

 * no 0x7E escaping?
 
 * section 8.3.2 "LLC PDU format" in Green-Book-Ed-83-Excerpt.pdf
   shows the fixed LLC sublayer for a "response”:
 
 LSAP | DSAP | Quality | Information
 0xE6 | 0xE7 |  0x00   |  n octets

The Information field carries the LSDU. Note that n == 0 is valid.

 * section 8.4.1 "The MAC PDU and the HDLC frame" in
   Green-Book-Ed-83-Excerpt.pdf documents the outer MAC layer of the
   DLMS HDLC transport:

 Flag | Frame format | Dest. address | Src. address | Control | HCS | Information | FCS | Flag

Flag is 0x7E. Note that consequtive frames my be separated by a single
flag, which then serves as both closing and opening flag.

Frames without an "Information" field will not have HCS (redundant
since HCS and FCS would cover the same data), thus compressing the
format to

 Flag | Frame format | Dest. address | Src. address | Control | FCS | Flag

Frame format is 16 bit, split as
 *  4 bits format. Fixed value: 1010b (0xa).  Meaning "type 3", which is the only defined type for DLMS
 *  1 segmentation bit
 * 11 length bits

The value of the frame length subfield is the count of octets in the
frame excluding the opening and closing frame flag sequences.
 
Control field is 1 octet, indicating the type of command or response.
Will contain sequence numbers where appropriate.  See
https://en.wikipedia.org/wiki/High-Level_Data_Link_Control#Control_Field
Observed values: 0x10 and 0x13, meaning:
 * seq no 0
 * final bit set
 * 0x10: I-frame referring send seq no 0
 * 0x13: U-frame with type == 0, meaning UI (Unnumbered Information) according to http://www.acacia-net.com/wwwcla/protocol/iso_4335.htm
 

HCS is two octets, covering the bits between the opening flag sequence and the HCS.

The "Information" field may be any sequence of bytes. Meaning that
0x7E is allowed too? It carried the MSDU of data (I and IU) frames

FCS is two octets, covering the bits between the opening flag sequence
and the FCS.  Excluding "any start and stop elements", whatever that
means?

Depending on the direction of the data transfer, both the client and
the server addresses can be destination or source addresses.  The
client address shall always be expressed on one byte. This will be the
destination address in our case, where we only ever receive
"responses" from the server (AMS).  The length of a complete server
address field is restricted to be one, two or four bytes long.


### COSEM decoding


See IEC 62056-7-5:2017 Annex G.1 "xDLMS APDUs used (without protection
and without general-block-transfer)" for the application protocol with
examples.


 0F            data-notification [15]
 00 00 00 00   long-invoke-id-and-priority
 0C 07 E1 0A 14 05 03 3A 1E FF 80 00 00  date-time
 02 19 ....    notification-body


#### long-invoke-id-and-priority

 bits
  0-23 | invoke-id
 24-27 | reserved
    28 | self-descriptive
	29 | processing-option (0=continue, 1=break on error)
	30 | service-class (0=unconfirmed, 1=confirmed)
	31 | priority (0=normal, 1=high)


#### ACCESS service

See section 9.3.9  in Green-Book-Ed-83-Excerpt.pdf

The ACCESS service is a unified service which can be used to access
multiple COSEM object attributes and/or methods with a single .request
/ .response.

This allows the 4 byte long-invoke-id-and-priority:

The Invoke-Id parameter of the GET, SET and ACTION services allows the
client and the server to pair requests and responses. The range of the
Invoke-Id is 0...15.  In some cases this is not sufficient. To support
those cases, the ACCESS service uses a Long-Invok-Id parameter. The
range of the Long-Invoke-Id is 0...16 777 215.
 
ACCESS service primitives provide a service parameter to carry the
time stamp holding the date and time of invoking the service
primitive. This further reduces overhead


See also 14.2 "ACCESS service example"


Note that NEK referes to IEC 62056-7-5, which simplifies stuff for
one-way communication from meter to client.


 * 1 byte. Fixed?  0x0F (xDLMS service "data-notification" is 15, ref https://www.saso.gov.sa/ar/about/PublicConsultation/Documents/SASO-IEC-62056-7-5-2017-E.pdf section 7.3 "xDLMS services")
 * 4 bytes long-invoke-id-and-priority 0x00000000 or 0x40000000
 * 1 or 13 bytes date-time 0x00 or 0x0C.........
 * n bytes body
 


### OBIS mapping

configurable code to MQTT topic
configurable packed struct to JSON mapping

LUA configuration?

### MQTT publish

use libmosquitto
support TLS by default


### possible laguages with libraries

#### C

hdlc
cosem
obis
json
lua
libmosquitto


#### perl


#### lua


lua-mosquitto


## references

Lots of real samples, collected docs and sample parsing code:
https://github.com/roarfred/AmsToMqttBridge.git

local clone: /usr/local/src/git/AmsToMqttBridge


The file ```Samples/Kaifa/readme.md``` contains a near complete breakdown of a HDLC frame - extremely useful!
