# obinsect - configurable energy meter HAN M-BUS to MQTT proxy

## configuration

The HAN reader can be used for debugging without any
configuration. Publishing data to MQTT requires a configuration file
in JSON format. This maps the meter values to MQTT topic(s).

The configuration file must specificy two fields:
 1. "topicmap" - a map of MQTT "topic" to "value"
 2. "obisdefs" - a list of files defining the supported OBIS Lists.
                 one file for each known list name
 
This is a sample configuration file:
```javascript
{
	"topicmap" :
		{
			"/obinsect/debug/channel"   : "debug",
			"/obinsect/debug/raw"       : "rawhexdump",
			"/obinsect/json/normalized" : "normal",
			"/obinsect/json/full"       : "full",
			"/obinsect/value/power"     : "1-1:0.2.129.255",
			"/obinsect/value/export"    : "PowerExport",
			"/obinsect/value/custom"    : [ "timestamp", "1-1:0.2.129.255" ]
		},
	"obisdefs" :
		[
			"/etc/obinsect/aidon_v0001.json",
			"/etc/obinsect/kaifa_v0001.json",
			"/etc/obinsect/kamstrup_v0001.json"
		]
}
```

Any number of MQTT topics can be given, including zero. Data will be
published to the topics with an updated value every time a DLMS/COSEM
packet is received from the energy meter.

Note that Kaifa meters transmits arrays of values only.  The OBIS list
definition is used to map these to the correct OBIS code keys, with
any associated scale and units. This implies that list of codes for
these meters must be complete and in the exxact order received from
the meter.


### value specification

Each topic points to the key used to look up values for that topic.

These keys can:


#### an obis code

Any obis code listed in the "obisdef" file.  The value will divided by
the "scale" field, if present, and used directly without any JSON
wrapping.

Integer or float values are converted to their text representation.

Date and time values are converted the text representation of an UNIX
epoch time.

Units are not published with the value.

#### a named obis code

The name of an obis code, as it is defined in the **"obisdef"** file,
can be used as an alias for the code. See above for value specification.


#### "timestamp"

Text representation of the UNIX epoch time we received the packet
causing this publish event

#### "debug"

Debugging messages from obinsectd.  Any format.  ASCII text can be
assumed.


#### "rawhexdump"

The packet as a string of hex codes.


#### "full"

A JSON struct with all parts of the received packet, including headers
and checksum. The received DLMS/COSEM structure is preserved in JSON
as received, so the exact format is meter dependent.

This is mostly useful for debugging. The original packet can be
reconstructed from this JSON struct.


#### "normal"

A JSON struct where the received data has been flattened to an a set
of **"obiscode" : "value"**.  A timestamp field is also included
 
This format is similar for all meter types, but the values are not
sscaled so they cannot be directly compared.


#### array of keys

Will publishing a JSON struct with the given set of values, regardless
of what the meter sends in each packet. Each element of the array
becomes a key in the JSON, having values as defined above.

Only fields having an updated value will be included, so the actual
number of fields can vary from packet to packet.

A JSON struct will be published when any of the included fields are
updated, except for **"timestamp"**.

Note that including any of **"normal"**, **"full"** or
**"rawhexdump"** will result in a publish to the topic for every
received packed, which might not be what you wanted.

Array mesting is not allowed.

 
## reading multiple meters

Run one process per meter. The configuration files may be shared if
applicable.

