# obinsect is a Work-in-Progress

There might be changes to the configuration file syntax!

Items which needs to be fixed in arbitrary order:

## Memory management

* All json-c allocations should be audited.  We are most likely
  getting some reference counts wrong
  
## MQTT connection

* complete and verify all supported authentication methods, including
  TLS client certificates

## HAN port reader

* test and robustify, making sure the serial device can go away and
  reappear without breaking stuff

## Code refactoring

* cleanup is obviously necessary
* audit comments, making sure they are still semi-correct and relevant
* move most of the documentation comments to docs, or delete

## System integration

* add daemon mode?
* include OpenWrt packaging example?
* include systemd unit?


## Main config file

* allow per-topic settings? Some possibilities:
   1. timestamp format
   2. unit/scaling
   3. QoS
   4. retained messages - useful for units, scalers, counters

* move some of the far too many command line option into the config file?


## ++++
