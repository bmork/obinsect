# obinsect is a Work-in-Progress

There will be changes to the configuration file syntax!

Items which needs to be fixed in arbitrary order:

## OBIS list parsing

* Simplify the parsing by changing the list format to match what the
  application needs
* Add a metadata level to the list files while at it, for stuff like
  source, revision, author, comments etc

## Memory management

* All json-c allocations should be audited.  We are most likely
  getting some reference counts wrong
  
## MQTT connection

* make sure the client will attempt to reconnect on errors
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
* add working config example, which could be used as-is?

## Logging

* add real logging, with at least debug, error and info levels
* support logging of all levels to MQTT.  Maybe modify topic config
  syntax to allow a LEVEL variable or similar?

## Main config file

* provide some sane defaults?
* add log configuration?


## ++++
