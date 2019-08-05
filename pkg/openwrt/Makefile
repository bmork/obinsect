include $(TOPDIR)/rules.mk

PKG_NAME:=obinsect
PKG_RELEASE:=1

PKG_VERSION=0.03
PKG_SOURCE_PROTO:=git
PKG_SOURCE_URL=https://github.com/bmork/obinsect.git
PKG_SOURCE_VERSION:=v$(PKG_VERSION)
PKG_SOURCE_SUBDIR:=$(PKG_NAME)-$(PKG_SOURCE_VERSION)
PKG_BUILD_DIR:=$(BUILD_DIR)/$(PKG_SOURCE_SUBDIR)

PKG_MAINTAINER:=Bjørn Mork <bjorn@mork.no>

PKG_LICENSE:=GPL-2.0

PKG_BUILD_PARALLEL:=1

include $(INCLUDE_DIR)/package.mk


define Package/obinsect
  SECTION:=net
  CATEGORY:=Network
  DEPENDS:=+libjson-c +libmosquitto
  TITLE:=AMS HAN M-BUS to MQTT proxy
endef

define Package/obinsect/description
  Read energy meter M-BUS HAN port and publish data to a MQTT broker
endef

define Package/obinsect/install
	$(INSTALL_DIR) $(1)/usr/sbin $(1)/usr/share/obinsect $(1)/etc/init.d/ $(1)/etc/config
	$(INSTALL_BIN) $(PKG_BUILD_DIR)/obinsectd $(1)/usr/sbin/
	$(INSTALL_DATA) $(PKG_BUILD_DIR)/*.json $(1)/usr/share/obinsect/
	$(INSTALL_BIN) ./files/obinsect.init $(1)/etc/init.d/obinsect
	$(INSTALL_CONF) ./files/obinsect.config $(1)/etc/config/obinsect
endef

define Package/obinsect/conffiles
/etc/config/obinsect
endef

$(eval $(call BuildPackage,obinsect))
