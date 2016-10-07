#!/usr/bin/env python

from collections import OrderedDict
from socket import gethostbyname
import sys

import yaml

from charmhelpers.core.hookenv import (
    Hooks,
    UnregisteredHookError,
    config,
    log,
    related_units,
    relation_get,
    relation_ids,
    relation_set
)

from charmhelpers.fetch import (
    apt_install,
    apt_upgrade,
    configure_sources
)

from ceilometer_contrail_utils import contrail_analytics_api_units, units

PACKAGES = [ "ceilometer-plugin-contrail" ]

hooks = Hooks()
config = config()

@hooks.hook("ceilometer-plugin-relation-joined")
def ceilometer_plugin_joined():
    if contrail_analytics_api_units():
        configure_plugin()

@hooks.hook("config-changed")
def config_changed():
    pass

def configure_plugin():
    # create plugin config
    api_ip, api_port = [
       (vip if vip else gethostbyname(relation_get("private-address", unit, rid)),
        port)
       for rid in relation_ids("contrail-analytics-api")
       for unit, port, vip in
       ((unit, relation_get("port", unit, rid), relation_get("vip", unit, rid))
        for unit in related_units(rid))
       if port ][0]
    meter_sources = [
       OrderedDict([("name", "contrail_source"),
                    ("interval", 600),
                    ("meters", ["ip.floating.receive.packets",
                                "ip.floating.transmit.packets",
                                "ip.floating.receive.bytes",
                                "ip.floating.transmit.bytes"]),
                    ("resources", ["contrail://{}:{}".format(api_ip, api_port)]),
                    ("sinks", ["contrail_sink"])])]
    meter_sinks = [
       OrderedDict([("name", "contrail_sink"),
                    ("publishers", ["rpc://"]),
                    ("transformers", [])])]
    settings = { "meter-sources": yaml.dump(meter_sources),
                 "meter-sinks": yaml.dump(meter_sinks) }
    for rid in relation_ids("ceilometer-plugin"):
        relation_set(relation_id=rid, relation_settings=settings)

@hooks.hook("contrail-analytics-api-relation-changed")
def contrail_analytics_api_changed():
    if not relation_get("port"):
        log("Relation not ready")
        return
    if units("ceilometer-plugin"):
        configure_plugin()

@hooks.hook("contrail-analytics-api-relation-departed")
@hooks.hook("contrail-analytics-api-relation-broken")
def contrail_analytics_api_departed():
    if not units("contrail-analytics-api"):
        remove_plugin()

@hooks.hook()
def install():
    configure_sources(True, "install-sources", "install-keys")
    apt_upgrade(fatal=True, dist=True)
    apt_install(PACKAGES, fatal=True)

def main():
    try:
        hooks.execute(sys.argv)
    except UnregisteredHookError as e:
        log("Unknown hook {} - skipping.".format(e))

def remove_plugin():
    settings = { "meter-sources": None, "meter-sinks": None }
    for rid in relation_ids("ceilometer-plugin"):
        relation_set(relation_id=rid, relation_settings=settings)

@hooks.hook("upgrade-charm")
def upgrade_charm():
    pass

if __name__ == "__main__":
    main()
