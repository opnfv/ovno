#!/usr/bin/env python

from socket import gethostbyname
import sys
import shutil

from apt_pkg import version_compare
import yaml

from charmhelpers.contrib.openstack.utils import configure_installation_source

from charmhelpers.core.hookenv import (
    Hooks,
    UnregisteredHookError,
    config,
    local_unit,
    log,
    relation_get,
    relation_ids,
    relation_set,
    unit_get
)

from charmhelpers.core.host import (
    restart_on_change,
    service_restart
)

from charmhelpers.fetch import (
    apt_install,
    apt_upgrade,
    configure_sources
)

from contrail_analytics_utils import (
    CONTRAIL_VERSION,
    cassandra_units,
    fix_hostname,
    fix_nodemgr,
    fix_permissions,
    fix_services,
    kafka_units,
    provision_analytics,
    units,
    unprovision_analytics,
    write_alarm_config,
    write_analytics_api_config,
    write_collector_config,
    write_nodemgr_config,
    write_query_engine_config,
    write_snmp_collector_config,
    write_topology_config,
    write_vnc_api_config,
    write_keystone_auth_config
)

PACKAGES = [ "contrail-analytics", "contrail-utils", "contrail-nodemgr",
             "python-jinja2" ]

hooks = Hooks()
config = config()

def add_analytics():
    # check relation dependencies
    if not config.get("analytics-configured") \
       and config.get("cassandra-ready") \
       and config.get("kafka-ready") \
       and config.get("zookeeper-ready") \
       and config.get("contrail-api-ready") \
       and config.get("contrail-discovery-ready") \
       and config.get("identity-admin-ready"):
        # provision analytics on 3.0.2.0+
        if version_compare(CONTRAIL_VERSION, "3.0.2.0-34") >= 0:
            provision_analytics()
        config["analytics-configured"] = True

@hooks.hook("cassandra-relation-changed")
def cassandra_changed():
    if not relation_get("native_transport_port"):
        log("Relation not ready")
        return
    if not config.get("cassandra-ready"):
        units = len(cassandra_units())
        required = config["cassandra-units"]
        if units < required:
            log("{} cassandra unit(s) ready, require {} more".format(units, required - units))
            return
    config["cassandra-ready"] = True
    cassandra_relation()
    add_analytics()

@hooks.hook("cassandra-relation-departed")
@hooks.hook("cassandra-relation-broken")
def cassandra_departed():
    if not units("cassandra"):
        remove_analytics()
        config["cassandra-ready"] = False
    cassandra_relation()

@restart_on_change({"/etc/contrail/contrail-collector.conf": ["contrail-collector"],
                    "/etc/contrail/contrail-query-engine.conf": ["contrail-query-engine"],
                    "/etc/contrail/contrail-analytics-api.conf": ["contrail-analytics-api"]})
def cassandra_relation():
    write_collector_config()
    write_query_engine_config()
    write_analytics_api_config()

@hooks.hook("config-changed")
def config_changed():
    vip = config.get("vip")
    for rid in relation_ids("contrail-analytics-api"):
        relation_set(relation_id=rid, vip=vip)

@hooks.hook("contrail-analytics-api-relation-joined")
def contrail_analytics_api_joined():
    relation_set(port=8081, vip=config.get("vip"))

@hooks.hook("contrail-api-relation-changed")
def contrail_api_changed():
    if not relation_get("port"):
        log("Relation not ready")
        return
    contrail_api_relation()
    config["contrail-api-ready"] = True
    add_analytics()

@hooks.hook("contrail-api-relation-departed")
@hooks.hook("contrail-api-relation-broken")
def contrail_api_departed():
    if not units("contrail-api"):
        remove_analytics()
        config["contrail-api-ready"] = False
    contrail_api_relation()

@restart_on_change({"/etc/contrail/contrail-snmp-collector.conf": ["contrail-snmp-collector"],
                    "/etc/contrail/vnc_api_lib.ini": ["contrail-topology"]})
def contrail_api_relation():
    write_snmp_collector_config()
    write_vnc_api_config()
    write_analytics_api_config()

@hooks.hook("contrail-discovery-relation-changed")
def contrail_discovery_changed():
    if not relation_get("port"):
        log("Relation not ready")
        return
    contrail_discovery_relation()
    config["contrail-discovery-ready"] = True
    add_analytics()

@hooks.hook("contrail-discovery-relation-departed")
@hooks.hook("contrail-discovery-relation-broken")
def contrail_discovery_departed():
    if not units("contrail-discovery"):
        remove_analytics()
        config["contrail-discovery-ready"] = False
    contrail_discovery_relation()

@restart_on_change({"/etc/contrail/contrail-collector.conf": ["contrail-collector"],
                    "/etc/contrail/contrail-query-engine.conf": ["contrail-query-engine"],
                    "/etc/contrail/contrail-analytics-api.conf": ["contrail-analytics-api"],
                    "/etc/contrail/contrail-alarm-gen.conf": ["contrail-alarm-gen"],
                    "/etc/contrail/contrail-topology.conf": ["contrail-topology"],
                    "/etc/contrail/contrail-snmp-collector.conf": ["contrail-snmp-collector"],
                    "/etc/contrail/contrail-analytics-nodemgr.conf": ["contrail-analytics-nodemgr"]})
def contrail_discovery_relation():
    write_collector_config()
    write_query_engine_config()
    write_analytics_api_config()
    write_alarm_config()
    write_topology_config()
    write_snmp_collector_config()
    write_nodemgr_config()

@hooks.hook("http-services-relation-joined")
def http_services_joined():
    name = local_unit().replace("/", "-")
    addr = gethostbyname(unit_get("private-address"))
    services = [ { "service_name": "contrail-analytics-api",
                   "service_host": "0.0.0.0",
                   "service_port": 8081,
                   "service_options": [ "mode http", "balance leastconn", "option httpchk GET /analytics HTTP/1.0" ],
                   "servers": [ [ name, addr, 8081, "check" ] ] } ]
    relation_set(services=yaml.dump(services))

@hooks.hook("identity-admin-relation-changed")
def identity_admin_changed():
    if not relation_get("service_hostname"):
        log("Relation not ready")
        return
    identity_admin_relation()
    config["identity-admin-ready"] = True
    add_analytics()

@hooks.hook("identity-admin-relation-departed")
@hooks.hook("identity-admin-relation-broken")
def identity_admin_departed():
    if not units("identity-admin"):
        remove_analytics()
        config["identity-admin-ready"] = False
    identity_admin_relation()

@restart_on_change({"/etc/contrail/contrail-snmp-collector.conf": ["contrail-snmp-collector"],
                    "/etc/contrail/vnc_api_lib.ini": ["contrail-topology"],
                    "/etc/contrail/contrail-keystone-auth.conf": ["contrail-keystone-auth"]})
def identity_admin_relation():
    write_snmp_collector_config()
    write_vnc_api_config()
    write_keystone_auth_config()

@hooks.hook()
def install():
    fix_hostname()
    shutil.copy('files/contrail', '/etc/apt/preferences.d')
    configure_installation_source(config["openstack-origin"])
    configure_sources(True, "install-sources", "install-keys")
    apt_upgrade(fatal=True, dist=True)
    apt_install(PACKAGES, fatal=True)
    fix_permissions()
    fix_services()
    fix_nodemgr()

@hooks.hook("kafka-relation-changed")
def kafka_changed():
    if not relation_get("port"):
        log("Relation not ready")
        return
    if not config.get("kafka-ready"):
        units = len(kafka_units())
        required = config["kafka-units"]
        if units < required:
            log("{} kafka unit(s) ready, require {} more".format(units, required - units))
            return
    config["kafka-ready"] = True
    kafka_relation()
    add_analytics()

@hooks.hook("kafka-relation-departed")
@hooks.hook("kafka-relation-broken")
def kafka_departed():
    if not units("kafka"):
        remove_analytics()
        config["kafka-ready"] = False
    kafka_relation()

@restart_on_change({"/etc/contrail/contrail-collector.conf": ["contrail-collector"],
                    "/etc/contrail/contrail-alarm-gen.conf": ["contrail-alarm-gen"]})
def kafka_relation():
    write_collector_config()
    write_alarm_config()

def main():
    try:
        hooks.execute(sys.argv)
    except UnregisteredHookError as e:
        log("Unknown hook {} - skipping.".format(e))

def remove_analytics():
    if config.get("analytics-configured"):
        # unprovision analytics on 3.0.2.0+
        if version_compare(CONTRAIL_VERSION, "3.0.2.0-34") >= 0:
            unprovision_analytics()
        config["analytics-configured"] = False

@hooks.hook("upgrade-charm")
def upgrade_charm():
    write_collector_config()
    write_query_engine_config()
    write_analytics_api_config()
    write_alarm_config()
    write_topology_config()
    write_snmp_collector_config()
    write_vnc_api_config()
    write_nodemgr_config()
    service_restart("supervisor-analytics")

@hooks.hook("zookeeper-relation-changed")
def zookeeper_changed():
    if not relation_get("port"):
        log("Relation not ready")
        return
    zookeeper_relation()
    config["zookeeper-ready"] = True
    add_analytics()

@hooks.hook("zookeeper-relation-departed")
@hooks.hook("zookeeper-relation-broken")
def zookeeper_departed():
    if not units("zookeeper"):
        remove_analytics()
        config["zookeeper-ready"] = False
    zookeeper_relation()

@restart_on_change({"/etc/contrail/contrail-collector.conf": ["contrail-collector"],
                    "/etc/contrail/contrail-alarm-gen.conf": ["contrail-alarm-gen"],
                    "/etc/contrail/contrail-topology.conf": ["contrail-topology"],
                    "/etc/contrail/contrail-snmp-collector.conf": ["contrail-snmp-collector"]})
def zookeeper_relation():
    write_collector_config()
    write_alarm_config()
    write_topology_config()
    write_snmp_collector_config()

if __name__ == "__main__":
    main()
