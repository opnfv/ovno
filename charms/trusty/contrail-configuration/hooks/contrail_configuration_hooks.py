#!/usr/bin/env python

from socket import gethostbyname
import sys

from apt_pkg import version_compare
import json
import urllib2
import yaml

from charmhelpers.contrib.openstack.utils import configure_installation_source

from charmhelpers.core.hookenv import (
    Hooks,
    UnregisteredHookError,
    config,
    is_leader,
    leader_get,
    leader_set,
    local_unit,
    log,
    related_units,
    relation_get,
    relation_id,
    relation_ids,
    relation_set,
    remote_unit,
    unit_get
)

from charmhelpers.core.host import (
    pwgen,
    restart_on_change,
    service_restart
)

from charmhelpers.fetch import (
    apt_install,
    apt_upgrade,
    configure_sources
)

from contrail_configuration_utils import (
    CONTRAIL_VERSION,
    api_port,
    cassandra_units,
    check_url,
    contrail_floating_ip_create,
    contrail_floating_ip_deactivate,
    contrail_floating_ip_delete,
    contrail_floating_ip_use,
    discovery_port,
    dpkg_version,
    fix_services,
    provision_configuration,
    provision_metadata,
    units,
    unprovision_configuration,
    unprovision_metadata,
    write_barbican_auth_config,
    write_contrail_api_config,
    write_contrail_schema_config,
    write_contrail_svc_monitor_config,
    write_device_manager_config,
    write_discovery_config,
    write_ifmap_config,
    write_nodemgr_config,
    write_vnc_api_config
)

PACKAGES = [ "ifmap-server", "contrail-config", "contrail-config-openstack",
             "neutron-common", "contrail-utils", "contrail-nodemgr" ]

PACKAGES_BARBICAN = [ "python-barbicanclient" ]

hooks = Hooks()
config = config()

def add_contrail_api():
    # check relation dependencies
    if not config_get("contrail-api-configured") \
       and config_get("amqp-ready") \
       and config_get("cassandra-ready") \
       and config_get("identity-admin-ready") \
       and config_get("zookeeper-ready"):
        api_p = api_port()
        port = str(api_p)
        try:
            # wait until api is up
            check_url("http://localhost:" + port)
        except urllib2.URLError:
            log("contrail-api service has failed to start correctly on port {}".format(port),
                "CRITICAL")
            log("This is typically due to a runtime error in related services",
                "CRITICAL")
            raise
        # provision configuration on 3.0.2.0+
        if version_compare(CONTRAIL_VERSION, "3.0.2.0-34") >= 0:
            provision_configuration()
        config["contrail-api-configured"] = True

        # inform relations
        for rid in relation_ids("contrail-api"):
            relation_set(relation_id=rid, port=api_p, vip=config.get("vip"))

        configure_floating_ip_pools()

def add_metadata():
    # check relation dependencies
    if is_leader() \
       and not leader_get("metadata-provisioned") \
       and config_get("contrail-api-configured") \
       and config_get("neutron-metadata-ready"):
        provision_metadata()
        leader_set({"metadata-provisioned": True})

@hooks.hook("amqp-relation-changed")
def amqp_changed():
    if not relation_get("password"):
        log("Relation not ready")
        return
    amqp_relation()
    config["amqp-ready"] = True
    add_contrail_api()
    add_metadata()

@hooks.hook("amqp-relation-departed")
@hooks.hook("amqp-relation-broken")
def amqp_departed():
    if not units("amqp"):
        remove_metadata()
        remove_contrail_api()
        config["amqp-ready"] = False
    amqp_relation()

@restart_on_change({"/etc/contrail/contrail-api.conf": ["supervisor-config"],
                    "/etc/contrail/contrail-device-manager.conf": ["supervisor-config"],
                    "/etc/contrail/contrail-schema.conf": ["supervisor-config"],
                    "/etc/contrail/contrail-svc-monitor.conf": ["supervisor-config"]})
def amqp_relation():
    write_contrail_api_config()
    write_contrail_svc_monitor_config()
    write_device_manager_config()
    if version_compare(CONTRAIL_VERSION, "3.0") >= 0:
        write_contrail_schema_config()

@hooks.hook("amqp-relation-joined")
def amqp_joined():
    relation_set(username="contrail", vhost="contrail")

@hooks.hook("cassandra-relation-changed")
def cassandra_changed():
    # 'port' is used in legacy precise charm
    if not relation_get("rpc_port") and not relation_get("port"):
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
    add_contrail_api()
    add_metadata()

@hooks.hook("cassandra-relation-departed")
@hooks.hook("cassandra-relation-broken")
def cassandra_departed():
    if not units("cassandra"):
        remove_metadata()
        remove_contrail_api()
        config["cassandra-ready"] = False
    cassandra_relation()

@restart_on_change({"/etc/contrail/contrail-api.conf": ["supervisor-config"],
                    "/etc/contrail/contrail-device-manager.conf": ["supervisor-config"],
                    "/etc/contrail/contrail-discovery.conf": ["supervisor-config"],
                    "/etc/contrail/contrail-schema.conf": ["supervisor-config"],
                    "/etc/contrail/contrail-svc-monitor.conf": ["supervisor-config"],
                    "/etc/contrail/discovery.conf": ["supervisor-config"]})
def cassandra_relation():
    write_contrail_api_config()
    write_contrail_schema_config()
    write_discovery_config()
    write_contrail_svc_monitor_config()
    write_device_manager_config()

@hooks.hook("config-changed")
def config_changed():
    if config_get("contrail-api-configured"):
        configure_floating_ip_pools()
    vip = config.get("vip")
    for rid in relation_ids("contrail-api"):
        relation_set(relation_id=rid, vip=vip)
    for rid in relation_ids("contrail-discovery"):
        relation_set(relation_id=rid, vip=vip)

def config_get(key):
    try:
        return config[key]
    except KeyError:
        return None

def configure_floating_ip_pools():
    if is_leader():
        floating_pools = config.get("floating-ip-pools")
        previous_floating_pools = leader_get("floating-ip-pools")
        if floating_pools != previous_floating_pools:
            # create/destroy pools, activate/deactivate projects
            # according to new value
            pools = { (pool["project"],
                       pool["network"],
                       pool["pool-name"]): set(pool["target-projects"])
                      for pool in yaml.safe_load(floating_pools) } \
                    if floating_pools else {}
            previous_pools = {}
            if previous_floating_pools:
                for pool in yaml.safe_load(previous_floating_pools):
                    projects = pool["target-projects"]
                    name = (pool["project"], pool["network"], pool["pool-name"])
                    if name in pools:
                        previous_pools[name] = set(projects)
                    else:
                        floating_ip_pool_delete(name, projects)
            for name, projects in pools.iteritems():
                if name not in previous_pools:
                    floating_ip_pool_create(name, projects)
                else:
                    floating_ip_pool_update(name, projects, previous_pools[name])

            leader_set({"floating-ip-pools": floating_pools})

@hooks.hook("contrail-analytics-api-relation-changed")
def contrail_analytics_api_changed():
    if not relation_get("port"):
        log("Relation not ready")
        return
    contrail_analytics_api_relation()

@hooks.hook("contrail-analytics-api-relation-departed")
@hooks.hook("contrail-analytics-api-relation-broken")
@restart_on_change({"/etc/contrail/contrail-svc-monitor.conf": ["supervisor-config"]})
def contrail_analytics_api_relation():
    write_contrail_svc_monitor_config()

@hooks.hook("contrail-api-relation-joined")
def contrail_api_joined():
    if config_get("contrail-api-configured"):
        relation_set(port=api_port(), vip=config.get("vip"))

@hooks.hook("contrail-discovery-relation-joined")
def contrail_discovery_joined():
    relation_set(port=discovery_port(), vip=config.get("vip"))

@hooks.hook("contrail-ifmap-relation-joined")
def contrail_ifmap_joined():
    if is_leader():
        creds = leader_get("ifmap-creds")
        creds = json.loads(creds) if creds else {}

        # prune credentials because we can't remove them directly lp #1469731
        creds = { rid: { unit: units[unit]
                         for unit, units in
                         ((unit, creds[rid]) for unit in related_units(rid))
                         if unit in units }
                  for rid in relation_ids("contrail-ifmap")
                  if rid in creds }

        rid = relation_id()
        if rid not in creds:
            creds[rid] = {}
        cs = creds[rid]
        unit = remote_unit()
        if unit in cs:
            return
        # generate new credentials for unit
        cs[unit] = { "username": unit, "password": pwgen(32) }
        leader_set({"ifmap-creds": json.dumps(creds)})
        write_ifmap_config()
        service_restart("supervisor-config")
        relation_set(creds=json.dumps(cs))

def floating_ip_pool_create(name, projects):
    # create pool
    fq_network = "default-domain:" + ":".join(name[:2])
    contrail_floating_ip_create(fq_network, name[2])

    # activate pool for projects
    fq_pool_name = "default-domain:" + ":".join(name)
    for project in projects:
        fq_project = "default-domain:" + project
        contrail_floating_ip_use(fq_project, fq_pool_name)

def floating_ip_pool_delete(name, projects):
    # deactivate pool for projects
    fq_pool_name = "default-domain:" + ":".join(name)
    for project in projects:
        fq_project = "default-domain:" + project
        contrail_floating_ip_deactivate(fq_project, fq_pool_name)

    # delete pool
    fq_network = "default-domain:" + ":".join(name[:2])
    contrail_floating_ip_delete(fq_network, name[2])

def floating_ip_pool_update(name, projects, previous_projects):
    fq_pool_name = "default-domain:" + ":".join(name)

    # deactivate pool for projects
    for project in (previous_projects - projects):
        fq_project = "default-domain:" + project
        contrail_floating_ip_deactivate(fq_project, fq_pool_name)

    # activate pool for projects
    for project in (projects - previous_projects):
        fq_project = "default-domain:" + project
        contrail_floating_ip_use(fq_project, fq_pool_name)

@hooks.hook("http-services-relation-joined")
def http_services_joined():
    name = local_unit().replace("/", "-")
    addr = gethostbyname(unit_get("private-address"))
    services = [ { "service_name": "contrail-api",
                   "service_host": "0.0.0.0",
                   "service_port": 8082,
                   "service_options": [ "mode http", "balance leastconn", "option httpchk GET /Snh_SandeshUVECacheReq?x=NodeStatus HTTP/1.0" ],
                   "servers": [ [ name, addr, api_port(), "check port 8084" ] ] },
                 { "service_name": "contrail-discovery",
                   "service_host": "0.0.0.0",
                   "service_port": 5998,
                   "service_options": [ "mode http", "balance leastconn", "option httpchk GET /services HTTP/1.0" ],
                   "servers": [ [ name, addr, discovery_port(), "check" ] ] } ]
    relation_set(services=yaml.dump(services))

@hooks.hook("identity-admin-relation-changed")
def identity_admin_changed():
    if not relation_get("service_hostname"):
        log("Relation not ready")
        return
    identity_admin_relation()
    config["identity-admin-ready"] = True
    add_contrail_api()
    add_metadata()

@hooks.hook("identity-admin-relation-departed")
@hooks.hook("identity-admin-relation-broken")
def identity_admin_departed():
    if not units("identity-admin"):
        remove_metadata()
        remove_contrail_api()
        config["identity-admin-ready"] = False
    identity_admin_relation()

@restart_on_change({"/etc/contrail/contrail-api.conf": ["supervisor-config"],
                    "/etc/contrail/contrail-device-manager.conf": ["supervisor-config"],
                    "/etc/contrail/contrail-schema.conf": ["supervisor-config"],
                    "/etc/contrail/contrail-svc-monitor.conf": ["supervisor-config"]})
def identity_admin_relation():
    write_contrail_api_config()
    write_contrail_schema_config()
    write_contrail_svc_monitor_config()
    write_device_manager_config()
    write_vnc_api_config()
    if version_compare(CONTRAIL_VERSION, "3.0.2.0-34") >= 0:
        write_barbican_auth_config()

@hooks.hook()
def install():
    configure_installation_source(config["openstack-origin"])
    configure_sources(True, "install-sources", "install-keys")
    apt_upgrade(fatal=True, dist=True)
    apt_install(PACKAGES, fatal=True)

    contrail_version = dpkg_version("contrail-config")
    openstack_version = dpkg_version("neutron-common")
    if version_compare(contrail_version, "3.0.2.0-34") >= 0 \
       and version_compare(openstack_version, "2:7.0.0") >= 0:
        # install barbican packages
        apt_install(PACKAGES_BARBICAN, fatal=True)

    fix_services()
    write_nodemgr_config()
    service_restart("contrail-config-nodemgr")

@hooks.hook("leader-settings-changed")
@restart_on_change({"/etc/ifmap-server/basicauthusers.properties": ["supervisor-config"]})
def leader_changed():
    write_ifmap_config()
    creds = leader_get("ifmap-creds")
    creds = json.loads(creds) if creds else {}
    # set same credentials on relation
    for rid in relation_ids("contrail-ifmap"):
        if rid in creds:
            relation_set(relation_id=rid, creds=json.dumps(creds[rid]))

def main():
    try:
        hooks.execute(sys.argv)
    except UnregisteredHookError as e:
        log("Unknown hook {} - skipping.".format(e))

@hooks.hook("neutron-metadata-relation-changed")
def neutron_metadata_changed():
    if not relation_get("shared-secret"):
        log("Relation not ready")
        return
    config["neutron-metadata-ready"] = True
    add_metadata()

@hooks.hook("neutron-metadata-relation-departed")
@hooks.hook("neutron-metadata-relation-broken")
def neutron_metadata_departed():
    if not units("neutron-metadata"):
        remove_metadata()
        config["neutron-metadata-ready"] = False

def remove_contrail_api():
    if config_get("contrail-api-configured"):
        # unprovision configuration on 3.0.2.0+
        if version_compare(CONTRAIL_VERSION, "3.0.2.0-34") >= 0:
            unprovision_configuration()
        config["contrail-api-configured"] = False

def remove_metadata():
    if is_leader() and leader_get("metadata-provisioned"):
        # impossible to know if current hook is firing because
        # relation or leader is being removed lp #1469731
        if not relation_ids("cluster"):
            unprovision_metadata()
        leader_set({"metadata-provisioned": ""})

@hooks.hook("upgrade-charm")
def upgrade_charm():
    write_ifmap_config()
    write_contrail_api_config()
    write_contrail_schema_config()
    write_discovery_config()
    write_contrail_svc_monitor_config()
    write_device_manager_config()
    write_vnc_api_config()
    write_nodemgr_config()
    service_restart("supervisor-config")

@hooks.hook("zookeeper-relation-changed")
def zookeeper_changed():
    if not relation_get("port"):
        log("Relation not ready")
        return
    zookeeper_relation()
    config["zookeeper-ready"] = True
    add_contrail_api()
    add_metadata()

@hooks.hook("zookeeper-relation-departed")
@hooks.hook("zookeeper-relation-broken")
def zookeeper_departed():
    if not units("zookeeper"):
        remove_metadata()
        remove_contrail_api()
        config["zookeeper-ready"] = False
    zookeeper_relation()

@restart_on_change({"/etc/contrail/contrail-api.conf": ["supervisor-config"],
                    "/etc/contrail/contrail-device-manager.conf": ["supervisor-config"],
                    "/etc/contrail/contrail-discovery.conf": ["supervisor-config"],
                    "/etc/contrail/contrail-schema.conf": ["supervisor-config"],
                    "/etc/contrail/contrail-svc-monitor.conf": ["supervisor-config"],
                    "/etc/contrail/discovery.conf": ["supervisor-config"]})
def zookeeper_relation():
    write_contrail_api_config()
    write_contrail_schema_config()
    write_discovery_config()
    write_contrail_svc_monitor_config()
    write_device_manager_config()

if __name__ == "__main__":
    main()
