#!/usr/bin/env python

import sys

import json

from charmhelpers.contrib.openstack.utils import configure_installation_source

from charmhelpers.core.hookenv import (
    Hooks,
    UnregisteredHookError,
    config,
    local_unit,
    log,
    relation_get
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

from contrail_control_utils import (
    fix_nodemgr,
    fix_permissions,
    provision_control,
    units,
    unprovision_control,
    write_control_config,
    write_nodemgr_config,
    write_vnc_api_config
)

PACKAGES = [ "contrail-control", "contrail-utils", "contrail-nodemgr" ]

hooks = Hooks()
config = config()

def add_control():
    # check relation dependencies
    if not config_get("control-provisioned") \
       and config_get("contrail-api-ready") \
       and config_get("contrail-discovery-ready") \
       and config_get("contrail-ifmap-ready") \
       and config_get("identity-admin-ready"):
        provision_control()
        config["control-provisioned"] = True

@hooks.hook("config-changed")
def config_changed():
    pass

def config_get(key):
    try:
        return config[key]
    except KeyError:
        return None

@hooks.hook("contrail-api-relation-changed")
def contrail_api_changed():
    if not relation_get("port"):
        log("Relation not ready")
        return
    write_vnc_api_config()
    config["contrail-api-ready"] = True
    add_control()

@hooks.hook("contrail-api-relation-departed")
@hooks.hook("contrail-api-relation-broken")
def contrail_api_departed():
    if not units("contrail-api"):
        remove_control()
        config["contrail-api-ready"] = False
    write_vnc_api_config()

@hooks.hook("contrail-discovery-relation-changed")
def contrail_discovery_changed():
    if not relation_get("port"):
        log("Relation not ready")
        return
    contrail_discovery_relation()
    config["contrail-discovery-ready"] = True
    add_control()

@hooks.hook("contrail-discovery-relation-departed")
@hooks.hook("contrail-discovery-relation-broken")
def contrail_discovery_departed():
    if not units("contrail-discovery"):
        remove_control()
        config["contrail-discovery-ready"] = False
    contrail_discovery_relation()

@restart_on_change({"/etc/contrail/contrail-control.conf": ["contrail-control"],
                    "/etc/contrail/control-node.conf": ["contrail-control"],
                    "/etc/contrail/contrail-control-nodemgr.conf": ["contrail-control-nodemgr"]})
def contrail_discovery_relation():
    write_control_config()
    write_nodemgr_config()

@hooks.hook("contrail-ifmap-relation-changed")
def contrail_ifmap_changed():
    creds = relation_get("creds")
    creds = json.loads(creds) if creds else {}
    if local_unit() not in creds:
        log("Relation not ready")
        return
    contrail_ifmap_relation()
    config["contrail-ifmap-ready"] = True
    add_control()

@hooks.hook("contrail-ifmap-relation-departed")
@hooks.hook("contrail-ifmap-relation-broken")
def contrail_ifmap_departed():
    if not units("contrail-ifmap"):
        remove_control()
        config["contrail-ifmap-ready"] = False
    contrail_ifmap_relation()

@restart_on_change({"/etc/contrail/contrail-control.conf": ["contrail-control"],
                    "/etc/contrail/control-node.conf": ["contrail-control"]})
def contrail_ifmap_relation():
    write_control_config()

@hooks.hook("identity-admin-relation-changed")
def identity_admin_changed():
    if not relation_get("service_hostname"):
        log("Relation not ready")
        return
    write_vnc_api_config()
    config["identity-admin-ready"] = True
    add_control()

@hooks.hook("identity-admin-relation-departed")
@hooks.hook("identity-admin-relation-broken")
def identity_admin_departed():
    if not units("identity-admin"):
        remove_control()
        config["identity-admin-ready"] = False
    write_vnc_api_config()

@hooks.hook()
def install():
    configure_installation_source(config["openstack-origin"])
    configure_sources(True, "install-sources", "install-keys")
    apt_upgrade(fatal=True, dist=True)
    apt_install(PACKAGES, fatal=True)
    fix_permissions()
    fix_nodemgr()

def main():
    try:
        hooks.execute(sys.argv)
    except UnregisteredHookError as e:
        log("Unknown hook {} - skipping.".format(e))

def remove_control():
    if config_get("control-provisioned"):
        unprovision_control()
        config["control-provisioned"] = False

@hooks.hook("upgrade-charm")
def upgrade_charm():
    write_control_config()
    write_nodemgr_config()
    service_restart("supervisor-control")

if __name__ == "__main__":
    main()
