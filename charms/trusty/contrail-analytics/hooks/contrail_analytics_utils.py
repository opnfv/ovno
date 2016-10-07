import os
import pwd
import shutil
from socket import gaierror, gethostbyname, gethostname
from subprocess import (
    CalledProcessError,
    check_call,
    check_output
)

import apt_pkg
from apt_pkg import version_compare

from charmhelpers.core.hookenv import (
    config,
    log,
    related_units,
    relation_get,
    relation_ids,
    relation_type,
    remote_unit,
    unit_get
)

from charmhelpers.core.host import service_restart

from charmhelpers.core.templating import render

apt_pkg.init()

def dpkg_version(pkg):
    try:
        return check_output(["dpkg-query", "-f", "${Version}\\n", "-W", pkg]).rstrip()
    except CalledProcessError:
        return None

CONTRAIL_VERSION = dpkg_version("contrail-analytics")

config = config()

def contrail_ctx():
    return { "host_ip": gethostbyname(unit_get("private-address")) }

def cassandra_ctx():
    key = "native_transport_port" \
          if version_compare(CONTRAIL_VERSION, "3.0.2.0-34") >= 0 \
          else "rpc_port"
    servers = [ gethostbyname(relation_get("private-address", unit, rid))
                + ":" + port
                for rid in relation_ids("cassandra")
                for unit, port in
                ((unit, relation_get(key, unit, rid)) for unit in related_units(rid))
                if port ] \
              if config.get("cassandra-ready") else []
    return { "cassandra_servers": servers }

def cassandra_units():
    """Return a list of cassandra units"""
    return [ unit for rid in relation_ids("cassandra")
                  for unit in related_units(rid)
                  if relation_get("native_transport_port", unit, rid) ]

def contrail_api_ctx():
    ctxs = [ { "api_server": vip if vip \
                 else gethostbyname(relation_get("private-address", unit, rid)),
               "api_port": port }
             for rid in relation_ids("contrail-api")
             for unit, port, vip in
             ((unit, relation_get("port", unit, rid), relation_get("vip", unit, rid))
              for unit in related_units(rid))
             if port ]
    return ctxs[0] if ctxs else {}

def discovery_ctx():
    ctxs = [ { "disc_server_ip": vip if vip \
                 else gethostbyname(relation_get("private-address", unit, rid)),
               "disc_server_port": port }
             for rid in relation_ids("contrail-discovery")
             for unit, port, vip in
             ((unit, relation_get("port", unit, rid), relation_get("vip", unit, rid))
              for unit in related_units(rid))
             if port ]
    return ctxs[0] if ctxs else {}

def fix_hostname():
    # ensure hostname is resolvable
    hostname = gethostname()
    try:
        gethostbyname(hostname)
    except gaierror:
        check_call(["sed", "-E", "-i", "-e",
                    "/127.0.0.1[[:blank:]]+/a \\\n127.0.1.1 " + hostname,
                    "/etc/hosts"])

def fix_nodemgr():
    # add files missing from contrail-nodemgr package
    shutil.copy("files/contrail-nodemgr-analytics.ini",
                "/etc/contrail/supervisord_analytics_files")
    shutil.copy("files/contrail-analytics-api.ini",
                "/etc/contrail/supervisord_analytics_files")
    shutil.copy("files/contrail-collector.ini",
                "/etc/contrail/supervisord_analytics_files")
    shutil.copy("files/contrail-alarm-gen.ini",
                "/etc/contrail/supervisord_analytics_files")
    shutil.copy("files/contrail-topology.ini",
                "/etc/contrail/supervisord_analytics_files")
    shutil.copy("files/contrail-snmp-collector.ini",
                "/etc/contrail/supervisord_analytics_files")
    pw = pwd.getpwnam("contrail")
    os.chown("/etc/contrail/supervisord_analytics_files/contrail-nodemgr-analytics.ini",
             pw.pw_uid, pw.pw_gid)
    shutil.copy("files/contrail-analytics-nodemgr", "/etc/init.d")
    os.chmod("/etc/init.d/contrail-analytics-nodemgr", 0755)

    # fake ntp status when inside a container
    if is_container():
        shutil.copy("files/ntpq-nodemgr", "/usr/local/bin/ntpq")

    service_restart("supervisor-analytics")

def fix_permissions():
    os.chmod("/etc/contrail", 0755)
    os.chown("/etc/contrail", 0, 0)

def fix_services():
    # redis listens on localhost by default
    check_output(["sed", "-i", "-e",
                  "s/^bind /# bind /",
                  "/etc/redis/redis.conf"])
    service_restart("redis-server")

def identity_admin_ctx():
    ctxs = [ { "auth_host": gethostbyname(hostname),
               "auth_port": relation_get("service_port", unit, rid),
               "admin_user": relation_get("service_username", unit, rid),
               "admin_password": relation_get("service_password", unit, rid),
               "admin_tenant_name": relation_get("service_tenant_name", unit, rid) }
             for rid in relation_ids("identity-admin")
             for unit, hostname in
             ((unit, relation_get("service_hostname", unit, rid)) for unit in related_units(rid))
             if hostname ]
    return ctxs[0] if ctxs else {}

def is_container():
    """Return boolean determining if inside container"""
    try:
        check_call(["running-in-container"])
        return True
    except CalledProcessError:
        return False

def kafka_ctx():
    servers = [ gethostbyname(relation_get("private-address", unit, rid))
                + ":" + port
                for rid in relation_ids("kafka")
                for unit, port in
                ((unit, relation_get("port", unit, rid))
                 for unit in related_units(rid))
                if port ] \
              if config.get("kafka-ready") else []
    return { "kafka_servers": servers }

def kafka_units():
    """Return a list of kafka units"""
    return [ unit for rid in relation_ids("kafka")
                  for unit in related_units(rid)
                  if relation_get("port", unit, rid) ]

def provision_analytics():
    hostname = gethostname()
    ip = gethostbyname(unit_get("private-address"))
    api_ip, api_port = [ (gethostbyname(relation_get("private-address", unit, rid)),
                          port)
                         for rid in relation_ids("contrail-api")
                         for unit, port in
                         ((unit, relation_get("port", unit, rid)) for unit in related_units(rid))
                         if port ][0]
    user, password, tenant = [ (relation_get("service_username", unit, rid),
                                relation_get("service_password", unit, rid),
                                relation_get("service_tenant_name", unit, rid))
                               for rid in relation_ids("identity-admin")
                               for unit in related_units(rid)
                               if relation_get("service_hostname", unit, rid) ][0]
    log("Provisioning analytics {}".format(ip))
    check_call(["contrail-provision-analytics",
                "--host_name", hostname,
                "--host_ip", ip,
                "--api_server_ip", api_ip,
                "--api_server_port", str(api_port),
                "--oper", "add",
                "--admin_user", user,
                "--admin_password", password,
                "--admin_tenant_name", tenant])

def units(relation):
    """Return a list of units for the specified relation"""
    return [ unit for rid in relation_ids(relation)
                  for unit in related_units(rid) ]

def unprovision_analytics():
    if not remote_unit():
        return
    hostname = gethostname()
    ip = gethostbyname(unit_get("private-address"))
    relation = relation_type()
    api_ip = None
    api_port = None
    if relation == "contrail-api":
        api_ip = gethostbyname(relation_get("private-address"))
        api_port = relation_get("port")
    else:
        api_ip, api_port = [ (gethostbyname(relation_get("private-address", unit, rid)),
                              relation_get("port", unit, rid))
                             for rid in relation_ids("contrail-api")
                             for unit in related_units(rid) ][0]
    user = None
    password = None
    tenant = None
    if relation == "identity-admin":
        user = relation_get("service_username")
        password = relation_get("service_password")
        tenant = relation_get("service_tenant_name")
    else:
        user, password, tenant = [ (relation_get("service_username", unit, rid),
                                    relation_get("service_password", unit, rid),
                                    relation_get("service_tenant_name", unit, rid))
                                   for rid in relation_ids("identity-admin")
                                   for unit in related_units(rid) ][0]
    log("Unprovisioning analytics {}".format(ip))
    check_call(["contrail-provision-analytics",
                "--host_name", hostname,
                "--host_ip", ip,
                "--api_server_ip", api_ip,
                "--api_server_port", str(api_port),
                "--oper", "del",
                "--admin_user", user,
                "--admin_password", password,
                "--admin_tenant_name", tenant])

def write_alarm_config():
    ctx = {}
    ctx.update(contrail_ctx())
    ctx.update(kafka_ctx())
    ctx.update(zookeeper_ctx())
    ctx.update(discovery_ctx())
    render("contrail-alarm-gen.conf",
           "/etc/contrail/contrail-alarm-gen.conf", ctx)

def write_analytics_api_config():
    ctx = {}
    ctx.update(contrail_ctx())
    ctx.update(cassandra_ctx())
    ctx.update(discovery_ctx())
    ctx.update(contrail_api_ctx())
    render("contrail-analytics-api.conf",
           "/etc/contrail/contrail-analytics-api.conf", ctx)

def write_collector_config():
    ctx = {}
    ctx.update(contrail_ctx())
    ctx.update(cassandra_ctx())
    ctx.update(kafka_ctx())
    ctx.update(discovery_ctx())
    if version_compare(CONTRAIL_VERSION, "3.0.2.0-34") >= 0:
        ctx["zookeeper"] = True
        ctx.update(zookeeper_ctx())
    render("contrail-collector.conf",
           "/etc/contrail/contrail-collector.conf", ctx)

def write_nodemgr_config():
    ctx = discovery_ctx()
    render("contrail-analytics-nodemgr.conf",
           "/etc/contrail/contrail-analytics-nodemgr.conf", ctx)

def write_query_engine_config():
    ctx = {}
    ctx.update(cassandra_ctx())
    ctx.update(discovery_ctx())
    render("contrail-query-engine.conf",
           "/etc/contrail/contrail-query-engine.conf", ctx)

def write_snmp_collector_config():
    ctx = {}
    ctx.update(contrail_api_ctx())
    ctx.update(zookeeper_ctx())
    ctx.update(discovery_ctx())
    ctx.update(identity_admin_ctx())
    render("contrail-snmp-collector.conf",
           "/etc/contrail/contrail-snmp-collector.conf", ctx, "root",
           "contrail", 0440)

def write_topology_config():
    ctx = {}
    ctx.update(zookeeper_ctx())
    ctx.update(discovery_ctx())
    render("contrail-topology.conf",
           "/etc/contrail/contrail-topology.conf", ctx)

def write_vnc_api_config():
    ctx = {}
    ctx.update(contrail_api_ctx())
    ctx.update(identity_admin_ctx())
    render("vnc_api_lib.ini", "/etc/contrail/vnc_api_lib.ini", ctx)

def write_keystone_auth_config():
    ctx = {}
    ctx.update(contrail_api_ctx())
    ctx.update(identity_admin_ctx())
    render("contrail-keystone-auth.conf",
           "/etc/contrail/contrail-keystone-auth.conf", ctx)

def zookeeper_ctx():
    return { "zk_servers": [ gethostbyname(relation_get("private-address", unit, rid))
                             + ":" + port
                             for rid in relation_ids("zookeeper")
                             for unit, port in
                             ((unit, relation_get("port", unit, rid)) for unit in related_units(rid))
                             if port ] }
