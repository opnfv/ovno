from collections import OrderedDict
import functools
import os
import pwd
import shutil
from socket import gethostbyname, gethostname
from subprocess import (
    CalledProcessError,
    check_call,
    check_output
)
from time import sleep, time

import apt_pkg
from apt_pkg import version_compare
import json
import urllib2

from charmhelpers.core.hookenv import (
    config,
    leader_get,
    log,
    related_units,
    relation_get,
    relation_ids,
    relation_type,
    remote_unit,
    unit_get
)

from charmhelpers.core.host import (
    service_available,
    service_restart,
    service_stop
)

from charmhelpers.core.templating import render

apt_pkg.init()

def dpkg_version(pkg):
    try:
        return check_output(["dpkg-query", "-f", "${Version}\\n", "-W", pkg]).rstrip()
    except CalledProcessError:
        return None

CONTRAIL_VERSION = dpkg_version("contrail-config")

config = config()

def retry(f=None, timeout=10, delay=2):
    """Retry decorator.

    Provides a decorator that can be used to retry a function if it raises
    an exception.

    :param timeout: timeout in seconds (default 10)
    :param delay: retry delay in seconds (default 2)

    Examples::

        # retry fetch_url function
        @retry
        def fetch_url():
            # fetch url

        # retry fetch_url function for 60 secs
        @retry(timeout=60)
        def fetch_url():
            # fetch url
    """
    if not f:
        return functools.partial(retry, timeout=timeout, delay=delay)
    @functools.wraps(f)
    def func(*args, **kwargs):
        start = time()
        error = None
        while True:
            try:
                return f(*args, **kwargs)
            except Exception as e:
                error = e
            elapsed = time() - start
            if elapsed >= timeout:
                raise error
            remaining = timeout - elapsed
            if delay <= remaining:
                sleep(delay)
            else:
                sleep(remaining)
                raise error
    return func

def amqp_ctx():
    ctxs = []
    servers = OrderedDict()
    for rid in relation_ids("amqp"):
        for unit in related_units(rid):
            password = relation_get("password", unit, rid)
            if not password:
                continue
            ctxs.append({"rabbit_user": "contrail",
                         "rabbit_password": password,
                         "rabbit_vhost": "contrail"})
            vip = relation_get("vip", unit, rid)
            server = (vip if vip \
              else gethostbyname(relation_get("hostname", unit, rid))) + ":5672"
            servers[server] = None
    ctx = ctxs[0] if ctxs else {}
    ctx["rabbit_servers"] = servers.keys()
    return ctx

def analytics_api_ctx():
    ctxs = [ { "analytics_server_ip": vip if vip \
                 else gethostbyname(relation_get("private-address", unit, rid)),
               "analytics_server_port": port }
             for rid in relation_ids("contrail-analytics-api")
             for unit, port, vip in
             ((unit, relation_get("port", unit, rid), relation_get("vip", unit, rid))
              for unit in related_units(rid))
             if port ]
    return ctxs[0] if ctxs else {}

def api_port():
    return 8082

def cassandra_ctx():
    servers = [ gethostbyname(relation_get("private-address", unit, rid))
                + ":" + (rpc_port if rpc_port else port)
                for rid in relation_ids("cassandra")
                for unit, rpc_port, port in
                ((unit, relation_get("rpc_port", unit, rid), relation_get("port", unit, rid))
                 for unit in related_units(rid))
                if rpc_port or port ] \
              if config.get("cassandra-ready") else []
    return { "cassandra_servers": servers }

def cassandra_units():
    """Return a list of cassandra units"""
    return [ unit for rid in relation_ids("cassandra")
                  for unit in related_units(rid)
                  if relation_get("rpc_port", unit, rid) or relation_get("port", unit, rid) ]

@retry(timeout=300, delay=10)
def check_url(url):
    try:
        urllib2.urlopen(url)
    except urllib2.HTTPError:
        pass

def contrail_ctx():
    addr = gethostbyname(unit_get("private-address"))
    return { "api_port": api_port(),
             "ifmap_server": addr,
             "disc_server": addr,
             "disc_port": discovery_port() }

def contrail_floating_ip_create(network, name):
    user, password, tenant = [ (relation_get("service_username", unit, rid),
                                relation_get("service_password", unit, rid),
                                relation_get("service_tenant_name", unit, rid))
                               for rid in relation_ids("identity-admin")
                               for unit in related_units(rid) ][0]
    log("Creating floating ip pool {} for network {}".format(name, network))
    check_call(["python", "/usr/share/contrail-utils/create_floating_pool.py",
                "--public_vn_name", network,
                "--floating_ip_pool_name", name,
                "--api_server_ip", "127.0.0.1",
                "--api_server_port", str(api_port()),
                "--admin_user", user,
                "--admin_password", password,
                "--admin_tenant", tenant])

def contrail_floating_ip_deactivate(project, name):
    user, password, tenant = [ (relation_get("service_username", unit, rid),
                                relation_get("service_password", unit, rid),
                                relation_get("service_tenant_name", unit, rid))
                               for rid in relation_ids("identity-admin")
                               for unit in related_units(rid) ][0]
    log("Deactivating floating ip pool {} for project {}".format(name, project))
    check_call(["scripts/deactivate_floating_pool.py",
                "--api_server_ip", "127.0.0.1",
                "--api_server_port", str(api_port()),
                "--admin_user", user,
                "--admin_password", password,
                "--admin_tenant", tenant,
                project, name])

def contrail_floating_ip_delete(network, name):
    user, password, tenant = [ (relation_get("service_username", unit, rid),
                                relation_get("service_password", unit, rid),
                                relation_get("service_tenant_name", unit, rid))
                               for rid in relation_ids("identity-admin")
                               for unit in related_units(rid) ][0]
    log("Deleting floating ip pool {} for network {}".format(name, network))
    check_call(["scripts/delete_floating_pool.py",
                "--api_server_ip", "127.0.0.1",
                "--api_server_port", str(api_port()),
                "--admin_user", user,
                "--admin_password", password,
                "--admin_tenant", tenant,
                network, name])

def contrail_floating_ip_use(project, name):
    user, password, tenant = [ (relation_get("service_username", unit, rid),
                                relation_get("service_password", unit, rid),
                                relation_get("service_tenant_name", unit, rid))
                               for rid in relation_ids("identity-admin")
                               for unit in related_units(rid) ][0]
    log("Activating floating ip pool {} for project {}".format(name, project))
    check_call(["python", "/usr/share/contrail-utils/use_floating_pool.py",
                "--project_name", project,
                "--floating_ip_pool_name", name,
                "--api_server_ip", "127.0.0.1",
                "--api_server_port", str(api_port()),
                "--admin_user", user,
                "--admin_password", password,
                "--admin_tenant", tenant])

def contrail_ifmap_ctx():
    creds = []
    cs = leader_get("ifmap-creds")
    if cs:
        cs = json.loads(cs)
        for units in cs.itervalues():
            for c in units.itervalues():
                creds.append(c)
    return { "ifmap_creds": creds }

def discovery_port():
    return 5998

def fix_ifmap_server():
    # disable ifmap-server upstart service
    if service_available("ifmap-server"):
        service_stop("ifmap-server")
        with open("/etc/init/ifmap-server.override", "w") as conf:
            conf.write("manual\n")

    # use supervisord config
    shutil.copy("files/ifmap.ini", "/etc/contrail/supervisord_config_files")
    pw = pwd.getpwnam("contrail")
    os.chown("/etc/contrail/supervisord_config_files/ifmap.ini", pw.pw_uid,
             pw.pw_gid)
    shutil.copy("files/ifmap", "/etc/init.d")
    os.chmod("/etc/init.d/ifmap", 0755)

def fix_nodemgr():
    # add files missing from contrail-nodemgr package
    shutil.copy("files/contrail-nodemgr-config.ini",
                "/etc/contrail/supervisord_config_files")
    pw = pwd.getpwnam("contrail")
    os.chown("/etc/contrail/supervisord_config_files/contrail-nodemgr-config.ini",
             pw.pw_uid, pw.pw_gid)
    shutil.copy("files/contrail-config-nodemgr", "/etc/init.d")
    os.chmod("/etc/init.d/contrail-config-nodemgr", 0755)

    # fake ntp status when inside a container
    if is_container():
        shutil.copy("files/ntpq-nodemgr", "/usr/local/bin/ntpq")

def fix_permissions():
    os.chmod("/etc/contrail", 0755)
    os.chown("/etc/contrail", 0, 0)

def fix_scripts():
    version = dpkg_version("contrail-config")
    if version_compare(version, "2.01") >= 0:
        # supervisord and init scripts need correcting on contrail 2.01+
        for service in [ "contrail-api", "contrail-discovery" ]:
            # remove hardcoded port
            check_call(["sed", "-E", "-i", "-e",
                        "s/ --listen_port [^[:blank:]]+//",
                        "/etc/contrail/supervisord_config_files/{}.ini".format(service)])

            # fix init script
            check_call(["sed", "-i", "-e",
                        "s/`basename ${0}`$/\"`basename ${0}`:*\"/",
                        "/etc/init.d/{}".format(service)])

def fix_services():
    fix_permissions()
    fix_ifmap_server()
    fix_nodemgr()
    fix_scripts()
    service_restart("supervisor-config")

def identity_admin_ctx():
    ctxs = [ { "auth_host": gethostbyname(hostname),
               "auth_port": relation_get("service_port", unit, rid),
               "admin_user": relation_get("service_username", unit, rid),
               "admin_password": relation_get("service_password", unit, rid),
               "admin_tenant_name": relation_get("service_tenant_name", unit, rid),
               "auth_region": relation_get("service_region", unit, rid) }
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

def provision_configuration():
    hostname = gethostname()
    ip = gethostbyname(unit_get("private-address"))
    user, password, tenant = [ (relation_get("service_username", unit, rid),
                                relation_get("service_password", unit, rid),
                                relation_get("service_tenant_name", unit, rid))
                               for rid in relation_ids("identity-admin")
                               for unit in related_units(rid)
                               if relation_get("service_hostname", unit, rid) ][0]
    log("Provisioning configuration {}".format(ip))
    check_call(["contrail-provision-config",
                "--host_name", hostname,
                "--host_ip", ip,
                "--api_server_ip", "127.0.0.1",
                "--api_server_port", str(api_port()),
                "--oper", "add",
                "--admin_user", user,
                "--admin_password", password,
                "--admin_tenant_name", tenant])

def provision_metadata():
    ip = [ gethostbyname(relation_get("private-address", unit, rid))
           for rid in relation_ids("neutron-metadata")
           for unit in related_units(rid) ][0]
    user, password = [ (relation_get("service_username", unit, rid),
                        relation_get("service_password", unit, rid))
                       for rid in relation_ids("identity-admin")
                       for unit in related_units(rid)
                       if relation_get("service_hostname", unit, rid) ][0]
    log("Provisioning metadata service {}:8775".format(ip))
    check_call(["contrail-provision-linklocal",
                "--api_server_ip", "127.0.0.1",
                "--api_server_port", str(api_port()),
                "--linklocal_service_name", "metadata",
                "--linklocal_service_ip", "169.254.169.254",
                "--linklocal_service_port", "80",
                "--ipfabric_service_ip", ip,
                "--ipfabric_service_port", "8775",
                "--oper", "add",
                "--admin_user", user,
                "--admin_password", password])

def units(relation):
    """Return a list of units for the specified relation"""
    return [ unit for rid in relation_ids(relation)
                  for unit in related_units(rid) ]

def unprovision_configuration():
    if not remote_unit():
        return
    hostname = gethostname()
    ip = gethostbyname(unit_get("private-address"))
    relation = relation_type()
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
    log("Unprovisioning configuration {}".format(ip))
    check_call(["contrail-provision-config",
                "--host_name", hostname,
                "--host_ip", ip,
                "--api_server_ip", "127.0.0.1",
                "--api_server_port", str(api_port()),
                "--oper", "del",
                "--admin_user", user,
                "--admin_password", password,
                "--admin_tenant_name", tenant])

def unprovision_metadata():
    if not remote_unit():
        return
    relation = relation_type()
    ip = None
    if relation == "neutron-metadata":
        ip = gethostbyname(relation_get("private-address"))
    else:
        ip = [ gethostbyname(relation_get("private-address", unit, rid))
               for rid in relation_ids("neutron-metadata")
               for unit in related_units(rid) ][0]
    user = None
    password = None
    if relation == "identity-admin":
        user = relation_get("service_username")
        password = relation_get("service_password")
    else:
        user, password = [ (relation_get("service_username", unit, rid),
                            relation_get("service_password", unit, rid))
                           for rid in relation_ids("identity-admin")
                           for unit in related_units(rid) ][0]
    log("Unprovisioning metadata service {}:8775".format(ip))
    check_call(["contrail-provision-linklocal",
                "--api_server_ip", "127.0.0.1",
                "--api_server_port", str(api_port()),
                "--linklocal_service_name", "metadata",
                "--linklocal_service_ip", "169.254.169.254",
                "--linklocal_service_port", "80",
                "--ipfabric_service_ip", ip,
                "--ipfabric_service_port", "8775",
                "--oper", "del",
                "--admin_user", user,
                "--admin_password", password])

def write_barbican_auth_config():
    ctx = identity_admin_ctx()
    render("contrail-barbican-auth.conf",
           "/etc/contrail/contrail-barbican-auth.conf", ctx, "root", "contrail",
           0440)

def write_contrail_api_config():
    ctx = {}
    ctx.update(contrail_ctx())
    ctx.update(cassandra_ctx())
    ctx.update(zookeeper_ctx())
    ctx.update(amqp_ctx())
    ctx.update(identity_admin_ctx())
    render("contrail-api.conf", "/etc/contrail/contrail-api.conf", ctx, "root",
           "contrail", 0440)

def write_contrail_schema_config():
    ctx = {}
    ctx.update(cassandra_ctx())
    ctx.update(zookeeper_ctx())
    ctx.update(identity_admin_ctx())
    ctx.update(contrail_ctx())
    if version_compare(CONTRAIL_VERSION, "3.0") >= 0:
        ctx["rabbitmq"] = True
        ctx.update(amqp_ctx())
    render("contrail-schema.conf", "/etc/contrail/contrail-schema.conf",
           ctx, "root", "contrail", 0440)

def write_contrail_svc_monitor_config():
    ctx = {}
    ctx.update(contrail_ctx())
    ctx.update(cassandra_ctx())
    ctx.update(zookeeper_ctx())
    ctx.update(amqp_ctx())
    ctx.update(identity_admin_ctx())
    ctx.update(analytics_api_ctx())
    render("contrail-svc-monitor.conf",
           "/etc/contrail/contrail-svc-monitor.conf", ctx, "root", "contrail",
           0440)

def write_device_manager_config():
    ctx = {}
    ctx.update(contrail_ctx())
    ctx.update(zookeeper_ctx())
    ctx.update(cassandra_ctx())
    ctx.update(amqp_ctx())
    ctx.update(identity_admin_ctx())
    render("contrail-device-manager.conf",
           "/etc/contrail/contrail-device-manager.conf", ctx, "root",
           "contrail", 0440)

def write_discovery_config():
    ctx = {}
    ctx.update(zookeeper_ctx())
    ctx.update(cassandra_ctx())
    target = "/etc/contrail/contrail-discovery.conf" \
             if version_compare(CONTRAIL_VERSION, "1.20~") >= 0 \
             else "/etc/contrail/discovery.conf"
    render("discovery.conf", target, ctx)

def write_ifmap_config():
    ctx = contrail_ifmap_ctx()
    render("basicauthusers.properties",
           "/etc/ifmap-server/basicauthusers.properties", ctx, "root",
           "contrail", 0440)

def write_nodemgr_config():
    ctx = contrail_ctx()
    render("contrail-config-nodemgr.conf",
           "/etc/contrail/contrail-config-nodemgr.conf", ctx)

def write_vnc_api_config():
    ctx = {}
    ctx.update(contrail_ctx())
    ctx.update(identity_admin_ctx())
    render("vnc_api_lib.ini", "/etc/contrail/vnc_api_lib.ini", ctx)

def zookeeper_ctx():
    return { "zk_servers": [ gethostbyname(relation_get("private-address", unit, rid))
                             + ":" + port
                             for rid in relation_ids("zookeeper")
                             for unit, port in
                             ((unit, relation_get("port", unit, rid)) for unit in related_units(rid))
                             if port ] }
