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

from charmhelpers.core.hookenv import (
    local_unit,
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

CONTRAIL_VERSION = dpkg_version("contrail-control")

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

def contrail_api_ctx():
    ctxs = [ { "api_server": gethostbyname(relation_get("private-address", unit, rid)),
               "api_port": port }
             for rid in relation_ids("contrail-api")
             for unit, port in
             ((unit, relation_get("port", unit, rid)) for unit in related_units(rid))
             if port ]
    return ctxs[0] if ctxs else {}

def contrail_ctx():
    return { "host_ip": gethostbyname(unit_get("private-address")) }

def contrail_discovery_ctx():
    ctxs = [ { "discovery_server": vip if vip \
                 else gethostbyname(relation_get("private-address", unit, rid)),
               "discovery_port": port }
             for rid in relation_ids("contrail-discovery")
             for unit, port, vip in
             ((unit, relation_get("port", unit, rid), relation_get("vip", unit, rid))
              for unit in related_units(rid))
             if port ]
    return ctxs[0] if ctxs else {}

def contrail_ifmap_ctx():
    ctxs = []
    unit = local_unit()
    for rid in relation_ids("contrail-ifmap"):
        for u in related_units(rid):
            creds = relation_get("creds", u, rid)
            if creds:
                creds = json.loads(creds)
                if unit in creds:
                    cs = creds[unit]
                    ctx = {}
                    ctx["ifmap_user"] = cs["username"]
                    ctx["ifmap_password"] = cs["password"]
                    ctxs.append(ctx)
    return ctxs[0] if ctxs else {}

@retry(timeout=300)
def contrail_provision_control(hostname, ip, router_asn, api_ip, api_port, op,
                               user, password, tenant):
    check_call(["contrail-provision-control",
                "--host_name", hostname,
                "--host_ip", ip,
                "--router_asn", str(router_asn),
                "--api_server_ip", api_ip,
                "--api_server_port", str(api_port),
                "--oper", op,
                "--admin_user", user,
                "--admin_password", password,
                "--admin_tenant_name", tenant])

def fix_nodemgr():
    # add files missing from contrail-nodemgr package
    shutil.copy("files/contrail-nodemgr-control.ini",
                "/etc/contrail/supervisord_control_files")
    pw = pwd.getpwnam("contrail")
    os.chown("/etc/contrail/supervisord_control_files/contrail-nodemgr-control.ini",
             pw.pw_uid, pw.pw_gid)
    shutil.copy("files/contrail-control-nodemgr", "/etc/init.d")
    os.chmod("/etc/init.d/contrail-control-nodemgr", 0755)

    # fake ntp status when inside a container
    if is_container():
        shutil.copy("files/ntpq-nodemgr", "/usr/local/bin/ntpq")

    service_restart("supervisor-control")

def fix_permissions():
    os.chmod("/etc/contrail", 0755)
    os.chown("/etc/contrail", 0, 0)

def identity_admin_ctx():
    ctxs = [ { "auth_host": gethostbyname(hostname),
               "auth_port": relation_get("service_port", unit, rid) }
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

def provision_control():
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
    log("Provisioning control {}".format(ip))
    contrail_provision_control(hostname, ip, 64512, api_ip, api_port, "add",
                               user, password, tenant)

def units(relation):
    """Return a list of units for the specified relation"""
    return [ unit for rid in relation_ids(relation)
                  for unit in related_units(rid) ]

def unprovision_control():
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
    log("Unprovisioning control {}".format(ip))
    contrail_provision_control(hostname, ip, 64512, api_ip, api_port, "del",
                               user, password, tenant)

def write_control_config():
    ctx = {}
    ctx.update(contrail_ctx())
    ctx.update(contrail_discovery_ctx())
    ctx.update(contrail_ifmap_ctx())
    target = "/etc/contrail/contrail-control.conf" \
             if version_compare(CONTRAIL_VERSION, "2.0") >= 0 \
             else "/etc/contrail/control-node.conf"
    render("control-node.conf", target, ctx, "root", "contrail", 0440)

def write_nodemgr_config():
    ctx = contrail_discovery_ctx()
    render("contrail-control-nodemgr.conf",
           "/etc/contrail/contrail-control-nodemgr.conf", ctx)

def write_vnc_api_config():
    ctx = {}
    ctx.update(contrail_api_ctx())
    ctx.update(identity_admin_ctx())
    render("vnc_api_lib.ini", "/etc/contrail/vnc_api_lib.ini", ctx)
