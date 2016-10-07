#!/usr/bin/env python

"""Delete Floating IP pool.

This is a workaround whilst OpenContrail doesn't provide the needed
functionality in *_floating_pool.py scripts.
"""

import argparse

from vnc_api import vnc_api

parser = argparse.ArgumentParser()

parser.add_argument("public_vn_name",
                    help="Colon separated fully qualified name")

parser.add_argument("floating_ip_pool_name",
                    help="Name of the floating IP pool")

parser.add_argument("--api_server_ip", help="IP address of api server",
                    default="127.0.0.1")

parser.add_argument("--api_server_port", help="Port of api server",
                    default="8082")

parser.add_argument("--admin_user", help="Name of keystone admin user")

parser.add_argument("--admin_password", help="Password of keystone admin user")

parser.add_argument("--admin_tenant_name",
                    help="Tenant name for keystone admin user")

args = parser.parse_args()

vnc_lib = vnc_api.VncApi(api_server_host=args.api_server_ip,
                         api_server_port=args.api_server_port,
                         username=args.admin_user,
                         password=args.admin_password,
                         tenant_name=args.admin_tenant_name)

name = args.public_vn_name.split(":")
name.append(args.floating_ip_pool_name)
vnc_lib.floating_ip_pool_delete(fq_name=name)
