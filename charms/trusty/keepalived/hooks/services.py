#!/usr/bin/python

import os

from charmhelpers.core import host
from charmhelpers.core import hookenv
from charmhelpers.core.services.base import ServiceManager
from charmhelpers.core.services import helpers

import actions

SYSCTL_FILE = os.path.join(os.sep, 'etc', 'sysctl.d', '50-keepalived.conf')
KEEPALIVED_CONFIG_FILE = os.path.join(os.sep, 'etc', 'keepalived',
                                      'keepalived.conf')
config = hookenv.config()


def manage():
    manager = ServiceManager([
        {
            'service': 'keepalived',
            'required_data': [
                helpers.RequiredConfig('virtual-ip',
                                       'router-id'),
                {'is_leader': hookenv.is_leader()}
            ],
            'data_ready': [
                actions.log_start,
                helpers.template(
                    source='keepalived.conf',
                    target=KEEPALIVED_CONFIG_FILE,
                    perms=0o644
                )
            ],
            # keepalived has no "status" command
            'stop': [
                lambda arg: host.service_stop('keepalived')
            ],
            'start': [
                lambda arg: host.service_restart('keepalived')
            ],
        },
        {
            'service': 'procps',
            'required_data': [
                {'sysctl': {'net.ipv4.ip_nonlocal_bind': 1}},
            ],
            'data_ready': [
                helpers.template(
                    source='50-keepalived.conf',
                    target=SYSCTL_FILE,
                    perms=0o644
                )
            ],
        }
    ])
    manager.manage()
