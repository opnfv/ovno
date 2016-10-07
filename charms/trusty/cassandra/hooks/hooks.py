#!/usr/bin/python3
# Copyright 2015 Canonical Ltd.
#
# This file is part of the Cassandra Charm for Juju.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License version 3, as
# published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranties of
# MERCHANTABILITY, SATISFACTORY QUALITY, or FITNESS FOR A PARTICULAR
# PURPOSE.  See the GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
from charmhelpers import fetch
from charmhelpers.core import hookenv


def set_proxy():
    import os
    config = hookenv.config()
    if config['http_proxy']:
        os.environ['ftp_proxy'] = config['http_proxy']
        os.environ['http_proxy'] = config['http_proxy']
        os.environ['https_proxy'] = config['http_proxy']


def bootstrap():
    try:
        import bcrypt     # NOQA: flake8
        import cassandra  # NOQA: flake8
    except ImportError:
        packages = ['python3-bcrypt', 'python3-cassandra']
        set_proxy()
        fetch.configure_sources(update=True)
        fetch.apt_install(packages, fatal=True)
        import bcrypt     # NOQA: flake8
        import cassandra  # NOQA: flake8


def default_hook():
    if not hookenv.has_juju_version('1.24'):
        hookenv.status_set('blocked', 'Requires Juju 1.24 or higher')
        # Error state, since we don't have 1.24 to give a nice blocked state.
        raise SystemExit(1)

    # These need to be imported after bootstrap() or required Python
    # packages may not have been installed.
    import definitions

    # Only useful for debugging, or perhaps have this enabled with a config
    # option?
    # from loglog import loglog
    # loglog('/var/log/cassandra/system.log', prefix='C*: ')

    hookenv.log('*** {} Hook Start'.format(hookenv.hook_name()))
    sm = definitions.get_service_manager()
    sm.manage()
    hookenv.log('*** {} Hook Done'.format(hookenv.hook_name()))
