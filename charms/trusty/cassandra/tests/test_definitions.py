#!.venv3/bin/python3

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

from itertools import chain
import functools
import unittest
from unittest.mock import patch

from charmhelpers.core import hookenv
from charmhelpers.core.services import ServiceManager

from tests.base import TestCaseBase

import definitions


patch = functools.partial(patch, autospec=True)


class TestDefinitions(TestCaseBase):
    def test_get_service_definitions(self):
        # We can't really test this in unit tests, but at least we can
        # ensure the basic data structure is returned and accepted.
        defs = definitions.get_service_definitions()
        self.assertIsInstance(defs, list)
        for d in defs:
            with self.subTest(d=d):
                self.assertIsInstance(d, dict)

    def test_get_service_definitions_open_ports(self):
        config = hookenv.config()
        defs = definitions.get_service_definitions()
        expected_ports = set([config['rpc_port'],
                              config['native_transport_port'],
                              config['storage_port'],
                              config['ssl_storage_port']])
        opened_ports = set(chain(*(d.get('ports', []) for d in defs)))
        self.assertSetEqual(opened_ports, expected_ports)

    def test_get_service_manager(self):
        self.assertIsInstance(definitions.get_service_manager(),
                              ServiceManager)

    @patch('helpers.get_unit_superusers')
    @patch('helpers.is_decommissioned')
    @patch('helpers.is_cassandra_running')
    def test_requires_live_node(self, is_running, is_decommissioned, get_sup):
        is_decommissioned.return_value = False  # Is not decommissioned.
        is_running.return_value = True  # Is running.
        get_sup.return_value = set([hookenv.local_unit()])  # Creds exist.

        self.assertTrue(bool(definitions.RequiresLiveNode()))

    @patch('helpers.get_unit_superusers')
    @patch('helpers.is_decommissioned')
    @patch('helpers.is_cassandra_running')
    def test_requires_live_node_decommissioned(self, is_running,
                                               is_decommissioned, get_sup):
        is_decommissioned.return_value = True  # Is decommissioned.
        is_running.return_value = True  # Is running.
        get_sup.return_value = set([hookenv.local_unit()])  # Creds exist.

        self.assertFalse(bool(definitions.RequiresLiveNode()))

    @patch('helpers.get_unit_superusers')
    @patch('helpers.is_decommissioned')
    @patch('helpers.is_cassandra_running')
    def test_requires_live_node_down(self, is_running,
                                     is_decommissioned, get_sup):
        is_decommissioned.return_value = False  # Is not decommissioned.
        is_running.return_value = False  # Is not running.
        get_sup.return_value = set([hookenv.local_unit()])  # Creds exist.

        self.assertFalse(bool(definitions.RequiresLiveNode()))

    @patch('helpers.get_unit_superusers')
    @patch('helpers.is_decommissioned')
    @patch('helpers.is_cassandra_running')
    def test_requires_live_node_creds(self, is_running,
                                      is_decommissioned, get_sup):
        is_decommissioned.return_value = False  # Is not decommissioned.
        is_running.return_value = True  # Is running.
        get_sup.return_value = set()  # Creds do not exist.

        self.assertFalse(bool(definitions.RequiresLiveNode()))


if __name__ == '__main__':
    unittest.main(verbosity=2)
