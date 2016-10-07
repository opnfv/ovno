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

import functools
from itertools import count
import unittest
from unittest.mock import patch

from testing.mocks import mock_charmhelpers

patch = functools.partial(patch, autospec=True)  # autospec by default.


class TestCaseBase(unittest.TestCase):
    def setUp(self):
        super(TestCaseBase, self).setUp()

        mock_charmhelpers(self)

        is_lxc = patch('helpers.is_lxc', return_value=False)
        is_lxc.start()
        self.addCleanup(is_lxc.stop)

        emit = patch('helpers.emit')
        emit.start()
        self.addCleanup(emit.stop)

        time = patch('time.time', side_effect=count(1))
        time.start()
        self.addCleanup(time.stop)
