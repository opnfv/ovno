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

import atexit
import subprocess
import threading
import time

from charmhelpers.core import hookenv


def loglog(filename, prefix='', level=hookenv.DEBUG):
    '''Mirror an arbitrary log file to the Juju hook log in the background.'''
    tailproc = subprocess.Popen(['tail', '-F', filename],
                                stdout=subprocess.PIPE,
                                universal_newlines=True)
    atexit.register(tailproc.terminate)

    def loglog_t(tailproc=tailproc):
        while True:
            line = tailproc.stdout.readline()
            if line:
                hookenv.log('{}{}'.format(prefix, line), level)
            else:
                time.sleep(0.1)
                continue

    t = threading.Thread(target=loglog_t, daemon=True)
    t.start()
