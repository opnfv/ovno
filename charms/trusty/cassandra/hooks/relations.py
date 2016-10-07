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

import os.path

import yaml

from charmhelpers.core import hookenv, host
from charmhelpers.core.hookenv import log, WARNING
from charmhelpers.core.services.helpers import RelationContext

from coordinator import coordinator


class PeerRelation(RelationContext):
    interface = 'cassandra-cluster'
    name = 'cluster'

    def is_ready(self):
        # All units except the leader need to wait until the peer
        # relation is available.
        if coordinator.relid is not None or hookenv.is_leader():
            return True
        return False


# FOR CHARMHELPERS (if we can integrate Juju 1.24 storage too)
class StorageRelation(RelationContext):
    '''Wait for the block storage mount to become available.

    Charms using this should add a 'wait_for_storage_broker' boolean
    configuration option in their config.yaml file. This is necessary
    to avoid potential data loss race conditions, because otherwise a
    unit will be started up using local disk before it becomes aware
    that it should be using external storage.

    'relname' is the relation name.

    'mountpount' is the mountpoint. Use the default if you have a single
    block storage broker relation. The default is calculated to avoid
    configs using the unit name (/srv/${service}_${unitnumber}).
    '''
    interface = 'block-storage'
    mountpoint = None

    def __init__(self, name=None, mountpoint=None):
        if name is None:
            name = self._get_relation_name()
        super(StorageRelation, self).__init__(name)

        if mountpoint is None:
            mountpoint = os.path.join('/srv/',
                                      hookenv.local_unit().replace('/', '_'))
        self._requested_mountpoint = mountpoint

        if len(self.get('data', [])) == 0:
            self.mountpoint = None
        elif mountpoint == self['data'][0].get('mountpoint', None):
            self.mountpoint = mountpoint
        else:
            self.mountpoint = None

    def _get_relation_name(self):
        with open(os.path.join(hookenv.charm_dir(),
                               'metadata.yaml'), 'r') as mdf:
            md = yaml.safe_load(mdf)
        for section in ['requires', 'provides']:
            for relname in md.get(section, {}).keys():
                if md[section][relname]['interface'] == 'block-storage':
                    return relname
        raise LookupError('No block-storage relation defined')

    def is_ready(self):
        if hookenv.config('wait_for_storage_broker'):
            if self.mountpoint:
                log("External storage mounted at {}".format(self.mountpoint))
                return True
            else:
                log("Waiting for block storage broker to mount {}".format(
                    self._requested_mountpoint), WARNING)
                return False
        return True

    def provide_data(self, remote_service, service_ready):
        hookenv.log('Requesting mountpoint {} from {}'
                    .format(self._requested_mountpoint, remote_service))
        return dict(mountpoint=self._requested_mountpoint)

    def needs_remount(self):
        config = hookenv.config()
        return config.get('live_mountpoint') != self.mountpoint

    def migrate(self, src_dir, subdir):
        assert self.needs_remount()
        assert subdir, 'Can only migrate to a subdirectory on a mount'

        config = hookenv.config()
        config['live_mountpoint'] = self.mountpoint

        if self.mountpoint is None:
            hookenv.log('External storage AND DATA gone.'
                        'Reverting to original local storage', WARNING)
            return

        dst_dir = os.path.join(self.mountpoint, subdir)
        if os.path.exists(dst_dir):
            hookenv.log('{} already exists. Not migrating data.'.format(
                dst_dir))
            return

        # We are migrating the contents of src_dir, so we want a
        # trailing slash to ensure rsync's behavior.
        if not src_dir.endswith('/'):
            src_dir += '/'

        # We don't migrate data directly into the new destination,
        # which allows us to detect a failed migration and recover.
        tmp_dst_dir = dst_dir + '.migrating'
        hookenv.log('Migrating data from {} to {}'.format(
            src_dir, tmp_dst_dir))
        host.rsync(src_dir, tmp_dst_dir, flags='-av')

        hookenv.log('Moving {} to {}'.format(tmp_dst_dir, dst_dir))
        os.rename(tmp_dst_dir, dst_dir)

        assert not self.needs_remount()
