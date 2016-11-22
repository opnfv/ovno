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
'''
charm-helpers mocks.
'''
import os.path
import shutil
import tempfile
from unittest.mock import patch

import yaml

from charmhelpers import fetch
from charmhelpers.core import hookenv


CHARM_DIR = os.path.abspath(os.path.join(
    os.path.dirname(__file__), os.pardir))


def mock_charmhelpers(test_case):
    hookenv.cache.clear()  # Clear the hookenv memorisation.

    mocks = []

    # Mock environment
    charm_dir = tempfile.TemporaryDirectory()
    test_case.addCleanup(charm_dir.cleanup)
    mock_env = patch.dict(os.environ, dict(CHARM_DIR=charm_dir.name))
    mock_env.start()
    test_case.addCleanup(mock_env.stop)
    shutil.copyfile(os.path.join(CHARM_DIR, 'metadata.yaml'),
                    os.path.join(charm_dir.name, 'metadata.yaml'))

    # Mock config.
    # Set items:
    #     hookenv.config()['foo'] = 'bar'
    # Reset 'previous' state:
    #     hookenv.config().save();
    #     hookenv.config().load_previous()
    config = hookenv.Config()
    tmp = tempfile.NamedTemporaryFile(suffix='.config')
    config.CONFIG_FILE_NAME = tmp.name
    test_case.addCleanup(tmp.close)
    with open(os.path.join(CHARM_DIR, 'config.yaml'), 'rb') as f:
        defaults = yaml.safe_load(f)['options']
    for k, v in defaults.items():
        opt_type = v.get('type', 'string')
        opt_val = v.get('default', None)
        if opt_val is None:
            config[k] = None
        elif opt_type == 'string':
            config[k] = str(opt_val)
        elif opt_type == 'int':
            config[k] = int(opt_val)
        elif opt_type == 'boolean':
            config[k] = bool(opt_val)

    def mock_config(scope=None):
        if scope is None:
            return config
        return config.get(scope, None)
    mocks.append(patch('charmhelpers.core.hookenv.config',
                       side_effect=mock_config, autospec=True))

    # Magic mocks.
    methods = [
        'charmhelpers.core.hookenv.log',
        'charmhelpers.core.hookenv.hook_name',
        'charmhelpers.core.hookenv.related_units',
        'charmhelpers.core.hookenv.relation_get',
        'charmhelpers.core.hookenv.relation_set',
        'charmhelpers.core.hookenv.relation_ids',
        'charmhelpers.core.hookenv.relation_type',
        'charmhelpers.core.hookenv.service_name',
        'charmhelpers.core.hookenv.local_unit',
        'charmhelpers.core.hookenv.unit_private_ip',
        'charmhelpers.core.hookenv.unit_public_ip',
        'charmhelpers.core.host.log',
        'charmhelpers.fetch.filter_installed_packages',
        'os.chown', 'os.fchown',
    ]
    for m in methods:
        mocks.append(patch(m, autospec=True))

    for mock in mocks:
        mock.start()
        test_case.addCleanup(mock.stop)

    hookenv.local_unit.return_value = 'service/1'

    def mock_unit_private_ip():
        return '10.20.0.{}'.format(hookenv.local_unit().split('/')[-1])
    hookenv.unit_private_ip.side_effect = mock_unit_private_ip

    def mock_unit_public_ip():
        return '10.30.0.{}'.format(hookenv.local_unit().split('/')[-1])
    hookenv.unit_public_ip.side_effect = mock_unit_public_ip

    def mock_service_name():
        return hookenv.local_unit().split('/')[0]
    hookenv.service_name.side_effect = mock_service_name

    hookenv.relation_ids.side_effect = (
        lambda x: ['{}:1'.format(x)] if x else [])
    hookenv.related_units.return_value = ('service/2', 'service/3')

    relinfos = dict()

    def mock_relation_set(relation_id=None, relation_settings=None, **kwargs):
        if relation_id is None:
            relation_id = hookenv.relation_id()
        unit = hookenv.local_unit()
        relinfo = mock_relation_get(unit=unit, rid=relation_id)
        if relation_settings is not None:
            relinfo.update(relation_settings)
        relinfo.update(kwargs)
        return None
    hookenv.relation_set.side_effect = mock_relation_set

    def mock_relation_get(attribute=None, unit=None, rid=None):
        if rid is None:
            rid = hookenv.relation_id()
        if unit is None:
            unit = hookenv.remove_unit()
        service, unit_num = unit.split('/')
        unit_num = int(unit_num)
        relinfos.setdefault(rid, {})
        relinfos[rid].setdefault(
            unit, {'private-address': '10.20.0.{}'.format(unit_num)})
        relinfo = relinfos[rid][unit]
        if attribute is None or attribute == '-':
            return relinfo
        return relinfo.get(attribute)
    hookenv.relation_get.side_effect = mock_relation_get

    def mock_chown(target, uid, gid):
        assert uid == 0
        assert gid == 0
        assert os.path.exists(target)
    os.chown.side_effect = mock_chown

    def mock_fchown(fd, uid, gid):
        assert uid == 0
        assert gid == 0
    os.fchown.side_effect = mock_fchown

    fetch.filter_installed_packages.side_effect = lambda pkgs: list(pkgs)

    def mock_relation_for_unit(unit=None, rid=None):
        if unit is None:
            unit = hookenv.remote_unit()
        service, unit_num = unit.split('/')
        unit_num = int(unit_num)
        return {'private-address': '10.20.0.{}'.format(unit_num)}
    hookenv.relation_for_unit.side_effect = mock_relation_for_unit

    def mock_chown(target, uid, gid):
        assert uid == 0
        assert gid == 0
        assert os.path.exists(target)
    os.chown.side_effect = mock_chown

    def mock_fchown(fd, uid, gid):
        assert uid == 0
        assert gid == 0
    os.fchown.side_effect = mock_fchown

    fetch.filter_installed_packages.side_effect = lambda pkgs: list(pkgs)
