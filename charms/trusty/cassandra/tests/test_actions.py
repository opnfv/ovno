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

import errno
from itertools import repeat
import os.path
import re
import shutil
import subprocess
import tempfile
from textwrap import dedent
import unittest
from unittest.mock import ANY, call, patch, sentinel
import yaml

import cassandra
from charmhelpers.core import hookenv

from tests.base import TestCaseBase
import actions
from coordinator import coordinator
import helpers


class TestActions(TestCaseBase):
    def test_action_wrapper(self):
        @actions.action
        def somefunc(*args, **kw):
            return 42, args, kw

        hookenv.hook_name.return_value = 'catch-fire'

        # The wrapper stripts the servicename argument, which we have no
        # use for, logs a message and invokes the wrapped function.
        hookenv.remote_unit.return_value = None
        self.assertEqual(somefunc('sn', 1, foo=4), (42, (1,), dict(foo=4)))
        hookenv.log.assert_called_once_with('** Action catch-fire/somefunc')

        # Different log message if there is a remote unit.
        hookenv.log.reset_mock()
        os.environ['JUJU_REMOTE_UNIT'] = 'foo'
        self.assertEqual(somefunc('sn', 1, foo=4), (42, (1,), dict(foo=4)))
        hookenv.log.assert_called_once_with(
            '** Action catch-fire/somefunc (foo)')

    def test_revert_unchangeable_config(self):
        config = hookenv.config()

        self.assertIn('datacenter', actions.UNCHANGEABLE_KEYS)

        # In the first hook, revert does nothing as there is nothing to
        # revert too.
        config['datacenter'] = 'mission_control'
        self.assertTrue(config.changed('datacenter'))
        actions.revert_unchangeable_config('')
        self.assertEqual(config['datacenter'], 'mission_control')

        config.save()
        config.load_previous()
        config['datacenter'] = 'orbital_1'

        actions.revert_unchangeable_config('')
        self.assertEqual(config['datacenter'], 'mission_control')  # Reverted

        hookenv.log.assert_any_call(ANY, hookenv.ERROR)  # Logged the problem.

    @patch('charmhelpers.core.hookenv.is_leader')
    def test_leader_only(self, is_leader):

        @actions.leader_only
        def f(*args, **kw):
            return args, kw

        is_leader.return_value = False
        self.assertIsNone(f(1, foo='bar'))

        is_leader.return_value = True
        self.assertEqual(f(1, foo='bar'), ((1,), dict(foo='bar')))

    def test_set_proxy(self):
        # NB. Environment is already mocked.
        os.environ['http_proxy'] = ''
        os.environ['https_proxy'] = ''
        actions.set_proxy('')
        self.assertEqual(os.environ['http_proxy'], '')
        self.assertEqual(os.environ['https_proxy'], '')
        hookenv.config()['http_proxy'] = 'foo'
        actions.set_proxy('')
        self.assertEqual(os.environ['http_proxy'], 'foo')
        self.assertEqual(os.environ['https_proxy'], 'foo')

    @patch('subprocess.check_call')
    def test_preinstall(self, check_call):
        # Noop if there are no preinstall hooks found running the
        # install hook.
        hookenv.hook_name.return_value = 'install'
        actions.preinstall('')
        self.assertFalse(check_call.called)
        hookenv.log.assert_any_call('No preinstall hooks found')

        # If preinstall hooks are found running the install hook,
        # the preinstall hooks are run.
        hook_dirs = []
        hook_files = []
        for i in range(1, 3):
            hook_dirs.append(os.path.join(hookenv.charm_dir(),
                                          'exec.d', str(i)))
            hook_files.append(os.path.join(hook_dirs[-1], 'charm-pre-install'))

            os.makedirs(hook_dirs[-1])
            with open(hook_files[-1], 'w') as f1:
                print('mocked', file=f1)
            os.chmod(hook_files[-1], 0o755)

        check_call.reset_mock()
        actions.preinstall('')

        calls = [call(['sh', '-c', f2]) for f2 in hook_files]
        check_call.assert_has_calls(calls)

        # If a preinstall hook is not executable, a warning is raised.
        hook_dir = os.path.join(hookenv.charm_dir(), 'exec.d', '55')
        hook_file = os.path.join(hook_dir, 'charm-pre-install')
        os.makedirs(hook_dir)
        with open(hook_file, 'w') as f1:
            print('whoops', file=f1)
        os.chmod(hook_file, 0o644)
        check_call.reset_mock()
        hookenv.log.reset_mock()
        actions.preinstall('')
        check_call.assert_has_calls(calls)  # Only previous hooks run.
        hookenv.log.assert_has_calls([
            call(ANY),
            call(ANY),
            call(ANY, hookenv.WARNING)])

        # Nothing happens if the install hook is not being run.
        hookenv.hook_name.return_value = 'config-changed'
        check_call.reset_mock()
        actions.preinstall('')
        self.assertFalse(check_call.called)

    @patch('subprocess.check_call')
    def test_swapoff(self, check_call):
        fstab = (
            b'UUID=abc / ext4 errors=remount-ro 0 1\n'
            b'/dev/mapper/cryptswap1 none swap sw 0 0')
        with tempfile.NamedTemporaryFile() as f:
            f.write(fstab)
            f.flush()
            actions.swapoff('', f.name)
            f.seek(0)
            self.assertTrue(b'swap' not in f.read())

        check_call.assert_called_once_with(['swapoff', '-a'])

    @patch('subprocess.check_call')
    def test_swapoff_fails(self, check_call):
        check_call.side_effect = RuntimeError()
        actions.swapoff('', '')
        # A warning is generated if swapoff fails.
        hookenv.log.assert_any_call(ANY, hookenv.WARNING)

    @patch('subprocess.check_call')
    def test_swapoff_lxc(self, check_call):
        # Under LXC, the swapoff action does nothing except log.
        helpers.is_lxc.return_value = True
        actions.swapoff('')
        self.assertFalse(check_call.called)

    @patch('charmhelpers.fetch.configure_sources')
    def test_configure_sources(self, configure_sources):
        config = hookenv.config()

        # fetch.configure_sources called the first time
        actions.configure_sources('')
        configure_sources.assert_called_once_with(True)

        # fetch.configure_sources not called if relevant config is unchanged.
        config.save()
        config.load_previous()
        configure_sources.reset_mock()
        actions.configure_sources('')
        self.assertFalse(configure_sources.called)

        # Changing install_sources causes fetch.configure_sources to be
        # called.
        config.save()
        config.load_previous()
        configure_sources.reset_mock()
        config['install_sources'] = 'foo'
        actions.configure_sources('')
        configure_sources.assert_called_once_with(True)

        # Changing install_keys causes fetch.configure_sources to be
        # called.
        config.save()
        config.load_previous()
        configure_sources.reset_mock()
        config['install_keys'] = 'foo'
        actions.configure_sources('')
        configure_sources.assert_called_once_with(True)

    @patch('charmhelpers.core.hookenv.charm_dir')
    @patch('subprocess.check_call')
    def test_add_implicit_package_signing_keys(self, check_call, charm_dir):
        charm_dir.return_value = os.path.join(os.path.dirname(__file__),
                                              os.pardir)
        actions.add_implicit_package_signing_keys('')

        keys = ['apache', 'datastax']

        self.assertEqual(check_call.call_count, len(keys))

        for k in keys:
            with self.subTest(key=k):
                path = os.path.join(hookenv.charm_dir(),
                                    'lib', '{}.key'.format(k))
                self.assertTrue(os.path.exists(path))
                check_call.assert_any_call(['apt-key', 'add', path],
                                           stdin=subprocess.DEVNULL)

    @patch('charmhelpers.core.host.write_file')
    @patch('subprocess.check_call')
    def test_reset_sysctl(self, check_call, write_file):
        actions.reset_sysctl('')

        ctl_file = '/etc/sysctl.d/99-cassandra.conf'
        # Magic value per Cassandra best practice.
        write_file.assert_called_once_with(ctl_file,
                                           b"vm.max_map_count = 131072\n")
        check_call.assert_called_once_with(['sysctl', '-p',
                                            '/etc/sysctl.d/99-cassandra.conf'])

    @patch('subprocess.check_call')
    @patch('charmhelpers.core.host.write_file')
    def test_reset_sysctl_expected_fails(self, write_file, check_call):
        check_call.side_effect = repeat(OSError(errno.EACCES,
                                                'Permission Denied'))
        actions.reset_sysctl('')
        # A warning is generated if permission denied was raised.
        hookenv.log.assert_any_call(ANY, hookenv.WARNING)

    @patch('subprocess.check_call')
    @patch('charmhelpers.core.host.write_file')
    def test_reset_sysctl_fails_badly(self, write_file, check_call):
        # Other OSErrors are reraised since we don't know how to handle
        # them.
        check_call.side_effect = repeat(OSError(errno.EFAULT, 'Whoops'))
        self.assertRaises(OSError, actions.reset_sysctl, '')

    @patch('subprocess.check_call')
    def test_reset_sysctl_lxc(self, check_call):
        helpers.is_lxc.return_value = True
        actions.reset_sysctl('')
        self.assertFalse(check_call.called)
        hookenv.log.assert_any_call('In an LXC. '
                                    'Leaving sysctl unchanged.')

    @patch('helpers.get_cassandra_packages')
    @patch('helpers.ensure_package_status')
    def test_ensure_cassandra_package_status(self, ensure_package_status,
                                             get_cassandra_packages):
        get_cassandra_packages.return_value = sentinel.cassandra_packages
        actions.ensure_cassandra_package_status('')
        ensure_package_status.assert_called_once_with(
            sentinel.cassandra_packages)

    @patch('subprocess.check_call')
    @patch('helpers.get_jre')
    @patch('helpers.get_cassandra_packages')
    @patch('helpers.install_packages')
    def test_install_cassandra_packages(self, install_packages,
                                        get_cassandra_packages,
                                        get_jre, check_call):
        get_cassandra_packages.return_value = sentinel.cassandra_packages
        get_jre.return_value = 'openjdk'
        actions.install_cassandra_packages('')
        install_packages.assert_called_once_with(sentinel.cassandra_packages)
        check_call.assert_called_once_with(['update-java-alternatives',
                                            '--jre-headless', '--set',
                                            'java-1.8.0-openjdk-amd64'])

    @patch('subprocess.check_call')
    @patch('helpers.get_jre')
    @patch('helpers.get_cassandra_packages')
    @patch('helpers.install_packages')
    def test_install_cassandra_packages_oracle(self, install_packages,
                                               get_cassandra_packages,
                                               get_jre, check_call):
        get_cassandra_packages.return_value = sentinel.cassandra_packages
        get_jre.return_value = 'oracle'
        actions.install_cassandra_packages('')
        install_packages.assert_called_once_with(sentinel.cassandra_packages)
        # No alternatives selected, as the Oracle JRE installer method
        # handles this.
        self.assertFalse(check_call.called)

    @patch('actions._install_oracle_jre_tarball')
    @patch('actions._fetch_oracle_jre')
    def test_install_oracle_jre(self, fetch, install_tarball):
        fetch.return_value = sentinel.tarball

        actions.install_oracle_jre('')
        self.assertFalse(fetch.called)
        self.assertFalse(install_tarball.called)

        hookenv.config()['jre'] = 'oracle'
        actions.install_oracle_jre('')
        fetch.assert_called_once_with()
        install_tarball.assert_called_once_with(sentinel.tarball)

    @patch('helpers.status_set')
    @patch('urllib.request')
    def test_fetch_oracle_jre(self, req, status_set):
        config = hookenv.config()
        url = 'https://foo.example.com/server-jre-7u42-linux-x64.tar.gz'
        expected_tarball = os.path.join(hookenv.charm_dir(), 'lib',
                                        'server-jre-7u42-linux-x64.tar.gz')
        config['private_jre_url'] = url

        # Create a dummy tarball, since the mock urlretrieve won't.
        os.makedirs(os.path.dirname(expected_tarball))
        with open(expected_tarball, 'w'):
            pass  # Empty file

        self.assertEqual(actions._fetch_oracle_jre(), expected_tarball)
        req.urlretrieve.assert_called_once_with(url, expected_tarball)

    def test_fetch_oracle_jre_local(self):
        # Create an existing tarball. If it is found, it will be used
        # without needing to specify a remote url or actually download
        # anything.
        expected_tarball = os.path.join(hookenv.charm_dir(), 'lib',
                                        'server-jre-7u42-linux-x64.tar.gz')
        os.makedirs(os.path.dirname(expected_tarball))
        with open(expected_tarball, 'w'):
            pass  # Empty file

        self.assertEqual(actions._fetch_oracle_jre(), expected_tarball)

    @patch('helpers.status_set')
    def test_fetch_oracle_jre_notfound(self, status_set):
        with self.assertRaises(SystemExit) as x:
            actions._fetch_oracle_jre()
            self.assertEqual(x.code, 0)
            status_set.assert_called_once_with('blocked', ANY)

    @patch('subprocess.check_call')
    @patch('charmhelpers.core.host.mkdir')
    @patch('os.path.isdir')
    def test_install_oracle_jre_tarball(self, isdir, mkdir, check_call):
        isdir.return_value = False

        dest = '/usr/lib/jvm/java-8-oracle'

        actions._install_oracle_jre_tarball(sentinel.tarball)
        mkdir.assert_called_once_with(dest)
        check_call.assert_has_calls([
            call(['tar', '-xz', '-C', dest,
                  '--strip-components=1', '-f', sentinel.tarball]),
            call(['update-alternatives', '--install',
                  '/usr/bin/java', 'java',
                  os.path.join(dest, 'bin', 'java'), '1']),
            call(['update-alternatives', '--set', 'java',
                  os.path.join(dest, 'bin', 'java')]),
            call(['update-alternatives', '--install',
                  '/usr/bin/javac', 'javac',
                  os.path.join(dest, 'bin', 'javac'), '1']),
            call(['update-alternatives', '--set', 'javac',
                  os.path.join(dest, 'bin', 'javac')])])

    @patch('os.path.exists')
    @patch('subprocess.check_call')
    @patch('charmhelpers.core.host.mkdir')
    @patch('os.path.isdir')
    def test_install_oracle_jre_tarball_already(self, isdir,
                                                mkdir, check_call, exists):
        isdir.return_value = True
        exists.return_value = True  # jre already installed

        # Store the version previously installed.
        hookenv.config()['oracle_jre_tarball'] = sentinel.tarball

        dest = '/usr/lib/jvm/java-8-oracle'

        actions._install_oracle_jre_tarball(sentinel.tarball)

        self.assertFalse(mkdir.called)  # The jvm dir already existed.

        exists.assert_called_once_with('/usr/lib/jvm/java-8-oracle/bin/java')

        # update-alternatives done, but tarball not extracted.
        check_call.assert_has_calls([
            call(['update-alternatives', '--install',
                  '/usr/bin/java', 'java',
                  os.path.join(dest, 'bin', 'java'), '1']),
            call(['update-alternatives', '--set', 'java',
                  os.path.join(dest, 'bin', 'java')]),
            call(['update-alternatives', '--install',
                  '/usr/bin/javac', 'javac',
                  os.path.join(dest, 'bin', 'javac'), '1']),
            call(['update-alternatives', '--set', 'javac',
                  os.path.join(dest, 'bin', 'javac')])])

    @patch('subprocess.check_output')
    def test_emit_java_version(self, check_output):
        check_output.return_value = 'Line 1\nLine 2'
        actions.emit_java_version('')
        check_output.assert_called_once_with(['java', '-version'],
                                             universal_newlines=True)
        hookenv.log.assert_has_calls([call(ANY),
                                      call('JRE: Line 1'),
                                      call('JRE: Line 2')])

    @patch('helpers.configure_cassandra_yaml')
    def test_configure_cassandra_yaml(self, configure_cassandra_yaml):
        # actions.configure_cassandra_yaml is just a wrapper around the
        # helper.
        actions.configure_cassandra_yaml('')
        configure_cassandra_yaml.assert_called_once_with()

    @patch('helpers.get_cassandra_env_file')
    @patch('charmhelpers.core.host.write_file')
    def test_configure_cassandra_env(self, write_file, env_file):
        def _wf(path, contents, perms=None):
            with open(path, 'wb') as f:
                f.write(contents)
        write_file.side_effect = _wf

        # cassandra-env.sh is a shell script that unfortunately
        # embeds configuration we need to change.
        existing_config = dedent('''\
                                 Everything is ignored
                                 unless a regexp matches
                                 #MAX_HEAP_SIZE="1G"
                                 #HEAP_NEWSIZE="800M"
                                 #JMX_PORT="1234"
                                 And done
                                 ''')

        with tempfile.TemporaryDirectory() as tempdir:
            cassandra_env = os.path.join(tempdir, 'c.sh')
            env_file.return_value = cassandra_env

            with open(cassandra_env, 'w', encoding='UTF-8') as f:
                f.write(existing_config)

            overrides = dict(
                max_heap_size=re.compile('^MAX_HEAP_SIZE=(.*)$', re.M),
                heap_newsize=re.compile('^HEAP_NEWSIZE=(.*)$', re.M))

            for key in overrides:
                hookenv.config()[key] = ''

            # By default, the settings will be commented out.
            actions.configure_cassandra_env('')
            with open(cassandra_env, 'r', encoding='UTF-8') as f:
                generated_env = f.read()
            for config_key, regexp in overrides.items():
                with self.subTest(override=config_key):
                    self.assertIsNone(regexp.search(generated_env))

            # Settings can be overridden.
            for config_key, regexp in overrides.items():
                hookenv.config()[config_key] = '{} val'.format(config_key)
            actions.configure_cassandra_env('')
            with open(cassandra_env, 'r') as f:
                generated_env = f.read()
            for config_key, regexp in overrides.items():
                with self.subTest(override=config_key):
                    match = regexp.search(generated_env)
                    self.assertIsNotNone(match)
                    # Note the value has been shell quoted.
                    self.assertTrue(
                        match.group(1).startswith(
                            "'{} val'".format(config_key)))

            # Settings can be returned to the defaults.
            for config_key, regexp in overrides.items():
                hookenv.config()[config_key] = ''
            actions.configure_cassandra_env('')
            with open(cassandra_env, 'r', encoding='UTF-8') as f:
                generated_env = f.read()
            for config_key, regexp in overrides.items():
                with self.subTest(override=config_key):
                    self.assertIsNone(regexp.search(generated_env))

    @patch('helpers.get_cassandra_rackdc_file')
    def test_configure_cassandra_rackdc(self, rackdc_file):
        hookenv.config()['datacenter'] = 'test_dc'
        hookenv.config()['rack'] = 'test_rack'
        with tempfile.NamedTemporaryFile() as rackdc:
            rackdc_file.return_value = rackdc.name
            actions.configure_cassandra_rackdc('')
            with open(rackdc.name, 'r') as f:
                self.assertEqual(f.read().strip(),
                                 'dc=test_dc\nrack=test_rack')

    @patch('helpers.connect')
    @patch('helpers.get_auth_keyspace_replication')
    @patch('helpers.num_nodes')
    def test_needs_reset_auth_keyspace_replication(self, num_nodes,
                                                   get_auth_ks_rep,
                                                   connect):
        num_nodes.return_value = 4
        connect().__enter__.return_value = sentinel.session
        connect().__exit__.return_value = False
        get_auth_ks_rep.return_value = {'another': '8'}
        self.assertTrue(actions.needs_reset_auth_keyspace_replication())

    @patch('helpers.connect')
    @patch('helpers.get_auth_keyspace_replication')
    @patch('helpers.num_nodes')
    def test_needs_reset_auth_keyspace_replication_false(self, num_nodes,
                                                         get_auth_ks_rep,
                                                         connect):
        config = hookenv.config()
        config['datacenter'] = 'mydc'
        connect().__enter__.return_value = sentinel.session
        connect().__exit__.return_value = False

        num_nodes.return_value = 3
        get_auth_ks_rep.return_value = {'another': '8',
                                        'mydc': '3'}
        self.assertFalse(actions.needs_reset_auth_keyspace_replication())

    @patch('helpers.set_active')
    @patch('helpers.repair_auth_keyspace')
    @patch('helpers.connect')
    @patch('helpers.set_auth_keyspace_replication')
    @patch('helpers.get_auth_keyspace_replication')
    @patch('helpers.num_nodes')
    @patch('charmhelpers.core.hookenv.is_leader')
    def test_reset_auth_keyspace_replication(self, is_leader, num_nodes,
                                             get_auth_ks_rep,
                                             set_auth_ks_rep,
                                             connect, repair, set_active):
        is_leader.return_value = True
        num_nodes.return_value = 4
        coordinator.grants = {}
        coordinator.requests = {hookenv.local_unit(): {}}
        coordinator.grant('repair', hookenv.local_unit())
        config = hookenv.config()
        config['datacenter'] = 'mydc'
        connect().__enter__.return_value = sentinel.session
        connect().__exit__.return_value = False
        get_auth_ks_rep.return_value = {'another': '8'}
        self.assertTrue(actions.needs_reset_auth_keyspace_replication())
        actions.reset_auth_keyspace_replication('')
        set_auth_ks_rep.assert_called_once_with(
            sentinel.session,
            {'class': 'NetworkTopologyStrategy', 'another': '8', 'mydc': 4})
        repair.assert_called_once_with()
        set_active.assert_called_once_with()

    def test_store_unit_private_ip(self):
        hookenv.unit_private_ip.side_effect = None
        hookenv.unit_private_ip.return_value = sentinel.ip
        actions.store_unit_private_ip('')
        self.assertEqual(hookenv.config()['unit_private_ip'], sentinel.ip)

    @patch('charmhelpers.core.host.service_start')
    @patch('helpers.status_set')
    @patch('helpers.actual_seed_ips')
    @patch('helpers.get_seed_ips')
    @patch('relations.StorageRelation.needs_remount')
    @patch('helpers.is_bootstrapped')
    @patch('helpers.is_cassandra_running')
    @patch('helpers.is_decommissioned')
    def test_needs_restart(self, is_decom, is_running, is_bootstrapped,
                           needs_remount, seed_ips, actual_seeds,
                           status_set, service_start):
        is_decom.return_value = False
        is_running.return_value = True
        needs_remount.return_value = False
        seed_ips.return_value = set(['1.2.3.4'])
        actual_seeds.return_value = set(['1.2.3.4'])

        config = hookenv.config()
        config['configured_seeds'] = list(sorted(seed_ips()))
        config.save()
        config.load_previous()  # Ensure everything flagged as unchanged.

        self.assertFalse(actions.needs_restart())

        # Decommissioned nodes are not restarted.
        is_decom.return_value = True
        self.assertFalse(actions.needs_restart())
        is_decom.return_value = False
        self.assertFalse(actions.needs_restart())

        # Nodes not running need to be restarted.
        is_running.return_value = False
        self.assertTrue(actions.needs_restart())
        is_running.return_value = True
        self.assertFalse(actions.needs_restart())

        # If we have a new mountpoint, we need to restart in order to
        # migrate data.
        needs_remount.return_value = True
        self.assertTrue(actions.needs_restart())
        needs_remount.return_value = False
        self.assertFalse(actions.needs_restart())

        # Certain changed config items trigger a restart.
        config['max_heap_size'] = '512M'
        self.assertTrue(actions.needs_restart())
        config.save()
        config.load_previous()
        self.assertFalse(actions.needs_restart())

        # A new IP address requires a restart.
        config['unit_private_ip'] = 'new'
        self.assertTrue(actions.needs_restart())
        config.save()
        config.load_previous()
        self.assertFalse(actions.needs_restart())

        # If the seeds have changed, we need to restart.
        seed_ips.return_value = set(['9.8.7.6'])
        actual_seeds.return_value = set(['9.8.7.6'])
        self.assertTrue(actions.needs_restart())
        is_running.side_effect = iter([False, True])
        helpers.start_cassandra()
        is_running.side_effect = None
        is_running.return_value = True
        self.assertFalse(actions.needs_restart())

    @patch('charmhelpers.core.hookenv.is_leader')
    @patch('helpers.is_bootstrapped')
    @patch('helpers.ensure_database_directories')
    @patch('helpers.remount_cassandra')
    @patch('helpers.start_cassandra')
    @patch('helpers.stop_cassandra')
    @patch('helpers.status_set')
    def test_maybe_restart(self, status_set, stop_cassandra, start_cassandra,
                           remount, ensure_directories, is_bootstrapped,
                           is_leader):
        coordinator.grants = {}
        coordinator.requests = {hookenv.local_unit(): {}}
        coordinator.relid = 'cluster:1'
        coordinator.grant('restart', hookenv.local_unit())
        actions.maybe_restart('')
        stop_cassandra.assert_called_once_with()
        remount.assert_called_once_with()
        ensure_directories.assert_called_once_with()
        start_cassandra.assert_called_once_with()

    @patch('helpers.stop_cassandra')
    def test_stop_cassandra(self, helpers_stop_cassandra):
        actions.stop_cassandra('ignored')
        helpers_stop_cassandra.assert_called_once_with()

    @patch('helpers.start_cassandra')
    def test_start_cassandra(self, helpers_start_cassandra):
        actions.start_cassandra('ignored')
        helpers_start_cassandra.assert_called_once_with()

    @patch('os.path.isdir')
    @patch('helpers.get_all_database_directories')
    @patch('helpers.set_io_scheduler')
    def test_reset_all_io_schedulers(self, set_io_scheduler, dbdirs, isdir):
        hookenv.config()['io_scheduler'] = sentinel.io_scheduler
        dbdirs.return_value = dict(
            data_file_directories=[sentinel.d1, sentinel.d2],
            commitlog_directory=sentinel.cl,
            saved_caches_directory=sentinel.sc)
        isdir.return_value = True
        actions.reset_all_io_schedulers('')
        set_io_scheduler.assert_has_calls([
            call(sentinel.io_scheduler, sentinel.d1),
            call(sentinel.io_scheduler, sentinel.d2),
            call(sentinel.io_scheduler, sentinel.cl),
            call(sentinel.io_scheduler, sentinel.sc)],
            any_order=True)

        # If directories don't exist yet, nothing happens.
        set_io_scheduler.reset_mock()
        isdir.return_value = False
        actions.reset_all_io_schedulers('')
        self.assertFalse(set_io_scheduler.called)

    def test_config_key_lists_complete(self):
        # Ensure that we have listed all keys in either
        # RESTART_REQUIRED_KEYS, RESTART_NOT_REQUIRED_KEYS or
        # UNCHANGEABLE_KEYS. This is to ensure that RESTART_REQUIRED_KEYS
        # is maintained as new config items are added over time.
        config_path = os.path.join(os.path.dirname(__file__), os.pardir,
                                   'config.yaml')
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)

        combined = actions.RESTART_REQUIRED_KEYS.union(
            actions.RESTART_NOT_REQUIRED_KEYS).union(
                actions.UNCHANGEABLE_KEYS)

        for key in config['options']:
            with self.subTest(key=key):
                self.assertIn(key, combined)

    @patch('actions._publish_database_relation')
    def test_publish_database_relations(self, publish_db_rel):
        actions.publish_database_relations('')
        publish_db_rel.assert_called_once_with('database:1', superuser=False)

    @patch('actions._publish_database_relation')
    def test_publish_database_admin_relations(self, publish_db_rel):
        actions.publish_database_admin_relations('')
        publish_db_rel.assert_called_once_with('database-admin:1',
                                               superuser=True)

    @patch('helpers.leader_ping')
    @patch('helpers.ensure_user')
    @patch('helpers.connect')
    @patch('helpers.get_service_name')
    @patch('helpers.encrypt_password')
    @patch('charmhelpers.core.host.pwgen')
    @patch('charmhelpers.core.hookenv.is_leader')
    @patch('actions._client_credentials')
    def test_publish_database_relation_leader(self, client_creds, is_leader,
                                              pwgen, encrypt_password,
                                              get_service_name,
                                              connect, ensure_user,
                                              leader_ping):
        is_leader.return_value = True  # We are the leader.
        client_creds.return_value = (None, None)  # No creds published yet.

        get_service_name.return_value = 'cservice'
        pwgen.side_effect = iter(['secret1', 'secret2'])
        encrypt_password.side_effect = iter(['crypt1', 'crypt2'])
        connect().__enter__.return_value = sentinel.session

        config = hookenv.config()
        config['native_transport_port'] = 666
        config['rpc_port'] = 777
        config['cluster_name'] = 'fred'
        config['datacenter'] = 'mission_control'
        config['rack'] = '01'

        actions._publish_database_relation('database:1', superuser=False)

        ensure_user.assert_called_once_with(sentinel.session,
                                            'juju_cservice', 'crypt1',
                                            False)
        leader_ping.assert_called_once_with()  # Peers woken.

        hookenv.relation_set.assert_has_calls([
            call('database:1',
                 username='juju_cservice', password='secret1',
                 host='10.30.0.1', native_transport_port=666, rpc_port=777,
                 cluster_name='fred', datacenter='mission_control',
                 rack='01')])

    @patch('helpers.leader_ping')
    @patch('helpers.ensure_user')
    @patch('helpers.connect')
    @patch('helpers.get_service_name')
    @patch('helpers.encrypt_password')
    @patch('charmhelpers.core.host.pwgen')
    @patch('charmhelpers.core.hookenv.is_leader')
    @patch('actions._client_credentials')
    def test_publish_database_relation_super(self, client_creds, is_leader,
                                             pwgen, encrypt_password,
                                             get_service_name,
                                             connect, ensure_user,
                                             leader_ping):
        is_leader.return_value = True  # We are the leader.
        client_creds.return_value = (None, None)  # No creds published yet.

        get_service_name.return_value = 'cservice'
        pwgen.side_effect = iter(['secret1', 'secret2'])
        encrypt_password.side_effect = iter(['crypt1', 'crypt2'])
        connect().__enter__.return_value = sentinel.session

        config = hookenv.config()
        config['native_transport_port'] = 666
        config['rpc_port'] = 777
        config['cluster_name'] = 'fred'
        config['datacenter'] = 'mission_control'
        config['rack'] = '01'

        actions._publish_database_relation('database:1', superuser=True)

        ensure_user.assert_called_once_with(sentinel.session,
                                            'juju_cservice_admin', 'crypt1',
                                            True)

    @patch('charmhelpers.core.host.write_file')
    def test_install_maintenance_crontab(self, write_file):
        # First 7 units get distributed, one job per day.
        hookenv.local_unit.return_value = 'foo/0'
        actions.install_maintenance_crontab('')
        write_file.assert_called_once_with('/etc/cron.d/cassandra-maintenance',
                                           ANY)
        contents = write_file.call_args[0][1]
        # Not the complete command, but includes all the expanded
        # variables.
        expected = (b'\n0 0 * * 0 cassandra run-one-until-success '
                    b'nodetool repair -pr')
        self.assertIn(expected, contents)

        # Next 7 units distributed 12 hours out of sync with the first
        # batch.
        hookenv.local_unit.return_value = 'foo/8'
        actions.install_maintenance_crontab('')
        contents = write_file.call_args[0][1]
        expected = (b'\n0 12 * * 1 cassandra run-one-until-success '
                    b'nodetool repair -pr')
        self.assertIn(expected, contents)

        # Later units per helpers.week_spread()
        hookenv.local_unit.return_value = 'foo/411'
        actions.install_maintenance_crontab('')
        contents = write_file.call_args[0][1]
        expected = (b'\n37 8 * * 5 cassandra run-one-until-success '
                    b'nodetool repair -pr')
        self.assertIn(expected, contents)

    @patch('helpers.emit_netstats')
    @patch('helpers.emit_status')
    @patch('helpers.emit_describe_cluster')
    def test_emit_cluster_info(self, emit_desc, emit_status, emit_netstats):
        actions.emit_cluster_info('')
        emit_desc.assert_called_once_with()
        emit_status.assert_called_once_with()
        emit_netstats.assert_called_once_with()

    @patch('charmhelpers.core.hookenv.relations_of_type')
    @patch('actions.ufw')
    def test_configure_firewall(self, ufw, rel_of_type):
        rel_of_type.side_effect = iter([[{'private-address': '1.1.0.1'},
                                         {'private-address': '1.1.0.2'}],
                                        []])
        actions.configure_firewall('')

        # Confirm our mock provided the expected data.
        rel_of_type.assert_has_calls([call('cluster'), call('database-admin')])

        ufw.enable.assert_called_once_with(soft_fail=True)  # Always enabled.

        # SSH and the client protocol ports are always fully open.
        ufw.service.assert_has_calls([call('ssh', 'open'),
                                      call('nrpe', 'open'),
                                      call('rsync', 'open'),
                                      call(9042, 'open'),
                                      call(9160, 'open')])

        # This test is running for the first time, so there are no
        # previously applied rules to remove. It opens necessary access
        # to peers and other related units. The 1.1.* addresses are
        # peers, and they get storage (7000), ssl_storage (7001),
        # JMX (7199), Thrift (9160) and native (9042). The remaining
        # addresses are clients, getting just Thrift and native.
        ufw.grant_access.assert_has_calls([call('1.1.0.1', 'any', 7000),
                                           call('1.1.0.1', 'any', 7001),

                                           call('1.1.0.2', 'any', 7000),
                                           call('1.1.0.2', 'any', 7001)],
                                          any_order=True)

        # If things change in a later hook, unwanted rules are removed
        # and new ones added.
        config = hookenv.config()
        config.save()
        config.load_previous()
        config['native_transport_port'] = 7777  # 9042 -> 7777
        config['storage_port'] = 7002  # 7000 -> 7002
        config['open_client_ports'] = True
        ufw.reset_mock()

        rel_of_type.side_effect = iter([[],
                                        [{'private-address': '1.1.0.1'},
                                         {'private-address': '1.1.0.2'}]])
        actions.configure_firewall('')

        # Three ports now globally open. Yes, having the globally open
        # native and Thrift ports does make the later more specific
        # rules meaningless, but we add the specific rules anyway.
        ufw.service.assert_has_calls([call('ssh', 'open'),
                                      call('nrpe', 'open'),
                                      call(9042, 'close'),
                                      call(7777, 'open'),
                                      call(9160, 'open')], any_order=True)
        ufw.revoke_access.assert_has_calls([call('1.1.0.1', 'any', 7000),
                                            call('1.1.0.2', 'any', 7000)],
                                           any_order=True)
        ufw.grant_access.assert_has_calls([call('1.1.0.1', 'any', 7001),
                                           call('1.1.0.1', 'any', 7002),
                                           call('1.1.0.2', 'any', 7001),
                                           call('1.1.0.2', 'any', 7002)],
                                          any_order=True)

    @patch('helpers.mountpoint')
    @patch('helpers.get_cassandra_version')
    @patch('charmhelpers.core.host.write_file')
    @patch('charmhelpers.contrib.charmsupport.nrpe.NRPE')
    @patch('helpers.local_plugins_dir')
    def test_nrpe_external_master_relation(self, local_plugins_dir, nrpe,
                                           write_file, cassandra_version,
                                           mountpoint):
        mountpoint.side_effect = os.path.dirname
        cassandra_version.return_value = '2.2'
        # The fake charm_dir() needs populating.
        plugin_src_dir = os.path.join(os.path.dirname(__file__),
                                      os.pardir, 'files')
        shutil.copytree(plugin_src_dir,
                        os.path.join(hookenv.charm_dir(), 'files'))

        with tempfile.TemporaryDirectory() as d:
            local_plugins_dir.return_value = d
            actions.nrpe_external_master_relation('')

            # The expected file was written to the expected filename
            # with required perms.
            with open(os.path.join(plugin_src_dir, 'check_cassandra_heap.sh'),
                      'rb') as f:
                write_file.assert_called_once_with(
                    os.path.join(d, 'check_cassandra_heap.sh'), f.read(),
                    perms=0o555)

            nrpe().add_check.assert_has_calls([
                call(shortname='cassandra_heap',
                     description='Check Cassandra Heap',
                     check_cmd='check_cassandra_heap.sh localhost 80 90'),
                call(description=('Check Cassandra Disk '
                                  '/var/lib/cassandra'),
                     shortname='cassandra_disk_var_lib_cassandra',
                     check_cmd=('check_disk -u GB -w 50% -c 25% -K 5% '
                                '-p /var/lib/cassandra'))],
                any_order=True)

            nrpe().write.assert_called_once_with()

    @patch('helpers.get_cassandra_version')
    @patch('charmhelpers.core.host.write_file')
    @patch('os.path.exists')
    @patch('charmhelpers.contrib.charmsupport.nrpe.NRPE')
    def test_nrpe_external_master_relation_no_local(self, nrpe, exists,
                                                    write_file, ver):
        ver.return_value = '2.2'
        # If the local plugins directory doesn't exist, we don't attempt
        # to write files to it. Wait until the subordinate has set it
        # up.
        exists.return_value = False
        actions.nrpe_external_master_relation('')
        self.assertFalse(write_file.called)

    @patch('helpers.mountpoint')
    @patch('helpers.get_cassandra_version')
    @patch('os.path.exists')
    @patch('charmhelpers.contrib.charmsupport.nrpe.NRPE')
    def test_nrpe_external_master_relation_disable_heapchk(self, nrpe, exists,
                                                           ver, mountpoint):
        ver.return_value = '2.2'
        exists.return_value = False
        mountpoint.side_effect = os.path.dirname

        # Disable our checks
        config = hookenv.config()
        config['nagios_heapchk_warn_pct'] = 0  # Only one needs to be disabled.
        config['nagios_heapchk_crit_pct'] = 90

        actions.nrpe_external_master_relation('')
        exists.assert_called_once_with(helpers.local_plugins_dir())

        nrpe().add_check.assert_has_calls([
            call(shortname='cassandra_disk_var_lib_cassandra',
                 description=ANY, check_cmd=ANY)], any_order=True)

    @patch('helpers.get_cassandra_version')
    @patch('os.path.exists')
    @patch('charmhelpers.contrib.charmsupport.nrpe.NRPE')
    def test_nrpe_external_master_relation_disable_diskchk(self, nrpe,
                                                           exists, ver):
        ver.return_value = '2.2'
        exists.return_value = False

        # Disable our checks
        config = hookenv.config()
        config['nagios_disk_warn_pct'] = 0  # Only one needs to be disabled.
        config['magios_disk_crit_pct'] = 50

        actions.nrpe_external_master_relation('')
        exists.assert_called_once_with(helpers.local_plugins_dir())

        nrpe().add_check.assert_called_once_with(shortname='cassandra_heap',
                                                 description=ANY,
                                                 check_cmd=ANY)

    @patch('helpers.get_bootstrapped_ips')
    @patch('helpers.get_seed_ips')
    @patch('charmhelpers.core.hookenv.leader_set')
    @patch('charmhelpers.core.hookenv.is_leader')
    def test_maintain_seeds(self, is_leader, leader_set,
                            seed_ips, bootstrapped_ips):
        is_leader.return_value = True

        seed_ips.return_value = set(['1.2.3.4'])
        bootstrapped_ips.return_value = set(['2.2.3.4', '3.2.3.4',
                                             '4.2.3.4', '5.2.3.4'])

        actions.maintain_seeds('')
        leader_set.assert_called_once_with(seeds='2.2.3.4,3.2.3.4,4.2.3.4')

    @patch('helpers.get_bootstrapped_ips')
    @patch('helpers.get_seed_ips')
    @patch('charmhelpers.core.hookenv.leader_set')
    @patch('charmhelpers.core.hookenv.is_leader')
    def test_maintain_seeds_start(self, is_leader, leader_set,
                                  seed_ips, bootstrapped_ips):
        seed_ips.return_value = set()
        bootstrapped_ips.return_value = set()
        actions.maintain_seeds('')
        # First seed is the first leader, which lets is get everything
        # started.
        leader_set.assert_called_once_with(seeds=hookenv.unit_private_ip())

    @patch('charmhelpers.core.host.pwgen')
    @patch('helpers.query')
    @patch('helpers.set_unit_superusers')
    @patch('helpers.ensure_user')
    @patch('helpers.encrypt_password')
    @patch('helpers.superuser_credentials')
    @patch('helpers.connect')
    @patch('charmhelpers.core.hookenv.is_leader')
    @patch('charmhelpers.core.hookenv.leader_set')
    @patch('charmhelpers.core.hookenv.leader_get')
    def test_reset_default_password(self, leader_get, leader_set, is_leader,
                                    connect, sup_creds, encrypt_password,
                                    ensure_user, set_sups, query, pwgen):
        is_leader.return_value = True
        leader_get.return_value = None
        connect().__enter__.return_value = sentinel.session
        connect().__exit__.return_value = False
        connect.reset_mock()

        sup_creds.return_value = (sentinel.username, sentinel.password)
        encrypt_password.return_value = sentinel.pwhash
        pwgen.return_value = sentinel.random_password

        actions.reset_default_password('')

        # First, a superuser account for the unit was created.
        connect.assert_called_once_with('cassandra', 'cassandra',
                                        timeout=120, auth_timeout=120)
        encrypt_password.assert_called_once_with(sentinel.password)
        ensure_user.assert_called_once_with(sentinel.session,
                                            sentinel.username,
                                            sentinel.pwhash,
                                            superuser=True)
        set_sups.assert_called_once_with([hookenv.local_unit()])

        # After that, the default password is reset.
        query.assert_called_once_with(sentinel.session,
                                      'ALTER USER cassandra WITH PASSWORD %s',
                                      cassandra.ConsistencyLevel.ALL,
                                      (sentinel.random_password,))

        # Flag stored to avoid attempting this again.
        leader_set.assert_called_once_with(default_admin_password_changed=True)

    @patch('helpers.connect')
    @patch('charmhelpers.core.hookenv.is_leader')
    @patch('charmhelpers.core.hookenv.leader_get')
    def test_reset_default_password_noop(self, leader_get, is_leader, connect):
        leader_get.return_value = True
        is_leader.return_value = True
        actions.reset_default_password('')  # noop
        self.assertFalse(connect.called)

    @patch('helpers.get_seed_ips')
    @patch('helpers.status_set')
    @patch('charmhelpers.core.hookenv.status_get')
    @patch('charmhelpers.core.hookenv.is_leader')
    def test_set_active(self, is_leader, status_get, status_set, seed_ips):
        is_leader.return_value = False
        status_get.return_value = ('waiting', '')
        seed_ips.return_value = set()
        actions.set_active('')
        status_set.assert_called_once_with('active', 'Live node')

    @patch('helpers.get_seed_ips')
    @patch('helpers.status_set')
    @patch('charmhelpers.core.hookenv.status_get')
    @patch('charmhelpers.core.hookenv.is_leader')
    def test_set_active_seed(self, is_leader,
                             status_get, status_set, seed_ips):
        is_leader.return_value = False
        status_get.return_value = ('waiting', '')
        seed_ips.return_value = set([hookenv.unit_private_ip()])
        actions.set_active('')
        status_set.assert_called_once_with('active', 'Live seed')

    @patch('helpers.num_nodes')
    @patch('helpers.get_seed_ips')
    @patch('helpers.service_status_set')
    @patch('helpers.status_set')
    @patch('charmhelpers.core.hookenv.status_get')
    @patch('charmhelpers.core.hookenv.is_leader')
    def test_set_active_service(self, is_leader,
                                status_get, status_set, service_status_set,
                                seed_ips, num_nodes):
        status_get.return_value = ('waiting', '')
        is_leader.return_value = True
        seed_ips.return_value = set([hookenv.unit_private_ip()])
        num_nodes.return_value = 1
        actions.set_active('')
        service_status_set.assert_called_once_with('active',
                                                   'Single node cluster')

        service_status_set.reset_mock()
        num_nodes.return_value = 6
        actions.set_active('')
        service_status_set.assert_called_once_with('active',
                                                   '6 node cluster')

    @patch('helpers.encrypt_password')
    @patch('helpers.superuser_credentials')
    @patch('helpers.peer_relid')
    def test_request_unit_superuser(self, peer_relid, sup_creds, crypt):
        peer_relid.return_value = sentinel.peer_relid
        sup_creds.return_value = (sentinel.username, sentinel.password)
        crypt.return_value = sentinel.pwhash
        hookenv.relation_get.return_value = dict()
        actions.request_unit_superuser('')
        hookenv.relation_set.assert_called_once_with(
            sentinel.peer_relid,
            username=sentinel.username, pwhash=sentinel.pwhash)

    @patch('helpers.update_hosts_file')
    @patch('socket.gethostname')
    def test_update_etc_hosts(self, gethostname, update_hosts_file):
        gethostname.return_value = sentinel.hostname
        actions.update_etc_hosts('')
        update_hosts_file.assert_called_once_with(
            '/etc/hosts', {'10.20.0.1': sentinel.hostname})


if __name__ == '__main__':
    unittest.main(verbosity=2)
