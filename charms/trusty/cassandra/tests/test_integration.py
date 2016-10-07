#!.venv3/bin/python3
#
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

import configparser
from functools import wraps
import glob
import http.server
from itertools import count
import logging
import multiprocessing
import os
import socket
import subprocess
import sys
import time
import unittest
import uuid
import warnings

warnings.filterwarnings('ignore', 'The blist library is not available')

import amulet.deployer
import amulet.helpers
from cassandra import Unavailable, ConsistencyLevel, AuthenticationFailed
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, NoHostAvailable
from cassandra.query import SimpleStatement
import yaml

import helpers
from testing.amuletfixture import AmuletFixture


SERIES = os.environ.get('SERIES', 'trusty')

WAIT_TIMEOUT = int(os.environ.get('AMULET_TIMEOUT', 3600))

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))


class TestDeploymentBase(unittest.TestCase):
    rf = 1
    deployment = None

    common_config = dict(max_heap_size='96M',
                         heap_newsize='4M')
    test_config = dict()

    @classmethod
    def setUpClass(cls):
        deployment = AmuletFixture(series=SERIES)
        deployment.setUp()
        cls.deployment = deployment

        deployment.add('cassandra', units=cls.rf,
                       constraints=dict(mem="2G"))
        deployment.expose('cassandra')  # Tests need client access.
        config = dict()
        config.update(cls.common_config)
        config.update(cls.test_config)  # Test subclass overrides
        deployment.configure('cassandra', config)

        deployment.add('storage',
                       'cs:~stub/{}/storage'.format(SERIES))
        deployment.configure('storage', dict(provider='local'))

        # A stub client charm.
        empty_path = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                                  os.pardir, 'lib',
                                                  'testcharms', 'empty'))
        deployment.add('client', empty_path)
        deployment.relate('cassandra:database', 'client:database')
        deployment.relate('cassandra:database-admin', 'client:database-admin')

        # No official trusty branch of the nrpe-external-master charm, yet.
        # This is a problem as it means tests may not be running against
        # the lastest version.
        deployment.add('nrpe',
                       'cs:~stub/{}/nrpe-external-master'
                       ''.format(SERIES))
        deployment.relate('cassandra:nrpe-external-master',
                          'nrpe:nrpe-external-master')

        deployment.deploy(timeout=WAIT_TIMEOUT)

        # Silence noise - we are testing the charm, not the Cassandra
        # driver.
        cassandra_log = logging.getLogger('cassandra')
        cassandra_log.setLevel(logging.CRITICAL)

    @classmethod
    def tearDownClass(cls):
        cls.deployment.tearDown()
        cls.deployment = None

    def juju_status(self):
        status_yaml = subprocess.check_output(['juju', 'status',
                                               '--format=yaml'])
        if not status_yaml.strip():
            return None
        return yaml.safe_load(status_yaml)

    def cluster(self, username=None, password=None, hosts=None, port=9042):
        status = self.juju_status()

        if username is None or password is None:
            # Get some valid credentials - unit's superuser account will do.
            unit = sorted(status['services']['cassandra']['units'].keys())[0]
            cqlshrc_path = helpers.get_cqlshrc_path()
            cqlshrc = configparser.ConfigParser(interpolation=None)
            cqlshrc.read_string(
                self.deployment.sentry[unit].file_contents(cqlshrc_path))
            username = cqlshrc['authentication']['username']
            password = cqlshrc['authentication']['password']

        auth_provider = PlainTextAuthProvider(username=username,
                                              password=password)

        if hosts is None:
            # Get the IP addresses
            hosts = []
            for unit, d in status['services']['cassandra']['units'].items():
                hosts.append(d['public-address'])
        cluster = Cluster(hosts, auth_provider=auth_provider, port=port)
        self.addCleanup(cluster.shutdown)
        return cluster

    def session(self):
        '''A session using the server's superuser credentials.'''
        session = self.cluster().connect()
        self.addCleanup(session.shutdown)
        return session

    def client_session(self, relname):
        '''A session using the client's credentials.

        We currently just steal the client's credentials and use
        them from the local machine, but we could tunnel through the
        client with a little more effort.
        '''
        relinfo = self.get_client_relinfo(relname)
        self.assertIn('host', relinfo.keys())
        cluster = self.cluster(relinfo['username'],
                               relinfo['password'],
                               [relinfo['host']],
                               int(relinfo['native_transport_port']))
        session = cluster.connect()
        self.addCleanup(session.shutdown)
        return session

    keyspace_ids = count()

    def new_keyspace(self, session, rf=None):
        if rf is None:
            # We create a keyspace with a replication factor equal
            # to the number of units. This ensures that all records
            # are replicated to all nodes, and we can cofirm that
            # all nodes are working by doing an insert with
            # ConsistencyLevel.ALL.
            rf = self.rf
        keyspace = 'test{}'.format(next(TestDeploymentBase.keyspace_ids))
        q = SimpleStatement(
            'CREATE KEYSPACE {} WITH REPLICATION ='.format(keyspace) +
            "{'class': 'SimpleStrategy', 'replication_factor': %s}",
            consistency_level=ConsistencyLevel.ALL)
        session.execute(q, (rf,))
        session.set_keyspace(keyspace)
        return keyspace

    def get_client_relinfo(self, relname):
        # We only need one unit, even if rf > 1
        s = self.deployment.sentry['cassandra'][0]
        relinfo = s.relation(relname, 'client:{}'.format(relname))
        return relinfo

    def is_port_open(self, port):
        status = self.juju_status()
        detail = list(status['services']['cassandra']['units'].values())[0]
        address = detail['public-address']
        rc = subprocess.call(['nc', '-z', '-w', '2', address, str(port)])
        return rc == 0

    def reconfigure_cassandra(self, **overrides):
        config = dict()
        config.update(self.common_config)
        config.update(self.test_config)
        config.update(overrides)
        self.deployment.configure('cassandra', config)
        self.deployment.wait()


class Test1UnitDeployment(TestDeploymentBase):
    """Tests run on both a single node cluster and a 3 node cluster."""
    rf = 1
    test_config = dict(jre='openjdk')

    def test_basics_unit_superuser(self):
        # Basic tests using unit superuser credentials
        session = self.session()
        self.new_keyspace(session)
        self._test_database_basics(session)

    def test_basics_client_relation(self):
        # Create a keyspace using superuser credentials
        super_session = self.session()
        keyspace = self.new_keyspace(super_session)

        # Basic tests using standard client relation credentials.
        session = self.client_session('database')
        session.set_keyspace(keyspace)
        self._test_database_basics(session)

    def test_basics_client_admin_relation(self):
        # Basic tests using administrative client relation credentials.
        session = self.client_session('database-admin')
        self.new_keyspace(session)
        self._test_database_basics(session)

    def _test_database_basics(self, session):
        session.execute('CREATE TABLE Foo (x varchar PRIMARY KEY)')

        # Insert some data, ensuring that it has been stored on
        # all of our juju units. Note that the replication factor
        # of our keyspace has been set to the number of units we
        # deployed. Because it might take a while for the cluster to get
        # its act together, we retry this in a loop with a timeout.
        timeout = time.time() + 120
        while True:
            value = 'hello {}'.format(time.time())
            query = SimpleStatement(
                "INSERT INTO Foo (x) VALUES (%s)",
                consistency_level=ConsistencyLevel.ALL)
            try:
                session.execute(query, (value,))
                break
            except Exception:
                if time.time() > timeout:
                    raise

        # We can get the data out again. This isn't testing our charm,
        # but nice to know anyway...
        r = session.execute('SELECT * FROM Foo LIMIT 1')
        self.assertTrue(r[0].x.startswith('hello'))

    def test_external_mount(self):
        # Not only does this test migrating data from local disk to an
        # external mount, it also exercises the rolling restart logic.
        # If rf==1, the restart will happen in the
        # storage-relation-changed hook as soon as the mount is ready.
        # If rf > 1, the restart will happen in the
        # cluster-relation-changed hook once the unit has determined
        # that it is its turn to restart.

        # First, create a keyspace pre-migration so we can confirm the
        # data was migrated rather than being reset to an empty system.
        session = self.session()
        keyspace = self.new_keyspace(session)
        session.execute('CREATE TABLE dat (x varchar PRIMARY KEY)')
        total = self.rf * 50
        q = SimpleStatement('INSERT INTO dat (x) VALUES (%s)')
        for _ in range(0, total):
            session.execute(q, (str(uuid.uuid1()),))
        session.shutdown()

        self.deployment.relate('cassandra:data', 'storage:data')
        self.deployment.wait()
        # Per Bug #1254766 and Bug #1254766, the sentry.wait() above
        # will return before the hooks have actually finished running
        # and data migrated. Instead, keep checking until our condition
        # is met, or a timeout reached.
        timeout = time.time() + 300
        for s in self.deployment.sentry['cassandra']:
            unit = s.info['unit_name']
            unit_num = s.info['unit']
            with self.subTest(unit=unit):
                while True:
                    # Attempting to diagnose Amulet failures. I suspect
                    # SSH host keys again, per Bug #802117
                    try:
                        s.directory_contents('/')
                    except (subprocess.CalledProcessError, OSError):
                        self.skipTest('sentry[{!r}].directory_contents({!r}) '
                                      'failed!'.format(unit, '/'))
                    parents = ['/srv', '/srv/cassandra_{}'.format(unit_num),
                               '/srv/cassandra_{}/cassandra'.format(unit_num)]
                    for path in parents:
                        try:
                            s.directory_contents('/srv')
                        except (subprocess.CalledProcessError, OSError):
                            raise AssertionError('Failed to scan {!r} on {}'
                                                 .format(path, unit))
                    try:
                        contents = s.directory_contents(
                            '/srv/cassandra_{}/cassandra/data'.format(
                                unit_num))
                        found = set(contents['directories'])
                        self.assertIn(keyspace, found)
                        self.assertIn('system', found)
                        break
                    except Exception:
                        if time.time() > timeout:
                            raise
                        time.sleep(5)

        # Confirm no data has been lost, which could happen if we badly
        # shutdown and memtables were not flushed.
        session = self.session()
        session.set_keyspace(keyspace)
        q = SimpleStatement('SELECT COUNT(*) FROM dat',
                            consistency_level=ConsistencyLevel.QUORUM)
        results = session.execute(q)
        self.assertEqual(results[0][0], total)

    def test_cluster_ports_closed(self):
        # The internal Cassandra ports are protected by ufw firewall
        # rules, and are closed to everyone except for peers and the
        # force_seed_nodes list. This is required for security, since
        # the protocols are unauthenticated. It also stops rogue nodes
        # on failed units from rejoining the cluster and causing chaos.
        self.assertFalse(self.is_port_open(7000), 'Storage port open')
        self.assertFalse(self.is_port_open(7001), 'SSL Storage port open')
        self.assertFalse(self.is_port_open(7199), 'JMX port open')

    def test_client_ports_open(self):
        self.assertTrue(self.is_port_open(9042), 'Native trans port closed')
        self.assertTrue(self.is_port_open(9160), 'Thrift RPC port closed')

    def test_default_superuser_account_closed(self):
        cluster = self.cluster(username='cassandra', password='cassandra')
        try:
            cluster.connect()
            self.fail('Default credentials not reset')
        except NoHostAvailable as x:
            for fail in x.errors.values():
                self.assertIsInstance(fail, AuthenticationFailed)

    def test_cqlsh(self):
        unit = self.deployment.sentry['cassandra'][0].info['unit_name']
        subprocess.check_output(['juju', 'ssh', unit,
                                 'sudo -H cqlsh -e exit'],
                                stderr=subprocess.STDOUT)

    def test_z_add_and_drop_node(self):  # 'z' to run this test last.
        # We need to be able to add a node correctly into the ring,
        # without an operator needing to repair keyspaces to ensure data
        # is located on the expected nodes.
        # To test this, first create a keyspace with rf==1 and put enough
        # data in it so each node will have some.
        cluster = self.cluster()
        s = cluster.connect()
        keyspace = self.new_keyspace(s, rf=1)
        q = SimpleStatement('CREATE TABLE dat (x varchar PRIMARY KEY)',
                            consistency_level=ConsistencyLevel.ALL)
        s.execute(q)

        total = self.rf * 50
        q = SimpleStatement('INSERT INTO dat (x) VALUES (%s)',
                            consistency_level=ConsistencyLevel.QUORUM)
        for _ in range(0, total):
            s.execute(q, (str(uuid.uuid1()),))
        cluster.shutdown()

        def count(expected):
            until = time.time() + 180
            while True:
                cluster = self.cluster()
                try:
                    s = cluster.connect(keyspace)
                    results = s.execute(SimpleStatement(
                        'SELECT count(*) FROM dat',
                        consistency_level=ConsistencyLevel.QUORUM))
                    found = results[0][0]
                    if found == expected or time.time() > until:
                        return found
                    time.sleep(0.2)
                except Unavailable:
                    if time.time() > until:
                        raise
                finally:
                    cluster.shutdown()

        self.assertEqual(count(total), total)

        self.deployment.add_unit('cassandra')
        self.deployment.wait()
        status = self.juju_status()
        unit = sorted(status['services']['cassandra']['units'].keys())[-1]
        try:
            self.assertEqual(count(total), total)
        finally:
            # When a node is dropped, it needs to decommission itself and
            # move its data to the remaining nodes so no data is lost.
            # Alas, per Bug #1417874 we can't yet do this with Juju.
            # First, the node must be manually decommissioned before we
            # remove the unit.
            self._decommission(unit)
            self.deployment.remove_unit(unit)
            self.deployment.wait()

        self.assertEqual(count(total), total)

    def _decommission(self, unit):
        until = time.time() + WAIT_TIMEOUT
        while True:
            try:
                subprocess.check_output(['juju', 'run', '--unit', unit,
                                         'nodetool decommission'],
                                        stderr=subprocess.STDOUT,
                                        universal_newlines=True)
                break
            except subprocess.CalledProcessError:
                if time.time() > until:
                    raise

        until = time.time() + WAIT_TIMEOUT
        while True:
            try:
                cmd = ['juju', 'run', '--unit', unit, 'nodetool netstats']
                raw = subprocess.check_output(cmd, stderr=subprocess.STDOUT,
                                              universal_newlines=True)
                if 'Mode: DECOMMISSIONED' in raw:
                    return
                if time.time() > until:
                    raise subprocess.TimeoutExpired(cmd, WAIT_TIMEOUT, raw)
            except subprocess.CalledProcessError:
                if time.time() > until:
                    raise
            time.sleep(3)


class Test3UnitDeployment(Test1UnitDeployment):
    """Tests run on a three node cluster."""
    rf = 3


_jre_url = None


def _serve(cwd, host, port):
    sys.stderr = open('/dev/null', 'w')
    os.chdir(cwd)
    httpd = http.server.HTTPServer((host, port),
                                   http.server.SimpleHTTPRequestHandler)
    httpd.serve_forever()


_procs = []


def get_jre_url():
    '''Return the URL to the Oracle Java SE 8 Server Runtime tarball, or None.

    The tarball needs to be placed in ../lib.

    Spawns a web server as a subprocess to serve the file.
    '''
    global _jre_url
    if _jre_url is not None:
        return _jre_url

    jre_dir = os.path.join(ROOT, 'lib')

    jre_tarballs = sorted(glob.glob(os.path.join(jre_dir,
                                                 'server-jre-?u*.tar.gz')))
    if not jre_tarballs:
        return None

    # Get the local IP address, only available via hackish means and
    # quite possibly incorrect.
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(('www.canonical.com', 80))
    host = s.getsockname()[0]
    s.close()

    # Get a free port.
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind((host, 0))
    port = s.getsockname()[1]
    s.close()

    p = multiprocessing.Process(target=_serve, args=(jre_dir, host, port),
                                daemon=True)
    p.start()
    _procs.append(p)

    _jre_url = 'http://{}:{}/{}'.format(host, port,
                                        os.path.basename(jre_tarballs[-1]))
    return _jre_url


class TestOracleJREDeployment(Test1UnitDeployment):
    """Basic test with the Oracle JRE.

    Unfortunately these tests cannot be run by the automatic test runners,
    as the Oracle JRE is protected from public download by Oracle's
    click-through license agreement.
    """
    rf = 1
    test_config = dict(jre='Oracle', edition='community',
                       private_jre_url=get_jre_url())

    @classmethod
    @unittest.skipUnless(get_jre_url(), 'No Oracle JRE tarballs available')
    def setUpClass(cls):
        super(TestOracleJREDeployment, cls).setUpClass()


class TestDSEDeployment(Test1UnitDeployment):
    """Tests run a single node DataStax Enterprise cluster.

    Unfortunately these tests cannot be run by the automatic test
    runners, as the DSE packages are not available for public download.
    """
    rf = 1
    test_config = dict(
        edition='DSE',  # Forces Oracle JRE
        install_sources=yaml.safe_dump([os.environ.get('DSE_SOURCE'),
                                        'ppa:stub/cassandra']),
        install_keys=yaml.safe_dump([None, None]),
        private_jre_url=get_jre_url())

    @classmethod
    @unittest.skipUnless(get_jre_url(), 'No Oracle JRE tarballs available')
    @unittest.skipIf('DSE_SOURCE' not in os.environ,
                     'DSE_SOURCE environment variable not configured')
    def setUpClass(cls):
        super(TestDSEDeployment, cls).setUpClass()


class TestAllowAllAuthenticatorDeployment(Test3UnitDeployment):
    test_config = dict(authenticator='AllowAllAuthenticator')

    def cluster(self, username=None, password=None, hosts=None, port=9042):
        '''A cluster using invalid credentials.'''
        return super(TestAllowAllAuthenticatorDeployment,
                     self).cluster(username='wat', password='eva')

    def client_session(self, relname):
        '''A session using invalid credentials.'''
        relinfo = self.get_client_relinfo(relname)
        self.assertIn('host', relinfo.keys())
        cluster = self.cluster('random', 'nonsense',
                               [relinfo['host']],
                               int(relinfo['native_transport_port']))
        session = cluster.connect()
        self.addCleanup(session.shutdown)
        return session

    test_default_superuser_account_closed = None


class Test20Deployment(Test1UnitDeployment):
    """Tests run on a single node Apache Cassandra 2.0 cluster.
    """
    rf = 1
    test_config = dict(
        edition='community',
        install_sources=yaml.safe_dump([
            'ppa:stub/cassandra',
            'ppa:openjdk-r/ppa',
            'deb http://www.apache.org/dist/cassandra/debian 20x main']),
        install_keys=yaml.safe_dump([None, None, None]))


class Test21Deployment(Test1UnitDeployment):
    """Tests run on a single node Apache Cassandra 2.1 cluster.
    """
    rf = 1
    test_config = dict(
        edition='community',
        install_sources=yaml.safe_dump([
            'ppa:stub/cassandra',
            'ppa:openjdk-r/ppa',
            'deb http://www.apache.org/dist/cassandra/debian 21x main']),
        install_keys=yaml.safe_dump([None, None, None]))


class Test30Deployment(Test1UnitDeployment):
    """Tests run on a single node Apache Cassandra 3.0 cluster.
    """
    rf = 1
    test_config = dict(
        edition='community',
        install_sources=yaml.safe_dump([
            'ppa:stub/cassandra',
            'ppa:openjdk-r/ppa',
            'deb http://www.apache.org/dist/cassandra/debian 30x main']),
        install_keys=yaml.safe_dump([None, None, None]))


# Bug #1417097 means we need to monkey patch Amulet for now.
real_juju = amulet.helpers.juju


@wraps(real_juju)
def patched_juju(args, env=None):
    args = [str(a) for a in args]
    return real_juju(args, env)

amulet.helpers.juju = patched_juju
amulet.deployer.juju = patched_juju


if __name__ == '__main__':
    unittest.main(verbosity=2)
