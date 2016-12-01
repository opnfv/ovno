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
from contextlib import contextmanager
from datetime import timedelta
from distutils.version import LooseVersion
import errno
from functools import wraps
import io
import json
import os.path
import re
import shutil
import subprocess
import sys
import tempfile
from textwrap import dedent
import time

import bcrypt
from cassandra import ConsistencyLevel
import cassandra.auth
import cassandra.cluster
import cassandra.query
import yaml

from charmhelpers.core import hookenv, host
from charmhelpers.core.hookenv import DEBUG, ERROR, WARNING
from charmhelpers import fetch

from coordinator import coordinator


RESTART_TIMEOUT = 600


def logged(func):
    @wraps(func)
    def wrapper(*args, **kw):
        hookenv.log("* Helper {}/{}".format(hookenv.hook_name(),
                                            func.__name__))
        return func(*args, **kw)
    return wrapper


def backoff(what_for, max_pause=60):
    i = 0
    while True:
        yield True
        i += 1
        pause = min(max_pause, 2 ** i)
        time.sleep(pause)
        if pause > 10:
            hookenv.log('Recheck {} for {}'.format(i, what_for))


# FOR CHARMHELPERS
@contextmanager
def autostart_disabled(services=None, _policy_rc='/usr/sbin/policy-rc.d'):
    '''Tell well behaved Debian packages to not start services when installed.
    '''
    script = ['#!/bin/sh']
    if services is not None:
        for service in services:
            script.append(
                'if [ "$1" = "{}" ]; then exit 101; fi'.format(service))
        script.append('exit 0')
    else:
        script.append('exit 101')  # By default, all daemons disabled.
    try:
        if os.path.exists(_policy_rc):
            shutil.move(_policy_rc, "{}-orig".format(_policy_rc))
        host.write_file(_policy_rc, '\n'.join(script).encode('ASCII'),
                        perms=0o555)
        yield
    finally:
        os.unlink(_policy_rc)
        if os.path.exists("{}-orig".format(_policy_rc)):
            shutil.move("{}-orig".format(_policy_rc), _policy_rc)


# FOR CHARMHELPERS
@logged
def install_packages(packages):
    packages = list(packages)
    if hookenv.config('extra_packages'):
        packages.extend(hookenv.config('extra_packages').split())
    packages = fetch.filter_installed_packages(packages)
    if packages:
        # The DSE packages are huge, so this might take some time.
        status_set('maintenance', 'Installing packages')
        with autostart_disabled(['cassandra']):
            fetch.apt_install(packages, options="--force-yes", fatal=True)


# FOR CHARMHELPERS
@logged
def ensure_package_status(packages):
    config_dict = hookenv.config()

    package_status = config_dict['package_status']

    if package_status not in ['install', 'hold']:
        raise RuntimeError("package_status must be 'install' or 'hold', "
                           "not {!r}".format(package_status))

    selections = []
    for package in packages:
        selections.append('{} {}\n'.format(package, package_status))
    dpkg = subprocess.Popen(['dpkg', '--set-selections'],
                            stdin=subprocess.PIPE)
    dpkg.communicate(input=''.join(selections).encode('US-ASCII'))


def get_seed_ips():
    '''Return the set of seed ip addresses.

    We use ip addresses rather than unit names, as we may need to use
    external seed ips at some point.
    '''
    return set((hookenv.leader_get('seeds') or '').split(','))


def actual_seed_ips():
    '''Return the seeds currently in cassandra.yaml'''
    cassandra_yaml = read_cassandra_yaml()
    s = cassandra_yaml['seed_provider'][0]['parameters'][0]['seeds']
    return set(s.split(','))


def get_database_directory(config_path):
    '''Convert a database path from the service config to an absolute path.

    Entries in the config file may be absolute, relative to
    /var/lib/cassandra, or relative to the mountpoint.
    '''
    import relations
    storage = relations.StorageRelation()
    if storage.mountpoint:
        root = os.path.join(storage.mountpoint, 'cassandra')
    else:
        root = '/var/lib/cassandra'
    return os.path.join(root, config_path)


def ensure_database_directory(config_path):
    '''Create the database directory if it doesn't exist, resetting
    ownership and other settings while we are at it.

    Returns the absolute path.
    '''
    absdir = get_database_directory(config_path)

    # Work around Bug #1427150 by ensuring components of the path are
    # created with the required permissions, if necessary.
    component = os.sep
    for p in absdir.split(os.sep)[1:-1]:
        component = os.path.join(component, p)
        if not os.path.exists(p):
            host.mkdir(component)
    assert component == os.path.split(absdir)[0]
    host.mkdir(absdir, owner='cassandra', group='cassandra', perms=0o750)
    return absdir


def get_all_database_directories():
    config = hookenv.config()
    dirs = dict(
        data_file_directories=[get_database_directory(d)
                               for d in (config['data_file_directories'] or
                                         'data').split()],
        commitlog_directory=get_database_directory(
            config['commitlog_directory'] or 'commitlog'),
        saved_caches_directory=get_database_directory(
            config['saved_caches_directory'] or 'saved_caches'))
    if has_cassandra_version('3.0'):
        # Not yet configurable. Make configurable with Juju native storage.
        dirs['hints_directory'] = get_database_directory('hints')
    return dirs


def mountpoint(path):
    '''Return the mountpoint that path exists on.'''
    path = os.path.realpath(path)
    while path != '/' and not os.path.ismount(path):
        path = os.path.dirname(path)
    return path


# FOR CHARMHELPERS
def is_lxc():
    '''Return True if we are running inside an LXC container.'''
    with open('/proc/1/cgroup', 'r') as f:
        return ':/lxc/' in f.readline()


# FOR CHARMHELPERS
def set_io_scheduler(io_scheduler, directory):
    '''Set the block device io scheduler.'''

    assert os.path.isdir(directory)

    # The block device regex may be a tad simplistic.
    block_regex = re.compile('\/dev\/([a-z]*)', re.IGNORECASE)

    output = subprocess.check_output(['df', directory],
                                     universal_newlines=True)

    if not is_lxc():
        hookenv.log("Setting block device of {} to IO scheduler {}"
                    "".format(directory, io_scheduler))
        try:
            block_dev = re.findall(block_regex, output)[0]
        except IndexError:
            hookenv.log("Unable to locate block device of {} (in container?)"
                        "".format(directory))
            return
        sys_file = os.path.join("/", "sys", "block", block_dev,
                                "queue", "scheduler")
        try:
            host.write_file(sys_file, io_scheduler.encode('ascii'),
                            perms=0o644)
        except OSError as e:
            if e.errno == errno.EACCES:
                hookenv.log("Got Permission Denied trying to set the "
                            "IO scheduler at {}. We may be in an LXC. "
                            "Exiting gracefully".format(sys_file),
                            WARNING)
            elif e.errno == errno.ENOENT:
                hookenv.log("Got no such file or directory trying to "
                            "set the IO scheduler at {}. It may be "
                            "this is an LXC, the device name is as "
                            "yet unknown to the charm, or LVM/RAID is "
                            "hiding the underlying device name. "
                            "Exiting gracefully".format(sys_file),
                            WARNING)
            else:
                raise e
    else:
        # Make no change if we are in an LXC
        hookenv.log("In an LXC. Cannot set io scheduler {}"
                    "".format(io_scheduler))


# FOR CHARMHELPERS
def recursive_chown(directory, owner="root", group="root"):
    '''Change ownership of all files and directories in 'directory'.

    Ownership of 'directory' is also reset.
    '''
    shutil.chown(directory, owner, group)
    for root, dirs, files in os.walk(directory):
        for dirname in dirs:
            shutil.chown(os.path.join(root, dirname), owner, group)
        for filename in files:
            shutil.chown(os.path.join(root, filename), owner, group)


def maybe_backup(path):
    '''Copy a file to file.orig, if file.orig does not already exist.'''
    backup_path = path + '.orig'
    if not os.path.exists(backup_path):
        with open(path, 'rb') as f:
            host.write_file(backup_path, f.read(), perms=0o600)


# FOR CHARMHELPERS
def get_package_version(package):
    cache = fetch.apt_cache()
    if package not in cache:
        return None
    pkgver = cache[package].current_ver
    if pkgver is not None:
        return pkgver.ver_str
    return None


def get_jre():
    # DataStax Enterprise requires the Oracle JRE.
    if get_cassandra_edition() == 'dse':
        return 'oracle'

    config = hookenv.config()
    jre = config['jre'].lower()
    if jre not in ('openjdk', 'oracle'):
        hookenv.log('Unknown JRE {!r} specified. Using OpenJDK'.format(jre),
                    ERROR)
        jre = 'openjdk'
    return jre


def get_cassandra_edition():
    config = hookenv.config()
    edition = config['edition'].lower()
    if edition not in ('community', 'dse'):
        hookenv.log('Unknown edition {!r}. Using community.'.format(edition),
                    ERROR)
        edition = 'community'
    return edition


def get_cassandra_service():
    '''Cassandra upstart service'''
    if get_cassandra_edition() == 'dse':
        return 'dse'
    return 'cassandra'


def get_cassandra_version():
    if get_cassandra_edition() == 'dse':
        dse_ver = get_package_version('dse-full')
        if not dse_ver:
            return None
        elif LooseVersion(dse_ver) >= LooseVersion('5.0'):
            return '3.0'
        elif LooseVersion(dse_ver) >= LooseVersion('4.7'):
            return '2.1'
        else:
            return '2.0'
    return get_package_version('cassandra')


def has_cassandra_version(minimum_ver):
    cassandra_version = get_cassandra_version()
    assert cassandra_version is not None, 'Cassandra package not yet installed'
    return LooseVersion(cassandra_version) >= LooseVersion(minimum_ver)


def get_cassandra_config_dir():
    if get_cassandra_edition() == 'dse':
        return '/etc/dse/cassandra'
    else:
        return '/etc/cassandra'


def get_cassandra_yaml_file():
    return os.path.join(get_cassandra_config_dir(), "cassandra.yaml")


def get_cassandra_env_file():
    return os.path.join(get_cassandra_config_dir(), "cassandra-env.sh")


def get_cassandra_rackdc_file():
    return os.path.join(get_cassandra_config_dir(),
                        "cassandra-rackdc.properties")


def get_cassandra_pid_file():
    edition = get_cassandra_edition()
    if edition == 'dse':
        pid_file = "/var/run/dse/dse.pid"
    else:
        pid_file = "/var/run/cassandra/cassandra.pid"
    return pid_file


def get_cassandra_packages():
    edition = get_cassandra_edition()
    if edition == 'dse':
        packages = set(['dse-full'])
    else:
        packages = set(['cassandra'])  # 'cassandra-tools'

    packages.add('ntp')
    packages.add('run-one')
    packages.add('netcat')

    jre = get_jre()
    if jre == 'oracle':
        # We can't use a packaged version of the Oracle JRE, as we
        # are not allowed to bypass Oracle's click through license
        # agreement.
        pass
    else:
        # NB. OpenJDK 8 not available in trusty. This needs to come
        # from a PPA or some other configured source.
        packages.add('openjdk-8-jre-headless')

    return packages


@logged
def stop_cassandra():
    if is_cassandra_running():
        hookenv.log('Shutting down Cassandra')
        host.service_stop(get_cassandra_service())
    if is_cassandra_running():
        hookenv.status_set('blocked', 'Cassandra failed to shut down')
        raise SystemExit(0)


@logged
def start_cassandra():
    if is_cassandra_running():
        return

    actual_seeds = sorted(actual_seed_ips())
    assert actual_seeds, 'Attempting to start cassandra with empty seed list'
    hookenv.config()['configured_seeds'] = actual_seeds

    if is_bootstrapped():
        status_set('maintenance',
                   'Starting Cassandra with seeds {!r}'
                   .format(','.join(actual_seeds)))
    else:
        status_set('maintenance',
                   'Bootstrapping with seeds {}'
                   .format(','.join(actual_seeds)))

    host.service_start(get_cassandra_service())

    # Wait for Cassandra to actually start, or abort.
    timeout = time.time() + RESTART_TIMEOUT
    while time.time() < timeout:
        if is_cassandra_running():
            return
        time.sleep(1)
    status_set('blocked', 'Cassandra failed to start')
    raise SystemExit(0)


@logged
def reconfigure_and_restart_cassandra(overrides={}):
    stop_cassandra()
    configure_cassandra_yaml(overrides)
    start_cassandra()


@logged
def remount_cassandra():
    '''If a new mountpoint is ready, migrate data across to it.'''
    assert not is_cassandra_running()  # Guard against data loss.
    import relations
    storage = relations.StorageRelation()
    if storage.needs_remount():
        status_set('maintenance', 'Migrating data to new mountpoint')
        hookenv.config()['bootstrapped_into_cluster'] = False
        if storage.mountpoint is None:
            hookenv.log('External storage AND DATA gone. '
                        'Reverting to local storage. '
                        'In danger of resurrecting old data. ',
                        WARNING)
        else:
            storage.migrate('/var/lib/cassandra', 'cassandra')
            root = os.path.join(storage.mountpoint, 'cassandra')
            os.chmod(root, 0o750)


@logged
def ensure_database_directories():
    '''Ensure that directories Cassandra expects to store its data in exist.'''
    # Guard against changing perms on a running db. Although probably
    # harmless, it causes shutil.chown() to fail.
    assert not is_cassandra_running()
    db_dirs = get_all_database_directories()
    ensure_database_directory(db_dirs['commitlog_directory'])
    ensure_database_directory(db_dirs['saved_caches_directory'])
    if 'hints_directory' in db_dirs:
        ensure_database_directory(db_dirs['hints_directory'])
    for db_dir in db_dirs['data_file_directories']:
        ensure_database_directory(db_dir)


CONNECT_TIMEOUT = 10


@contextmanager
def connect(username=None, password=None, timeout=CONNECT_TIMEOUT,
            auth_timeout=CONNECT_TIMEOUT):
    # We pull the currently configured listen address and port from the
    # yaml, rather than the service configuration, as it may have been
    # overridden.
    cassandra_yaml = read_cassandra_yaml()
    address = cassandra_yaml['rpc_address']
    if address == '0.0.0.0':
        address = 'localhost'
    port = cassandra_yaml['native_transport_port']

    if username is None or password is None:
        username, password = superuser_credentials()

    auth = hookenv.config()['authenticator']
    if auth == 'AllowAllAuthenticator':
        auth_provider = None
    else:
        auth_provider = cassandra.auth.PlainTextAuthProvider(username=username,
                                                             password=password)

    # Although we specify a reconnection_policy, it does not apply to
    # the initial connection so we retry in a loop.
    start = time.time()
    until = start + timeout
    auth_until = start + auth_timeout
    while True:
        cluster = cassandra.cluster.Cluster([address], port=port,
                                            auth_provider=auth_provider)
        try:
            session = cluster.connect()
            session.default_timeout = timeout
            break
        except cassandra.cluster.NoHostAvailable as x:
            cluster.shutdown()
            now = time.time()
            # If every node failed auth, reraise one of the
            # AuthenticationFailed exceptions. Unwrapping the exception
            # means call sites don't have to sniff the exception bundle.
            # We don't retry on auth fails; this method should not be
            # called if the system_auth data is inconsistent.
            auth_fails = [af for af in x.errors.values()
                          if isinstance(af, cassandra.AuthenticationFailed)]
            if auth_fails:
                if now > auth_until:
                    raise auth_fails[0]
            if now > until:
                raise
        time.sleep(1)
    try:
        yield session
    finally:
        cluster.shutdown()


QUERY_TIMEOUT = 60


def query(session, statement, consistency_level, args=None):
    q = cassandra.query.SimpleStatement(statement,
                                        consistency_level=consistency_level)

    until = time.time() + QUERY_TIMEOUT
    for _ in backoff('query to execute'):
        try:
            return session.execute(q, args)
        except Exception:
            if time.time() > until:
                raise


def encrypt_password(password):
    return bcrypt.hashpw(password, bcrypt.gensalt())


@logged
def ensure_user(session, username, encrypted_password, superuser=False):
    '''Create the DB user if it doesn't already exist & reset the password.'''
    auth = hookenv.config()['authenticator']
    if auth == 'AllowAllAuthenticator':
        return  # No authentication means we cannot create users

    if superuser:
        hookenv.log('Creating SUPERUSER {}'.format(username))
    else:
        hookenv.log('Creating user {}'.format(username))
    if has_cassandra_version('2.2'):
        query(session,
              'INSERT INTO system_auth.roles '
              '(role, can_login, is_superuser, salted_hash) '
              'VALUES (%s, TRUE, %s, %s)',
              ConsistencyLevel.ALL,
              (username, superuser, encrypted_password))
    else:
        query(session,
              'INSERT INTO system_auth.users (name, super) VALUES (%s, %s)',
              ConsistencyLevel.ALL, (username, superuser))
        query(session,
              'INSERT INTO system_auth.credentials (username, salted_hash) '
              'VALUES (%s, %s)',
              ConsistencyLevel.ALL, (username, encrypted_password))


@logged
def create_unit_superuser_hard():
    '''Create or recreate the unit's superuser account.

    This method is used when there are no known superuser credentials
    to use. We restart the node using the AllowAllAuthenticator and
    insert our credentials directly into the system_auth keyspace.
    '''
    username, password = superuser_credentials()
    pwhash = encrypt_password(password)
    hookenv.log('Creating unit superuser {}'.format(username))

    # Restart cassandra without authentication & listening on localhost.
    reconfigure_and_restart_cassandra(
        dict(authenticator='AllowAllAuthenticator', rpc_address='localhost'))
    for _ in backoff('superuser creation'):
        try:
            with connect() as session:
                ensure_user(session, username, pwhash, superuser=True)
                break
        except Exception as x:
            print(str(x))

    # Restart Cassandra with regular config.
    nodetool('flush')  # Ensure our backdoor updates are flushed.
    reconfigure_and_restart_cassandra()


def get_cqlshrc_path():
    return os.path.expanduser('~root/.cassandra/cqlshrc')


def superuser_username():
    return 'juju_{}'.format(re.subn(r'\W', '_', hookenv.local_unit())[0])


def superuser_credentials():
    '''Return (username, password) to connect to the Cassandra superuser.

    The credentials are persisted in the root user's cqlshrc file,
    making them easily accessible to the command line tools.
    '''
    cqlshrc_path = get_cqlshrc_path()
    cqlshrc = configparser.ConfigParser(interpolation=None)
    cqlshrc.read([cqlshrc_path])

    username = superuser_username()

    try:
        section = cqlshrc['authentication']
        # If there happened to be an existing cqlshrc file, it might
        # contain invalid credentials. Ignore them.
        if section['username'] == username:
            return section['username'], section['password']
    except KeyError:
        hookenv.log('Generating superuser credentials into {}'.format(
            cqlshrc_path))

    config = hookenv.config()

    password = host.pwgen()

    hookenv.log('Generated username {}'.format(username))

    # We set items separately, rather than together, so that we have a
    # defined order for the ConfigParser to preserve and the tests to
    # rely on.
    cqlshrc.setdefault('authentication', {})
    cqlshrc['authentication']['username'] = username
    cqlshrc['authentication']['password'] = password
    cqlshrc.setdefault('connection', {})
    cqlshrc['connection']['hostname'] = hookenv.unit_public_ip()
    if get_cassandra_version().startswith('2.0'):
        cqlshrc['connection']['port'] = str(config['rpc_port'])
    else:
        cqlshrc['connection']['port'] = str(config['native_transport_port'])

    ini = io.StringIO()
    cqlshrc.write(ini)
    host.mkdir(os.path.dirname(cqlshrc_path), perms=0o700)
    host.write_file(cqlshrc_path, ini.getvalue().encode('UTF-8'), perms=0o400)

    return username, password


def emit(*args, **kw):
    # Just like print, but with plumbing and mocked out in the test suite.
    print(*args, **kw)
    sys.stdout.flush()


def nodetool(*cmd, timeout=120):
    cmd = ['nodetool'] + [str(i) for i in cmd]
    i = 0
    until = time.time() + timeout
    for _ in backoff('nodetool to work'):
        i += 1
        try:
            if timeout is not None:
                timeout = max(0, until - time.time())
            raw = subprocess.check_output(cmd, universal_newlines=True,
                                          timeout=timeout,
                                          stderr=subprocess.STDOUT)

            # Work around CASSANDRA-8776.
            if 'status' in cmd and 'Error:' in raw:
                hookenv.log('Error detected but nodetool returned success.',
                            WARNING)
                raise subprocess.CalledProcessError(99, cmd, raw)

            hookenv.log('{} succeeded'.format(' '.join(cmd)), DEBUG)
            out = raw.expandtabs()
            emit(out)
            return out

        except subprocess.CalledProcessError as x:
            if i > 1:
                emit(x.output.expandtabs())  # Expand tabs for juju debug-log.
            if not is_cassandra_running():
                status_set('blocked',
                           'Cassandra has unexpectedly shutdown')
                raise SystemExit(0)
            if time.time() >= until:
                raise


def num_nodes():
    return len(get_bootstrapped_ips())


def read_cassandra_yaml():
    cassandra_yaml_path = get_cassandra_yaml_file()
    with open(cassandra_yaml_path, 'rb') as f:
        return yaml.safe_load(f)


@logged
def write_cassandra_yaml(cassandra_yaml):
    cassandra_yaml_path = get_cassandra_yaml_file()
    host.write_file(cassandra_yaml_path,
                    yaml.safe_dump(cassandra_yaml).encode('UTF-8'))


def configure_cassandra_yaml(overrides={}, seeds=None):
    cassandra_yaml_path = get_cassandra_yaml_file()
    config = hookenv.config()

    maybe_backup(cassandra_yaml_path)  # Its comments may be useful.

    cassandra_yaml = read_cassandra_yaml()

    # Most options just copy from config.yaml keys with the same name.
    # Using the same name is preferred to match the actual Cassandra
    # documentation.
    simple_config_keys = ['cluster_name', 'num_tokens',
                          'partitioner', 'authorizer', 'authenticator',
                          'compaction_throughput_mb_per_sec',
                          'stream_throughput_outbound_megabits_per_sec',
                          'tombstone_warn_threshold',
                          'tombstone_failure_threshold',
                          'native_transport_port', 'rpc_port',
                          'storage_port', 'ssl_storage_port']
    cassandra_yaml.update((k, config[k]) for k in simple_config_keys)

    seeds = ','.join(seeds or get_seed_ips())  # Don't include whitespace!
    hookenv.log('Configuring seeds as {!r}'.format(seeds), DEBUG)
    cassandra_yaml['seed_provider'][0]['parameters'][0]['seeds'] = seeds

    cassandra_yaml['listen_address'] = hookenv.unit_private_ip()
    cassandra_yaml['rpc_address'] = '0.0.0.0'
    if not get_cassandra_version().startswith('2.0'):
        cassandra_yaml['broadcast_rpc_address'] = hookenv.unit_public_ip()

    dirs = get_all_database_directories()
    cassandra_yaml.update(dirs)

    # GossipingPropertyFileSnitch is the only snitch recommended for
    # production. It we allow others, we need to consider how to deal
    # with the system_auth keyspace replication settings.
    cassandra_yaml['endpoint_snitch'] = 'GossipingPropertyFileSnitch'

    # Per Bug #1523546 and CASSANDRA-9319, Thrift is disabled by default in
    # Cassandra 2.2. Ensure it is enabled if rpc_port is non-zero.
    if int(config['rpc_port']) > 0:
        cassandra_yaml['start_rpc'] = True

    cassandra_yaml.update(overrides)

    write_cassandra_yaml(cassandra_yaml)


def get_pid_from_file(pid_file):
    try:
        with open(pid_file, 'r') as f:
            pid = int(f.read().strip().split()[0])
            if pid <= 1:
                raise ValueError('Illegal pid {}'.format(pid))
            return pid
    except (ValueError, IndexError) as e:
        hookenv.log("Invalid PID in {}.".format(pid_file))
        raise ValueError(e)


def is_cassandra_running():
    pid_file = get_cassandra_pid_file()

    try:
        for _ in backoff('Cassandra to respond'):
            # We reload the pid every time, in case it has gone away.
            # If it goes away, a FileNotFound exception is raised.
            pid = get_pid_from_file(pid_file)

            # This does not kill the process but checks for its
            # existence. It raises an ProcessLookupError if the process
            # is not running.
            os.kill(pid, 0)

            if subprocess.call(["nodetool", "status"],
                               stdout=subprocess.DEVNULL,
                               stderr=subprocess.DEVNULL) == 0:
                hookenv.log(
                    "Cassandra PID {} is running and responding".format(pid))
                return True
    except FileNotFoundError:
        hookenv.log("Cassandra is not running. PID file does not exist.")
        return False
    except ProcessLookupError:
        if os.path.exists(pid_file):
            # File disappeared between reading the PID and checking if
            # the PID is running.
            hookenv.log("Cassandra is not running, but pid file exists.",
                        WARNING)
        else:
            hookenv.log("Cassandra is not running. PID file does not exist.")
        return False


def get_auth_keyspace_replication(session):
    if has_cassandra_version('3.0'):
        statement = dedent('''\
            SELECT replication FROM system_schema.keyspaces
            WHERE keyspace_name='system_auth'
            ''')
        r = query(session, statement, ConsistencyLevel.QUORUM)
        return dict(r[0][0])
    else:
        statement = dedent('''\
            SELECT strategy_options FROM system.schema_keyspaces
            WHERE keyspace_name='system_auth'
            ''')
        r = query(session, statement, ConsistencyLevel.QUORUM)
        return json.loads(r[0][0])


@logged
def set_auth_keyspace_replication(session, settings):
    # Live operation, so keep status the same.
    status_set(hookenv.status_get()[0],
               'Updating system_auth rf to {!r}'.format(settings))
    statement = 'ALTER KEYSPACE system_auth WITH REPLICATION = %s'
    query(session, statement, ConsistencyLevel.ALL, (settings,))


@logged
def repair_auth_keyspace():
    # Repair takes a long time, and may need to be retried due to 'snapshot
    # creation' errors, but should certainly complete within an hour since
    # the keyspace is tiny.
    status_set(hookenv.status_get()[0],
               'Repairing system_auth keyspace')
    nodetool('repair', 'system_auth', timeout=3600)


def is_bootstrapped(unit=None):
    '''Return True if the node has already bootstrapped into the cluster.'''
    if unit is None or unit == hookenv.local_unit():
        return hookenv.config().get('bootstrapped', False)
    elif coordinator.relid:
        return bool(hookenv.relation_get(rid=coordinator.relid,
                                         unit=unit).get('bootstrapped'))
    else:
        return False


def set_bootstrapped():
    # We need to store this flag in two locations. The peer relation,
    # so peers can see it, and local state, for when we haven't joined
    # the peer relation yet. actions.publish_bootstrapped_flag()
    # calls this method again when necessary to ensure that state is
    # propagated # if/when the peer relation is joined.
    config = hookenv.config()
    config['bootstrapped'] = True
    if coordinator.relid is not None:
        hookenv.relation_set(coordinator.relid, bootstrapped="1")
    if config.changed('bootstrapped'):
        hookenv.log('Bootstrapped')
    else:
        hookenv.log('Already bootstrapped')


def get_bootstrapped():
    units = [hookenv.local_unit()]
    if coordinator.relid is not None:
        units.extend(hookenv.related_units(coordinator.relid))
    return set([unit for unit in units if is_bootstrapped(unit)])


def get_bootstrapped_ips():
    return set([unit_to_ip(unit) for unit in get_bootstrapped()])


def unit_to_ip(unit):
    if unit is None or unit == hookenv.local_unit():
        return hookenv.unit_private_ip()
    elif coordinator.relid:
        pa = hookenv.relation_get(rid=coordinator.relid,
                                  unit=unit).get('private-address')
        return hookenv._ensure_ip(pa)
    else:
        return None


def get_node_status():
    '''Return the Cassandra node status.

    May be NORMAL, JOINING, DECOMMISSIONED etc., or None if we can't tell.
    '''
    if not is_cassandra_running():
        return None
    raw = nodetool('netstats')
    m = re.search(r'(?m)^Mode:\s+(\w+)$', raw)
    if m is None:
        return None
    return m.group(1).upper()


def is_decommissioned():
    status = get_node_status()
    if status in ('DECOMMISSIONED', 'LEAVING'):
        hookenv.log('This node is {}'.format(status), WARNING)
        return True
    return False


@logged
def emit_describe_cluster():
    '''Run nodetool describecluster for the logs.'''
    nodetool('describecluster')  # Implicit emit


@logged
def emit_status():
    '''Run 'nodetool status' for the logs.'''
    nodetool('status')  # Implicit emit


@logged
def emit_netstats():
    '''Run 'nodetool netstats' for the logs.'''
    nodetool('netstats')  # Implicit emit


def emit_cluster_info():
    emit_describe_cluster()
    emit_status()
    emit_netstats()


# FOR CHARMHELPERS (and think of a better name)
def week_spread(unit_num):
    '''Pick a time for a unit's weekly job.

    Jobs are spread out evenly throughout the week as best we can.
    The chosen time only depends on the unit number, and does not change
    if other units are added and removed; while the chosen time will not
    be perfect, we don't have to worry about skipping a weekly job if
    units are added or removed at the wrong moment.

    Returns (dow, hour, minute) suitable for cron.
    '''
    def vdc(n, base=2):
        '''Van der Corpet sequence. 0, 0.5, 0.25, 0.75, 0.125, 0.625, ...

        http://rosettacode.org/wiki/Van_der_Corput_sequence#Python
        '''
        vdc, denom = 0, 1
        while n:
            denom *= base
            n, remainder = divmod(n, base)
            vdc += remainder / denom
        return vdc
    # We could use the vdc() function to distribute jobs evenly throughout
    # the week, so unit 0==0, unit 1==3.5days, unit 2==1.75 etc. But
    # plain modulo for the day of week is easier for humans and what
    # you expect for 7 units or less.
    sched_dow = unit_num % 7
    # We spread time of day so each batch of 7 units gets the same time,
    # as far spread out from the other batches of 7 units as possible.
    minutes_in_day = 24 * 60
    sched = timedelta(minutes=int(minutes_in_day * vdc(unit_num // 7)))
    sched_hour = sched.seconds // (60 * 60)
    sched_minute = sched.seconds // 60 - sched_hour * 60
    return (sched_dow, sched_hour, sched_minute)


# FOR CHARMHELPERS. This should be a constant in nrpe.py
def local_plugins_dir():
    return '/usr/local/lib/nagios/plugins'


def leader_ping():
    '''Make a change in the leader settings, waking the non-leaders.'''
    assert hookenv.is_leader()
    last = int(hookenv.leader_get('ping') or 0)
    hookenv.leader_set(ping=str(last + 1))


def get_unit_superusers():
    '''Return the set of units that have had their superuser accounts created.
    '''
    raw = hookenv.leader_get('superusers')
    return set(json.loads(raw or '[]'))


def set_unit_superusers(superusers):
    hookenv.leader_set(superusers=json.dumps(sorted(superusers)))


def status_set(state, message):
    '''Set the unit status and log a message.'''
    hookenv.status_set(state, message)
    hookenv.log('{} unit state: {}'.format(state, message))


def service_status_set(state, message):
    '''Set the service status and log a message.'''
    subprocess.check_call(['status-set', '--service', state, message])
    hookenv.log('{} service state: {}'.format(state, message))


def get_service_name(relid):
    '''Return the service name for the other end of relid.'''
    units = hookenv.related_units(relid)
    if units:
        return units[0].split('/', 1)[0]
    else:
        return None


def peer_relid():
    return coordinator.relid


@logged
def set_active():
    '''Set happy state'''
    if hookenv.unit_private_ip() in get_seed_ips():
        msg = 'Live seed'
    else:
        msg = 'Live node'
    status_set('active', msg)

    if hookenv.is_leader():
        n = num_nodes()
        if n == 1:
            n = 'Single'
        service_status_set('active', '{} node cluster'.format(n))


def update_hosts_file(hosts_file, hosts_map):
    """Older versions of Cassandra need own hostname resolution."""
    with open(hosts_file, 'r') as hosts:
        lines = hosts.readlines()

    newlines = []
    for ip, hostname in hosts_map.items():
        if not ip or not hostname:
            continue

        keepers = []
        for line in lines:
            _line = line.split()
            if len(_line) < 2 or not (_line[0] == ip or hostname in _line[1:]):
                keepers.append(line)
            else:
                hookenv.log('Marking line {!r} for update or removal'
                            ''.format(line.strip()), level=DEBUG)

        lines = keepers
        newlines.append('{} {}\n'.format(ip, hostname))

    lines += newlines

    with tempfile.NamedTemporaryFile(delete=False) as tmpfile:
        with open(tmpfile.name, 'w') as hosts:
            for line in lines:
                hosts.write(line)

    os.rename(tmpfile.name, hosts_file)
    os.chmod(hosts_file, 0o644)
