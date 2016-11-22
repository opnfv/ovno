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

from contextlib import closing
import errno
from functools import wraps
import glob
import os.path
import re
import shlex
import socket
import subprocess
from textwrap import dedent
import time
import urllib.request

from charmhelpers import fetch
from charmhelpers.contrib.charmsupport import nrpe
from charmhelpers.contrib.network import ufw
from charmhelpers.contrib.templating import jinja
from charmhelpers.core import hookenv, host
from charmhelpers.core.fstab import Fstab
from charmhelpers.core.hookenv import DEBUG, ERROR, WARNING

import cassandra

from coordinator import coordinator
import helpers
import relations


# These config keys cannot be changed after service deployment.
UNCHANGEABLE_KEYS = set(['cluster_name', 'datacenter', 'rack', 'edition'])

# If any of these config items are changed, Cassandra needs to be
# restarted and maybe remounted.
RESTART_REQUIRED_KEYS = set([
    'data_file_directories',
    'commitlog_directory',
    'saved_caches_directory',
    'storage_port',
    'ssl_storage_port',
    'rpc_port',
    'native_transport_port',
    'partitioner',
    'num_tokens',
    'max_heap_size',
    'heap_newsize',
    'authenticator',
    'authorizer',
    'compaction_throughput_mb_per_sec',
    'stream_throughput_outbound_megabits_per_sec',
    'tombstone_warn_threshold',
    'tombstone_failure_threshold',
    'jre',
    'private_jre_url'])

ALL_CONFIG_KEYS = UNCHANGEABLE_KEYS.union(RESTART_REQUIRED_KEYS)


# All other config items. By maintaining both lists, we can detect if
# someone forgot to update these lists when they added a new config item.
RESTART_NOT_REQUIRED_KEYS = set([
    'extra_packages',
    'package_status',
    'install_sources',
    'install_keys',
    'http_proxy',
    'wait_for_storage_broker',
    'io_scheduler',
    'nagios_context',
    'nagios_servicegroups',
    'nagios_heapchk_warn_pct',
    'nagios_heapchk_crit_pct',
    'nagios_disk_warn_pct',
    'nagios_disk_crit_pct'])


def action(func):
    '''Log and call func, stripping the undesirable servicename argument.
    '''
    @wraps(func)
    def wrapper(servicename, *args, **kw):
        if hookenv.remote_unit():
            hookenv.log("** Action {}/{} ({})".format(hookenv.hook_name(),
                                                      func.__name__,
                                                      hookenv.remote_unit()))
        else:
            hookenv.log("** Action {}/{}".format(hookenv.hook_name(),
                                                 func.__name__))
        return func(*args, **kw)
    return wrapper


def leader_only(func):
    '''Decorated function is only run on the leader.'''
    @wraps(func)
    def wrapper(*args, **kw):
        if hookenv.is_leader():
            return func(*args, **kw)
        else:
            return None
    return wrapper


def authentication(func):
    '''Decorated function is skipped if authentication is disabled.'''
    @wraps(func)
    def wrapper(*args, **kw):
        auth = hookenv.config()['authenticator']
        if auth == 'PasswordAuthenticator':
            return func(*args, **kw)
        elif auth == 'AllowAllAuthenticator':
            hookenv.log('Skipped. Authentication disabled.', DEBUG)
            return None
        helpers.status_set('blocked', 'Unknown authenticator {}'.format(auth))
        raise SystemExit(0)
    return wrapper


@action
def set_proxy():
    import hooks
    hooks.set_proxy()


@action
def revert_unchangeable_config():
    config = hookenv.config()

    # config.previous() only becomes meaningful after the install
    # hook has run. During the first run on the unit hook, it
    # reports everything has having None as the previous value.
    if config._prev_dict is None:
        return

    for key in UNCHANGEABLE_KEYS:
        if config.changed(key):
            previous = config.previous(key)
            hookenv.log('{} cannot be changed after service deployment. '
                        'Using original setting {!r}'.format(key, previous),
                        ERROR)
            config[key] = previous


# FOR CHARMHELPERS
@action
def preinstall():
    '''Preinstallation data_ready hook.'''
    # Only run the preinstall hooks from the actual install hook.
    if hookenv.hook_name() == 'install':
        # Pre-exec
        pattern = os.path.join(hookenv.charm_dir(),
                               'exec.d', '*', 'charm-pre-install')
        for f in sorted(glob.glob(pattern)):
            if os.path.isfile(f) and os.access(f, os.X_OK):
                hookenv.log('Running preinstall hook {}'.format(f))
                subprocess.check_call(['sh', '-c', f])
            else:
                hookenv.log('Ingnoring preinstall hook {}'.format(f),
                            WARNING)
        else:
            hookenv.log('No preinstall hooks found')


# FOR CHARMHELPERS
@action
def swapoff(fstab='/etc/fstab'):
    '''Turn off swapping in the container, permanently.'''
    # Turn off swap in the current session
    if helpers.is_lxc():
        hookenv.log("In an LXC. Not touching swap.")
        return
    else:
        try:
            subprocess.check_call(['swapoff', '-a'])
        except Exception as e:
            hookenv.log("Got an error trying to turn off swapping. {}. "
                        "We may be in an LXC. Exiting gracefully"
                        "".format(e), WARNING)
            return

    # Disable swap permanently
    with closing(Fstab(fstab)) as fstab:
        while True:
            swap_entry = fstab.get_entry_by_attr('filesystem', 'swap')
            if swap_entry is None:
                break
            fstab.remove_entry(swap_entry)


# FOR CHARMHELPERS
@action
def configure_sources():
    '''Standard charmhelpers package source configuration.'''
    config = hookenv.config()
    if config.changed('install_sources') or config.changed('install_keys'):
        fetch.configure_sources(True)


@action
def add_implicit_package_signing_keys():
    # Rather than blindly add these keys, we should sniff
    # config['install_sources'] for apache.org or datastax.com urls and
    # add only the appropriate keys.
    for key in ('apache', 'datastax'):
        path = os.path.join(hookenv.charm_dir(), 'lib', '{}.key'.format(key))
        subprocess.check_call(['apt-key', 'add', path],
                              stdin=subprocess.DEVNULL)


@action
def reset_sysctl():
    '''Configure sysctl settings for Cassandra'''
    if helpers.is_lxc():
        hookenv.log("In an LXC. Leaving sysctl unchanged.")
    else:
        cassandra_sysctl_file = os.path.join('/', 'etc', 'sysctl.d',
                                             '99-cassandra.conf')
        contents = b"vm.max_map_count = 131072\n"
        try:
            host.write_file(cassandra_sysctl_file, contents)
            subprocess.check_call(['sysctl', '-p', cassandra_sysctl_file])
        except OSError as e:
            if e.errno == errno.EACCES:
                hookenv.log("Got Permission Denied trying to set the "
                            "sysctl settings at {}. We may be in an LXC. "
                            "Exiting gracefully".format(cassandra_sysctl_file),
                            WARNING)
            else:
                raise


@action
def reset_limits():
    '''Set /etc/security/limits.d correctly for Ubuntu, so the
    startup scripts don't emit a spurious warning.

    Per Cassandra documentation, Ubuntu needs some extra
    twiddling in /etc/security/limits.d. I have no idea why
    the packages don't do this, since they are already
    setting limits for the cassandra user correctly. The real
    bug is that the limits of the user running the startup script
    are being checked, rather than the limits of the user that will
    actually run the process.
    '''
    contents = dedent('''\
                      # Maintained by Juju
                      root - memlock unlimited
                      root - nofile 100000
                      root - nproc 32768
                      root - as unlimited
                      ubuntu - memlock unlimited
                      ubuntu - nofile 100000
                      ubuntu - nproc 32768
                      ubuntu - as unlimited
                      ''')
    host.write_file('/etc/security/limits.d/cassandra-charm.conf',
                    contents.encode('US-ASCII'))


@action
def install_cassandra_packages():
    helpers.install_packages(helpers.get_cassandra_packages())
    if helpers.get_jre() != 'oracle':
        subprocess.check_call(['update-java-alternatives',
                               '--jre-headless',
                               '--set', 'java-1.8.0-openjdk-amd64'])


@action
def ensure_cassandra_package_status():
    helpers.ensure_package_status(helpers.get_cassandra_packages())


def _fetch_oracle_jre():
    config = hookenv.config()
    url = config.get('private_jre_url', None)
    if url and config.get('retrieved_jre', None) != url:
        filename = os.path.join(hookenv.charm_dir(),
                                'lib', url.split('/')[-1])
        if not filename.endswith('-linux-x64.tar.gz'):
            helpers.status_set('blocked',
                               'Invalid private_jre_url {}'.format(url))
            raise SystemExit(0)
        helpers.status_set(hookenv.status_get()[0],
                           'Downloading Oracle JRE')
        hookenv.log('Oracle JRE URL is {}'.format(url))
        urllib.request.urlretrieve(url, filename)
        config['retrieved_jre'] = url

    pattern = os.path.join(hookenv.charm_dir(),
                           'lib', 'server-jre-?u*-linux-x64.tar.gz')
    tarballs = glob.glob(pattern)
    if not (url or tarballs):
        helpers.status_set('blocked',
                           'private_jre_url not set and no local tarballs.')
        raise SystemExit(0)

    elif not tarballs:
        helpers.status_set('blocked',
                           'Oracle JRE tarball not found ({})'.format(pattern))
        raise SystemExit(0)

    # Latest tarball by filename/version num. Lets hope they don't hit
    # 99 (currently at 76).
    tarball = sorted(tarballs)[-1]
    return tarball


def _install_oracle_jre_tarball(tarball):
    # Same directory as webupd8 to avoid surprising people, but it could
    # be anything.
    if 'jre-7u' in str(tarball):
        dest = '/usr/lib/jvm/java-7-oracle'
    else:
        dest = '/usr/lib/jvm/java-8-oracle'

    if not os.path.isdir(dest):
        host.mkdir(dest)

    jre_exists = os.path.exists(os.path.join(dest, 'bin', 'java'))

    config = hookenv.config()

    # Unpack the latest tarball if necessary.
    if config.get('oracle_jre_tarball', '') == tarball and jre_exists:
        hookenv.log('Already installed {}'.format(tarball))
    else:
        hookenv.log('Unpacking {}'.format(tarball))
        subprocess.check_call(['tar', '-xz', '-C', dest,
                               '--strip-components=1', '-f', tarball])
        config['oracle_jre_tarball'] = tarball

    # Set alternatives, so /usr/bin/java does what we want.
    for tool in ['java', 'javac']:
        tool_path = os.path.join(dest, 'bin', tool)
        subprocess.check_call(['update-alternatives', '--install',
                               os.path.join('/usr/bin', tool),
                               tool, tool_path, '1'])
        subprocess.check_call(['update-alternatives',
                               '--set', tool, tool_path])


@action
def install_oracle_jre():
    if helpers.get_jre() != 'oracle':
        return

    tarball = _fetch_oracle_jre()
    _install_oracle_jre_tarball(tarball)


@action
def emit_java_version():
    # Log the version for posterity. Could be useful since Oracle JRE
    # security updates are not automated.
    version = subprocess.check_output(['java', '-version'],
                                      universal_newlines=True)
    for line in version.splitlines():
        hookenv.log('JRE: {}'.format(line))


@action
def emit_meminfo():
    helpers.emit(subprocess.check_output(['free', '--human'],
                                         universal_newlines=True))


@action
def configure_cassandra_yaml():
    helpers.configure_cassandra_yaml()


@action
def configure_cassandra_env():
    cassandra_env_path = helpers.get_cassandra_env_file()
    assert os.path.exists(cassandra_env_path)

    helpers.maybe_backup(cassandra_env_path)

    overrides = [
        ('max_heap_size', re.compile(r'^#?(MAX_HEAP_SIZE)=(.*)$', re.M)),
        ('heap_newsize', re.compile(r'^#?(HEAP_NEWSIZE)=(.*)$', re.M)),
        # We don't allow this to be overridden to ensure that tools
        # will find JMX using the default port.
        # ('jmx_port', re.compile(r'^#?(JMX_PORT)=(.*)$', re.M)),
    ]

    with open(cassandra_env_path, 'r') as f:
        env = f.read()

    config = hookenv.config()
    for key, regexp in overrides:
        if config[key]:
            val = shlex.quote(str(config[key]))
            env = regexp.sub(r'\g<1>={}'.format(val),
                             env)
        else:
            env = regexp.sub(r'#\1=\2', env)
    host.write_file(cassandra_env_path, env.encode('UTF-8'))


@action
def configure_cassandra_rackdc():
    config = hookenv.config()
    datacenter = config['datacenter'].strip()
    rack = config['rack'].strip() or hookenv.service_name()
    rackdc_properties = dedent('''\
                               dc={}
                               rack={}
                               ''').format(datacenter, rack)
    rackdc_path = helpers.get_cassandra_rackdc_file()
    host.write_file(rackdc_path, rackdc_properties.encode('UTF-8'))


def needs_reset_auth_keyspace_replication():
    '''Guard for reset_auth_keyspace_replication.'''
    num_nodes = helpers.num_nodes()
    datacenter = hookenv.config()['datacenter']
    with helpers.connect() as session:
        strategy_opts = helpers.get_auth_keyspace_replication(session)
        rf = int(strategy_opts.get(datacenter, -1))
        hookenv.log('system_auth rf={!r}'.format(strategy_opts))
        # If the node count has changed, we should change the rf.
        return rf != num_nodes


@leader_only
@action
@authentication
@coordinator.require('repair', needs_reset_auth_keyspace_replication)
def reset_auth_keyspace_replication():
    # Cassandra requires you to manually set the replication factor of
    # the system_auth keyspace, to ensure availability and redundancy.
    # The recommendation is to set the replication factor so that every
    # node has a copy.
    num_nodes = helpers.num_nodes()
    datacenter = hookenv.config()['datacenter']
    with helpers.connect() as session:
        strategy_opts = helpers.get_auth_keyspace_replication(session)
        rf = int(strategy_opts.get(datacenter, -1))
        hookenv.log('system_auth rf={!r}'.format(strategy_opts))
        if rf != num_nodes:
            strategy_opts['class'] = 'NetworkTopologyStrategy'
            strategy_opts[datacenter] = num_nodes
            if 'replication_factor' in strategy_opts:
                del strategy_opts['replication_factor']
            helpers.set_auth_keyspace_replication(session, strategy_opts)
            if rf < num_nodes:
                # Increasing rf, need to run repair.
                helpers.repair_auth_keyspace()
            helpers.set_active()


@action
def store_unit_private_ip():
    '''Store the unit's private ip address, so we can tell if it changes.'''
    hookenv.config()['unit_private_ip'] = hookenv.unit_private_ip()


def needs_restart():
    '''Return True if Cassandra is not running or needs to be restarted.'''
    if helpers.is_decommissioned():
        # Decommissioned nodes are never restarted. They remain up
        # telling everyone they are decommissioned.
        helpers.status_set('blocked', 'Decommissioned node')
        return False

    if not helpers.is_cassandra_running():
        if helpers.is_bootstrapped():
            helpers.status_set('waiting', 'Waiting for permission to start')
        else:
            helpers.status_set('waiting',
                               'Waiting for permission to bootstrap')
        return True

    config = hookenv.config()

    # If our IP address has changed, we need to restart.
    if config.changed('unit_private_ip'):
        helpers.status_set('waiting', 'IP address changed. '
                           'Waiting for restart permission.')
        return True

    # If the directory paths have changed, we need to migrate data
    # during a restart.
    storage = relations.StorageRelation()
    if storage.needs_remount():
        helpers.status_set(hookenv.status_get()[0],
                           'New mounts. Waiting for restart permission')
        return True

    # If any of these config items changed, a restart is required.
    for key in RESTART_REQUIRED_KEYS:
        if config.changed(key):
            hookenv.log('{} changed. Restart required.'.format(key))
    for key in RESTART_REQUIRED_KEYS:
        if config.changed(key):
            helpers.status_set(hookenv.status_get()[0],
                               'Config changes. '
                               'Waiting for restart permission.')
            return True

    # If we have new seeds, we should restart.
    new_seeds = helpers.get_seed_ips()
    if config.get('configured_seeds') != sorted(new_seeds):
        old_seeds = set(config.previous('configured_seeds') or [])
        changed = old_seeds.symmetric_difference(new_seeds)
        # We don't care about the local node in the changes.
        changed.discard(hookenv.unit_private_ip())
        if changed:
            helpers.status_set(hookenv.status_get()[0],
                               'Updated seeds {!r}. '
                               'Waiting for restart permission.'
                               ''.format(new_seeds))
            return True

    hookenv.log('Restart not required')
    return False


@action
@coordinator.require('restart', needs_restart)
def maybe_restart():
    '''Restart sequence.

    If a restart is needed, shutdown Cassandra, perform all pending operations
    that cannot be be done while Cassandra is live, and restart it.
    '''
    helpers.status_set('maintenance', 'Stopping Cassandra')
    helpers.stop_cassandra()
    helpers.remount_cassandra()
    helpers.ensure_database_directories()
    if helpers.peer_relid() and not helpers.is_bootstrapped():
        helpers.status_set('maintenance', 'Bootstrapping')
    else:
        helpers.status_set('maintenance', 'Starting Cassandra')
    helpers.start_cassandra()


@action
def post_bootstrap():
    '''Maintain state on if the node has bootstrapped into the cluster.

    Per documented procedure for adding new units to a cluster, wait 2
    minutes if the unit has just bootstrapped to ensure other units
    do not attempt bootstrap too soon. Also, wait until completed joining
    to ensure we keep the lock and ensure other nodes don't restart or
    bootstrap.
    '''
    if not helpers.is_bootstrapped():
        if coordinator.relid is not None:
            helpers.status_set('maintenance', 'Post-bootstrap 2 minute delay')
            hookenv.log('Post-bootstrap 2 minute delay')
            time.sleep(120)  # Must wait 2 minutes between bootstrapping nodes.

        join_msg_set = False
        while True:
            status = helpers.get_node_status()
            if status == 'NORMAL':
                break
            elif status == 'JOINING':
                if not join_msg_set:
                    helpers.status_set('maintenance', 'Still joining cluster')
                    join_msg_set = True
                time.sleep(10)
                continue
            else:
                if status is None:
                    helpers.status_set('blocked',
                                       'Unexpectedly shutdown during '
                                       'bootstrap')
                else:
                    helpers.status_set('blocked',
                                       'Failed to bootstrap ({})'
                                       ''.format(status))
                raise SystemExit(0)

    # Unconditionally call this to publish the bootstrapped flag to
    # the peer relation, as the first unit was bootstrapped before
    # the peer relation existed.
    helpers.set_bootstrapped()


@action
def stop_cassandra():
    helpers.stop_cassandra()


@action
def start_cassandra():
    helpers.start_cassandra()


@leader_only
@action
@authentication
def create_unit_superusers():
    # The leader creates and updates accounts for nodes, using the
    # encrypted password they provide in relations.PeerRelation. We
    # don't end up with unencrypted passwords leaving the unit, and we
    # don't need to restart Cassandra in no-auth mode which is slow and
    # I worry may cause issues interrupting the bootstrap.
    if not coordinator.relid:
        return  # No peer relation, no requests yet.

    created_units = helpers.get_unit_superusers()
    uncreated_units = [u for u in hookenv.related_units(coordinator.relid)
                       if u not in created_units]
    for peer in uncreated_units:
        rel = hookenv.relation_get(unit=peer, rid=coordinator.relid)
        username = rel.get('username')
        pwhash = rel.get('pwhash')
        if not username:
            continue
        hookenv.log('Creating {} account for {}'.format(username, peer))
        with helpers.connect() as session:
            helpers.ensure_user(session, username, pwhash, superuser=True)
        created_units.add(peer)
        helpers.set_unit_superusers(created_units)


@action
def reset_all_io_schedulers():
    dirs = helpers.get_all_database_directories()
    dirs = (dirs['data_file_directories'] + [dirs['commitlog_directory']] +
            [dirs['saved_caches_directory']])
    config = hookenv.config()
    for d in dirs:
        if os.path.isdir(d):  # Directory may not exist yet.
            helpers.set_io_scheduler(config['io_scheduler'], d)


def _client_credentials(relid):
    '''Return the client credentials used by relation relid.'''
    relinfo = hookenv.relation_get(unit=hookenv.local_unit(), rid=relid)
    username = relinfo.get('username')
    password = relinfo.get('password')
    if username is None or password is None:
        for unit in hookenv.related_units(coordinator.relid):
            try:
                relinfo = hookenv.relation_get(unit=unit, rid=relid)
                username = relinfo.get('username')
                password = relinfo.get('password')
                if username is not None and password is not None:
                    return username, password
            except subprocess.CalledProcessError:
                pass  # Assume the remote unit has not joined yet.
        return None, None
    else:
        return username, password


def _publish_database_relation(relid, superuser):
    # The Casandra service needs to provide a common set of credentials
    # to a client unit. The leader creates these, if none of the other
    # units are found to have published them already (a previously elected
    # leader may have done this). The leader then tickles the other units,
    # firing a hook and giving them the opportunity to copy and publish
    # these credentials.
    username, password = _client_credentials(relid)
    if username is None:
        if hookenv.is_leader():
            # Credentials not set. The leader must generate them. We use
            # the service name so that database permissions remain valid
            # even after the relation is dropped and recreated, or the
            # juju environment rebuild and the database restored from
            # backups.
            service_name = helpers.get_service_name(relid)
            if not service_name:
                # Per Bug #1555261, we might not yet have related units,
                # so no way to calculate the remote service name and thus
                # the user.
                return  # Try again later.
            username = 'juju_{}'.format(helpers.get_service_name(relid))
            if superuser:
                username += '_admin'
            password = host.pwgen()
            pwhash = helpers.encrypt_password(password)
            with helpers.connect() as session:
                helpers.ensure_user(session, username, pwhash, superuser)
            # Wake the peers, if any.
            helpers.leader_ping()
        else:
            return  # No credentials yet. Nothing to do.

    # Publish the information the client needs on the relation where
    # they can find it.
    #  - authentication credentials
    #  - address and port
    #  - cluster_name, so clients can differentiate multiple clusters
    #  - datacenter + rack, so clients know what names they can use
    #    when altering keyspace replication settings.
    config = hookenv.config()
    hookenv.relation_set(relid,
                         username=username, password=password,
                         host=hookenv.unit_public_ip(),
                         native_transport_port=config['native_transport_port'],
                         rpc_port=config['rpc_port'],
                         cluster_name=config['cluster_name'],
                         datacenter=config['datacenter'],
                         rack=config['rack'])


@action
def publish_database_relations():
    for relid in hookenv.relation_ids('database'):
        _publish_database_relation(relid, superuser=False)


@action
def publish_database_admin_relations():
    for relid in hookenv.relation_ids('database-admin'):
        _publish_database_relation(relid, superuser=True)


@action
def install_maintenance_crontab():
    # Every unit should run repair once per week (at least once per
    # GCGraceSeconds, which defaults to 10 days but can be changed per
    # keyspace). # Distribute the repair time evenly over the week.
    unit_num = int(hookenv.local_unit().split('/')[-1])
    dow, hour, minute = helpers.week_spread(unit_num)
    contents = jinja.render('cassandra_maintenance_cron.tmpl', vars())
    cron_path = "/etc/cron.d/cassandra-maintenance"
    host.write_file(cron_path, contents.encode('US-ASCII'))


@action
def emit_cluster_info():
    helpers.emit_describe_cluster()
    helpers.emit_status()
    helpers.emit_netstats()


@action
def configure_firewall():
    '''Configure firewall rules using ufw.

    This is primarily to block access to the replication and JMX ports,
    as juju's default port access controls are not strict enough and
    allow access to the entire environment.
    '''
    config = hookenv.config()
    ufw.enable(soft_fail=True)

    # Enable SSH from anywhere, relying on Juju and external firewalls
    # to control access.
    ufw.service('ssh', 'open')
    ufw.service('nrpe', 'open')  # Also NRPE for nagios checks.
    ufw.service('rsync', 'open')  # Also rsync for data transfer and backups.

    # Clients need client access. These protocols are configured to
    # require authentication.
    client_keys = ['native_transport_port', 'rpc_port']
    client_ports = [config[key] for key in client_keys]

    # Peers need replication access. This protocols does not
    # require authentication, so firewall it from other nodes.
    peer_ports = [config['storage_port'], config['ssl_storage_port']]

    # Enable client access from anywhere. Juju and external firewalls
    # can still restrict this further of course (ie. 'juju expose').
    for key in client_keys:
        if config.changed(key) and config.previous(key) is not None:
            # First close old ports. We use this order in the unlikely case
            # someone is trying to swap the native and Thrift ports.
            ufw.service(config.previous(key), 'close')
    for port in client_ports:
        # Then open or close the configured ports.
        ufw.service(port, 'open')

    desired_rules = set()  # ufw.grant_access/remove_access commands.

    # Rules for peers
    for relinfo in hookenv.relations_of_type('cluster'):
        if relinfo['private-address']:
            pa = hookenv._ensure_ip(relinfo['private-address'])
            for port in peer_ports:
                desired_rules.add((pa, 'any', port))
    # Rules for admin connections. We allow database-admin relations access
    # to the cluster communication ports so that tools like sstableloader
    # can run.
    for relinfo in hookenv.relations_of_type('database-admin'):
        if relinfo['private-address']:
            pa = hookenv._ensure_ip(relinfo['private-address'])
            for port in peer_ports:
                desired_rules.add((pa, 'any', port))

    previous_rules = set(tuple(rule) for rule in config.get('ufw_rules', []))

    # Close any rules previously opened that are no longer desired.
    for rule in sorted(list(previous_rules - desired_rules)):
        ufw.revoke_access(*rule)

    # Open all the desired rules.
    for rule in sorted(list(desired_rules)):
        ufw.grant_access(*rule)

    # Store our rules for next time. Note that this is inherantly racy -
    # this value is only persisted if the hook exits cleanly. If the
    # hook fails, then someone changes port configuration or IP
    # addresses change, then the failed hook retried, we can lose track
    # of previously granted rules and they will never be revoked. It is
    # impossible to remove this race entirely, so we stick with this
    # simple approach.
    config['ufw_rules'] = list(desired_rules)  # A list because JSON.


@action
def nrpe_external_master_relation():
    ''' Configure the nrpe-external-master relation '''
    local_plugins = helpers.local_plugins_dir()
    if os.path.exists(local_plugins):
        src = os.path.join(hookenv.charm_dir(),
                           "files", "check_cassandra_heap.sh")
        with open(src, 'rb') as f:
            host.write_file(os.path.join(local_plugins,
                                         'check_cassandra_heap.sh'),
                            f.read(), perms=0o555)

    nrpe_compat = nrpe.NRPE()
    conf = hookenv.config()

    cassandra_heap_warn = conf.get('nagios_heapchk_warn_pct')
    cassandra_heap_crit = conf.get('nagios_heapchk_crit_pct')
    if cassandra_heap_warn and cassandra_heap_crit:
        nrpe_compat.add_check(
            shortname="cassandra_heap",
            description="Check Cassandra Heap",
            check_cmd="check_cassandra_heap.sh localhost {} {}"
                      "".format(cassandra_heap_warn, cassandra_heap_crit))

    cassandra_disk_warn = conf.get('nagios_disk_warn_pct')
    cassandra_disk_crit = conf.get('nagios_disk_crit_pct')
    dirs = helpers.get_all_database_directories()
    dirs = set(dirs['data_file_directories'] +
               [dirs['commitlog_directory'], dirs['saved_caches_directory']])
    # We need to check the space on the mountpoint, not on the actual
    # directory, as the nagios user won't have access to the actual directory.
    mounts = set(helpers.mountpoint(d) for d in dirs)
    for disk in mounts:
        check_name = re.sub('[^A-Za-z0-9_]', '_', disk)
        if cassandra_disk_warn and cassandra_disk_crit:
            shortname = "cassandra_disk{}".format(check_name)
            hookenv.log("Adding disk utilization check {}".format(shortname),
                        DEBUG)
            nrpe_compat.add_check(
                shortname=shortname,
                description="Check Cassandra Disk {}".format(disk),
                check_cmd="check_disk -u GB -w {}% -c {}% -K 5% -p {}"
                          "".format(cassandra_disk_warn, cassandra_disk_crit,
                                    disk))
    nrpe_compat.write()


@leader_only
@action
def maintain_seeds():
    '''The leader needs to maintain the list of seed nodes'''
    seed_ips = helpers.get_seed_ips()
    hookenv.log('Current seeds == {!r}'.format(seed_ips), DEBUG)

    bootstrapped_ips = helpers.get_bootstrapped_ips()
    hookenv.log('Bootstrapped == {!r}'.format(bootstrapped_ips), DEBUG)

    # Remove any seeds that are no longer bootstrapped, such as dropped
    # units.
    seed_ips.intersection_update(bootstrapped_ips)

    # Add more bootstrapped nodes, if necessary, to get to our maximum
    # of 3 seeds.
    potential_seed_ips = list(reversed(sorted(bootstrapped_ips)))
    while len(seed_ips) < 3 and potential_seed_ips:
        seed_ips.add(potential_seed_ips.pop())

    # If there are no seeds or bootstrapped nodes, start with the leader. Us.
    if len(seed_ips) == 0:
        seed_ips.add(hookenv.unit_private_ip())

    hookenv.log('Updated seeds == {!r}'.format(seed_ips), DEBUG)

    hookenv.leader_set(seeds=','.join(sorted(seed_ips)))


@leader_only
@action
@authentication
def reset_default_password():
    if hookenv.leader_get('default_admin_password_changed'):
        hookenv.log('Default admin password already changed')
        return

    # Cassandra ships with well known credentials, rather than
    # providing a tool to reset credentials. This is a huge security
    # hole we must close.
    try:
        # We need a big timeout here, as the cassandra user actually
        # springs into existence some time after Cassandra has started
        # up and is accepting connections.
        with helpers.connect('cassandra', 'cassandra',
                             timeout=120, auth_timeout=120) as session:
            # But before we close this security hole, we need to use these
            # credentials to create a different admin account for the
            # leader, allowing it to create accounts for other nodes as they
            # join. The alternative is restarting Cassandra without
            # authentication, which this charm will likely need to do in the
            # future when we allow Cassandra services to be related together.
            helpers.status_set('maintenance',
                               'Creating initial superuser account')
            username, password = helpers.superuser_credentials()
            pwhash = helpers.encrypt_password(password)
            helpers.ensure_user(session, username, pwhash, superuser=True)
            helpers.set_unit_superusers([hookenv.local_unit()])

            helpers.status_set('maintenance',
                               'Changing default admin password')
            helpers.query(session, 'ALTER USER cassandra WITH PASSWORD %s',
                          cassandra.ConsistencyLevel.ALL, (host.pwgen(),))
    except cassandra.AuthenticationFailed:
        hookenv.log('Default superuser account already reset')
        try:
            with helpers.connect():
                hookenv.log("Leader's superuser account already created")
        except cassandra.AuthenticationFailed:
            # We have no known superuser credentials. Create the account
            # the hard, slow way. This will be the normal method
            # of creating the service's initial account when we allow
            # services to be related together.
            helpers.create_unit_superuser_hard()

    hookenv.leader_set(default_admin_password_changed=True)


@action
def set_active():
    # If we got this far, the unit is active. Update the status if it is
    # not already active. We don't do this unconditionally, as the charm
    # may be active but doing stuff, like active but waiting for restart
    # permission.
    if hookenv.status_get()[0] != 'active':
        helpers.set_active()
    else:
        hookenv.log('Unit status already active', DEBUG)


@action
@authentication
def request_unit_superuser():
    relid = helpers.peer_relid()
    if relid is None:
        hookenv.log('Request deferred until peer relation exists')
        return

    relinfo = hookenv.relation_get(unit=hookenv.local_unit(),
                                   rid=relid)
    if relinfo and relinfo.get('username'):
        # We must avoid blindly setting the pwhash on the relation,
        # as we will likely get a different value everytime we
        # encrypt the password due to the random salt.
        hookenv.log('Superuser account request previously made')
    else:
        # Publish the requested superuser and hash to our peers.
        username, password = helpers.superuser_credentials()
        pwhash = helpers.encrypt_password(password)
        hookenv.relation_set(relid, username=username, pwhash=pwhash)
        hookenv.log('Requested superuser account creation')


@action
def update_etc_hosts():
    hostname = socket.gethostname()
    addr = hookenv.unit_private_ip()
    hosts_map = {addr: hostname}
    # only need to add myself to /etc/hosts
    helpers.update_hosts_file('/etc/hosts', hosts_map)
