import os
import socket
import urllib

import yaml

import actions
from charmhelpers.core import hookenv
from charmhelpers.core import services
from charmhelpers.core import templating

from setup import (
    create_ssl_certificate,
    is_opencontrail,
    write_ssl_certificate
)


CONFIG_FILE = os.path.join(os.sep, 'etc', 'contrail', 'config.global.js')


class CassandraRelation(services.RelationContext):
    name = 'cassandra'
    interface = 'cassandra'
    required_keys = ['private-address']


class ContrailAPIRelation(services.RelationContext):
    name = 'contrail_api'
    interface = 'contrail-api'
    required_keys = ['private-address', 'port']


class ContrailDiscoveryRelation(services.RelationContext):
    name = 'contrail_discovery'
    interface = 'contrail-discovery'
    required_keys = ['private-address', 'port']


class KeystoneRelation(services.RelationContext):
    name = 'identity_admin'
    interface = 'keystone-admin'
    required_keys = ['service_hostname', 'service_port', 'service_username',
            'service_tenant_name', 'service_password']


class RedisRelation(services.RelationContext):
    name = 'redis'
    interface = 'redis-master'
    required_keys = ['hostname', 'port']


class ContrailWebUIConfig(services.ManagerCallback):

    context_contrail = {
        'webcontroller_path': '/usr/src/contrail/contrail-web-controller',
        'logo_file': '/usr/src/contrail/contrail-web-core/webroot/img/juniper-networks-logo.png',
        'favicon_file': '/usr/src/contrail/contrail-web-core/webroot/img/juniper-networks-favicon.ico'
    }

    context_opencontrail = {
        'webcontroller_path': '/var/lib/contrail-webui/contrail-web-controller',
        'logo_file': '/var/lib/contrail/contrail-web-core/webroot/img/opencontrail-logo.png',
        'favicon_file': '/var/lib/contrail/contrail-web-core/webroot/img/opencontrail-favicon.ico'
    }

    def __call__(self, manager, service_name, event_name):
        config = hookenv.config()
        context = {
            'config': config
        }

        context.update(self.context_opencontrail if is_opencontrail()
                       else self.context_contrail)

        context.update(ContrailAPIRelation())
        context.update(ContrailDiscoveryRelation())
        context.update(CassandraRelation())
        context.update(KeystoneRelation())

        # Redis relation is optional
        redis = RedisRelation()
        if redis.is_ready():
            context.update(redis)
        else:
            context.update({
                'redis': [{
                    'hostname': '127.0.0.1',
                    'port': '6379'
                }]
            })

        # Download logo and favicon or use the cached one
        # if failed, falling back to the defaults
        for target in ('logo', 'favicon'):
            url = context['config']['{0}-url'.format(target)]
            filename = os.path.join(os.sep, 'etc', 'contrail',
                                    os.path.basename(url))
            context['config']['{0}-filename'.format(target)] = ''
            if url:
                try:
                    urllib.urlretrieve(url, filename)
                except IOError:
                    pass

                try:
                    if os.stat(filename).st_size > 0:
                        context['config']['{0}-filename'.format(target)] = (
                            filename
                        )
                except OSError:
                    pass

        templating.render(
            context=context,
            source='config.global.js.j2',
            target=CONFIG_FILE,
            perms=0o644
        )

        templating.render(
            context=context,
            source='contrail-webui-userauth.js',
            target='/etc/contrail/contrail-webui-userauth.js',
            perms=0o640,
            owner='root',
            group='contrail'
        )


class SSLConfig(services.ManagerCallback):
    def __call__(self, manager, service_name, event_name):
        if hookenv.is_leader():
            config = hookenv.config()
            cert = config.get('ssl-cert')
            key = config.get('ssl-key')
            if cert and key:
                write_ssl_certificate(cert, key)
                hookenv.leader_set({'ssl-cert': cert, 'ssl-key': key,
                                    'ssl-cert-created': ''})
            elif not hookenv.leader_get('ssl-cert-created'):
                cert, key = create_ssl_certificate()
                hookenv.leader_set({'ssl-cert': cert, 'ssl-key': key,
                                    'ssl-cert-created': True})


class LeaderCallback(services.ManagerCallback):
    def __call__(self, manager, service_name, event_name):
        if not hookenv.is_leader():
            cert = hookenv.leader_get('ssl-cert')
            key = hookenv.leader_get('ssl-key')
            if cert and key:
                write_ssl_certificate(cert, key)


class ContrailWebRelation(services.ManagerCallback):
    def __call__(self, manager, service_name, event_name):
        config = hookenv.config()
        name = hookenv.local_unit().replace('/', '-')
        addr = socket.gethostbyname(hookenv.unit_get('private-address'))
        http_port = config['http-port']
        https_port = config['https-port']
        services = [ { 'service_name': 'contrail-webui-http',
                       'service_host': '0.0.0.0',
                       'service_port': http_port,
                       'service_options': [ 'mode http', 'balance leastconn', 'option httpchk GET / HTTP/1.1\\r\\nHost:\\ www', 'stick on src table contrail-webui-https' ],
                       'servers': [ [ name, addr, http_port, 'check' ] ] },
                     { 'service_name': 'contrail-webui-https',
                       'service_host': '0.0.0.0',
                       'service_port': https_port,
                       'service_options': [ 'mode tcp', 'balance leastconn', 'stick-table type ip size 10k expire 25h', 'stick on src' ],
                       'servers': [ [ name, addr, https_port, 'check' ] ] } ]

        for relation in hookenv.relation_ids('website'):
            hookenv.relation_set(relation, services=yaml.dump(services))


def manage():
    config = hookenv.config()
    cassandra = CassandraRelation()
    contrail_api = ContrailAPIRelation()
    contrail_discovery = ContrailDiscoveryRelation()
    keystone = KeystoneRelation()

    config_callback = ContrailWebUIConfig()
    ssl_callback = SSLConfig()
    leader_callback = LeaderCallback()
    website_callback = ContrailWebRelation()

    manager = services.ServiceManager([
        {
            'service': 'supervisor-webui',
            'ports': (config['http-port'], config['https-port']),
            'required_data': [
                config,
                cassandra,
                contrail_api,
                contrail_discovery,
                keystone,
            ],
            'data_ready': [
                actions.log_start,
                config_callback,
                ssl_callback,
                leader_callback,
                website_callback,
            ],
        },
    ])
    manager.manage()
