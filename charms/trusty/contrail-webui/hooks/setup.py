import os
import pwd
import shutil
import subprocess

from charmhelpers.core.hookenv import log

from charmhelpers.core.host import (
    adduser,
    mkdir,
    service_available,
    service_restart,
    service_stop,
    user_exists
)

def pre_install():
    """Do any setup required before the install hook."""
    install_charmhelpers()


def install_charmhelpers():
    """Install the charmhelpers library, if not present."""
    try:
        import charmhelpers  # noqa
    except ImportError:
        import subprocess
        subprocess.check_call(['apt-get', 'install', '-y', 'python-pip'])
        subprocess.check_call(['pip', 'install', 'charmhelpers'])


def create_ssl_certificate():
    base = web_install_dir()
    cert = base + '/contrail-web-core/keys/cs-cert.pem'
    key = base + '/contrail-web-core/keys/cs-key.pem'
    log('Creating self-signed X.509 certificate...')
    subprocess.check_call(['openssl', 'req', '-x509',
                           '-subj', '/CN=contrail-juju',
                           '-days', '3650', '-newkey', 'rsa:2048', '-nodes',
                           '-out', cert, '-keyout', key])
    log('...created self-signed X.509 certificate')
    with open(cert, 'r') as f:
        c = f.read()
    with open(key, 'r') as f:
        k = f.read()
    return c, k


def is_opencontrail():
    return os.path.exists('/var/lib/contrail-webui')


def fix_permissions():
    """Fix package permissions."""
    os.chmod('/etc/contrail', 0o755)
    os.chown('/etc/contrail', 0, 0)

    os.chmod(web_install_dir() + '/contrail-web-core/keys/cs-key.pem', 0o600)


def fix_services():
    fix_permissions()
    fix_supervisord()
    fix_webui()
    fix_webui_middleware()
    service_restart('supervisor-webui')


def fix_supervisord():
    # setup supervisord
    if not user_exists('contrail'):
        adduser('contrail', system_user=True)

    shutil.copy('files/supervisor-webui.conf', '/etc/init')
    shutil.copy('files/supervisord_webui.conf', '/etc/contrail')
    pw = pwd.getpwnam('contrail')
    os.chown('/etc/contrail/supervisord_webui.conf', pw.pw_uid, pw.pw_gid)
    mkdir('/etc/contrail/supervisord_webui_files', owner='contrail',
          group='contrail', perms=0o755)

    mkdir('/var/log/contrail', owner='contrail', group='adm', perms=0o750)


def fix_webui():
    # disable webui upstart service
    if service_available('contrail-webui-webserver'):
        service_stop('contrail-webui-webserver')
        with open('/etc/init/contrail-webui-webserver.override', 'w') as conf:
            conf.write('manual\n')

    # use supervisord config
    conf = 'files/contrail-webui-opencontrail.ini' \
           if is_opencontrail() \
           else 'files/contrail-webui-contrail.ini'
    shutil.copy(conf, '/etc/contrail/supervisord_webui_files/contrail-webui.ini')
    pw = pwd.getpwnam('contrail')
    os.chown('/etc/contrail/supervisord_webui_files/contrail-webui.ini',
             pw.pw_uid, pw.pw_gid)
    shutil.copy('files/contrail-webui', '/etc/init.d')
    os.chmod('/etc/init.d/contrail-webui', 0o755)


def fix_webui_middleware():
    # disable webui middleware upstart service
    if service_available('contrail-webui-jobserver'):
        service_stop('contrail-webui-jobserver')
        with open('/etc/init/contrail-webui-jobserver.override', 'w') as conf:
            conf.write('manual\n')

    # use supervisord config
    conf = 'files/contrail-webui-middleware-opencontrail.ini' \
           if is_opencontrail() \
           else 'files/contrail-webui-middleware-contrail.ini'
    shutil.copy(conf, '/etc/contrail/supervisord_webui_files/contrail-webui-middleware.ini')
    pw = pwd.getpwnam('contrail')
    os.chown('/etc/contrail/supervisord_webui_files/contrail-webui-middleware.ini',
             pw.pw_uid, pw.pw_gid)
    shutil.copy('files/contrail-webui-middleware', '/etc/init.d')
    os.chmod('/etc/init.d/contrail-webui-middleware', 0o755)


def web_install_dir():
    return '/var/lib/contrail-webui' \
           if is_opencontrail() \
           else '/usr/src/contrail'


def write_ssl_certificate(cert, key):
    base = web_install_dir()
    with open(base + '/contrail-web-core/keys/cs-cert.pem', 'w') as f:
        f.write(cert)
    with open(base + '/contrail-web-core/keys/cs-key.pem', 'w') as f:
        f.write(key)
