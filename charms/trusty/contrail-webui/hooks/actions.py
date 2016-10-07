from charmhelpers.core import hookenv


def log_start(service_name):
    hookenv.log('{0} starting'.format(service_name))
