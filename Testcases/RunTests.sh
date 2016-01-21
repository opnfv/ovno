#!/bin/bash

PATH=/home/opnfv/ovno/Testcases:$PATH
cd /home/opnfv/ovno/Testcases

# Create the config wrapper
OCL_IP=`echo $OS_AUTH_URL | cut -d "/" -f3 | cut -d ":" -f1`

cat <<EOF >config
#!/usr/bin/python

import sys
import os
from config_shell import *
default_client_args = [
    ('--username', 'admin'),
    ('--password', os.environ["OS_PASSWORD"]),
    ('--region', 'RegionOne'),
    ('--tenant', 'admin'),
    ('--api-server', os.environ["OCL_IP"])]


if __name__ == '__main__':
    for arg in default_client_args:
        if not arg[0] in sys.argv:
            sys.argv.insert(1, arg[0])
            sys.argv.insert(2, arg[1])
    ConfigShell().main()
EOF

chmod 777 config




echo "Starting OpenContrail test suite"
# Tests go here
echo "Finished OpenContrail test suite"
