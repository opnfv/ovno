#!/usr/bin/python

import subprocess
import sys

# Strip the -W option, as its noise messes with test output.
args = list(sys.argv[1:])
if '-W' in args:
    args.remove('-W')
cmd = ['juju-deployer'] + args
try:
    subprocess.check_output(cmd, stderr=subprocess.STDOUT)
except subprocess.CalledProcessError as x:
    sys.stderr.write(x.output)
    sys.exit(x.returncode)
