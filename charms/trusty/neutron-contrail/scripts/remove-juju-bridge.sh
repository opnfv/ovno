#!/bin/sh -e
#
# Script used to remove Juju LXC bridge on MAAS systems

if [ ! -e /sys/class/net/juju-br0 ]; then
	exit 0
fi

interface=$(find /sys/class/net/juju-br0/brif | sed -n -e '2p' | xargs basename)

ifdown --force $interface juju-br0; sleep 5
cp interfaces /etc/network
cat <<-EOF >> /etc/network/interfaces
	auto $interface
	iface $interface inet dhcp
	EOF
ifup $interface
