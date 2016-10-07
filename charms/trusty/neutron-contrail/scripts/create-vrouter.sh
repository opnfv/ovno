#!/bin/sh -e
#
# Script used to configure vRouter interface

configVRouter()
{
	cat juju-header
	echo "auto $1"
	if [ -e "$2" ]; then
		cat "$2"
	else
		echo "iface $1 inet manual"
	fi
	printf "\n%s\n"	"auto vhost0"
	if [ -e "$3" ]; then
		cat "$3"
	else
		echo "iface vhost0 inet dhcp"
	fi
	cat <<-EOF
		    pre-up ip link add address \$(cat /sys/class/net/$1/address) type vhost
		    pre-up vif --add $1 --mac \$(cat /sys/class/net/$1/address) --vrf 0 --vhost-phys --type physical
		    pre-up vif --add vhost0 --mac \$(cat /sys/class/net/$1/address) --vrf 0 --type vhost --xconnect $1
		    post-down vif --list | awk '/^vif.*OS: vhost0/ {split(\$1, arr, "\\/"); print arr[2];}' | xargs vif --delete
		    post-down vif --list | awk '/^vif.*OS: $1/ {split(\$1, arr, "\\/"); print arr[2];}' | xargs vif --delete
		    post-down ip link delete vhost0
		EOF
}

ifacedown()
{
	for iface; do
		# ifdown interface
		# if bridge, save list of interfaces
		# if bond, save list of slaves
		if [ ! -e /sys/class/net/$iface ]; then
			continue
		fi
		[ -d /sys/class/net/$iface/bridge ] && saveIfaces $iface
		[ -d /sys/class/net/$iface/bonding ] && saveSlaves $iface
		ifdown --force $iface
	done
}

ifaceup()
{
	for iface; do
		# ifup interface
		# if bridge, restore list of interfaces
		# restore list of slaves if exists (bond)
		restoreSlaves $iface
		ifup $iface
		[ -d /sys/class/net/$iface/bridge ] && restoreIfaces $iface
	done
	return 0
}

restoreIfaces()
{
	if [ -e $TMP/$1.ifaces ]; then
		cat $TMP/$1.ifaces | xargs -n 1 brctl addif $1 || true
	fi
}

restoreSlaves()
{
	if [ -e $TMP/$1.slaves ]; then
		cat $TMP/$1.slaves | xargs ifup
	fi
}

saveIfaces()
{
	if [ -z "$(find /sys/class/net/$1/brif -maxdepth 0 -empty)" ]; then
		find /sys/class/net/$1/brif | tail -n +2 | xargs -n 1 basename \
		    > $TMP/$1.ifaces
	fi
}

saveSlaves()
{
	if [ -s /sys/class/net/$1/bonding/slaves ]; then
		cat /sys/class/net/$1/bonding/slaves | tr " " "\n" \
		    > $TMP/$1.slaves
	fi
}

TMP=$(mktemp -d /tmp/create-vrouter.XXX)

if [ $# -ne 0 ]; then
	interface=$1
else
	# use default gateway interface
	interface=$(route -n | awk '$1 == "0.0.0.0" { print $8 }')
fi

ifacedown $interface vhost0; sleep 5
# add interfaces.d source line to /etc/network/interfaces
if ! grep -q '^[[:blank:]]*source /etc/network/interfaces\.d/\*\.cfg[[:blank:]]*$' \
    /etc/network/interfaces; then
	printf "\n%s\n" "source /etc/network/interfaces.d/*.cfg" \
	    >> /etc/network/interfaces
	# it's possible for conflicting network config to exist in
	# /etc/network/interfaces.d when we start sourcing it
	# so disable any config as a precautionary measure
	for cfg in /etc/network/interfaces.d/*.cfg; do
		[ -e "$cfg" ] || continue
		mv "$cfg" "$cfg.save"
	done
fi
mkdir -p /etc/network/interfaces.d
for cfg in /etc/network/interfaces /etc/network/interfaces.d/*.cfg \
    /etc/network/*.config; do
	# for each network interfaces config, extract the config for
	# the chosen interface whilst commenting it out in the subsequent
	# replacement config
	[ -e "$cfg" ] || continue
	awk -v interface=$interface -v interface_cfg=$TMP/interface.cfg \
	    -v vrouter_cfg=$TMP/vrouter.cfg -f vrouter-interfaces.awk "$cfg" \
	    > $TMP/interfaces.cfg
	if ! diff $TMP/interfaces.cfg "$cfg" > /dev/null; then
		# create backup
		mv "$cfg" "$cfg.save"
		# substitute replacement config for original config
		cat juju-header $TMP/interfaces.cfg > "$cfg"
	fi
done
# use extracted interface config to create new vrouter config
configVRouter $interface $TMP/interface.cfg $TMP/vrouter.cfg \
    > /etc/network/interfaces.d/vrouter.cfg
ifaceup $interface vhost0

rm -rf $TMP
