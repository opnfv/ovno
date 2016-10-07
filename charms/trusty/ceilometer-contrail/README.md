Overview
--------

OpenContrail (www.opencontrail.org) is a fully featured Software Defined
Networking (SDN) solution for private clouds. It supports high performance
isolated tenant networks without requiring external hardware support. It
provides a Neutron plugin to integrate with OpenStack.

This charm is designed to be used in conjunction with the rest of the OpenStack
related charms in the charm store to virtualize the network that Nova Compute
instances plug into.

This subordinate charm provides the Ceilometer plugin component.
Only OpenStack Icehouse or newer is supported.

Usage
-----

Ceilometer and Contrail Analytics are prerequisite services to deploy.

Once ready, deploy and relate as follows:

    juju deploy ceilometer-contrail
    juju add-relation ceilometer ceilometer-contrail
    juju add-relation ceilometer-contrail contrail-analytics

Install Sources
---------------

The version of Contrail installed when deploying can be changed using the
'install-sources' option. This is a multilined value that may refer to PPAs or
Deb repositories.
