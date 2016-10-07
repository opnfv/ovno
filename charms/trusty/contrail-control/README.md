Overview
--------

OpenContrail (www.opencontrail.org) is a fully featured Software Defined
Networking (SDN) solution for private clouds. It supports high performance
isolated tenant networks without requiring external hardware support. It
provides a Neutron plugin to integrate with OpenStack.

This charm is designed to be used in conjunction with the rest of the OpenStack
related charms in the charm store to virtualize the network that Nova Compute
instances plug into.

This charm provides the control node component which contains the
contrail-control service.
Only OpenStack Icehouse or newer is supported.

Usage
-----

Contrail Configuration and Keystone are prerequisite services to deploy.
Once ready, deploy and relate as follows:

    juju deploy contrail-control
    juju add-relation contrail-control:contrail-api contrail-configuration:contrail-api
    juju add-relation contrail-control:contrail-discovery contrail-configuration:contrail-discovery
    juju add-relation contrail-control:contrail-ifmap contrail-configuration:contrail-ifmap
    juju add-relation contrail-control keystone

Install Sources
---------------

The version of OpenContrail installed when deploying can be changed using the
'install-sources' option. This is a multilined value that may refer to PPAs or
Deb repositories.

The version of dependent OpenStack components installed when deploying can be
changed using the 'openstack-origin' option. When deploying to different
OpenStack versions, openstack-origin needs to be set across all OpenStack and
OpenContrail charms where available.

High Availability (HA)
----------------------

Multiple units of this charm can be deployed to support HA deployments:

    juju add-unit contrail-control
