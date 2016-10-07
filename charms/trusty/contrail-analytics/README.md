Overview
--------

OpenContrail (www.opencontrail.org) is a fully featured Software Defined
Networking (SDN) solution for private clouds. It supports high performance
isolated tenant networks without requiring external hardware support. It
provides a Neutron plugin to integrate with OpenStack.

This charm is designed to be used in conjunction with the rest of the OpenStack
related charms in the charm store to virtualize the network that Nova Compute
instances plug into.

This charm provides the analytics node component which includes
contrail-collector, contrail-query-engine and contrail-analytics-api services.
Only OpenStack Icehouse or newer is supported.

Usage
-----

Cassandra and Contrail Configuration are prerequisite services to deploy.
Once ready, deploy and relate as follows:

    juju deploy contrail-analytics
    juju add-relation contrail-analytics:cassandra cassandra:database
    juju add-relation contrail-analytics contrail-configuration

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

    juju add-unit contrail-analytics

Relating to haproxy charm (http-services relation) allows multiple units to be
load balanced:

    juju add-relation contrail-analytics haproxy
