Overview
--------

OpenContrail (www.opencontrail.org) is a fully featured Software Defined
Networking (SDN) solution for private clouds. It supports high performance
isolated tenant networks without requiring external hardware support. It
provides a Neutron plugin to integrate with OpenStack.

This charm is designed to be used in conjunction with the rest of the OpenStack
related charms in the charm store to virtualize the network that Nova Compute
instances plug into.

This charm provides the configuration node component which includes
contrail-api, contrail-schema, contrail-discovery and ifmap-server services.
Only OpenStack Icehouse or newer is supported.
Juju 1.23.2+ required.

Usage
-----

Cassandra, Zookeeper, RabbitMQ and Keystone are prerequisite services to deploy.
Once ready, deploy and relate as follows:

    juju deploy contrail-configuration
    juju add-relation contrail-configuration:cassandra cassandra:database
    juju add-relation contrail-configuration zookeeper
    juju add-relation contrail-configuration rabbitmq-server
    juju add-relation contrail-configuration keystone

After deployment, relate to neutron-api-contrail:

    juju add-relation neutron-api-contrail contrail-configuration

Install Sources
---------------

The version of OpenContrail installed when deploying can be changed using the
'install-sources' option. This is a multilined value that may refer to PPAs or
Deb repositories.

The version of dependent OpenStack components installed when deploying can be
changed using the 'openstack-origin' option. When deploying to different
OpenStack versions, openstack-origin needs to be set across all OpenStack and
OpenContrail charms where available.

Floating IP Pools
-----------------

To use OpenStack floating IP functionality, floating IP pools must be created
and activated. Creation of multiple pools for multiple projects is supported
using the 'floating-ip-pools' option.

A value is specified as a YAML encoded string, indicating one or more pools
using a list of maps, where each map consists of the following attributes:

    project - project name
    network - network name
    pool-name - floating pool name
    target-projects - list of projects allowed to use pool

For example to create a floating ip pool named 'floatingip_pool' on
'admin:public' network and allow 'admin' project to use:

    juju set contrail-configuration \
      "floating-ip-pools=[ { project: admin, network: public, pool-name: floatingip_pool, target-projects: [ admin ] } ]"

Previously specified pools will be deactivated and removed.

Nova Metadata
-------------

To use Nova Metadata with Nova Compute instances, a metadata service must first
be registered. Registration allows OpenContrail to create the appropriate
network config to proxy requests from instances to a nova-api service on the
network.

Relating to a charm implementing neutron-metadata interface will register a
linklocal metadata service:

    juju add-relation contrail-configuration neutron-metadata-charm

neutron-contrail charm also needs to be related to the same charm to use correct
configuration:

    juju add-relation neutron-contrail neutron-metadata-charm

*NOTE: neutron-contrail runs and registers its own nova-api-metadata service
on each Compute node by default ('local-metadata-server' option), so using
neutron-metadata relation isn't necessary unless you need more control over
deployment.*

High Availability (HA)
----------------------

Multiple units of this charm can be deployed to support HA deployments:

    juju add-unit contrail-configuration

Relating to haproxy charm (http-services relation) allows multiple units to be
load balanced:

    juju add-relation contrail-configuration haproxy

Setting the 'vip' option instructs related charms to use IP address specified
for accessing the configuration node:

    juju set contrail-configuration vip=x.x.x.x

When load balancing with HAProxy you would set vip to the IP address of the
deployed haproxy charm (or a shared Virtual IP address if doing clustered
HAProxy).
