.. _opnfv-user-config:

.. This work is licensed under a Creative Commons Attribution 4.0 International License.
.. SPDX-License-Identifier: CC-BY-4.0
.. (c) Sofia Wallin (sofia.wallin@ericssion.com)

=================================
OpenContrail in OPNFV
=================================

Introduction
============

OpenContrail provides virtual networking in OpenStack by providing a complete
implementation of the Neutron networking API in a combination of a controller 
and a forwarding element (vRouter) that is installed in place of Linux bridge
Open vSwitch. OpenContrail uses XMPP for the management and control plane between
the controller and the vRouters, and uses BGP for the control plane to physical devices.
 OpenContrail uses overlay networking between vRouters
to deliver highly scalable, multi-tenant connectivity with fine-grained network
policy and many L2 and L3 networking features not available in the standard
Neutron API, such as, ARP-proxy, ACLs, ECMP load-balancing, service chaining, 
port mirroring. In addition, OpenContrail provides analytics based on collection of metrics
from the virtual and physical infrastructure. 

More details on the OpenContrail architecture and its operation can be found at
http://www.opencontrail.org/opencontrail-architecture-documentation/.

Installation
============

In the Euphrates release of OPNFV, OpenContrail installation is supported using the
JOID installer. Installation instructions are provided in that project's
documentation. In summary, following download and deployment of JOID, OpenContrail
can be deployed using the following command in the <OPNFV_DIR>/joid/ci directory.

./deploy.sh -o ocata -t nonha|ha -s ocl -d xenial -l custom -m openstack

where "nonha" or "ha" indicate if a single instance, or highly available multi-instance
OpenStack/OpenContrail environment should be deployed.

Using OpenContrail
==================

The OpenContrail GUI is accessed at <public_ip>:8080 where "public_ip" is the IP address
that is used to access OpenStack services.

The OpenContrail REST API is found at <public_ip>:8082. Documentation for the API can be found at:

https://www.juniper.net/documentation/en_US/release-independent/contrail/information-products/pathway-pages/api-server/index.html

Python libraries are available for OpenContrail and their use is described at:

https://www.juniper.net/documentation/en_US/release-independent/contrail/information-products/pathway-pages/api-server/tutorial_with_library.html

The OpenContrail controller is deployed in docker containers that run on the corresponding
 juju services (contrail-controller, contrail-analytics, contrail-analyticsdb). This means 
that OpenContrail command line utilities need to be accessed using using "docker exec". E.g.
in order to check the status of an OpenContrail cluster, issue the following command from the 
Juju jumphost

$ juju ssh contrail-controller/0 "sudo docker exec contrail-controller contrail-status"
== Contrail Control ==
contrail-control:             active              
contrail-named:               active              
contrail-dns:                 active              
contrail-control-nodemgr:     active              
== Contrail Config ==
contrail-api:                 active              
contrail-schema:              active              
contrail-svc-monitor:         active              
contrail-device-manager:      active              
contrail-config-nodemgr:      active              
== Contrail Web UI ==
contrail-webui:               active              
contrail-webui-middleware:    active              
== Contrail Support Services ==
rabbitmq-server:              active               (disabled on boot)
zookeeper:                    active              
Connection to 172.16.50.153 closed.

Note that due to the distributed containerized deployment that is used in Juju, the contrail-status
command only shows the status of services running on that node. So the same command issued to a 
contrail-analytics node yields:

$ juju ssh contrail-analytics/0 "sudo docker exec contrail-analytics contrail-status"
== Contrail Analytics ==
contrail-collector:           active              
contrail-analytics-api:       active              
contrail-query-engine:        active              
contrail-alarm-gen:           active              
contrail-snmp-collector:      active              
contrail-topology:            active              
contrail-analytics-nodemgr:   active              
Connection to 172.16.50.153 closed.

A set of command line python utilities are provided that implement most OpenContrail features. These 
are located at /opt/contail/utils. Since the nova compute nodes have the contrail utilities installed in the base operating system, it
can be more convenient to run commands on these types of nodes after logging in using "juju ssh nova/compute/0". 

================================
OpenContrail Community Resources
================================

The OpenContrail main web site is at www.opencontrail.org.

There are various mailing lists that are used by community members to get answers about deployment and operation 
of OpenContrail. The mailing lists can be joined at http://www.opencontrail.org/newsletter-and-mailing-lists/


http://www.opencontrail.org/newsletter-and-mailing-lists/
