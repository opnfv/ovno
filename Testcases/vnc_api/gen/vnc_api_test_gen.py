
# AUTO-GENERATED file from IFMapApiGenerator. Do Not Edit!

import fixtures
import testtools

from resource_test import *

class VncApiTestGen(testtools.TestCase, fixtures.TestWithFixtures):
    def test_domain_crud(self):
        self.useFixture(DomainTestFixtureGen(self._vnc_lib))
    #end test_domain_crud

    def test_global_vrouter_config_crud(self):
        self.useFixture(GlobalVrouterConfigTestFixtureGen(self._vnc_lib))
    #end test_global_vrouter_config_crud

    def test_instance_ip_crud(self):
        self.useFixture(InstanceIpTestFixtureGen(self._vnc_lib))
    #end test_instance_ip_crud

    def test_network_policy_crud(self):
        self.useFixture(NetworkPolicyTestFixtureGen(self._vnc_lib))
    #end test_network_policy_crud

    def test_loadbalancer_pool_crud(self):
        self.useFixture(LoadbalancerPoolTestFixtureGen(self._vnc_lib))
    #end test_loadbalancer_pool_crud

    def test_virtual_DNS_record_crud(self):
        self.useFixture(VirtualDnsRecordTestFixtureGen(self._vnc_lib))
    #end test_virtual_DNS_record_crud

    def test_route_target_crud(self):
        self.useFixture(RouteTargetTestFixtureGen(self._vnc_lib))
    #end test_route_target_crud

    def test_floating_ip_crud(self):
        self.useFixture(FloatingIpTestFixtureGen(self._vnc_lib))
    #end test_floating_ip_crud

    def test_floating_ip_pool_crud(self):
        self.useFixture(FloatingIpPoolTestFixtureGen(self._vnc_lib))
    #end test_floating_ip_pool_crud

    def test_physical_router_crud(self):
        self.useFixture(PhysicalRouterTestFixtureGen(self._vnc_lib))
    #end test_physical_router_crud

    def test_bgp_router_crud(self):
        self.useFixture(BgpRouterTestFixtureGen(self._vnc_lib))
    #end test_bgp_router_crud

    def test_virtual_router_crud(self):
        self.useFixture(VirtualRouterTestFixtureGen(self._vnc_lib))
    #end test_virtual_router_crud

    def test_subnet_crud(self):
        self.useFixture(SubnetTestFixtureGen(self._vnc_lib))
    #end test_subnet_crud

    def test_global_system_config_crud(self):
        self.useFixture(GlobalSystemConfigTestFixtureGen(self._vnc_lib))
    #end test_global_system_config_crud

    def test_service_appliance_crud(self):
        self.useFixture(ServiceApplianceTestFixtureGen(self._vnc_lib))
    #end test_service_appliance_crud

    def test_service_instance_crud(self):
        self.useFixture(ServiceInstanceTestFixtureGen(self._vnc_lib))
    #end test_service_instance_crud

    def test_namespace_crud(self):
        self.useFixture(NamespaceTestFixtureGen(self._vnc_lib))
    #end test_namespace_crud

    def test_logical_interface_crud(self):
        self.useFixture(LogicalInterfaceTestFixtureGen(self._vnc_lib))
    #end test_logical_interface_crud

    def test_route_table_crud(self):
        self.useFixture(RouteTableTestFixtureGen(self._vnc_lib))
    #end test_route_table_crud

    def test_physical_interface_crud(self):
        self.useFixture(PhysicalInterfaceTestFixtureGen(self._vnc_lib))
    #end test_physical_interface_crud

    def test_access_control_list_crud(self):
        self.useFixture(AccessControlListTestFixtureGen(self._vnc_lib))
    #end test_access_control_list_crud

    def test_analytics_node_crud(self):
        self.useFixture(AnalyticsNodeTestFixtureGen(self._vnc_lib))
    #end test_analytics_node_crud

    def test_virtual_DNS_crud(self):
        self.useFixture(VirtualDnsTestFixtureGen(self._vnc_lib))
    #end test_virtual_DNS_crud

    def test_customer_attachment_crud(self):
        self.useFixture(CustomerAttachmentTestFixtureGen(self._vnc_lib))
    #end test_customer_attachment_crud

    def test_service_appliance_set_crud(self):
        self.useFixture(ServiceApplianceSetTestFixtureGen(self._vnc_lib))
    #end test_service_appliance_set_crud

    def test_config_node_crud(self):
        self.useFixture(ConfigNodeTestFixtureGen(self._vnc_lib))
    #end test_config_node_crud

    def test_qos_queue_crud(self):
        self.useFixture(QosQueueTestFixtureGen(self._vnc_lib))
    #end test_qos_queue_crud

    def test_virtual_machine_crud(self):
        self.useFixture(VirtualMachineTestFixtureGen(self._vnc_lib))
    #end test_virtual_machine_crud

    def test_interface_route_table_crud(self):
        self.useFixture(InterfaceRouteTableTestFixtureGen(self._vnc_lib))
    #end test_interface_route_table_crud

    def test_service_template_crud(self):
        self.useFixture(ServiceTemplateTestFixtureGen(self._vnc_lib))
    #end test_service_template_crud

    def test_virtual_ip_crud(self):
        self.useFixture(VirtualIpTestFixtureGen(self._vnc_lib))
    #end test_virtual_ip_crud

    def test_loadbalancer_member_crud(self):
        self.useFixture(LoadbalancerMemberTestFixtureGen(self._vnc_lib))
    #end test_loadbalancer_member_crud

    def test_security_group_crud(self):
        self.useFixture(SecurityGroupTestFixtureGen(self._vnc_lib))
    #end test_security_group_crud

    def test_provider_attachment_crud(self):
        self.useFixture(ProviderAttachmentTestFixtureGen(self._vnc_lib))
    #end test_provider_attachment_crud

    def test_virtual_machine_interface_crud(self):
        self.useFixture(VirtualMachineInterfaceTestFixtureGen(self._vnc_lib))
    #end test_virtual_machine_interface_crud

    def test_loadbalancer_healthmonitor_crud(self):
        self.useFixture(LoadbalancerHealthmonitorTestFixtureGen(self._vnc_lib))
    #end test_loadbalancer_healthmonitor_crud

    def test_virtual_network_crud(self):
        self.useFixture(VirtualNetworkTestFixtureGen(self._vnc_lib))
    #end test_virtual_network_crud

    def test_project_crud(self):
        self.useFixture(ProjectTestFixtureGen(self._vnc_lib))
    #end test_project_crud

    def test_qos_forwarding_class_crud(self):
        self.useFixture(QosForwardingClassTestFixtureGen(self._vnc_lib))
    #end test_qos_forwarding_class_crud

    def test_database_node_crud(self):
        self.useFixture(DatabaseNodeTestFixtureGen(self._vnc_lib))
    #end test_database_node_crud

    def test_routing_instance_crud(self):
        self.useFixture(RoutingInstanceTestFixtureGen(self._vnc_lib))
    #end test_routing_instance_crud

    def test_network_ipam_crud(self):
        self.useFixture(NetworkIpamTestFixtureGen(self._vnc_lib))
    #end test_network_ipam_crud

    def test_logical_router_crud(self):
        self.useFixture(LogicalRouterTestFixtureGen(self._vnc_lib))
    #end test_logical_router_crud

#end class VncApiTestGen
