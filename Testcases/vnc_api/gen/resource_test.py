'''
This module defines the fixture classes for all config elements
'''

# AUTO-GENERATED file from IFMapApiGenerator. Do Not Edit!

import cfixture
from vnc_api import vnc_api
from cfgm_common.exceptions import *

from generatedssuper import GeneratedsSuper

class DomainTestFixtureGen(cfixture.ContrailFixture):
    """
    Fixture for :class:`.Domain`
    """
    def __init__(self, conn_drv, domain_name=None, parent_fixt=None, auto_prop_val=False, domain_limits=None, api_access_list=None, id_perms=None, display_name=None):
        '''
        Create DomainTestFixtureGen object
        
        constructor

        Args:
            conn_drv (:class:`ConnectionDriver`): connection driver (eg. :class:`vnc_api.vnc_api.VncApi`, :class:`novaclient.client.Client`, etc)

        Kwargs:
            domain_name (str): Name of domain
            parent_fixt (:class:`.ConfigRootTestFixtureGen`): Parent fixture
            domain_limits (instance): instance of :class:`DomainLimitsType`
            api_access_list (instance): instance of :class:`ApiAccessListType`
            id_perms (instance): instance of :class:`IdPermsType`
            display_name (instance): instance of :class:`xsd:string`

        '''
        super(DomainTestFixtureGen, self).__init__()
        self._conn_drv = conn_drv
        if not domain_name:
            self._name = 'default-domain'
        else:
            self._name = domain_name
        self._obj = None
        self._parent_fixt = parent_fixt
        self._auto_prop_val = auto_prop_val
        self.domain_limits = domain_limits
        self.api_access_list = api_access_list
        self.id_perms = id_perms
        self.display_name = display_name
    #end __init__

    def _update_links (self, update_server):
        return None
    #end _update_links


    def populate (self):
        self._obj.set_domain_limits(self.domain_limits or vnc_api.gen.resource_xsd.DomainLimitsType.populate())
        self._obj.set_api_access_list(self.api_access_list or vnc_api.gen.resource_xsd.ApiAccessListType.populate())
        self._obj.set_id_perms(self.id_perms or vnc_api.gen.resource_xsd.IdPermsType.populate())
        self._obj.set_display_name(self.display_name or GeneratedsSuper.populate_string("display_name"))
    #end populate

    def setUp(self):
        super(DomainTestFixtureGen, self).setUp()
        # child of config-root
        self._obj = vnc_api.Domain(self._name)
        try:
            self._obj = self._conn_drv.domain_read (fq_name=self._obj.get_fq_name())
            self._update_links (update_server=True)
        except NoIdError:
            self._update_links (update_server=False)
            if self._auto_prop_val:
                self.populate ()
            else:
                self._obj.domain_limits = self.domain_limits
                self._obj.api_access_list = self.api_access_list
                self._obj.id_perms = self.id_perms
                self._obj.display_name = self.display_name
            self._conn_drv.domain_create(self._obj)
            # read back for server allocated values
            self._obj = self._conn_drv.domain_read(id = self._obj.uuid)
    #end setUp

    def cleanUp(self):
        if self._obj.get_projects() or self._obj.get_namespaces() or self._obj.get_service_templates() or self._obj.get_virtual_DNSs():
            return
        parent_fixt = getattr(self, '_parent_fixt', None)
        if parent_fixt:
            # non config-root child
            parent_obj = self._parent_fixt.getObj()
            # remove child from parent obj
            for child_obj in parent_obj.get_domains():
                if type(child_obj) == dict:
                    child_uuid = child_obj['uuid']
                else:
                    child_uuid = child_obj.uuid
                if child_uuid == self._obj.uuid:
                    parent_obj.domains.remove(child_obj)
                    break

        self._conn_drv.domain_delete(id = self._obj.uuid)
    #end cleanUp

    def getObj(self):
        return self._obj
    #end getObj

#end class DomainTestFixtureGen

class GlobalVrouterConfigTestFixtureGen(cfixture.ContrailFixture):
    """
    Fixture for :class:`.GlobalVrouterConfig`
    """
    def __init__(self, conn_drv, global_vrouter_config_name=None, parent_fixt=None, auto_prop_val=False, linklocal_services=None, encapsulation_priorities=None, vxlan_network_identifier_mode=None, id_perms=None, display_name=None):
        '''
        Create GlobalVrouterConfigTestFixtureGen object
        
        constructor

        Args:
            conn_drv (:class:`ConnectionDriver`): connection driver (eg. :class:`vnc_api.vnc_api.VncApi`, :class:`novaclient.client.Client`, etc)

        Kwargs:
            global_vrouter_config_name (str): Name of global_vrouter_config
            parent_fixt (:class:`.GlobalSystemConfigTestFixtureGen`): Parent fixture
            linklocal_services (instance): instance of :class:`LinklocalServicesTypes`
            encapsulation_priorities (instance): instance of :class:`EncapsulationPrioritiesType`
            vxlan_network_identifier_mode (instance): instance of :class:`xsd:string`
            id_perms (instance): instance of :class:`IdPermsType`
            display_name (instance): instance of :class:`xsd:string`

        '''
        super(GlobalVrouterConfigTestFixtureGen, self).__init__()
        self._conn_drv = conn_drv
        if not global_vrouter_config_name:
            self._name = 'default-global-vrouter-config'
        else:
            self._name = global_vrouter_config_name
        self._obj = None
        self._parent_fixt = parent_fixt
        self._auto_prop_val = auto_prop_val
        self.linklocal_services = linklocal_services
        self.encapsulation_priorities = encapsulation_priorities
        self.vxlan_network_identifier_mode = vxlan_network_identifier_mode
        self.id_perms = id_perms
        self.display_name = display_name
    #end __init__

    def _update_links (self, update_server):
        return None
    #end _update_links


    def populate (self):
        self._obj.set_linklocal_services(self.linklocal_services or vnc_api.gen.resource_xsd.LinklocalServicesTypes.populate())
        self._obj.set_encapsulation_priorities(self.encapsulation_priorities or vnc_api.gen.resource_xsd.EncapsulationPrioritiesType.populate())
        self._obj.set_vxlan_network_identifier_mode(self.vxlan_network_identifier_mode or GeneratedsSuper.populate_string("vxlan_network_identifier_mode"))
        self._obj.set_id_perms(self.id_perms or vnc_api.gen.resource_xsd.IdPermsType.populate())
        self._obj.set_display_name(self.display_name or GeneratedsSuper.populate_string("display_name"))
    #end populate

    def setUp(self):
        super(GlobalVrouterConfigTestFixtureGen, self).setUp()
        if not self._parent_fixt:
            self._parent_fixt = self.useFixture(GlobalSystemConfigTestFixtureGen(self._conn_drv, 'default-global-system-config'))

        self._obj = vnc_api.GlobalVrouterConfig(self._name, self._parent_fixt.getObj ())
        try:
            self._obj = self._conn_drv.global_vrouter_config_read (fq_name=self._obj.get_fq_name())
            self._update_links (update_server=True)
        except NoIdError:
            self._update_links (update_server=False)
            if self._auto_prop_val:
                self.populate ()
            else:
                self._obj.linklocal_services = self.linklocal_services
                self._obj.encapsulation_priorities = self.encapsulation_priorities
                self._obj.vxlan_network_identifier_mode = self.vxlan_network_identifier_mode
                self._obj.id_perms = self.id_perms
                self._obj.display_name = self.display_name
            self._conn_drv.global_vrouter_config_create(self._obj)
            # read back for server allocated values
            self._obj = self._conn_drv.global_vrouter_config_read(id = self._obj.uuid)
    #end setUp

    def cleanUp(self):
        parent_fixt = getattr(self, '_parent_fixt', None)
        if parent_fixt:
            # non config-root child
            parent_obj = self._parent_fixt.getObj()
            # remove child from parent obj
            for child_obj in parent_obj.get_global_vrouter_configs():
                if type(child_obj) == dict:
                    child_uuid = child_obj['uuid']
                else:
                    child_uuid = child_obj.uuid
                if child_uuid == self._obj.uuid:
                    parent_obj.global_vrouter_configs.remove(child_obj)
                    break

        self._conn_drv.global_vrouter_config_delete(id = self._obj.uuid)
    #end cleanUp

    def getObj(self):
        return self._obj
    #end getObj

#end class GlobalVrouterConfigTestFixtureGen

class InstanceIpTestFixtureGen(cfixture.ContrailFixture):
    """
    Fixture for :class:`.InstanceIp`
    """
    def __init__(self, conn_drv, instance_ip_name=None, auto_prop_val=False, virtual_network_refs = None, virtual_machine_interface_refs = None, instance_ip_address=None, instance_ip_family=None, instance_ip_mode=None, subnet_uuid=None, id_perms=None, display_name=None):
        '''
        Create InstanceIpTestFixtureGen object
        
        constructor

        Args:
            conn_drv (:class:`ConnectionDriver`): connection driver (eg. :class:`vnc_api.vnc_api.VncApi`, :class:`novaclient.client.Client`, etc)

        Kwargs:
            %s_name (str): Name of %s
            virtual_network (list): list of :class:`VirtualNetwork` type
            virtual_machine_interface (list): list of :class:`VirtualMachineInterface` type
            instance_ip_address (instance): instance of :class:`xsd:string`
            instance_ip_family (instance): instance of :class:`xsd:string`
            instance_ip_mode (instance): instance of :class:`xsd:string`
            subnet_uuid (instance): instance of :class:`xsd:string`
            id_perms (instance): instance of :class:`IdPermsType`
            display_name (instance): instance of :class:`xsd:string`

        '''
        super(InstanceIpTestFixtureGen, self).__init__()
        self._conn_drv = conn_drv
        if not instance_ip_name:
            self._name = 'default-instance-ip'
        else:
            self._name = instance_ip_name
        self._obj = None
        self._auto_prop_val = auto_prop_val
        if virtual_network_refs:
            for ln in virtual_network_refs:
                self.add_virtual_network (ln)
        if virtual_machine_interface_refs:
            for ln in virtual_machine_interface_refs:
                self.add_virtual_machine_interface (ln)
        self.instance_ip_address = instance_ip_address
        self.instance_ip_family = instance_ip_family
        self.instance_ip_mode = instance_ip_mode
        self.subnet_uuid = subnet_uuid
        self.id_perms = id_perms
        self.display_name = display_name
    #end __init__

    def _update_links (self, update_server):
        for ln in self.get_virtual_networks ():
            self.add_virtual_network (ln.fixture (), update_server = update_server, add_link = False)
        for ln in self.get_virtual_machine_interfaces ():
            self.add_virtual_machine_interface (ln.fixture (), update_server = update_server, add_link = False)
        return None
    #end _update_links

    def add_virtual_network (self, lo, update_server = True, add_link = True):
        '''
        add :class:`VirtualNetwork` link to :class:`InstanceIp`
        Args:
            lo (:class:`VirtualNetwork`): obj to link
        '''
        if self._obj:
            self._obj.add_virtual_network (lo)
            if update_server:
                self._conn_drv.instance_ip_update (self._obj)

        if add_link:
            self.add_link('virtual_network', cfixture.ConrtailLink('virtual_network', 'instance_ip', 'virtual_network', ['ref'], lo))
    #end add_virtual_network_link

    def get_virtual_networks (self):
        return self.get_links ('virtual_network')
    #end get_virtual_networks
    def add_virtual_machine_interface (self, lo, update_server = True, add_link = True):
        '''
        add :class:`VirtualMachineInterface` link to :class:`InstanceIp`
        Args:
            lo (:class:`VirtualMachineInterface`): obj to link
        '''
        if self._obj:
            self._obj.add_virtual_machine_interface (lo)
            if update_server:
                self._conn_drv.instance_ip_update (self._obj)

        if add_link:
            self.add_link('virtual_machine_interface', cfixture.ConrtailLink('virtual_machine_interface', 'instance_ip', 'virtual_machine_interface', ['ref'], lo))
    #end add_virtual_machine_interface_link

    def get_virtual_machine_interfaces (self):
        return self.get_links ('virtual_machine_interface')
    #end get_virtual_machine_interfaces

    def populate (self):
        self._obj.set_instance_ip_address(self.instance_ip_address or GeneratedsSuper.populate_string("instance_ip_address"))
        self._obj.set_instance_ip_family(self.instance_ip_family or GeneratedsSuper.populate_string("instance_ip_family"))
        self._obj.set_instance_ip_mode(self.instance_ip_mode or GeneratedsSuper.populate_string("instance_ip_mode"))
        self._obj.set_subnet_uuid(self.subnet_uuid or GeneratedsSuper.populate_string("subnet_uuid"))
        self._obj.set_id_perms(self.id_perms or vnc_api.gen.resource_xsd.IdPermsType.populate())
        self._obj.set_display_name(self.display_name or GeneratedsSuper.populate_string("display_name"))
    #end populate

    def setUp(self):
        super(InstanceIpTestFixtureGen, self).setUp()
        self._obj = vnc_api.InstanceIp(self._name)
        try:
            self._obj = self._conn_drv.instance_ip_read (fq_name=self._obj.get_fq_name())
            self._update_links (update_server=True)
        except NoIdError:
            self._update_links (update_server=False)
            if self._auto_prop_val:
                self.populate ()
            else:
                self._obj.instance_ip_address = self.instance_ip_address
                self._obj.instance_ip_family = self.instance_ip_family
                self._obj.instance_ip_mode = self.instance_ip_mode
                self._obj.subnet_uuid = self.subnet_uuid
                self._obj.id_perms = self.id_perms
                self._obj.display_name = self.display_name
            self._conn_drv.instance_ip_create(self._obj)
            # read back for server allocated values
            self._obj = self._conn_drv.instance_ip_read(id = self._obj.uuid)
    #end setUp

    def cleanUp(self):
        self._conn_drv.instance_ip_delete(id = self._obj.uuid)
    #end cleanUp

    def getObj(self):
        return self._obj
    #end getObj

#end class InstanceIpTestFixtureGen

class NetworkPolicyTestFixtureGen(cfixture.ContrailFixture):
    """
    Fixture for :class:`.NetworkPolicy`
    """
    def __init__(self, conn_drv, network_policy_name=None, parent_fixt=None, auto_prop_val=False, network_policy_entries=None, id_perms=None, display_name=None):
        '''
        Create NetworkPolicyTestFixtureGen object
        
        constructor

        Args:
            conn_drv (:class:`ConnectionDriver`): connection driver (eg. :class:`vnc_api.vnc_api.VncApi`, :class:`novaclient.client.Client`, etc)

        Kwargs:
            network_policy_name (str): Name of network_policy
            parent_fixt (:class:`.ProjectTestFixtureGen`): Parent fixture
            network_policy_entries (instance): instance of :class:`PolicyEntriesType`
            id_perms (instance): instance of :class:`IdPermsType`
            display_name (instance): instance of :class:`xsd:string`

        '''
        super(NetworkPolicyTestFixtureGen, self).__init__()
        self._conn_drv = conn_drv
        if not network_policy_name:
            self._name = 'default-network-policy'
        else:
            self._name = network_policy_name
        self._obj = None
        self._parent_fixt = parent_fixt
        self._auto_prop_val = auto_prop_val
        self.network_policy_entries = network_policy_entries
        self.id_perms = id_perms
        self.display_name = display_name
    #end __init__

    def _update_links (self, update_server):
        return None
    #end _update_links


    def populate (self):
        self._obj.set_network_policy_entries(self.network_policy_entries or vnc_api.gen.resource_xsd.PolicyEntriesType.populate())
        self._obj.set_id_perms(self.id_perms or vnc_api.gen.resource_xsd.IdPermsType.populate())
        self._obj.set_display_name(self.display_name or GeneratedsSuper.populate_string("display_name"))
    #end populate

    def setUp(self):
        super(NetworkPolicyTestFixtureGen, self).setUp()
        if not self._parent_fixt:
            self._parent_fixt = self.useFixture(ProjectTestFixtureGen(self._conn_drv, 'default-project'))

        self._obj = vnc_api.NetworkPolicy(self._name, self._parent_fixt.getObj ())
        try:
            self._obj = self._conn_drv.network_policy_read (fq_name=self._obj.get_fq_name())
            self._update_links (update_server=True)
        except NoIdError:
            self._update_links (update_server=False)
            if self._auto_prop_val:
                self.populate ()
            else:
                self._obj.network_policy_entries = self.network_policy_entries
                self._obj.id_perms = self.id_perms
                self._obj.display_name = self.display_name
            self._conn_drv.network_policy_create(self._obj)
            # read back for server allocated values
            self._obj = self._conn_drv.network_policy_read(id = self._obj.uuid)
    #end setUp

    def cleanUp(self):
        if self._obj.get_virtual_network_back_refs():
            return
        parent_fixt = getattr(self, '_parent_fixt', None)
        if parent_fixt:
            # non config-root child
            parent_obj = self._parent_fixt.getObj()
            # remove child from parent obj
            for child_obj in parent_obj.get_network_policys():
                if type(child_obj) == dict:
                    child_uuid = child_obj['uuid']
                else:
                    child_uuid = child_obj.uuid
                if child_uuid == self._obj.uuid:
                    parent_obj.network_policys.remove(child_obj)
                    break

        self._conn_drv.network_policy_delete(id = self._obj.uuid)
    #end cleanUp

    def getObj(self):
        return self._obj
    #end getObj

#end class NetworkPolicyTestFixtureGen

class LoadbalancerPoolTestFixtureGen(cfixture.ContrailFixture):
    """
    Fixture for :class:`.LoadbalancerPool`
    """
    def __init__(self, conn_drv, loadbalancer_pool_name=None, parent_fixt=None, auto_prop_val=False, service_instance_refs = None, virtual_machine_interface_refs = None, service_appliance_set_refs = None, loadbalancer_healthmonitor_refs = None, loadbalancer_pool_properties=None, loadbalancer_pool_provider=None, id_perms=None, display_name=None):
        '''
        Create LoadbalancerPoolTestFixtureGen object
        
        constructor

        Args:
            conn_drv (:class:`ConnectionDriver`): connection driver (eg. :class:`vnc_api.vnc_api.VncApi`, :class:`novaclient.client.Client`, etc)

        Kwargs:
            loadbalancer_pool_name (str): Name of loadbalancer_pool
            parent_fixt (:class:`.ProjectTestFixtureGen`): Parent fixture
            service_instance (list): list of :class:`ServiceInstance` type
            virtual_machine_interface (list): list of :class:`VirtualMachineInterface` type
            service_appliance_set (list): list of :class:`ServiceApplianceSet` type
            loadbalancer_healthmonitor (list): list of :class:`LoadbalancerHealthmonitor` type
            loadbalancer_pool_properties (instance): instance of :class:`LoadbalancerPoolType`
            loadbalancer_pool_provider (instance): instance of :class:`xsd:string`
            id_perms (instance): instance of :class:`IdPermsType`
            display_name (instance): instance of :class:`xsd:string`

        '''
        super(LoadbalancerPoolTestFixtureGen, self).__init__()
        self._conn_drv = conn_drv
        if not loadbalancer_pool_name:
            self._name = 'default-loadbalancer-pool'
        else:
            self._name = loadbalancer_pool_name
        self._obj = None
        self._parent_fixt = parent_fixt
        self._auto_prop_val = auto_prop_val
        if service_instance_refs:
            for ln in service_instance_refs:
                self.add_service_instance (ln)
        if virtual_machine_interface_refs:
            for ln in virtual_machine_interface_refs:
                self.add_virtual_machine_interface (ln)
        if service_appliance_set_refs:
            for ln in service_appliance_set_refs:
                self.add_service_appliance_set (ln)
        if loadbalancer_healthmonitor_refs:
            for ln in loadbalancer_healthmonitor_refs:
                self.add_loadbalancer_healthmonitor (ln)
        self.loadbalancer_pool_properties = loadbalancer_pool_properties
        self.loadbalancer_pool_provider = loadbalancer_pool_provider
        self.id_perms = id_perms
        self.display_name = display_name
    #end __init__

    def _update_links (self, update_server):
        for ln in self.get_service_instances ():
            self.add_service_instance (ln.fixture (), update_server = update_server, add_link = False)
        for ln in self.get_virtual_machine_interfaces ():
            self.add_virtual_machine_interface (ln.fixture (), update_server = update_server, add_link = False)
        for ln in self.get_service_appliance_sets ():
            self.add_service_appliance_set (ln.fixture (), update_server = update_server, add_link = False)
        for ln in self.get_loadbalancer_healthmonitors ():
            self.add_loadbalancer_healthmonitor (ln.fixture (), update_server = update_server, add_link = False)
        return None
    #end _update_links

    def add_service_instance (self, lo, update_server = True, add_link = True):
        '''
        add :class:`ServiceInstance` link to :class:`LoadbalancerPool`
        Args:
            lo (:class:`ServiceInstance`): obj to link
        '''
        if self._obj:
            self._obj.add_service_instance (lo)
            if update_server:
                self._conn_drv.loadbalancer_pool_update (self._obj)

        if add_link:
            self.add_link('service_instance', cfixture.ConrtailLink('service_instance', 'loadbalancer_pool', 'service_instance', ['ref'], lo))
    #end add_service_instance_link

    def get_service_instances (self):
        return self.get_links ('service_instance')
    #end get_service_instances
    def add_virtual_machine_interface (self, lo, update_server = True, add_link = True):
        '''
        add :class:`VirtualMachineInterface` link to :class:`LoadbalancerPool`
        Args:
            lo (:class:`VirtualMachineInterface`): obj to link
        '''
        if self._obj:
            self._obj.add_virtual_machine_interface (lo)
            if update_server:
                self._conn_drv.loadbalancer_pool_update (self._obj)

        if add_link:
            self.add_link('virtual_machine_interface', cfixture.ConrtailLink('virtual_machine_interface', 'loadbalancer_pool', 'virtual_machine_interface', ['ref'], lo))
    #end add_virtual_machine_interface_link

    def get_virtual_machine_interfaces (self):
        return self.get_links ('virtual_machine_interface')
    #end get_virtual_machine_interfaces
    def add_service_appliance_set (self, lo, update_server = True, add_link = True):
        '''
        add :class:`ServiceApplianceSet` link to :class:`LoadbalancerPool`
        Args:
            lo (:class:`ServiceApplianceSet`): obj to link
        '''
        if self._obj:
            self._obj.add_service_appliance_set (lo)
            if update_server:
                self._conn_drv.loadbalancer_pool_update (self._obj)

        if add_link:
            self.add_link('service_appliance_set', cfixture.ConrtailLink('service_appliance_set', 'loadbalancer_pool', 'service_appliance_set', ['ref'], lo))
    #end add_service_appliance_set_link

    def get_service_appliance_sets (self):
        return self.get_links ('service_appliance_set')
    #end get_service_appliance_sets
    def add_loadbalancer_healthmonitor (self, lo, update_server = True, add_link = True):
        '''
        add :class:`LoadbalancerHealthmonitor` link to :class:`LoadbalancerPool`
        Args:
            lo (:class:`LoadbalancerHealthmonitor`): obj to link
        '''
        if self._obj:
            self._obj.add_loadbalancer_healthmonitor (lo)
            if update_server:
                self._conn_drv.loadbalancer_pool_update (self._obj)

        if add_link:
            self.add_link('loadbalancer_healthmonitor', cfixture.ConrtailLink('loadbalancer_healthmonitor', 'loadbalancer_pool', 'loadbalancer_healthmonitor', ['ref'], lo))
    #end add_loadbalancer_healthmonitor_link

    def get_loadbalancer_healthmonitors (self):
        return self.get_links ('loadbalancer_healthmonitor')
    #end get_loadbalancer_healthmonitors

    def populate (self):
        self._obj.set_loadbalancer_pool_properties(self.loadbalancer_pool_properties or vnc_api.gen.resource_xsd.LoadbalancerPoolType.populate())
        self._obj.set_loadbalancer_pool_provider(self.loadbalancer_pool_provider or GeneratedsSuper.populate_string("loadbalancer_pool_provider"))
        self._obj.set_id_perms(self.id_perms or vnc_api.gen.resource_xsd.IdPermsType.populate())
        self._obj.set_display_name(self.display_name or GeneratedsSuper.populate_string("display_name"))
    #end populate

    def setUp(self):
        super(LoadbalancerPoolTestFixtureGen, self).setUp()
        if not self._parent_fixt:
            self._parent_fixt = self.useFixture(ProjectTestFixtureGen(self._conn_drv, 'default-project'))

        self._obj = vnc_api.LoadbalancerPool(self._name, self._parent_fixt.getObj ())
        try:
            self._obj = self._conn_drv.loadbalancer_pool_read (fq_name=self._obj.get_fq_name())
            self._update_links (update_server=True)
        except NoIdError:
            self._update_links (update_server=False)
            if self._auto_prop_val:
                self.populate ()
            else:
                self._obj.loadbalancer_pool_properties = self.loadbalancer_pool_properties
                self._obj.loadbalancer_pool_provider = self.loadbalancer_pool_provider
                self._obj.id_perms = self.id_perms
                self._obj.display_name = self.display_name
            self._conn_drv.loadbalancer_pool_create(self._obj)
            # read back for server allocated values
            self._obj = self._conn_drv.loadbalancer_pool_read(id = self._obj.uuid)
    #end setUp

    def cleanUp(self):
        if self._obj.get_virtual_ip_back_refs():
            return
        if self._obj.get_loadbalancer_members():
            return
        parent_fixt = getattr(self, '_parent_fixt', None)
        if parent_fixt:
            # non config-root child
            parent_obj = self._parent_fixt.getObj()
            # remove child from parent obj
            for child_obj in parent_obj.get_loadbalancer_pools():
                if type(child_obj) == dict:
                    child_uuid = child_obj['uuid']
                else:
                    child_uuid = child_obj.uuid
                if child_uuid == self._obj.uuid:
                    parent_obj.loadbalancer_pools.remove(child_obj)
                    break

        self._conn_drv.loadbalancer_pool_delete(id = self._obj.uuid)
    #end cleanUp

    def getObj(self):
        return self._obj
    #end getObj

#end class LoadbalancerPoolTestFixtureGen

class VirtualDnsRecordTestFixtureGen(cfixture.ContrailFixture):
    """
    Fixture for :class:`.VirtualDnsRecord`
    """
    def __init__(self, conn_drv, virtual_DNS_record_name=None, parent_fixt=None, auto_prop_val=False, virtual_DNS_record_data=None, id_perms=None, display_name=None):
        '''
        Create VirtualDnsRecordTestFixtureGen object
        
        constructor

        Args:
            conn_drv (:class:`ConnectionDriver`): connection driver (eg. :class:`vnc_api.vnc_api.VncApi`, :class:`novaclient.client.Client`, etc)

        Kwargs:
            virtual_DNS_record_name (str): Name of virtual_DNS_record
            parent_fixt (:class:`.VirtualDnsTestFixtureGen`): Parent fixture
            virtual_DNS_record_data (instance): instance of :class:`VirtualDnsRecordType`
            id_perms (instance): instance of :class:`IdPermsType`
            display_name (instance): instance of :class:`xsd:string`

        '''
        super(VirtualDnsRecordTestFixtureGen, self).__init__()
        self._conn_drv = conn_drv
        if not virtual_DNS_record_name:
            self._name = 'default-virtual-DNS-record'
        else:
            self._name = virtual_DNS_record_name
        self._obj = None
        self._parent_fixt = parent_fixt
        self._auto_prop_val = auto_prop_val
        self.virtual_DNS_record_data = virtual_DNS_record_data
        self.id_perms = id_perms
        self.display_name = display_name
    #end __init__

    def _update_links (self, update_server):
        return None
    #end _update_links


    def populate (self):
        self._obj.set_virtual_DNS_record_data(self.virtual_DNS_record_data or vnc_api.gen.resource_xsd.VirtualDnsRecordType.populate())
        self._obj.set_id_perms(self.id_perms or vnc_api.gen.resource_xsd.IdPermsType.populate())
        self._obj.set_display_name(self.display_name or GeneratedsSuper.populate_string("display_name"))
    #end populate

    def setUp(self):
        super(VirtualDnsRecordTestFixtureGen, self).setUp()
        if not self._parent_fixt:
            self._parent_fixt = self.useFixture(VirtualDnsTestFixtureGen(self._conn_drv, 'default-virtual-DNS'))

        self._obj = vnc_api.VirtualDnsRecord(self._name, self._parent_fixt.getObj ())
        try:
            self._obj = self._conn_drv.virtual_DNS_record_read (fq_name=self._obj.get_fq_name())
            self._update_links (update_server=True)
        except NoIdError:
            self._update_links (update_server=False)
            if self._auto_prop_val:
                self.populate ()
            else:
                self._obj.virtual_DNS_record_data = self.virtual_DNS_record_data
                self._obj.id_perms = self.id_perms
                self._obj.display_name = self.display_name
            self._conn_drv.virtual_DNS_record_create(self._obj)
            # read back for server allocated values
            self._obj = self._conn_drv.virtual_DNS_record_read(id = self._obj.uuid)
    #end setUp

    def cleanUp(self):
        parent_fixt = getattr(self, '_parent_fixt', None)
        if parent_fixt:
            # non config-root child
            parent_obj = self._parent_fixt.getObj()
            # remove child from parent obj
            for child_obj in parent_obj.get_virtual_DNS_records():
                if type(child_obj) == dict:
                    child_uuid = child_obj['uuid']
                else:
                    child_uuid = child_obj.uuid
                if child_uuid == self._obj.uuid:
                    parent_obj.virtual_DNS_records.remove(child_obj)
                    break

        self._conn_drv.virtual_DNS_record_delete(id = self._obj.uuid)
    #end cleanUp

    def getObj(self):
        return self._obj
    #end getObj

#end class VirtualDnsRecordTestFixtureGen

class RouteTargetTestFixtureGen(cfixture.ContrailFixture):
    """
    Fixture for :class:`.RouteTarget`
    """
    def __init__(self, conn_drv, route_target_name=None, auto_prop_val=False, id_perms=None, display_name=None):
        '''
        Create RouteTargetTestFixtureGen object
        
        constructor

        Args:
            conn_drv (:class:`ConnectionDriver`): connection driver (eg. :class:`vnc_api.vnc_api.VncApi`, :class:`novaclient.client.Client`, etc)

        Kwargs:
            %s_name (str): Name of %s
            id_perms (instance): instance of :class:`IdPermsType`
            display_name (instance): instance of :class:`xsd:string`

        '''
        super(RouteTargetTestFixtureGen, self).__init__()
        self._conn_drv = conn_drv
        if not route_target_name:
            self._name = 'default-route-target'
        else:
            self._name = route_target_name
        self._obj = None
        self._auto_prop_val = auto_prop_val
        self.id_perms = id_perms
        self.display_name = display_name
    #end __init__

    def _update_links (self, update_server):
        return None
    #end _update_links


    def populate (self):
        self._obj.set_id_perms(self.id_perms or vnc_api.gen.resource_xsd.IdPermsType.populate())
        self._obj.set_display_name(self.display_name or GeneratedsSuper.populate_string("display_name"))
    #end populate

    def setUp(self):
        super(RouteTargetTestFixtureGen, self).setUp()
        self._obj = vnc_api.RouteTarget(self._name)
        try:
            self._obj = self._conn_drv.route_target_read (fq_name=self._obj.get_fq_name())
            self._update_links (update_server=True)
        except NoIdError:
            self._update_links (update_server=False)
            if self._auto_prop_val:
                self.populate ()
            else:
                self._obj.id_perms = self.id_perms
                self._obj.display_name = self.display_name
            self._conn_drv.route_target_create(self._obj)
            # read back for server allocated values
            self._obj = self._conn_drv.route_target_read(id = self._obj.uuid)
    #end setUp

    def cleanUp(self):
        if self._obj.get_logical_router_back_refs() or self._obj.get_routing_instance_back_refs():
            return
        self._conn_drv.route_target_delete(id = self._obj.uuid)
    #end cleanUp

    def getObj(self):
        return self._obj
    #end getObj

#end class RouteTargetTestFixtureGen

class FloatingIpTestFixtureGen(cfixture.ContrailFixture):
    """
    Fixture for :class:`.FloatingIp`
    """
    def __init__(self, conn_drv, floating_ip_name=None, parent_fixt=None, auto_prop_val=False, project_refs = None, virtual_machine_interface_refs = None, floating_ip_address=None, floating_ip_is_virtual_ip=None, floating_ip_fixed_ip_address=None, floating_ip_address_family=None, id_perms=None, display_name=None):
        '''
        Create FloatingIpTestFixtureGen object
        
        constructor

        Args:
            conn_drv (:class:`ConnectionDriver`): connection driver (eg. :class:`vnc_api.vnc_api.VncApi`, :class:`novaclient.client.Client`, etc)

        Kwargs:
            floating_ip_name (str): Name of floating_ip
            parent_fixt (:class:`.FloatingIpPoolTestFixtureGen`): Parent fixture
            project (list): list of :class:`Project` type
            virtual_machine_interface (list): list of :class:`VirtualMachineInterface` type
            floating_ip_address (instance): instance of :class:`xsd:string`
            floating_ip_is_virtual_ip (instance): instance of :class:`xsd:boolean`
            floating_ip_fixed_ip_address (instance): instance of :class:`xsd:string`
            floating_ip_address_family (instance): instance of :class:`xsd:string`
            id_perms (instance): instance of :class:`IdPermsType`
            display_name (instance): instance of :class:`xsd:string`

        '''
        super(FloatingIpTestFixtureGen, self).__init__()
        self._conn_drv = conn_drv
        if not floating_ip_name:
            self._name = 'default-floating-ip'
        else:
            self._name = floating_ip_name
        self._obj = None
        self._parent_fixt = parent_fixt
        self._auto_prop_val = auto_prop_val
        if project_refs:
            for ln in project_refs:
                self.add_project (ln)
        if virtual_machine_interface_refs:
            for ln in virtual_machine_interface_refs:
                self.add_virtual_machine_interface (ln)
        self.floating_ip_address = floating_ip_address
        self.floating_ip_is_virtual_ip = floating_ip_is_virtual_ip
        self.floating_ip_fixed_ip_address = floating_ip_fixed_ip_address
        self.floating_ip_address_family = floating_ip_address_family
        self.id_perms = id_perms
        self.display_name = display_name
    #end __init__

    def _update_links (self, update_server):
        for ln in self.get_projects ():
            self.add_project (ln.fixture (), update_server = update_server, add_link = False)
        for ln in self.get_virtual_machine_interfaces ():
            self.add_virtual_machine_interface (ln.fixture (), update_server = update_server, add_link = False)
        return None
    #end _update_links

    def add_project (self, lo, update_server = True, add_link = True):
        '''
        add :class:`Project` link to :class:`FloatingIp`
        Args:
            lo (:class:`Project`): obj to link
        '''
        if self._obj:
            self._obj.add_project (lo)
            if update_server:
                self._conn_drv.floating_ip_update (self._obj)

        if add_link:
            self.add_link('project', cfixture.ConrtailLink('project', 'floating_ip', 'project', ['ref'], lo))
    #end add_project_link

    def get_projects (self):
        return self.get_links ('project')
    #end get_projects
    def add_virtual_machine_interface (self, lo, update_server = True, add_link = True):
        '''
        add :class:`VirtualMachineInterface` link to :class:`FloatingIp`
        Args:
            lo (:class:`VirtualMachineInterface`): obj to link
        '''
        if self._obj:
            self._obj.add_virtual_machine_interface (lo)
            if update_server:
                self._conn_drv.floating_ip_update (self._obj)

        if add_link:
            self.add_link('virtual_machine_interface', cfixture.ConrtailLink('virtual_machine_interface', 'floating_ip', 'virtual_machine_interface', ['ref'], lo))
    #end add_virtual_machine_interface_link

    def get_virtual_machine_interfaces (self):
        return self.get_links ('virtual_machine_interface')
    #end get_virtual_machine_interfaces

    def populate (self):
        self._obj.set_floating_ip_address(self.floating_ip_address or GeneratedsSuper.populate_string("floating_ip_address"))
        self._obj.set_floating_ip_is_virtual_ip(self.floating_ip_is_virtual_ip or GeneratedsSuper.populate_boolean("floating_ip_is_virtual_ip"))
        self._obj.set_floating_ip_fixed_ip_address(self.floating_ip_fixed_ip_address or GeneratedsSuper.populate_string("floating_ip_fixed_ip_address"))
        self._obj.set_floating_ip_address_family(self.floating_ip_address_family or GeneratedsSuper.populate_string("floating_ip_address_family"))
        self._obj.set_id_perms(self.id_perms or vnc_api.gen.resource_xsd.IdPermsType.populate())
        self._obj.set_display_name(self.display_name or GeneratedsSuper.populate_string("display_name"))
    #end populate

    def setUp(self):
        super(FloatingIpTestFixtureGen, self).setUp()
        if not self._parent_fixt:
            self._parent_fixt = self.useFixture(FloatingIpPoolTestFixtureGen(self._conn_drv, 'default-floating-ip-pool'))

        self._obj = vnc_api.FloatingIp(self._name, self._parent_fixt.getObj ())
        try:
            self._obj = self._conn_drv.floating_ip_read (fq_name=self._obj.get_fq_name())
            self._update_links (update_server=True)
        except NoIdError:
            self._update_links (update_server=False)
            if self._auto_prop_val:
                self.populate ()
            else:
                self._obj.floating_ip_address = self.floating_ip_address
                self._obj.floating_ip_is_virtual_ip = self.floating_ip_is_virtual_ip
                self._obj.floating_ip_fixed_ip_address = self.floating_ip_fixed_ip_address
                self._obj.floating_ip_address_family = self.floating_ip_address_family
                self._obj.id_perms = self.id_perms
                self._obj.display_name = self.display_name
            self._conn_drv.floating_ip_create(self._obj)
            # read back for server allocated values
            self._obj = self._conn_drv.floating_ip_read(id = self._obj.uuid)
    #end setUp

    def cleanUp(self):
        if self._obj.get_customer_attachment_back_refs():
            return
        parent_fixt = getattr(self, '_parent_fixt', None)
        if parent_fixt:
            # non config-root child
            parent_obj = self._parent_fixt.getObj()
            # remove child from parent obj
            for child_obj in parent_obj.get_floating_ips():
                if type(child_obj) == dict:
                    child_uuid = child_obj['uuid']
                else:
                    child_uuid = child_obj.uuid
                if child_uuid == self._obj.uuid:
                    parent_obj.floating_ips.remove(child_obj)
                    break

        self._conn_drv.floating_ip_delete(id = self._obj.uuid)
    #end cleanUp

    def getObj(self):
        return self._obj
    #end getObj

#end class FloatingIpTestFixtureGen

class FloatingIpPoolTestFixtureGen(cfixture.ContrailFixture):
    """
    Fixture for :class:`.FloatingIpPool`
    """
    def __init__(self, conn_drv, floating_ip_pool_name=None, parent_fixt=None, auto_prop_val=False, floating_ip_pool_prefixes=None, id_perms=None, display_name=None):
        '''
        Create FloatingIpPoolTestFixtureGen object
        
        constructor

        Args:
            conn_drv (:class:`ConnectionDriver`): connection driver (eg. :class:`vnc_api.vnc_api.VncApi`, :class:`novaclient.client.Client`, etc)

        Kwargs:
            floating_ip_pool_name (str): Name of floating_ip_pool
            parent_fixt (:class:`.VirtualNetworkTestFixtureGen`): Parent fixture
            floating_ip_pool_prefixes (instance): instance of :class:`FloatingIpPoolType`
            id_perms (instance): instance of :class:`IdPermsType`
            display_name (instance): instance of :class:`xsd:string`

        '''
        super(FloatingIpPoolTestFixtureGen, self).__init__()
        self._conn_drv = conn_drv
        if not floating_ip_pool_name:
            self._name = 'default-floating-ip-pool'
        else:
            self._name = floating_ip_pool_name
        self._obj = None
        self._parent_fixt = parent_fixt
        self._auto_prop_val = auto_prop_val
        self.floating_ip_pool_prefixes = floating_ip_pool_prefixes
        self.id_perms = id_perms
        self.display_name = display_name
    #end __init__

    def _update_links (self, update_server):
        return None
    #end _update_links


    def populate (self):
        self._obj.set_floating_ip_pool_prefixes(self.floating_ip_pool_prefixes or vnc_api.gen.resource_xsd.FloatingIpPoolType.populate())
        self._obj.set_id_perms(self.id_perms or vnc_api.gen.resource_xsd.IdPermsType.populate())
        self._obj.set_display_name(self.display_name or GeneratedsSuper.populate_string("display_name"))
    #end populate

    def setUp(self):
        super(FloatingIpPoolTestFixtureGen, self).setUp()
        if not self._parent_fixt:
            self._parent_fixt = self.useFixture(VirtualNetworkTestFixtureGen(self._conn_drv, 'default-virtual-network'))

        self._obj = vnc_api.FloatingIpPool(self._name, self._parent_fixt.getObj ())
        try:
            self._obj = self._conn_drv.floating_ip_pool_read (fq_name=self._obj.get_fq_name())
            self._update_links (update_server=True)
        except NoIdError:
            self._update_links (update_server=False)
            if self._auto_prop_val:
                self.populate ()
            else:
                self._obj.floating_ip_pool_prefixes = self.floating_ip_pool_prefixes
                self._obj.id_perms = self.id_perms
                self._obj.display_name = self.display_name
            self._conn_drv.floating_ip_pool_create(self._obj)
            # read back for server allocated values
            self._obj = self._conn_drv.floating_ip_pool_read(id = self._obj.uuid)
    #end setUp

    def cleanUp(self):
        if self._obj.get_project_back_refs():
            return
        if self._obj.get_floating_ips():
            return
        parent_fixt = getattr(self, '_parent_fixt', None)
        if parent_fixt:
            # non config-root child
            parent_obj = self._parent_fixt.getObj()
            # remove child from parent obj
            for child_obj in parent_obj.get_floating_ip_pools():
                if type(child_obj) == dict:
                    child_uuid = child_obj['uuid']
                else:
                    child_uuid = child_obj.uuid
                if child_uuid == self._obj.uuid:
                    parent_obj.floating_ip_pools.remove(child_obj)
                    break

        self._conn_drv.floating_ip_pool_delete(id = self._obj.uuid)
    #end cleanUp

    def getObj(self):
        return self._obj
    #end getObj

#end class FloatingIpPoolTestFixtureGen

class PhysicalRouterTestFixtureGen(cfixture.ContrailFixture):
    """
    Fixture for :class:`.PhysicalRouter`
    """
    def __init__(self, conn_drv, physical_router_name=None, parent_fixt=None, auto_prop_val=False, virtual_router_refs = None, bgp_router_refs = None, virtual_network_refs = None, physical_router_management_ip=None, physical_router_dataplane_ip=None, physical_router_vendor_name=None, physical_router_product_name=None, physical_router_vnc_managed=None, physical_router_user_credentials=None, physical_router_snmp_credentials=None, physical_router_junos_service_ports=None, id_perms=None, display_name=None):
        '''
        Create PhysicalRouterTestFixtureGen object
        
        constructor

        Args:
            conn_drv (:class:`ConnectionDriver`): connection driver (eg. :class:`vnc_api.vnc_api.VncApi`, :class:`novaclient.client.Client`, etc)

        Kwargs:
            physical_router_name (str): Name of physical_router
            parent_fixt (:class:`.GlobalSystemConfigTestFixtureGen`): Parent fixture
            virtual_router (list): list of :class:`VirtualRouter` type
            bgp_router (list): list of :class:`BgpRouter` type
            virtual_network (list): list of :class:`VirtualNetwork` type
            physical_router_management_ip (instance): instance of :class:`xsd:string`
            physical_router_dataplane_ip (instance): instance of :class:`xsd:string`
            physical_router_vendor_name (instance): instance of :class:`xsd:string`
            physical_router_product_name (instance): instance of :class:`xsd:string`
            physical_router_vnc_managed (instance): instance of :class:`xsd:boolean`
            physical_router_user_credentials (instance): instance of :class:`UserCredentials`
            physical_router_snmp_credentials (instance): instance of :class:`SNMPCredentials`
            physical_router_junos_service_ports (instance): instance of :class:`JunosServicePorts`
            id_perms (instance): instance of :class:`IdPermsType`
            display_name (instance): instance of :class:`xsd:string`

        '''
        super(PhysicalRouterTestFixtureGen, self).__init__()
        self._conn_drv = conn_drv
        if not physical_router_name:
            self._name = 'default-physical-router'
        else:
            self._name = physical_router_name
        self._obj = None
        self._parent_fixt = parent_fixt
        self._auto_prop_val = auto_prop_val
        if virtual_router_refs:
            for ln in virtual_router_refs:
                self.add_virtual_router (ln)
        if bgp_router_refs:
            for ln in bgp_router_refs:
                self.add_bgp_router (ln)
        if virtual_network_refs:
            for ln in virtual_network_refs:
                self.add_virtual_network (ln)
        self.physical_router_management_ip = physical_router_management_ip
        self.physical_router_dataplane_ip = physical_router_dataplane_ip
        self.physical_router_vendor_name = physical_router_vendor_name
        self.physical_router_product_name = physical_router_product_name
        self.physical_router_vnc_managed = physical_router_vnc_managed
        self.physical_router_user_credentials = physical_router_user_credentials
        self.physical_router_snmp_credentials = physical_router_snmp_credentials
        self.physical_router_junos_service_ports = physical_router_junos_service_ports
        self.id_perms = id_perms
        self.display_name = display_name
    #end __init__

    def _update_links (self, update_server):
        for ln in self.get_virtual_routers ():
            self.add_virtual_router (ln.fixture (), update_server = update_server, add_link = False)
        for ln in self.get_bgp_routers ():
            self.add_bgp_router (ln.fixture (), update_server = update_server, add_link = False)
        for ln in self.get_virtual_networks ():
            self.add_virtual_network (ln.fixture (), update_server = update_server, add_link = False)
        return None
    #end _update_links

    def add_virtual_router (self, lo, update_server = True, add_link = True):
        '''
        add :class:`VirtualRouter` link to :class:`PhysicalRouter`
        Args:
            lo (:class:`VirtualRouter`): obj to link
        '''
        if self._obj:
            self._obj.add_virtual_router (lo)
            if update_server:
                self._conn_drv.physical_router_update (self._obj)

        if add_link:
            self.add_link('virtual_router', cfixture.ConrtailLink('virtual_router', 'physical_router', 'virtual_router', ['ref'], lo))
    #end add_virtual_router_link

    def get_virtual_routers (self):
        return self.get_links ('virtual_router')
    #end get_virtual_routers
    def add_bgp_router (self, lo, update_server = True, add_link = True):
        '''
        add :class:`BgpRouter` link to :class:`PhysicalRouter`
        Args:
            lo (:class:`BgpRouter`): obj to link
        '''
        if self._obj:
            self._obj.add_bgp_router (lo)
            if update_server:
                self._conn_drv.physical_router_update (self._obj)

        if add_link:
            self.add_link('bgp_router', cfixture.ConrtailLink('bgp_router', 'physical_router', 'bgp_router', ['ref'], lo))
    #end add_bgp_router_link

    def get_bgp_routers (self):
        return self.get_links ('bgp_router')
    #end get_bgp_routers
    def add_virtual_network (self, lo, update_server = True, add_link = True):
        '''
        add :class:`VirtualNetwork` link to :class:`PhysicalRouter`
        Args:
            lo (:class:`VirtualNetwork`): obj to link
        '''
        if self._obj:
            self._obj.add_virtual_network (lo)
            if update_server:
                self._conn_drv.physical_router_update (self._obj)

        if add_link:
            self.add_link('virtual_network', cfixture.ConrtailLink('virtual_network', 'physical_router', 'virtual_network', ['ref'], lo))
    #end add_virtual_network_link

    def get_virtual_networks (self):
        return self.get_links ('virtual_network')
    #end get_virtual_networks

    def populate (self):
        self._obj.set_physical_router_management_ip(self.physical_router_management_ip or GeneratedsSuper.populate_string("physical_router_management_ip"))
        self._obj.set_physical_router_dataplane_ip(self.physical_router_dataplane_ip or GeneratedsSuper.populate_string("physical_router_dataplane_ip"))
        self._obj.set_physical_router_vendor_name(self.physical_router_vendor_name or GeneratedsSuper.populate_string("physical_router_vendor_name"))
        self._obj.set_physical_router_product_name(self.physical_router_product_name or GeneratedsSuper.populate_string("physical_router_product_name"))
        self._obj.set_physical_router_vnc_managed(self.physical_router_vnc_managed or GeneratedsSuper.populate_boolean("physical_router_vnc_managed"))
        self._obj.set_physical_router_user_credentials(self.physical_router_user_credentials or [vnc_api.gen.resource_xsd.UserCredentials.populate()])
        self._obj.set_physical_router_snmp_credentials(self.physical_router_snmp_credentials or vnc_api.gen.resource_xsd.SNMPCredentials.populate())
        self._obj.set_physical_router_junos_service_ports(self.physical_router_junos_service_ports or vnc_api.gen.resource_xsd.JunosServicePorts.populate())
        self._obj.set_id_perms(self.id_perms or vnc_api.gen.resource_xsd.IdPermsType.populate())
        self._obj.set_display_name(self.display_name or GeneratedsSuper.populate_string("display_name"))
    #end populate

    def setUp(self):
        super(PhysicalRouterTestFixtureGen, self).setUp()
        if not self._parent_fixt:
            self._parent_fixt = self.useFixture(GlobalSystemConfigTestFixtureGen(self._conn_drv, 'default-global-system-config'))

        self._obj = vnc_api.PhysicalRouter(self._name, self._parent_fixt.getObj ())
        try:
            self._obj = self._conn_drv.physical_router_read (fq_name=self._obj.get_fq_name())
            self._update_links (update_server=True)
        except NoIdError:
            self._update_links (update_server=False)
            if self._auto_prop_val:
                self.populate ()
            else:
                self._obj.physical_router_management_ip = self.physical_router_management_ip
                self._obj.physical_router_dataplane_ip = self.physical_router_dataplane_ip
                self._obj.physical_router_vendor_name = self.physical_router_vendor_name
                self._obj.physical_router_product_name = self.physical_router_product_name
                self._obj.physical_router_vnc_managed = self.physical_router_vnc_managed
                self._obj.physical_router_user_credentials = self.physical_router_user_credentials
                self._obj.physical_router_snmp_credentials = self.physical_router_snmp_credentials
                self._obj.physical_router_junos_service_ports = self.physical_router_junos_service_ports
                self._obj.id_perms = self.id_perms
                self._obj.display_name = self.display_name
            self._conn_drv.physical_router_create(self._obj)
            # read back for server allocated values
            self._obj = self._conn_drv.physical_router_read(id = self._obj.uuid)
    #end setUp

    def cleanUp(self):
        if self._obj.get_physical_interfaces() or self._obj.get_logical_interfaces():
            return
        parent_fixt = getattr(self, '_parent_fixt', None)
        if parent_fixt:
            # non config-root child
            parent_obj = self._parent_fixt.getObj()
            # remove child from parent obj
            for child_obj in parent_obj.get_physical_routers():
                if type(child_obj) == dict:
                    child_uuid = child_obj['uuid']
                else:
                    child_uuid = child_obj.uuid
                if child_uuid == self._obj.uuid:
                    parent_obj.physical_routers.remove(child_obj)
                    break

        self._conn_drv.physical_router_delete(id = self._obj.uuid)
    #end cleanUp

    def getObj(self):
        return self._obj
    #end getObj

#end class PhysicalRouterTestFixtureGen

class BgpRouterTestFixtureGen(cfixture.ContrailFixture):
    """
    Fixture for :class:`.BgpRouter`
    """
    def __init__(self, conn_drv, bgp_router_name=None, parent_fixt=None, auto_prop_val=False, bgp_router_ref_infos = None, bgp_router_parameters=None, id_perms=None, display_name=None):
        '''
        Create BgpRouterTestFixtureGen object
        
        constructor

        Args:
            conn_drv (:class:`ConnectionDriver`): connection driver (eg. :class:`vnc_api.vnc_api.VncApi`, :class:`novaclient.client.Client`, etc)

        Kwargs:
            bgp_router_name (str): Name of bgp_router
            parent_fixt (:class:`.RoutingInstanceTestFixtureGen`): Parent fixture
            bgp_router (list): list of tuple (:class:`BgpRouter`, :class: `BgpPeeringAttributes`) type
            bgp_router_parameters (instance): instance of :class:`BgpRouterParams`
            id_perms (instance): instance of :class:`IdPermsType`
            display_name (instance): instance of :class:`xsd:string`

        '''
        super(BgpRouterTestFixtureGen, self).__init__()
        self._conn_drv = conn_drv
        if not bgp_router_name:
            self._name = 'default-bgp-router'
        else:
            self._name = bgp_router_name
        self._obj = None
        self._parent_fixt = parent_fixt
        self._auto_prop_val = auto_prop_val
        if bgp_router_ref_infos:
            for ln, ref in bgp_router_ref_infos:
                self.add_bgp_router (ln, ref)
        self.bgp_router_parameters = bgp_router_parameters
        self.id_perms = id_perms
        self.display_name = display_name
    #end __init__

    def _update_links (self, update_server):
        for ln in self.get_bgp_routers ():
            self.add_bgp_router (*ln.fixture (), update_server = update_server, add_link = False)
        return None
    #end _update_links

    def add_bgp_router (self, lo, ref, update_server = True, add_link = True):
        '''
        add :class:`BgpRouter` link to :class:`BgpRouter`
        Args:
            lo (:class:`BgpRouter`): obj to link
            ref (:class:`BgpPeeringAttributes`): property of the link object
        '''
        if self._obj:
            self._obj.add_bgp_router (lo, ref)
            if update_server:
                self._conn_drv.bgp_router_update (self._obj)

        if add_link:
            self.add_link('bgp_router', cfixture.ConrtailLink('bgp_router', 'bgp_router', 'bgp_router', ['ref'], (lo, ref)))
    #end add_bgp_router_link

    def get_bgp_routers (self):
        return self.get_links ('bgp_router')
    #end get_bgp_routers

    def populate (self):
        self._obj.set_bgp_router_parameters(self.bgp_router_parameters or vnc_api.gen.resource_xsd.BgpRouterParams.populate())
        self._obj.set_id_perms(self.id_perms or vnc_api.gen.resource_xsd.IdPermsType.populate())
        self._obj.set_display_name(self.display_name or GeneratedsSuper.populate_string("display_name"))
    #end populate

    def setUp(self):
        super(BgpRouterTestFixtureGen, self).setUp()
        if not self._parent_fixt:
            self._parent_fixt = self.useFixture(RoutingInstanceTestFixtureGen(self._conn_drv, 'default-routing-instance'))

        self._obj = vnc_api.BgpRouter(self._name, self._parent_fixt.getObj ())
        try:
            self._obj = self._conn_drv.bgp_router_read (fq_name=self._obj.get_fq_name())
            self._update_links (update_server=True)
        except NoIdError:
            self._update_links (update_server=False)
            if self._auto_prop_val:
                self.populate ()
            else:
                self._obj.bgp_router_parameters = self.bgp_router_parameters
                self._obj.id_perms = self.id_perms
                self._obj.display_name = self.display_name
            self._conn_drv.bgp_router_create(self._obj)
            # read back for server allocated values
            self._obj = self._conn_drv.bgp_router_read(id = self._obj.uuid)
    #end setUp

    def cleanUp(self):
        if self._obj.get_global_system_config_back_refs() or self._obj.get_physical_router_back_refs() or self._obj.get_virtual_router_back_refs() or self._obj.get_bgp_router_back_refs():
            return
        parent_fixt = getattr(self, '_parent_fixt', None)
        if parent_fixt:
            # non config-root child
            parent_obj = self._parent_fixt.getObj()
            # remove child from parent obj
            for child_obj in parent_obj.get_bgp_routers():
                if type(child_obj) == dict:
                    child_uuid = child_obj['uuid']
                else:
                    child_uuid = child_obj.uuid
                if child_uuid == self._obj.uuid:
                    parent_obj.bgp_routers.remove(child_obj)
                    break

        self._conn_drv.bgp_router_delete(id = self._obj.uuid)
    #end cleanUp

    def getObj(self):
        return self._obj
    #end getObj

#end class BgpRouterTestFixtureGen

class VirtualRouterTestFixtureGen(cfixture.ContrailFixture):
    """
    Fixture for :class:`.VirtualRouter`
    """
    def __init__(self, conn_drv, virtual_router_name=None, parent_fixt=None, auto_prop_val=False, bgp_router_refs = None, virtual_machine_refs = None, virtual_router_type=None, virtual_router_ip_address=None, id_perms=None, display_name=None):
        '''
        Create VirtualRouterTestFixtureGen object
        
        constructor

        Args:
            conn_drv (:class:`ConnectionDriver`): connection driver (eg. :class:`vnc_api.vnc_api.VncApi`, :class:`novaclient.client.Client`, etc)

        Kwargs:
            virtual_router_name (str): Name of virtual_router
            parent_fixt (:class:`.GlobalSystemConfigTestFixtureGen`): Parent fixture
            bgp_router (list): list of :class:`BgpRouter` type
            virtual_machine (list): list of :class:`VirtualMachine` type
            virtual_router_type (instance): instance of :class:`xsd:string`
            virtual_router_ip_address (instance): instance of :class:`xsd:string`
            id_perms (instance): instance of :class:`IdPermsType`
            display_name (instance): instance of :class:`xsd:string`

        '''
        super(VirtualRouterTestFixtureGen, self).__init__()
        self._conn_drv = conn_drv
        if not virtual_router_name:
            self._name = 'default-virtual-router'
        else:
            self._name = virtual_router_name
        self._obj = None
        self._parent_fixt = parent_fixt
        self._auto_prop_val = auto_prop_val
        if bgp_router_refs:
            for ln in bgp_router_refs:
                self.add_bgp_router (ln)
        if virtual_machine_refs:
            for ln in virtual_machine_refs:
                self.add_virtual_machine (ln)
        self.virtual_router_type = virtual_router_type
        self.virtual_router_ip_address = virtual_router_ip_address
        self.id_perms = id_perms
        self.display_name = display_name
    #end __init__

    def _update_links (self, update_server):
        for ln in self.get_bgp_routers ():
            self.add_bgp_router (ln.fixture (), update_server = update_server, add_link = False)
        for ln in self.get_virtual_machines ():
            self.add_virtual_machine (ln.fixture (), update_server = update_server, add_link = False)
        return None
    #end _update_links

    def add_bgp_router (self, lo, update_server = True, add_link = True):
        '''
        add :class:`BgpRouter` link to :class:`VirtualRouter`
        Args:
            lo (:class:`BgpRouter`): obj to link
        '''
        if self._obj:
            self._obj.add_bgp_router (lo)
            if update_server:
                self._conn_drv.virtual_router_update (self._obj)

        if add_link:
            self.add_link('bgp_router', cfixture.ConrtailLink('bgp_router', 'virtual_router', 'bgp_router', ['ref'], lo))
    #end add_bgp_router_link

    def get_bgp_routers (self):
        return self.get_links ('bgp_router')
    #end get_bgp_routers
    def add_virtual_machine (self, lo, update_server = True, add_link = True):
        '''
        add :class:`VirtualMachine` link to :class:`VirtualRouter`
        Args:
            lo (:class:`VirtualMachine`): obj to link
        '''
        if self._obj:
            self._obj.add_virtual_machine (lo)
            if update_server:
                self._conn_drv.virtual_router_update (self._obj)

        if add_link:
            self.add_link('virtual_machine', cfixture.ConrtailLink('virtual_machine', 'virtual_router', 'virtual_machine', ['ref'], lo))
    #end add_virtual_machine_link

    def get_virtual_machines (self):
        return self.get_links ('virtual_machine')
    #end get_virtual_machines

    def populate (self):
        self._obj.set_virtual_router_type(self.virtual_router_type or GeneratedsSuper.populate_string("virtual_router_type"))
        self._obj.set_virtual_router_ip_address(self.virtual_router_ip_address or GeneratedsSuper.populate_string("virtual_router_ip_address"))
        self._obj.set_id_perms(self.id_perms or vnc_api.gen.resource_xsd.IdPermsType.populate())
        self._obj.set_display_name(self.display_name or GeneratedsSuper.populate_string("display_name"))
    #end populate

    def setUp(self):
        super(VirtualRouterTestFixtureGen, self).setUp()
        if not self._parent_fixt:
            self._parent_fixt = self.useFixture(GlobalSystemConfigTestFixtureGen(self._conn_drv, 'default-global-system-config'))

        self._obj = vnc_api.VirtualRouter(self._name, self._parent_fixt.getObj ())
        try:
            self._obj = self._conn_drv.virtual_router_read (fq_name=self._obj.get_fq_name())
            self._update_links (update_server=True)
        except NoIdError:
            self._update_links (update_server=False)
            if self._auto_prop_val:
                self.populate ()
            else:
                self._obj.virtual_router_type = self.virtual_router_type
                self._obj.virtual_router_ip_address = self.virtual_router_ip_address
                self._obj.id_perms = self.id_perms
                self._obj.display_name = self.display_name
            self._conn_drv.virtual_router_create(self._obj)
            # read back for server allocated values
            self._obj = self._conn_drv.virtual_router_read(id = self._obj.uuid)
    #end setUp

    def cleanUp(self):
        if self._obj.get_physical_router_back_refs() or self._obj.get_provider_attachment_back_refs():
            return
        parent_fixt = getattr(self, '_parent_fixt', None)
        if parent_fixt:
            # non config-root child
            parent_obj = self._parent_fixt.getObj()
            # remove child from parent obj
            for child_obj in parent_obj.get_virtual_routers():
                if type(child_obj) == dict:
                    child_uuid = child_obj['uuid']
                else:
                    child_uuid = child_obj.uuid
                if child_uuid == self._obj.uuid:
                    parent_obj.virtual_routers.remove(child_obj)
                    break

        self._conn_drv.virtual_router_delete(id = self._obj.uuid)
    #end cleanUp

    def getObj(self):
        return self._obj
    #end getObj

#end class VirtualRouterTestFixtureGen

class SubnetTestFixtureGen(cfixture.ContrailFixture):
    """
    Fixture for :class:`.Subnet`
    """
    def __init__(self, conn_drv, subnet_name=None, auto_prop_val=False, virtual_machine_interface_refs = None, subnet_ip_prefix=None, id_perms=None, display_name=None):
        '''
        Create SubnetTestFixtureGen object
        
        constructor

        Args:
            conn_drv (:class:`ConnectionDriver`): connection driver (eg. :class:`vnc_api.vnc_api.VncApi`, :class:`novaclient.client.Client`, etc)

        Kwargs:
            %s_name (str): Name of %s
            virtual_machine_interface (list): list of :class:`VirtualMachineInterface` type
            subnet_ip_prefix (instance): instance of :class:`SubnetType`
            id_perms (instance): instance of :class:`IdPermsType`
            display_name (instance): instance of :class:`xsd:string`

        '''
        super(SubnetTestFixtureGen, self).__init__()
        self._conn_drv = conn_drv
        if not subnet_name:
            self._name = 'default-subnet'
        else:
            self._name = subnet_name
        self._obj = None
        self._auto_prop_val = auto_prop_val
        if virtual_machine_interface_refs:
            for ln in virtual_machine_interface_refs:
                self.add_virtual_machine_interface (ln)
        self.subnet_ip_prefix = subnet_ip_prefix
        self.id_perms = id_perms
        self.display_name = display_name
    #end __init__

    def _update_links (self, update_server):
        for ln in self.get_virtual_machine_interfaces ():
            self.add_virtual_machine_interface (ln.fixture (), update_server = update_server, add_link = False)
        return None
    #end _update_links

    def add_virtual_machine_interface (self, lo, update_server = True, add_link = True):
        '''
        add :class:`VirtualMachineInterface` link to :class:`Subnet`
        Args:
            lo (:class:`VirtualMachineInterface`): obj to link
        '''
        if self._obj:
            self._obj.add_virtual_machine_interface (lo)
            if update_server:
                self._conn_drv.subnet_update (self._obj)

        if add_link:
            self.add_link('virtual_machine_interface', cfixture.ConrtailLink('virtual_machine_interface', 'subnet', 'virtual_machine_interface', ['ref'], lo))
    #end add_virtual_machine_interface_link

    def get_virtual_machine_interfaces (self):
        return self.get_links ('virtual_machine_interface')
    #end get_virtual_machine_interfaces

    def populate (self):
        self._obj.set_subnet_ip_prefix(self.subnet_ip_prefix or vnc_api.gen.resource_xsd.SubnetType.populate())
        self._obj.set_id_perms(self.id_perms or vnc_api.gen.resource_xsd.IdPermsType.populate())
        self._obj.set_display_name(self.display_name or GeneratedsSuper.populate_string("display_name"))
    #end populate

    def setUp(self):
        super(SubnetTestFixtureGen, self).setUp()
        self._obj = vnc_api.Subnet(self._name)
        try:
            self._obj = self._conn_drv.subnet_read (fq_name=self._obj.get_fq_name())
            self._update_links (update_server=True)
        except NoIdError:
            self._update_links (update_server=False)
            if self._auto_prop_val:
                self.populate ()
            else:
                self._obj.subnet_ip_prefix = self.subnet_ip_prefix
                self._obj.id_perms = self.id_perms
                self._obj.display_name = self.display_name
            self._conn_drv.subnet_create(self._obj)
            # read back for server allocated values
            self._obj = self._conn_drv.subnet_read(id = self._obj.uuid)
    #end setUp

    def cleanUp(self):
        self._conn_drv.subnet_delete(id = self._obj.uuid)
    #end cleanUp

    def getObj(self):
        return self._obj
    #end getObj

#end class SubnetTestFixtureGen

class GlobalSystemConfigTestFixtureGen(cfixture.ContrailFixture):
    """
    Fixture for :class:`.GlobalSystemConfig`
    """
    def __init__(self, conn_drv, global_system_config_name=None, parent_fixt=None, auto_prop_val=False, bgp_router_refs = None, autonomous_system=None, config_version=None, plugin_tuning=None, ibgp_auto_mesh=None, ip_fabric_subnets=None, id_perms=None, display_name=None):
        '''
        Create GlobalSystemConfigTestFixtureGen object
        
        constructor

        Args:
            conn_drv (:class:`ConnectionDriver`): connection driver (eg. :class:`vnc_api.vnc_api.VncApi`, :class:`novaclient.client.Client`, etc)

        Kwargs:
            global_system_config_name (str): Name of global_system_config
            parent_fixt (:class:`.ConfigRootTestFixtureGen`): Parent fixture
            bgp_router (list): list of :class:`BgpRouter` type
            autonomous_system (instance): instance of :class:`xsd:integer`
            config_version (instance): instance of :class:`xsd:string`
            plugin_tuning (instance): instance of :class:`PluginProperties`
            ibgp_auto_mesh (instance): instance of :class:`xsd:boolean`
            ip_fabric_subnets (instance): instance of :class:`SubnetListType`
            id_perms (instance): instance of :class:`IdPermsType`
            display_name (instance): instance of :class:`xsd:string`

        '''
        super(GlobalSystemConfigTestFixtureGen, self).__init__()
        self._conn_drv = conn_drv
        if not global_system_config_name:
            self._name = 'default-global-system-config'
        else:
            self._name = global_system_config_name
        self._obj = None
        self._parent_fixt = parent_fixt
        self._auto_prop_val = auto_prop_val
        if bgp_router_refs:
            for ln in bgp_router_refs:
                self.add_bgp_router (ln)
        self.autonomous_system = autonomous_system
        self.config_version = config_version
        self.plugin_tuning = plugin_tuning
        self.ibgp_auto_mesh = ibgp_auto_mesh
        self.ip_fabric_subnets = ip_fabric_subnets
        self.id_perms = id_perms
        self.display_name = display_name
    #end __init__

    def _update_links (self, update_server):
        for ln in self.get_bgp_routers ():
            self.add_bgp_router (ln.fixture (), update_server = update_server, add_link = False)
        return None
    #end _update_links

    def add_bgp_router (self, lo, update_server = True, add_link = True):
        '''
        add :class:`BgpRouter` link to :class:`GlobalSystemConfig`
        Args:
            lo (:class:`BgpRouter`): obj to link
        '''
        if self._obj:
            self._obj.add_bgp_router (lo)
            if update_server:
                self._conn_drv.global_system_config_update (self._obj)

        if add_link:
            self.add_link('bgp_router', cfixture.ConrtailLink('bgp_router', 'global_system_config', 'bgp_router', ['ref'], lo))
    #end add_bgp_router_link

    def get_bgp_routers (self):
        return self.get_links ('bgp_router')
    #end get_bgp_routers

    def populate (self):
        self._obj.set_autonomous_system(self.autonomous_system or GeneratedsSuper.populate_integer("autonomous_system"))
        self._obj.set_config_version(self.config_version or GeneratedsSuper.populate_string("config_version"))
        self._obj.set_plugin_tuning(self.plugin_tuning or vnc_api.gen.resource_xsd.PluginProperties.populate())
        self._obj.set_ibgp_auto_mesh(self.ibgp_auto_mesh or GeneratedsSuper.populate_boolean("ibgp_auto_mesh"))
        self._obj.set_ip_fabric_subnets(self.ip_fabric_subnets or vnc_api.gen.resource_xsd.SubnetListType.populate())
        self._obj.set_id_perms(self.id_perms or vnc_api.gen.resource_xsd.IdPermsType.populate())
        self._obj.set_display_name(self.display_name or GeneratedsSuper.populate_string("display_name"))
    #end populate

    def setUp(self):
        super(GlobalSystemConfigTestFixtureGen, self).setUp()
        # child of config-root
        self._obj = vnc_api.GlobalSystemConfig(self._name)
        try:
            self._obj = self._conn_drv.global_system_config_read (fq_name=self._obj.get_fq_name())
            self._update_links (update_server=True)
        except NoIdError:
            self._update_links (update_server=False)
            if self._auto_prop_val:
                self.populate ()
            else:
                self._obj.autonomous_system = self.autonomous_system
                self._obj.config_version = self.config_version
                self._obj.plugin_tuning = self.plugin_tuning
                self._obj.ibgp_auto_mesh = self.ibgp_auto_mesh
                self._obj.ip_fabric_subnets = self.ip_fabric_subnets
                self._obj.id_perms = self.id_perms
                self._obj.display_name = self.display_name
            self._conn_drv.global_system_config_create(self._obj)
            # read back for server allocated values
            self._obj = self._conn_drv.global_system_config_read(id = self._obj.uuid)
    #end setUp

    def cleanUp(self):
        if self._obj.get_global_vrouter_configs() or self._obj.get_physical_routers() or self._obj.get_virtual_routers() or self._obj.get_config_nodes() or self._obj.get_analytics_nodes() or self._obj.get_database_nodes() or self._obj.get_service_appliance_sets():
            return
        parent_fixt = getattr(self, '_parent_fixt', None)
        if parent_fixt:
            # non config-root child
            parent_obj = self._parent_fixt.getObj()
            # remove child from parent obj
            for child_obj in parent_obj.get_global_system_configs():
                if type(child_obj) == dict:
                    child_uuid = child_obj['uuid']
                else:
                    child_uuid = child_obj.uuid
                if child_uuid == self._obj.uuid:
                    parent_obj.global_system_configs.remove(child_obj)
                    break

        self._conn_drv.global_system_config_delete(id = self._obj.uuid)
    #end cleanUp

    def getObj(self):
        return self._obj
    #end getObj

#end class GlobalSystemConfigTestFixtureGen

class ServiceApplianceTestFixtureGen(cfixture.ContrailFixture):
    """
    Fixture for :class:`.ServiceAppliance`
    """
    def __init__(self, conn_drv, service_appliance_name=None, parent_fixt=None, auto_prop_val=False, service_appliance_user_credentials=None, service_appliance_ip_address=None, service_appliance_properties=None, id_perms=None, display_name=None):
        '''
        Create ServiceApplianceTestFixtureGen object
        
        constructor

        Args:
            conn_drv (:class:`ConnectionDriver`): connection driver (eg. :class:`vnc_api.vnc_api.VncApi`, :class:`novaclient.client.Client`, etc)

        Kwargs:
            service_appliance_name (str): Name of service_appliance
            parent_fixt (:class:`.ServiceApplianceSetTestFixtureGen`): Parent fixture
            service_appliance_user_credentials (instance): instance of :class:`UserCredentials`
            service_appliance_ip_address (instance): instance of :class:`xsd:string`
            service_appliance_properties (instance): instance of :class:`KeyValuePairs`
            id_perms (instance): instance of :class:`IdPermsType`
            display_name (instance): instance of :class:`xsd:string`

        '''
        super(ServiceApplianceTestFixtureGen, self).__init__()
        self._conn_drv = conn_drv
        if not service_appliance_name:
            self._name = 'default-service-appliance'
        else:
            self._name = service_appliance_name
        self._obj = None
        self._parent_fixt = parent_fixt
        self._auto_prop_val = auto_prop_val
        self.service_appliance_user_credentials = service_appliance_user_credentials
        self.service_appliance_ip_address = service_appliance_ip_address
        self.service_appliance_properties = service_appliance_properties
        self.id_perms = id_perms
        self.display_name = display_name
    #end __init__

    def _update_links (self, update_server):
        return None
    #end _update_links


    def populate (self):
        self._obj.set_service_appliance_user_credentials(self.service_appliance_user_credentials or [vnc_api.gen.resource_xsd.UserCredentials.populate()])
        self._obj.set_service_appliance_ip_address(self.service_appliance_ip_address or GeneratedsSuper.populate_string("service_appliance_ip_address"))
        self._obj.set_service_appliance_properties(self.service_appliance_properties or vnc_api.gen.resource_xsd.KeyValuePairs.populate())
        self._obj.set_id_perms(self.id_perms or vnc_api.gen.resource_xsd.IdPermsType.populate())
        self._obj.set_display_name(self.display_name or GeneratedsSuper.populate_string("display_name"))
    #end populate

    def setUp(self):
        super(ServiceApplianceTestFixtureGen, self).setUp()
        if not self._parent_fixt:
            self._parent_fixt = self.useFixture(ServiceApplianceSetTestFixtureGen(self._conn_drv, 'default-service-appliance-set'))

        self._obj = vnc_api.ServiceAppliance(self._name, self._parent_fixt.getObj ())
        try:
            self._obj = self._conn_drv.service_appliance_read (fq_name=self._obj.get_fq_name())
            self._update_links (update_server=True)
        except NoIdError:
            self._update_links (update_server=False)
            if self._auto_prop_val:
                self.populate ()
            else:
                self._obj.service_appliance_user_credentials = self.service_appliance_user_credentials
                self._obj.service_appliance_ip_address = self.service_appliance_ip_address
                self._obj.service_appliance_properties = self.service_appliance_properties
                self._obj.id_perms = self.id_perms
                self._obj.display_name = self.display_name
            self._conn_drv.service_appliance_create(self._obj)
            # read back for server allocated values
            self._obj = self._conn_drv.service_appliance_read(id = self._obj.uuid)
    #end setUp

    def cleanUp(self):
        parent_fixt = getattr(self, '_parent_fixt', None)
        if parent_fixt:
            # non config-root child
            parent_obj = self._parent_fixt.getObj()
            # remove child from parent obj
            for child_obj in parent_obj.get_service_appliances():
                if type(child_obj) == dict:
                    child_uuid = child_obj['uuid']
                else:
                    child_uuid = child_obj.uuid
                if child_uuid == self._obj.uuid:
                    parent_obj.service_appliances.remove(child_obj)
                    break

        self._conn_drv.service_appliance_delete(id = self._obj.uuid)
    #end cleanUp

    def getObj(self):
        return self._obj
    #end getObj

#end class ServiceApplianceTestFixtureGen

class ServiceInstanceTestFixtureGen(cfixture.ContrailFixture):
    """
    Fixture for :class:`.ServiceInstance`
    """
    def __init__(self, conn_drv, service_instance_name=None, parent_fixt=None, auto_prop_val=False, service_template_refs = None, service_instance_properties=None, id_perms=None, display_name=None):
        '''
        Create ServiceInstanceTestFixtureGen object
        
        constructor

        Args:
            conn_drv (:class:`ConnectionDriver`): connection driver (eg. :class:`vnc_api.vnc_api.VncApi`, :class:`novaclient.client.Client`, etc)

        Kwargs:
            service_instance_name (str): Name of service_instance
            parent_fixt (:class:`.ProjectTestFixtureGen`): Parent fixture
            service_template (list): list of :class:`ServiceTemplate` type
            service_instance_properties (instance): instance of :class:`ServiceInstanceType`
            id_perms (instance): instance of :class:`IdPermsType`
            display_name (instance): instance of :class:`xsd:string`

        '''
        super(ServiceInstanceTestFixtureGen, self).__init__()
        self._conn_drv = conn_drv
        if not service_instance_name:
            self._name = 'default-service-instance'
        else:
            self._name = service_instance_name
        self._obj = None
        self._parent_fixt = parent_fixt
        self._auto_prop_val = auto_prop_val
        if service_template_refs:
            for ln in service_template_refs:
                self.add_service_template (ln)
        self.service_instance_properties = service_instance_properties
        self.id_perms = id_perms
        self.display_name = display_name
    #end __init__

    def _update_links (self, update_server):
        for ln in self.get_service_templates ():
            self.add_service_template (ln.fixture (), update_server = update_server, add_link = False)
        return None
    #end _update_links

    def add_service_template (self, lo, update_server = True, add_link = True):
        '''
        add :class:`ServiceTemplate` link to :class:`ServiceInstance`
        Args:
            lo (:class:`ServiceTemplate`): obj to link
        '''
        if self._obj:
            self._obj.add_service_template (lo)
            if update_server:
                self._conn_drv.service_instance_update (self._obj)

        if add_link:
            self.add_link('service_template', cfixture.ConrtailLink('service_template', 'service_instance', 'service_template', ['ref'], lo))
    #end add_service_template_link

    def get_service_templates (self):
        return self.get_links ('service_template')
    #end get_service_templates

    def populate (self):
        self._obj.set_service_instance_properties(self.service_instance_properties or vnc_api.gen.resource_xsd.ServiceInstanceType.populate())
        self._obj.set_id_perms(self.id_perms or vnc_api.gen.resource_xsd.IdPermsType.populate())
        self._obj.set_display_name(self.display_name or GeneratedsSuper.populate_string("display_name"))
    #end populate

    def setUp(self):
        super(ServiceInstanceTestFixtureGen, self).setUp()
        if not self._parent_fixt:
            self._parent_fixt = self.useFixture(ProjectTestFixtureGen(self._conn_drv, 'default-project'))

        self._obj = vnc_api.ServiceInstance(self._name, self._parent_fixt.getObj ())
        try:
            self._obj = self._conn_drv.service_instance_read (fq_name=self._obj.get_fq_name())
            self._update_links (update_server=True)
        except NoIdError:
            self._update_links (update_server=False)
            if self._auto_prop_val:
                self.populate ()
            else:
                self._obj.service_instance_properties = self.service_instance_properties
                self._obj.id_perms = self.id_perms
                self._obj.display_name = self.display_name
            self._conn_drv.service_instance_create(self._obj)
            # read back for server allocated values
            self._obj = self._conn_drv.service_instance_read(id = self._obj.uuid)
    #end setUp

    def cleanUp(self):
        if self._obj.get_logical_router_back_refs() or self._obj.get_loadbalancer_pool_back_refs():
            return
        parent_fixt = getattr(self, '_parent_fixt', None)
        if parent_fixt:
            # non config-root child
            parent_obj = self._parent_fixt.getObj()
            # remove child from parent obj
            for child_obj in parent_obj.get_service_instances():
                if type(child_obj) == dict:
                    child_uuid = child_obj['uuid']
                else:
                    child_uuid = child_obj.uuid
                if child_uuid == self._obj.uuid:
                    parent_obj.service_instances.remove(child_obj)
                    break

        self._conn_drv.service_instance_delete(id = self._obj.uuid)
    #end cleanUp

    def getObj(self):
        return self._obj
    #end getObj

#end class ServiceInstanceTestFixtureGen

class NamespaceTestFixtureGen(cfixture.ContrailFixture):
    """
    Fixture for :class:`.Namespace`
    """
    def __init__(self, conn_drv, namespace_name=None, parent_fixt=None, auto_prop_val=False, namespace_cidr=None, id_perms=None, display_name=None):
        '''
        Create NamespaceTestFixtureGen object
        
        constructor

        Args:
            conn_drv (:class:`ConnectionDriver`): connection driver (eg. :class:`vnc_api.vnc_api.VncApi`, :class:`novaclient.client.Client`, etc)

        Kwargs:
            namespace_name (str): Name of namespace
            parent_fixt (:class:`.DomainTestFixtureGen`): Parent fixture
            namespace_cidr (instance): instance of :class:`SubnetType`
            id_perms (instance): instance of :class:`IdPermsType`
            display_name (instance): instance of :class:`xsd:string`

        '''
        super(NamespaceTestFixtureGen, self).__init__()
        self._conn_drv = conn_drv
        if not namespace_name:
            self._name = 'default-namespace'
        else:
            self._name = namespace_name
        self._obj = None
        self._parent_fixt = parent_fixt
        self._auto_prop_val = auto_prop_val
        self.namespace_cidr = namespace_cidr
        self.id_perms = id_perms
        self.display_name = display_name
    #end __init__

    def _update_links (self, update_server):
        return None
    #end _update_links


    def populate (self):
        self._obj.set_namespace_cidr(self.namespace_cidr or vnc_api.gen.resource_xsd.SubnetType.populate())
        self._obj.set_id_perms(self.id_perms or vnc_api.gen.resource_xsd.IdPermsType.populate())
        self._obj.set_display_name(self.display_name or GeneratedsSuper.populate_string("display_name"))
    #end populate

    def setUp(self):
        super(NamespaceTestFixtureGen, self).setUp()
        if not self._parent_fixt:
            self._parent_fixt = self.useFixture(DomainTestFixtureGen(self._conn_drv, 'default-domain'))

        self._obj = vnc_api.Namespace(self._name, self._parent_fixt.getObj ())
        try:
            self._obj = self._conn_drv.namespace_read (fq_name=self._obj.get_fq_name())
            self._update_links (update_server=True)
        except NoIdError:
            self._update_links (update_server=False)
            if self._auto_prop_val:
                self.populate ()
            else:
                self._obj.namespace_cidr = self.namespace_cidr
                self._obj.id_perms = self.id_perms
                self._obj.display_name = self.display_name
            self._conn_drv.namespace_create(self._obj)
            # read back for server allocated values
            self._obj = self._conn_drv.namespace_read(id = self._obj.uuid)
    #end setUp

    def cleanUp(self):
        if self._obj.get_project_back_refs():
            return
        parent_fixt = getattr(self, '_parent_fixt', None)
        if parent_fixt:
            # non config-root child
            parent_obj = self._parent_fixt.getObj()
            # remove child from parent obj
            for child_obj in parent_obj.get_namespaces():
                if type(child_obj) == dict:
                    child_uuid = child_obj['uuid']
                else:
                    child_uuid = child_obj.uuid
                if child_uuid == self._obj.uuid:
                    parent_obj.namespaces.remove(child_obj)
                    break

        self._conn_drv.namespace_delete(id = self._obj.uuid)
    #end cleanUp

    def getObj(self):
        return self._obj
    #end getObj

#end class NamespaceTestFixtureGen

class LogicalInterfaceTestFixtureGen(cfixture.ContrailFixture):
    """
    Fixture for :class:`.LogicalInterface`
    """
    def __init__(self, conn_drv, logical_interface_name=None, parent_fixt=None, auto_prop_val=False, virtual_machine_interface_refs = None, logical_interface_vlan_tag=None, logical_interface_type=None, id_perms=None, display_name=None):
        '''
        Create LogicalInterfaceTestFixtureGen object
        
        constructor

        Args:
            conn_drv (:class:`ConnectionDriver`): connection driver (eg. :class:`vnc_api.vnc_api.VncApi`, :class:`novaclient.client.Client`, etc)

        Kwargs:
            logical_interface_name (str): Name of logical_interface
            parent_fixt (:class:`.PhysicalRouterTestFixtureGen`): Parent fixture
            virtual_machine_interface (list): list of :class:`VirtualMachineInterface` type
            logical_interface_vlan_tag (instance): instance of :class:`xsd:integer`
            logical_interface_type (instance): instance of :class:`xsd:string`
            id_perms (instance): instance of :class:`IdPermsType`
            display_name (instance): instance of :class:`xsd:string`

        '''
        super(LogicalInterfaceTestFixtureGen, self).__init__()
        self._conn_drv = conn_drv
        if not logical_interface_name:
            self._name = 'default-logical-interface'
        else:
            self._name = logical_interface_name
        self._obj = None
        self._parent_fixt = parent_fixt
        self._auto_prop_val = auto_prop_val
        if virtual_machine_interface_refs:
            for ln in virtual_machine_interface_refs:
                self.add_virtual_machine_interface (ln)
        self.logical_interface_vlan_tag = logical_interface_vlan_tag
        self.logical_interface_type = logical_interface_type
        self.id_perms = id_perms
        self.display_name = display_name
    #end __init__

    def _update_links (self, update_server):
        for ln in self.get_virtual_machine_interfaces ():
            self.add_virtual_machine_interface (ln.fixture (), update_server = update_server, add_link = False)
        return None
    #end _update_links

    def add_virtual_machine_interface (self, lo, update_server = True, add_link = True):
        '''
        add :class:`VirtualMachineInterface` link to :class:`LogicalInterface`
        Args:
            lo (:class:`VirtualMachineInterface`): obj to link
        '''
        if self._obj:
            self._obj.add_virtual_machine_interface (lo)
            if update_server:
                self._conn_drv.logical_interface_update (self._obj)

        if add_link:
            self.add_link('virtual_machine_interface', cfixture.ConrtailLink('virtual_machine_interface', 'logical_interface', 'virtual_machine_interface', ['ref'], lo))
    #end add_virtual_machine_interface_link

    def get_virtual_machine_interfaces (self):
        return self.get_links ('virtual_machine_interface')
    #end get_virtual_machine_interfaces

    def populate (self):
        self._obj.set_logical_interface_vlan_tag(self.logical_interface_vlan_tag or GeneratedsSuper.populate_integer("logical_interface_vlan_tag"))
        self._obj.set_logical_interface_type(self.logical_interface_type or GeneratedsSuper.populate_string("logical_interface_type"))
        self._obj.set_id_perms(self.id_perms or vnc_api.gen.resource_xsd.IdPermsType.populate())
        self._obj.set_display_name(self.display_name or GeneratedsSuper.populate_string("display_name"))
    #end populate

    def setUp(self):
        super(LogicalInterfaceTestFixtureGen, self).setUp()
        if not self._parent_fixt:
            raise AmbiguousParentError("[[u'default-global-system-config', u'default-physical-router'], [u'default-global-system-config', u'default-physical-router', u'default-physical-interface']]")

        self._obj = vnc_api.LogicalInterface(self._name, self._parent_fixt.getObj ())
        try:
            self._obj = self._conn_drv.logical_interface_read (fq_name=self._obj.get_fq_name())
            self._update_links (update_server=True)
        except NoIdError:
            self._update_links (update_server=False)
            if self._auto_prop_val:
                self.populate ()
            else:
                self._obj.logical_interface_vlan_tag = self.logical_interface_vlan_tag
                self._obj.logical_interface_type = self.logical_interface_type
                self._obj.id_perms = self.id_perms
                self._obj.display_name = self.display_name
            self._conn_drv.logical_interface_create(self._obj)
            # read back for server allocated values
            self._obj = self._conn_drv.logical_interface_read(id = self._obj.uuid)
    #end setUp

    def cleanUp(self):
        parent_fixt = getattr(self, '_parent_fixt', None)
        if parent_fixt:
            # non config-root child
            parent_obj = self._parent_fixt.getObj()
            # remove child from parent obj
            for child_obj in parent_obj.get_logical_interfaces():
                if type(child_obj) == dict:
                    child_uuid = child_obj['uuid']
                else:
                    child_uuid = child_obj.uuid
                if child_uuid == self._obj.uuid:
                    parent_obj.logical_interfaces.remove(child_obj)
                    break

        self._conn_drv.logical_interface_delete(id = self._obj.uuid)
    #end cleanUp

    def getObj(self):
        return self._obj
    #end getObj

#end class LogicalInterfaceTestFixtureGen

class RouteTableTestFixtureGen(cfixture.ContrailFixture):
    """
    Fixture for :class:`.RouteTable`
    """
    def __init__(self, conn_drv, route_table_name=None, parent_fixt=None, auto_prop_val=False, routes=None, id_perms=None, display_name=None):
        '''
        Create RouteTableTestFixtureGen object
        
        constructor

        Args:
            conn_drv (:class:`ConnectionDriver`): connection driver (eg. :class:`vnc_api.vnc_api.VncApi`, :class:`novaclient.client.Client`, etc)

        Kwargs:
            route_table_name (str): Name of route_table
            parent_fixt (:class:`.ProjectTestFixtureGen`): Parent fixture
            routes (instance): instance of :class:`RouteTableType`
            id_perms (instance): instance of :class:`IdPermsType`
            display_name (instance): instance of :class:`xsd:string`

        '''
        super(RouteTableTestFixtureGen, self).__init__()
        self._conn_drv = conn_drv
        if not route_table_name:
            self._name = 'default-route-table'
        else:
            self._name = route_table_name
        self._obj = None
        self._parent_fixt = parent_fixt
        self._auto_prop_val = auto_prop_val
        self.routes = routes
        self.id_perms = id_perms
        self.display_name = display_name
    #end __init__

    def _update_links (self, update_server):
        return None
    #end _update_links


    def populate (self):
        self._obj.set_routes(self.routes or vnc_api.gen.resource_xsd.RouteTableType.populate())
        self._obj.set_id_perms(self.id_perms or vnc_api.gen.resource_xsd.IdPermsType.populate())
        self._obj.set_display_name(self.display_name or GeneratedsSuper.populate_string("display_name"))
    #end populate

    def setUp(self):
        super(RouteTableTestFixtureGen, self).setUp()
        if not self._parent_fixt:
            self._parent_fixt = self.useFixture(ProjectTestFixtureGen(self._conn_drv, 'default-project'))

        self._obj = vnc_api.RouteTable(self._name, self._parent_fixt.getObj ())
        try:
            self._obj = self._conn_drv.route_table_read (fq_name=self._obj.get_fq_name())
            self._update_links (update_server=True)
        except NoIdError:
            self._update_links (update_server=False)
            if self._auto_prop_val:
                self.populate ()
            else:
                self._obj.routes = self.routes
                self._obj.id_perms = self.id_perms
                self._obj.display_name = self.display_name
            self._conn_drv.route_table_create(self._obj)
            # read back for server allocated values
            self._obj = self._conn_drv.route_table_read(id = self._obj.uuid)
    #end setUp

    def cleanUp(self):
        if self._obj.get_virtual_network_back_refs():
            return
        parent_fixt = getattr(self, '_parent_fixt', None)
        if parent_fixt:
            # non config-root child
            parent_obj = self._parent_fixt.getObj()
            # remove child from parent obj
            for child_obj in parent_obj.get_route_tables():
                if type(child_obj) == dict:
                    child_uuid = child_obj['uuid']
                else:
                    child_uuid = child_obj.uuid
                if child_uuid == self._obj.uuid:
                    parent_obj.route_tables.remove(child_obj)
                    break

        self._conn_drv.route_table_delete(id = self._obj.uuid)
    #end cleanUp

    def getObj(self):
        return self._obj
    #end getObj

#end class RouteTableTestFixtureGen

class PhysicalInterfaceTestFixtureGen(cfixture.ContrailFixture):
    """
    Fixture for :class:`.PhysicalInterface`
    """
    def __init__(self, conn_drv, physical_interface_name=None, parent_fixt=None, auto_prop_val=False, id_perms=None, display_name=None):
        '''
        Create PhysicalInterfaceTestFixtureGen object
        
        constructor

        Args:
            conn_drv (:class:`ConnectionDriver`): connection driver (eg. :class:`vnc_api.vnc_api.VncApi`, :class:`novaclient.client.Client`, etc)

        Kwargs:
            physical_interface_name (str): Name of physical_interface
            parent_fixt (:class:`.PhysicalRouterTestFixtureGen`): Parent fixture
            id_perms (instance): instance of :class:`IdPermsType`
            display_name (instance): instance of :class:`xsd:string`

        '''
        super(PhysicalInterfaceTestFixtureGen, self).__init__()
        self._conn_drv = conn_drv
        if not physical_interface_name:
            self._name = 'default-physical-interface'
        else:
            self._name = physical_interface_name
        self._obj = None
        self._parent_fixt = parent_fixt
        self._auto_prop_val = auto_prop_val
        self.id_perms = id_perms
        self.display_name = display_name
    #end __init__

    def _update_links (self, update_server):
        return None
    #end _update_links


    def populate (self):
        self._obj.set_id_perms(self.id_perms or vnc_api.gen.resource_xsd.IdPermsType.populate())
        self._obj.set_display_name(self.display_name or GeneratedsSuper.populate_string("display_name"))
    #end populate

    def setUp(self):
        super(PhysicalInterfaceTestFixtureGen, self).setUp()
        if not self._parent_fixt:
            self._parent_fixt = self.useFixture(PhysicalRouterTestFixtureGen(self._conn_drv, 'default-physical-router'))

        self._obj = vnc_api.PhysicalInterface(self._name, self._parent_fixt.getObj ())
        try:
            self._obj = self._conn_drv.physical_interface_read (fq_name=self._obj.get_fq_name())
            self._update_links (update_server=True)
        except NoIdError:
            self._update_links (update_server=False)
            if self._auto_prop_val:
                self.populate ()
            else:
                self._obj.id_perms = self.id_perms
                self._obj.display_name = self.display_name
            self._conn_drv.physical_interface_create(self._obj)
            # read back for server allocated values
            self._obj = self._conn_drv.physical_interface_read(id = self._obj.uuid)
    #end setUp

    def cleanUp(self):
        if self._obj.get_logical_interfaces():
            return
        parent_fixt = getattr(self, '_parent_fixt', None)
        if parent_fixt:
            # non config-root child
            parent_obj = self._parent_fixt.getObj()
            # remove child from parent obj
            for child_obj in parent_obj.get_physical_interfaces():
                if type(child_obj) == dict:
                    child_uuid = child_obj['uuid']
                else:
                    child_uuid = child_obj.uuid
                if child_uuid == self._obj.uuid:
                    parent_obj.physical_interfaces.remove(child_obj)
                    break

        self._conn_drv.physical_interface_delete(id = self._obj.uuid)
    #end cleanUp

    def getObj(self):
        return self._obj
    #end getObj

#end class PhysicalInterfaceTestFixtureGen

class AccessControlListTestFixtureGen(cfixture.ContrailFixture):
    """
    Fixture for :class:`.AccessControlList`
    """
    def __init__(self, conn_drv, access_control_list_name=None, parent_fixt=None, auto_prop_val=False, access_control_list_entries=None, id_perms=None, display_name=None):
        '''
        Create AccessControlListTestFixtureGen object
        
        constructor

        Args:
            conn_drv (:class:`ConnectionDriver`): connection driver (eg. :class:`vnc_api.vnc_api.VncApi`, :class:`novaclient.client.Client`, etc)

        Kwargs:
            access_control_list_name (str): Name of access_control_list
            parent_fixt (:class:`.VirtualNetworkTestFixtureGen`): Parent fixture
            access_control_list_entries (instance): instance of :class:`AclEntriesType`
            id_perms (instance): instance of :class:`IdPermsType`
            display_name (instance): instance of :class:`xsd:string`

        '''
        super(AccessControlListTestFixtureGen, self).__init__()
        self._conn_drv = conn_drv
        if not access_control_list_name:
            self._name = 'default-access-control-list'
        else:
            self._name = access_control_list_name
        self._obj = None
        self._parent_fixt = parent_fixt
        self._auto_prop_val = auto_prop_val
        self.access_control_list_entries = access_control_list_entries
        self.id_perms = id_perms
        self.display_name = display_name
    #end __init__

    def _update_links (self, update_server):
        return None
    #end _update_links


    def populate (self):
        self._obj.set_access_control_list_entries(self.access_control_list_entries or vnc_api.gen.resource_xsd.AclEntriesType.populate())
        self._obj.set_id_perms(self.id_perms or vnc_api.gen.resource_xsd.IdPermsType.populate())
        self._obj.set_display_name(self.display_name or GeneratedsSuper.populate_string("display_name"))
    #end populate

    def setUp(self):
        super(AccessControlListTestFixtureGen, self).setUp()
        if not self._parent_fixt:
            raise AmbiguousParentError("[[u'default-domain', u'default-project', u'default-virtual-network'], [u'default-domain', u'default-project', u'default-security-group']]")

        self._obj = vnc_api.AccessControlList(self._name, self._parent_fixt.getObj ())
        try:
            self._obj = self._conn_drv.access_control_list_read (fq_name=self._obj.get_fq_name())
            self._update_links (update_server=True)
        except NoIdError:
            self._update_links (update_server=False)
            if self._auto_prop_val:
                self.populate ()
            else:
                self._obj.access_control_list_entries = self.access_control_list_entries
                self._obj.id_perms = self.id_perms
                self._obj.display_name = self.display_name
            self._conn_drv.access_control_list_create(self._obj)
            # read back for server allocated values
            self._obj = self._conn_drv.access_control_list_read(id = self._obj.uuid)
    #end setUp

    def cleanUp(self):
        parent_fixt = getattr(self, '_parent_fixt', None)
        if parent_fixt:
            # non config-root child
            parent_obj = self._parent_fixt.getObj()
            # remove child from parent obj
            for child_obj in parent_obj.get_access_control_lists():
                if type(child_obj) == dict:
                    child_uuid = child_obj['uuid']
                else:
                    child_uuid = child_obj.uuid
                if child_uuid == self._obj.uuid:
                    parent_obj.access_control_lists.remove(child_obj)
                    break

        self._conn_drv.access_control_list_delete(id = self._obj.uuid)
    #end cleanUp

    def getObj(self):
        return self._obj
    #end getObj

#end class AccessControlListTestFixtureGen

class AnalyticsNodeTestFixtureGen(cfixture.ContrailFixture):
    """
    Fixture for :class:`.AnalyticsNode`
    """
    def __init__(self, conn_drv, analytics_node_name=None, parent_fixt=None, auto_prop_val=False, analytics_node_ip_address=None, id_perms=None, display_name=None):
        '''
        Create AnalyticsNodeTestFixtureGen object
        
        constructor

        Args:
            conn_drv (:class:`ConnectionDriver`): connection driver (eg. :class:`vnc_api.vnc_api.VncApi`, :class:`novaclient.client.Client`, etc)

        Kwargs:
            analytics_node_name (str): Name of analytics_node
            parent_fixt (:class:`.GlobalSystemConfigTestFixtureGen`): Parent fixture
            analytics_node_ip_address (instance): instance of :class:`xsd:string`
            id_perms (instance): instance of :class:`IdPermsType`
            display_name (instance): instance of :class:`xsd:string`

        '''
        super(AnalyticsNodeTestFixtureGen, self).__init__()
        self._conn_drv = conn_drv
        if not analytics_node_name:
            self._name = 'default-analytics-node'
        else:
            self._name = analytics_node_name
        self._obj = None
        self._parent_fixt = parent_fixt
        self._auto_prop_val = auto_prop_val
        self.analytics_node_ip_address = analytics_node_ip_address
        self.id_perms = id_perms
        self.display_name = display_name
    #end __init__

    def _update_links (self, update_server):
        return None
    #end _update_links


    def populate (self):
        self._obj.set_analytics_node_ip_address(self.analytics_node_ip_address or GeneratedsSuper.populate_string("analytics_node_ip_address"))
        self._obj.set_id_perms(self.id_perms or vnc_api.gen.resource_xsd.IdPermsType.populate())
        self._obj.set_display_name(self.display_name or GeneratedsSuper.populate_string("display_name"))
    #end populate

    def setUp(self):
        super(AnalyticsNodeTestFixtureGen, self).setUp()
        if not self._parent_fixt:
            self._parent_fixt = self.useFixture(GlobalSystemConfigTestFixtureGen(self._conn_drv, 'default-global-system-config'))

        self._obj = vnc_api.AnalyticsNode(self._name, self._parent_fixt.getObj ())
        try:
            self._obj = self._conn_drv.analytics_node_read (fq_name=self._obj.get_fq_name())
            self._update_links (update_server=True)
        except NoIdError:
            self._update_links (update_server=False)
            if self._auto_prop_val:
                self.populate ()
            else:
                self._obj.analytics_node_ip_address = self.analytics_node_ip_address
                self._obj.id_perms = self.id_perms
                self._obj.display_name = self.display_name
            self._conn_drv.analytics_node_create(self._obj)
            # read back for server allocated values
            self._obj = self._conn_drv.analytics_node_read(id = self._obj.uuid)
    #end setUp

    def cleanUp(self):
        parent_fixt = getattr(self, '_parent_fixt', None)
        if parent_fixt:
            # non config-root child
            parent_obj = self._parent_fixt.getObj()
            # remove child from parent obj
            for child_obj in parent_obj.get_analytics_nodes():
                if type(child_obj) == dict:
                    child_uuid = child_obj['uuid']
                else:
                    child_uuid = child_obj.uuid
                if child_uuid == self._obj.uuid:
                    parent_obj.analytics_nodes.remove(child_obj)
                    break

        self._conn_drv.analytics_node_delete(id = self._obj.uuid)
    #end cleanUp

    def getObj(self):
        return self._obj
    #end getObj

#end class AnalyticsNodeTestFixtureGen

class VirtualDnsTestFixtureGen(cfixture.ContrailFixture):
    """
    Fixture for :class:`.VirtualDns`
    """
    def __init__(self, conn_drv, virtual_DNS_name=None, parent_fixt=None, auto_prop_val=False, virtual_DNS_data=None, id_perms=None, display_name=None):
        '''
        Create VirtualDnsTestFixtureGen object
        
        constructor

        Args:
            conn_drv (:class:`ConnectionDriver`): connection driver (eg. :class:`vnc_api.vnc_api.VncApi`, :class:`novaclient.client.Client`, etc)

        Kwargs:
            virtual_DNS_name (str): Name of virtual_DNS
            parent_fixt (:class:`.DomainTestFixtureGen`): Parent fixture
            virtual_DNS_data (instance): instance of :class:`VirtualDnsType`
            id_perms (instance): instance of :class:`IdPermsType`
            display_name (instance): instance of :class:`xsd:string`

        '''
        super(VirtualDnsTestFixtureGen, self).__init__()
        self._conn_drv = conn_drv
        if not virtual_DNS_name:
            self._name = 'default-virtual-DNS'
        else:
            self._name = virtual_DNS_name
        self._obj = None
        self._parent_fixt = parent_fixt
        self._auto_prop_val = auto_prop_val
        self.virtual_DNS_data = virtual_DNS_data
        self.id_perms = id_perms
        self.display_name = display_name
    #end __init__

    def _update_links (self, update_server):
        return None
    #end _update_links


    def populate (self):
        self._obj.set_virtual_DNS_data(self.virtual_DNS_data or vnc_api.gen.resource_xsd.VirtualDnsType.populate())
        self._obj.set_id_perms(self.id_perms or vnc_api.gen.resource_xsd.IdPermsType.populate())
        self._obj.set_display_name(self.display_name or GeneratedsSuper.populate_string("display_name"))
    #end populate

    def setUp(self):
        super(VirtualDnsTestFixtureGen, self).setUp()
        if not self._parent_fixt:
            self._parent_fixt = self.useFixture(DomainTestFixtureGen(self._conn_drv, 'default-domain'))

        self._obj = vnc_api.VirtualDns(self._name, self._parent_fixt.getObj ())
        try:
            self._obj = self._conn_drv.virtual_DNS_read (fq_name=self._obj.get_fq_name())
            self._update_links (update_server=True)
        except NoIdError:
            self._update_links (update_server=False)
            if self._auto_prop_val:
                self.populate ()
            else:
                self._obj.virtual_DNS_data = self.virtual_DNS_data
                self._obj.id_perms = self.id_perms
                self._obj.display_name = self.display_name
            self._conn_drv.virtual_DNS_create(self._obj)
            # read back for server allocated values
            self._obj = self._conn_drv.virtual_DNS_read(id = self._obj.uuid)
    #end setUp

    def cleanUp(self):
        if self._obj.get_network_ipam_back_refs():
            return
        if self._obj.get_virtual_DNS_records():
            return
        parent_fixt = getattr(self, '_parent_fixt', None)
        if parent_fixt:
            # non config-root child
            parent_obj = self._parent_fixt.getObj()
            # remove child from parent obj
            for child_obj in parent_obj.get_virtual_DNSs():
                if type(child_obj) == dict:
                    child_uuid = child_obj['uuid']
                else:
                    child_uuid = child_obj.uuid
                if child_uuid == self._obj.uuid:
                    parent_obj.virtual_DNSs.remove(child_obj)
                    break

        self._conn_drv.virtual_DNS_delete(id = self._obj.uuid)
    #end cleanUp

    def getObj(self):
        return self._obj
    #end getObj

#end class VirtualDnsTestFixtureGen

class CustomerAttachmentTestFixtureGen(cfixture.ContrailFixture):
    """
    Fixture for :class:`.CustomerAttachment`
    """
    def __init__(self, conn_drv, customer_attachment_name=None, auto_prop_val=False, virtual_machine_interface_refs = None, floating_ip_refs = None, attachment_address=None, id_perms=None, display_name=None):
        '''
        Create CustomerAttachmentTestFixtureGen object
        
        constructor

        Args:
            conn_drv (:class:`ConnectionDriver`): connection driver (eg. :class:`vnc_api.vnc_api.VncApi`, :class:`novaclient.client.Client`, etc)

        Kwargs:
            %s_name (str): Name of %s
            virtual_machine_interface (list): list of :class:`VirtualMachineInterface` type
            floating_ip (list): list of :class:`FloatingIp` type
            attachment_address (instance): instance of :class:`AttachmentAddressType`
            id_perms (instance): instance of :class:`IdPermsType`
            display_name (instance): instance of :class:`xsd:string`

        '''
        super(CustomerAttachmentTestFixtureGen, self).__init__()
        self._conn_drv = conn_drv
        if not customer_attachment_name:
            self._name = 'default-customer-attachment'
        else:
            self._name = customer_attachment_name
        self._obj = None
        self._auto_prop_val = auto_prop_val
        if virtual_machine_interface_refs:
            for ln in virtual_machine_interface_refs:
                self.add_virtual_machine_interface (ln)
        if floating_ip_refs:
            for ln in floating_ip_refs:
                self.add_floating_ip (ln)
        self.attachment_address = attachment_address
        self.id_perms = id_perms
        self.display_name = display_name
    #end __init__

    def _update_links (self, update_server):
        for ln in self.get_virtual_machine_interfaces ():
            self.add_virtual_machine_interface (ln.fixture (), update_server = update_server, add_link = False)
        for ln in self.get_floating_ips ():
            self.add_floating_ip (ln.fixture (), update_server = update_server, add_link = False)
        return None
    #end _update_links

    def add_virtual_machine_interface (self, lo, update_server = True, add_link = True):
        '''
        add :class:`VirtualMachineInterface` link to :class:`CustomerAttachment`
        Args:
            lo (:class:`VirtualMachineInterface`): obj to link
        '''
        if self._obj:
            self._obj.add_virtual_machine_interface (lo)
            if update_server:
                self._conn_drv.customer_attachment_update (self._obj)

        if add_link:
            self.add_link('virtual_machine_interface', cfixture.ConrtailLink('virtual_machine_interface', 'customer_attachment', 'virtual_machine_interface', ['ref'], lo))
    #end add_virtual_machine_interface_link

    def get_virtual_machine_interfaces (self):
        return self.get_links ('virtual_machine_interface')
    #end get_virtual_machine_interfaces
    def add_floating_ip (self, lo, update_server = True, add_link = True):
        '''
        add :class:`FloatingIp` link to :class:`CustomerAttachment`
        Args:
            lo (:class:`FloatingIp`): obj to link
        '''
        if self._obj:
            self._obj.add_floating_ip (lo)
            if update_server:
                self._conn_drv.customer_attachment_update (self._obj)

        if add_link:
            self.add_link('floating_ip', cfixture.ConrtailLink('floating_ip', 'customer_attachment', 'floating_ip', ['ref'], lo))
    #end add_floating_ip_link

    def get_floating_ips (self):
        return self.get_links ('floating_ip')
    #end get_floating_ips

    def populate (self):
        self._obj.set_attachment_address(self.attachment_address or vnc_api.gen.resource_xsd.AttachmentAddressType.populate())
        self._obj.set_id_perms(self.id_perms or vnc_api.gen.resource_xsd.IdPermsType.populate())
        self._obj.set_display_name(self.display_name or GeneratedsSuper.populate_string("display_name"))
    #end populate

    def setUp(self):
        super(CustomerAttachmentTestFixtureGen, self).setUp()
        self._obj = vnc_api.CustomerAttachment(self._name)
        try:
            self._obj = self._conn_drv.customer_attachment_read (fq_name=self._obj.get_fq_name())
            self._update_links (update_server=True)
        except NoIdError:
            self._update_links (update_server=False)
            if self._auto_prop_val:
                self.populate ()
            else:
                self._obj.attachment_address = self.attachment_address
                self._obj.id_perms = self.id_perms
                self._obj.display_name = self.display_name
            self._conn_drv.customer_attachment_create(self._obj)
            # read back for server allocated values
            self._obj = self._conn_drv.customer_attachment_read(id = self._obj.uuid)
    #end setUp

    def cleanUp(self):
        self._conn_drv.customer_attachment_delete(id = self._obj.uuid)
    #end cleanUp

    def getObj(self):
        return self._obj
    #end getObj

#end class CustomerAttachmentTestFixtureGen

class ServiceApplianceSetTestFixtureGen(cfixture.ContrailFixture):
    """
    Fixture for :class:`.ServiceApplianceSet`
    """
    def __init__(self, conn_drv, service_appliance_set_name=None, parent_fixt=None, auto_prop_val=False, service_appliance_set_properties=None, service_appliance_driver=None, service_appliance_ha_mode=None, id_perms=None, display_name=None):
        '''
        Create ServiceApplianceSetTestFixtureGen object
        
        constructor

        Args:
            conn_drv (:class:`ConnectionDriver`): connection driver (eg. :class:`vnc_api.vnc_api.VncApi`, :class:`novaclient.client.Client`, etc)

        Kwargs:
            service_appliance_set_name (str): Name of service_appliance_set
            parent_fixt (:class:`.GlobalSystemConfigTestFixtureGen`): Parent fixture
            service_appliance_set_properties (instance): instance of :class:`KeyValuePairs`
            service_appliance_driver (instance): instance of :class:`xsd:string`
            service_appliance_ha_mode (instance): instance of :class:`xsd:string`
            id_perms (instance): instance of :class:`IdPermsType`
            display_name (instance): instance of :class:`xsd:string`

        '''
        super(ServiceApplianceSetTestFixtureGen, self).__init__()
        self._conn_drv = conn_drv
        if not service_appliance_set_name:
            self._name = 'default-service-appliance-set'
        else:
            self._name = service_appliance_set_name
        self._obj = None
        self._parent_fixt = parent_fixt
        self._auto_prop_val = auto_prop_val
        self.service_appliance_set_properties = service_appliance_set_properties
        self.service_appliance_driver = service_appliance_driver
        self.service_appliance_ha_mode = service_appliance_ha_mode
        self.id_perms = id_perms
        self.display_name = display_name
    #end __init__

    def _update_links (self, update_server):
        return None
    #end _update_links


    def populate (self):
        self._obj.set_service_appliance_set_properties(self.service_appliance_set_properties or vnc_api.gen.resource_xsd.KeyValuePairs.populate())
        self._obj.set_service_appliance_driver(self.service_appliance_driver or GeneratedsSuper.populate_string("service_appliance_driver"))
        self._obj.set_service_appliance_ha_mode(self.service_appliance_ha_mode or GeneratedsSuper.populate_string("service_appliance_ha_mode"))
        self._obj.set_id_perms(self.id_perms or vnc_api.gen.resource_xsd.IdPermsType.populate())
        self._obj.set_display_name(self.display_name or GeneratedsSuper.populate_string("display_name"))
    #end populate

    def setUp(self):
        super(ServiceApplianceSetTestFixtureGen, self).setUp()
        if not self._parent_fixt:
            self._parent_fixt = self.useFixture(GlobalSystemConfigTestFixtureGen(self._conn_drv, 'default-global-system-config'))

        self._obj = vnc_api.ServiceApplianceSet(self._name, self._parent_fixt.getObj ())
        try:
            self._obj = self._conn_drv.service_appliance_set_read (fq_name=self._obj.get_fq_name())
            self._update_links (update_server=True)
        except NoIdError:
            self._update_links (update_server=False)
            if self._auto_prop_val:
                self.populate ()
            else:
                self._obj.service_appliance_set_properties = self.service_appliance_set_properties
                self._obj.service_appliance_driver = self.service_appliance_driver
                self._obj.service_appliance_ha_mode = self.service_appliance_ha_mode
                self._obj.id_perms = self.id_perms
                self._obj.display_name = self.display_name
            self._conn_drv.service_appliance_set_create(self._obj)
            # read back for server allocated values
            self._obj = self._conn_drv.service_appliance_set_read(id = self._obj.uuid)
    #end setUp

    def cleanUp(self):
        if self._obj.get_loadbalancer_pool_back_refs():
            return
        if self._obj.get_service_appliances():
            return
        parent_fixt = getattr(self, '_parent_fixt', None)
        if parent_fixt:
            # non config-root child
            parent_obj = self._parent_fixt.getObj()
            # remove child from parent obj
            for child_obj in parent_obj.get_service_appliance_sets():
                if type(child_obj) == dict:
                    child_uuid = child_obj['uuid']
                else:
                    child_uuid = child_obj.uuid
                if child_uuid == self._obj.uuid:
                    parent_obj.service_appliance_sets.remove(child_obj)
                    break

        self._conn_drv.service_appliance_set_delete(id = self._obj.uuid)
    #end cleanUp

    def getObj(self):
        return self._obj
    #end getObj

#end class ServiceApplianceSetTestFixtureGen

class ConfigNodeTestFixtureGen(cfixture.ContrailFixture):
    """
    Fixture for :class:`.ConfigNode`
    """
    def __init__(self, conn_drv, config_node_name=None, parent_fixt=None, auto_prop_val=False, config_node_ip_address=None, id_perms=None, display_name=None):
        '''
        Create ConfigNodeTestFixtureGen object
        
        constructor

        Args:
            conn_drv (:class:`ConnectionDriver`): connection driver (eg. :class:`vnc_api.vnc_api.VncApi`, :class:`novaclient.client.Client`, etc)

        Kwargs:
            config_node_name (str): Name of config_node
            parent_fixt (:class:`.GlobalSystemConfigTestFixtureGen`): Parent fixture
            config_node_ip_address (instance): instance of :class:`xsd:string`
            id_perms (instance): instance of :class:`IdPermsType`
            display_name (instance): instance of :class:`xsd:string`

        '''
        super(ConfigNodeTestFixtureGen, self).__init__()
        self._conn_drv = conn_drv
        if not config_node_name:
            self._name = 'default-config-node'
        else:
            self._name = config_node_name
        self._obj = None
        self._parent_fixt = parent_fixt
        self._auto_prop_val = auto_prop_val
        self.config_node_ip_address = config_node_ip_address
        self.id_perms = id_perms
        self.display_name = display_name
    #end __init__

    def _update_links (self, update_server):
        return None
    #end _update_links


    def populate (self):
        self._obj.set_config_node_ip_address(self.config_node_ip_address or GeneratedsSuper.populate_string("config_node_ip_address"))
        self._obj.set_id_perms(self.id_perms or vnc_api.gen.resource_xsd.IdPermsType.populate())
        self._obj.set_display_name(self.display_name or GeneratedsSuper.populate_string("display_name"))
    #end populate

    def setUp(self):
        super(ConfigNodeTestFixtureGen, self).setUp()
        if not self._parent_fixt:
            self._parent_fixt = self.useFixture(GlobalSystemConfigTestFixtureGen(self._conn_drv, 'default-global-system-config'))

        self._obj = vnc_api.ConfigNode(self._name, self._parent_fixt.getObj ())
        try:
            self._obj = self._conn_drv.config_node_read (fq_name=self._obj.get_fq_name())
            self._update_links (update_server=True)
        except NoIdError:
            self._update_links (update_server=False)
            if self._auto_prop_val:
                self.populate ()
            else:
                self._obj.config_node_ip_address = self.config_node_ip_address
                self._obj.id_perms = self.id_perms
                self._obj.display_name = self.display_name
            self._conn_drv.config_node_create(self._obj)
            # read back for server allocated values
            self._obj = self._conn_drv.config_node_read(id = self._obj.uuid)
    #end setUp

    def cleanUp(self):
        parent_fixt = getattr(self, '_parent_fixt', None)
        if parent_fixt:
            # non config-root child
            parent_obj = self._parent_fixt.getObj()
            # remove child from parent obj
            for child_obj in parent_obj.get_config_nodes():
                if type(child_obj) == dict:
                    child_uuid = child_obj['uuid']
                else:
                    child_uuid = child_obj.uuid
                if child_uuid == self._obj.uuid:
                    parent_obj.config_nodes.remove(child_obj)
                    break

        self._conn_drv.config_node_delete(id = self._obj.uuid)
    #end cleanUp

    def getObj(self):
        return self._obj
    #end getObj

#end class ConfigNodeTestFixtureGen

class QosQueueTestFixtureGen(cfixture.ContrailFixture):
    """
    Fixture for :class:`.QosQueue`
    """
    def __init__(self, conn_drv, qos_queue_name=None, parent_fixt=None, auto_prop_val=False, min_bandwidth=None, max_bandwidth=None, id_perms=None, display_name=None):
        '''
        Create QosQueueTestFixtureGen object
        
        constructor

        Args:
            conn_drv (:class:`ConnectionDriver`): connection driver (eg. :class:`vnc_api.vnc_api.VncApi`, :class:`novaclient.client.Client`, etc)

        Kwargs:
            qos_queue_name (str): Name of qos_queue
            parent_fixt (:class:`.ProjectTestFixtureGen`): Parent fixture
            min_bandwidth (instance): instance of :class:`xsd:integer`
            max_bandwidth (instance): instance of :class:`xsd:integer`
            id_perms (instance): instance of :class:`IdPermsType`
            display_name (instance): instance of :class:`xsd:string`

        '''
        super(QosQueueTestFixtureGen, self).__init__()
        self._conn_drv = conn_drv
        if not qos_queue_name:
            self._name = 'default-qos-queue'
        else:
            self._name = qos_queue_name
        self._obj = None
        self._parent_fixt = parent_fixt
        self._auto_prop_val = auto_prop_val
        self.min_bandwidth = min_bandwidth
        self.max_bandwidth = max_bandwidth
        self.id_perms = id_perms
        self.display_name = display_name
    #end __init__

    def _update_links (self, update_server):
        return None
    #end _update_links


    def populate (self):
        self._obj.set_min_bandwidth(self.min_bandwidth or GeneratedsSuper.populate_integer("min_bandwidth"))
        self._obj.set_max_bandwidth(self.max_bandwidth or GeneratedsSuper.populate_integer("max_bandwidth"))
        self._obj.set_id_perms(self.id_perms or vnc_api.gen.resource_xsd.IdPermsType.populate())
        self._obj.set_display_name(self.display_name or GeneratedsSuper.populate_string("display_name"))
    #end populate

    def setUp(self):
        super(QosQueueTestFixtureGen, self).setUp()
        if not self._parent_fixt:
            self._parent_fixt = self.useFixture(ProjectTestFixtureGen(self._conn_drv, 'default-project'))

        self._obj = vnc_api.QosQueue(self._name, self._parent_fixt.getObj ())
        try:
            self._obj = self._conn_drv.qos_queue_read (fq_name=self._obj.get_fq_name())
            self._update_links (update_server=True)
        except NoIdError:
            self._update_links (update_server=False)
            if self._auto_prop_val:
                self.populate ()
            else:
                self._obj.min_bandwidth = self.min_bandwidth
                self._obj.max_bandwidth = self.max_bandwidth
                self._obj.id_perms = self.id_perms
                self._obj.display_name = self.display_name
            self._conn_drv.qos_queue_create(self._obj)
            # read back for server allocated values
            self._obj = self._conn_drv.qos_queue_read(id = self._obj.uuid)
    #end setUp

    def cleanUp(self):
        if self._obj.get_qos_forwarding_class_back_refs():
            return
        parent_fixt = getattr(self, '_parent_fixt', None)
        if parent_fixt:
            # non config-root child
            parent_obj = self._parent_fixt.getObj()
            # remove child from parent obj
            for child_obj in parent_obj.get_qos_queues():
                if type(child_obj) == dict:
                    child_uuid = child_obj['uuid']
                else:
                    child_uuid = child_obj.uuid
                if child_uuid == self._obj.uuid:
                    parent_obj.qos_queues.remove(child_obj)
                    break

        self._conn_drv.qos_queue_delete(id = self._obj.uuid)
    #end cleanUp

    def getObj(self):
        return self._obj
    #end getObj

#end class QosQueueTestFixtureGen

class VirtualMachineTestFixtureGen(cfixture.ContrailFixture):
    """
    Fixture for :class:`.VirtualMachine`
    """
    def __init__(self, conn_drv, virtual_machine_name=None, auto_prop_val=False, service_instance_refs = None, id_perms=None, display_name=None):
        '''
        Create VirtualMachineTestFixtureGen object
        
        constructor

        Args:
            conn_drv (:class:`ConnectionDriver`): connection driver (eg. :class:`vnc_api.vnc_api.VncApi`, :class:`novaclient.client.Client`, etc)

        Kwargs:
            %s_name (str): Name of %s
            service_instance (list): list of :class:`ServiceInstance` type
            id_perms (instance): instance of :class:`IdPermsType`
            display_name (instance): instance of :class:`xsd:string`

        '''
        super(VirtualMachineTestFixtureGen, self).__init__()
        self._conn_drv = conn_drv
        if not virtual_machine_name:
            self._name = 'default-virtual-machine'
        else:
            self._name = virtual_machine_name
        self._obj = None
        self._auto_prop_val = auto_prop_val
        if service_instance_refs:
            for ln in service_instance_refs:
                self.add_service_instance (ln)
        self.id_perms = id_perms
        self.display_name = display_name
    #end __init__

    def _update_links (self, update_server):
        for ln in self.get_service_instances ():
            self.add_service_instance (ln.fixture (), update_server = update_server, add_link = False)
        return None
    #end _update_links

    def add_service_instance (self, lo, update_server = True, add_link = True):
        '''
        add :class:`ServiceInstance` link to :class:`VirtualMachine`
        Args:
            lo (:class:`ServiceInstance`): obj to link
        '''
        if self._obj:
            self._obj.add_service_instance (lo)
            if update_server:
                self._conn_drv.virtual_machine_update (self._obj)

        if add_link:
            self.add_link('service_instance', cfixture.ConrtailLink('service_instance', 'virtual_machine', 'service_instance', ['ref', 'derived'], lo))
    #end add_service_instance_link

    def get_service_instances (self):
        return self.get_links ('service_instance')
    #end get_service_instances

    def populate (self):
        self._obj.set_id_perms(self.id_perms or vnc_api.gen.resource_xsd.IdPermsType.populate())
        self._obj.set_display_name(self.display_name or GeneratedsSuper.populate_string("display_name"))
    #end populate

    def setUp(self):
        super(VirtualMachineTestFixtureGen, self).setUp()
        self._obj = vnc_api.VirtualMachine(self._name)
        try:
            self._obj = self._conn_drv.virtual_machine_read (fq_name=self._obj.get_fq_name())
            self._update_links (update_server=True)
        except NoIdError:
            self._update_links (update_server=False)
            if self._auto_prop_val:
                self.populate ()
            else:
                self._obj.id_perms = self.id_perms
                self._obj.display_name = self.display_name
            self._conn_drv.virtual_machine_create(self._obj)
            # read back for server allocated values
            self._obj = self._conn_drv.virtual_machine_read(id = self._obj.uuid)
    #end setUp

    def cleanUp(self):
        if self._obj.get_virtual_machine_interface_back_refs() or self._obj.get_virtual_router_back_refs():
            return
        if self._obj.get_virtual_machine_interfaces():
            return
        self._conn_drv.virtual_machine_delete(id = self._obj.uuid)
    #end cleanUp

    def getObj(self):
        return self._obj
    #end getObj

#end class VirtualMachineTestFixtureGen

class InterfaceRouteTableTestFixtureGen(cfixture.ContrailFixture):
    """
    Fixture for :class:`.InterfaceRouteTable`
    """
    def __init__(self, conn_drv, interface_route_table_name=None, parent_fixt=None, auto_prop_val=False, interface_route_table_routes=None, id_perms=None, display_name=None):
        '''
        Create InterfaceRouteTableTestFixtureGen object
        
        constructor

        Args:
            conn_drv (:class:`ConnectionDriver`): connection driver (eg. :class:`vnc_api.vnc_api.VncApi`, :class:`novaclient.client.Client`, etc)

        Kwargs:
            interface_route_table_name (str): Name of interface_route_table
            parent_fixt (:class:`.ProjectTestFixtureGen`): Parent fixture
            interface_route_table_routes (instance): instance of :class:`RouteTableType`
            id_perms (instance): instance of :class:`IdPermsType`
            display_name (instance): instance of :class:`xsd:string`

        '''
        super(InterfaceRouteTableTestFixtureGen, self).__init__()
        self._conn_drv = conn_drv
        if not interface_route_table_name:
            self._name = 'default-interface-route-table'
        else:
            self._name = interface_route_table_name
        self._obj = None
        self._parent_fixt = parent_fixt
        self._auto_prop_val = auto_prop_val
        self.interface_route_table_routes = interface_route_table_routes
        self.id_perms = id_perms
        self.display_name = display_name
    #end __init__

    def _update_links (self, update_server):
        return None
    #end _update_links


    def populate (self):
        self._obj.set_interface_route_table_routes(self.interface_route_table_routes or vnc_api.gen.resource_xsd.RouteTableType.populate())
        self._obj.set_id_perms(self.id_perms or vnc_api.gen.resource_xsd.IdPermsType.populate())
        self._obj.set_display_name(self.display_name or GeneratedsSuper.populate_string("display_name"))
    #end populate

    def setUp(self):
        super(InterfaceRouteTableTestFixtureGen, self).setUp()
        if not self._parent_fixt:
            self._parent_fixt = self.useFixture(ProjectTestFixtureGen(self._conn_drv, 'default-project'))

        self._obj = vnc_api.InterfaceRouteTable(self._name, self._parent_fixt.getObj ())
        try:
            self._obj = self._conn_drv.interface_route_table_read (fq_name=self._obj.get_fq_name())
            self._update_links (update_server=True)
        except NoIdError:
            self._update_links (update_server=False)
            if self._auto_prop_val:
                self.populate ()
            else:
                self._obj.interface_route_table_routes = self.interface_route_table_routes
                self._obj.id_perms = self.id_perms
                self._obj.display_name = self.display_name
            self._conn_drv.interface_route_table_create(self._obj)
            # read back for server allocated values
            self._obj = self._conn_drv.interface_route_table_read(id = self._obj.uuid)
    #end setUp

    def cleanUp(self):
        if self._obj.get_virtual_machine_interface_back_refs():
            return
        parent_fixt = getattr(self, '_parent_fixt', None)
        if parent_fixt:
            # non config-root child
            parent_obj = self._parent_fixt.getObj()
            # remove child from parent obj
            for child_obj in parent_obj.get_interface_route_tables():
                if type(child_obj) == dict:
                    child_uuid = child_obj['uuid']
                else:
                    child_uuid = child_obj.uuid
                if child_uuid == self._obj.uuid:
                    parent_obj.interface_route_tables.remove(child_obj)
                    break

        self._conn_drv.interface_route_table_delete(id = self._obj.uuid)
    #end cleanUp

    def getObj(self):
        return self._obj
    #end getObj

#end class InterfaceRouteTableTestFixtureGen

class ServiceTemplateTestFixtureGen(cfixture.ContrailFixture):
    """
    Fixture for :class:`.ServiceTemplate`
    """
    def __init__(self, conn_drv, service_template_name=None, parent_fixt=None, auto_prop_val=False, service_template_properties=None, id_perms=None, display_name=None):
        '''
        Create ServiceTemplateTestFixtureGen object
        
        constructor

        Args:
            conn_drv (:class:`ConnectionDriver`): connection driver (eg. :class:`vnc_api.vnc_api.VncApi`, :class:`novaclient.client.Client`, etc)

        Kwargs:
            service_template_name (str): Name of service_template
            parent_fixt (:class:`.DomainTestFixtureGen`): Parent fixture
            service_template_properties (instance): instance of :class:`ServiceTemplateType`
            id_perms (instance): instance of :class:`IdPermsType`
            display_name (instance): instance of :class:`xsd:string`

        '''
        super(ServiceTemplateTestFixtureGen, self).__init__()
        self._conn_drv = conn_drv
        if not service_template_name:
            self._name = 'default-service-template'
        else:
            self._name = service_template_name
        self._obj = None
        self._parent_fixt = parent_fixt
        self._auto_prop_val = auto_prop_val
        self.service_template_properties = service_template_properties
        self.id_perms = id_perms
        self.display_name = display_name
    #end __init__

    def _update_links (self, update_server):
        return None
    #end _update_links


    def populate (self):
        self._obj.set_service_template_properties(self.service_template_properties or vnc_api.gen.resource_xsd.ServiceTemplateType.populate())
        self._obj.set_id_perms(self.id_perms or vnc_api.gen.resource_xsd.IdPermsType.populate())
        self._obj.set_display_name(self.display_name or GeneratedsSuper.populate_string("display_name"))
    #end populate

    def setUp(self):
        super(ServiceTemplateTestFixtureGen, self).setUp()
        if not self._parent_fixt:
            self._parent_fixt = self.useFixture(DomainTestFixtureGen(self._conn_drv, 'default-domain'))

        self._obj = vnc_api.ServiceTemplate(self._name, self._parent_fixt.getObj ())
        try:
            self._obj = self._conn_drv.service_template_read (fq_name=self._obj.get_fq_name())
            self._update_links (update_server=True)
        except NoIdError:
            self._update_links (update_server=False)
            if self._auto_prop_val:
                self.populate ()
            else:
                self._obj.service_template_properties = self.service_template_properties
                self._obj.id_perms = self.id_perms
                self._obj.display_name = self.display_name
            self._conn_drv.service_template_create(self._obj)
            # read back for server allocated values
            self._obj = self._conn_drv.service_template_read(id = self._obj.uuid)
    #end setUp

    def cleanUp(self):
        if self._obj.get_service_instance_back_refs():
            return
        parent_fixt = getattr(self, '_parent_fixt', None)
        if parent_fixt:
            # non config-root child
            parent_obj = self._parent_fixt.getObj()
            # remove child from parent obj
            for child_obj in parent_obj.get_service_templates():
                if type(child_obj) == dict:
                    child_uuid = child_obj['uuid']
                else:
                    child_uuid = child_obj.uuid
                if child_uuid == self._obj.uuid:
                    parent_obj.service_templates.remove(child_obj)
                    break

        self._conn_drv.service_template_delete(id = self._obj.uuid)
    #end cleanUp

    def getObj(self):
        return self._obj
    #end getObj

#end class ServiceTemplateTestFixtureGen

class VirtualIpTestFixtureGen(cfixture.ContrailFixture):
    """
    Fixture for :class:`.VirtualIp`
    """
    def __init__(self, conn_drv, virtual_ip_name=None, parent_fixt=None, auto_prop_val=False, loadbalancer_pool_refs = None, virtual_machine_interface_refs = None, virtual_ip_properties=None, id_perms=None, display_name=None):
        '''
        Create VirtualIpTestFixtureGen object
        
        constructor

        Args:
            conn_drv (:class:`ConnectionDriver`): connection driver (eg. :class:`vnc_api.vnc_api.VncApi`, :class:`novaclient.client.Client`, etc)

        Kwargs:
            virtual_ip_name (str): Name of virtual_ip
            parent_fixt (:class:`.ProjectTestFixtureGen`): Parent fixture
            loadbalancer_pool (list): list of :class:`LoadbalancerPool` type
            virtual_machine_interface (list): list of :class:`VirtualMachineInterface` type
            virtual_ip_properties (instance): instance of :class:`VirtualIpType`
            id_perms (instance): instance of :class:`IdPermsType`
            display_name (instance): instance of :class:`xsd:string`

        '''
        super(VirtualIpTestFixtureGen, self).__init__()
        self._conn_drv = conn_drv
        if not virtual_ip_name:
            self._name = 'default-virtual-ip'
        else:
            self._name = virtual_ip_name
        self._obj = None
        self._parent_fixt = parent_fixt
        self._auto_prop_val = auto_prop_val
        if loadbalancer_pool_refs:
            for ln in loadbalancer_pool_refs:
                self.add_loadbalancer_pool (ln)
        if virtual_machine_interface_refs:
            for ln in virtual_machine_interface_refs:
                self.add_virtual_machine_interface (ln)
        self.virtual_ip_properties = virtual_ip_properties
        self.id_perms = id_perms
        self.display_name = display_name
    #end __init__

    def _update_links (self, update_server):
        for ln in self.get_loadbalancer_pools ():
            self.add_loadbalancer_pool (ln.fixture (), update_server = update_server, add_link = False)
        for ln in self.get_virtual_machine_interfaces ():
            self.add_virtual_machine_interface (ln.fixture (), update_server = update_server, add_link = False)
        return None
    #end _update_links

    def add_loadbalancer_pool (self, lo, update_server = True, add_link = True):
        '''
        add :class:`LoadbalancerPool` link to :class:`VirtualIp`
        Args:
            lo (:class:`LoadbalancerPool`): obj to link
        '''
        if self._obj:
            self._obj.add_loadbalancer_pool (lo)
            if update_server:
                self._conn_drv.virtual_ip_update (self._obj)

        if add_link:
            self.add_link('loadbalancer_pool', cfixture.ConrtailLink('loadbalancer_pool', 'virtual_ip', 'loadbalancer_pool', ['ref'], lo))
    #end add_loadbalancer_pool_link

    def get_loadbalancer_pools (self):
        return self.get_links ('loadbalancer_pool')
    #end get_loadbalancer_pools
    def add_virtual_machine_interface (self, lo, update_server = True, add_link = True):
        '''
        add :class:`VirtualMachineInterface` link to :class:`VirtualIp`
        Args:
            lo (:class:`VirtualMachineInterface`): obj to link
        '''
        if self._obj:
            self._obj.add_virtual_machine_interface (lo)
            if update_server:
                self._conn_drv.virtual_ip_update (self._obj)

        if add_link:
            self.add_link('virtual_machine_interface', cfixture.ConrtailLink('virtual_machine_interface', 'virtual_ip', 'virtual_machine_interface', ['ref'], lo))
    #end add_virtual_machine_interface_link

    def get_virtual_machine_interfaces (self):
        return self.get_links ('virtual_machine_interface')
    #end get_virtual_machine_interfaces

    def populate (self):
        self._obj.set_virtual_ip_properties(self.virtual_ip_properties or vnc_api.gen.resource_xsd.VirtualIpType.populate())
        self._obj.set_id_perms(self.id_perms or vnc_api.gen.resource_xsd.IdPermsType.populate())
        self._obj.set_display_name(self.display_name or GeneratedsSuper.populate_string("display_name"))
    #end populate

    def setUp(self):
        super(VirtualIpTestFixtureGen, self).setUp()
        if not self._parent_fixt:
            self._parent_fixt = self.useFixture(ProjectTestFixtureGen(self._conn_drv, 'default-project'))

        self._obj = vnc_api.VirtualIp(self._name, self._parent_fixt.getObj ())
        try:
            self._obj = self._conn_drv.virtual_ip_read (fq_name=self._obj.get_fq_name())
            self._update_links (update_server=True)
        except NoIdError:
            self._update_links (update_server=False)
            if self._auto_prop_val:
                self.populate ()
            else:
                self._obj.virtual_ip_properties = self.virtual_ip_properties
                self._obj.id_perms = self.id_perms
                self._obj.display_name = self.display_name
            self._conn_drv.virtual_ip_create(self._obj)
            # read back for server allocated values
            self._obj = self._conn_drv.virtual_ip_read(id = self._obj.uuid)
    #end setUp

    def cleanUp(self):
        parent_fixt = getattr(self, '_parent_fixt', None)
        if parent_fixt:
            # non config-root child
            parent_obj = self._parent_fixt.getObj()
            # remove child from parent obj
            for child_obj in parent_obj.get_virtual_ips():
                if type(child_obj) == dict:
                    child_uuid = child_obj['uuid']
                else:
                    child_uuid = child_obj.uuid
                if child_uuid == self._obj.uuid:
                    parent_obj.virtual_ips.remove(child_obj)
                    break

        self._conn_drv.virtual_ip_delete(id = self._obj.uuid)
    #end cleanUp

    def getObj(self):
        return self._obj
    #end getObj

#end class VirtualIpTestFixtureGen

class LoadbalancerMemberTestFixtureGen(cfixture.ContrailFixture):
    """
    Fixture for :class:`.LoadbalancerMember`
    """
    def __init__(self, conn_drv, loadbalancer_member_name=None, parent_fixt=None, auto_prop_val=False, loadbalancer_member_properties=None, id_perms=None, display_name=None):
        '''
        Create LoadbalancerMemberTestFixtureGen object
        
        constructor

        Args:
            conn_drv (:class:`ConnectionDriver`): connection driver (eg. :class:`vnc_api.vnc_api.VncApi`, :class:`novaclient.client.Client`, etc)

        Kwargs:
            loadbalancer_member_name (str): Name of loadbalancer_member
            parent_fixt (:class:`.LoadbalancerPoolTestFixtureGen`): Parent fixture
            loadbalancer_member_properties (instance): instance of :class:`LoadbalancerMemberType`
            id_perms (instance): instance of :class:`IdPermsType`
            display_name (instance): instance of :class:`xsd:string`

        '''
        super(LoadbalancerMemberTestFixtureGen, self).__init__()
        self._conn_drv = conn_drv
        if not loadbalancer_member_name:
            self._name = 'default-loadbalancer-member'
        else:
            self._name = loadbalancer_member_name
        self._obj = None
        self._parent_fixt = parent_fixt
        self._auto_prop_val = auto_prop_val
        self.loadbalancer_member_properties = loadbalancer_member_properties
        self.id_perms = id_perms
        self.display_name = display_name
    #end __init__

    def _update_links (self, update_server):
        return None
    #end _update_links


    def populate (self):
        self._obj.set_loadbalancer_member_properties(self.loadbalancer_member_properties or vnc_api.gen.resource_xsd.LoadbalancerMemberType.populate())
        self._obj.set_id_perms(self.id_perms or vnc_api.gen.resource_xsd.IdPermsType.populate())
        self._obj.set_display_name(self.display_name or GeneratedsSuper.populate_string("display_name"))
    #end populate

    def setUp(self):
        super(LoadbalancerMemberTestFixtureGen, self).setUp()
        if not self._parent_fixt:
            self._parent_fixt = self.useFixture(LoadbalancerPoolTestFixtureGen(self._conn_drv, 'default-loadbalancer-pool'))

        self._obj = vnc_api.LoadbalancerMember(self._name, self._parent_fixt.getObj ())
        try:
            self._obj = self._conn_drv.loadbalancer_member_read (fq_name=self._obj.get_fq_name())
            self._update_links (update_server=True)
        except NoIdError:
            self._update_links (update_server=False)
            if self._auto_prop_val:
                self.populate ()
            else:
                self._obj.loadbalancer_member_properties = self.loadbalancer_member_properties
                self._obj.id_perms = self.id_perms
                self._obj.display_name = self.display_name
            self._conn_drv.loadbalancer_member_create(self._obj)
            # read back for server allocated values
            self._obj = self._conn_drv.loadbalancer_member_read(id = self._obj.uuid)
    #end setUp

    def cleanUp(self):
        parent_fixt = getattr(self, '_parent_fixt', None)
        if parent_fixt:
            # non config-root child
            parent_obj = self._parent_fixt.getObj()
            # remove child from parent obj
            for child_obj in parent_obj.get_loadbalancer_members():
                if type(child_obj) == dict:
                    child_uuid = child_obj['uuid']
                else:
                    child_uuid = child_obj.uuid
                if child_uuid == self._obj.uuid:
                    parent_obj.loadbalancer_members.remove(child_obj)
                    break

        self._conn_drv.loadbalancer_member_delete(id = self._obj.uuid)
    #end cleanUp

    def getObj(self):
        return self._obj
    #end getObj

#end class LoadbalancerMemberTestFixtureGen

class SecurityGroupTestFixtureGen(cfixture.ContrailFixture):
    """
    Fixture for :class:`.SecurityGroup`
    """
    def __init__(self, conn_drv, security_group_name=None, parent_fixt=None, auto_prop_val=False, security_group_id=None, configured_security_group_id=None, security_group_entries=None, id_perms=None, display_name=None):
        '''
        Create SecurityGroupTestFixtureGen object
        
        constructor

        Args:
            conn_drv (:class:`ConnectionDriver`): connection driver (eg. :class:`vnc_api.vnc_api.VncApi`, :class:`novaclient.client.Client`, etc)

        Kwargs:
            security_group_name (str): Name of security_group
            parent_fixt (:class:`.ProjectTestFixtureGen`): Parent fixture
            security_group_id (instance): instance of :class:`xsd:string`
            configured_security_group_id (instance): instance of :class:`xsd:integer`
            security_group_entries (instance): instance of :class:`PolicyEntriesType`
            id_perms (instance): instance of :class:`IdPermsType`
            display_name (instance): instance of :class:`xsd:string`

        '''
        super(SecurityGroupTestFixtureGen, self).__init__()
        self._conn_drv = conn_drv
        if not security_group_name:
            self._name = 'default-security-group'
        else:
            self._name = security_group_name
        self._obj = None
        self._parent_fixt = parent_fixt
        self._auto_prop_val = auto_prop_val
        self.security_group_id = security_group_id
        self.configured_security_group_id = configured_security_group_id
        self.security_group_entries = security_group_entries
        self.id_perms = id_perms
        self.display_name = display_name
    #end __init__

    def _update_links (self, update_server):
        return None
    #end _update_links


    def populate (self):
        self._obj.set_security_group_id(self.security_group_id or GeneratedsSuper.populate_string("security_group_id"))
        self._obj.set_configured_security_group_id(self.configured_security_group_id or GeneratedsSuper.populate_integer("configured_security_group_id"))
        self._obj.set_security_group_entries(self.security_group_entries or vnc_api.gen.resource_xsd.PolicyEntriesType.populate())
        self._obj.set_id_perms(self.id_perms or vnc_api.gen.resource_xsd.IdPermsType.populate())
        self._obj.set_display_name(self.display_name or GeneratedsSuper.populate_string("display_name"))
    #end populate

    def setUp(self):
        super(SecurityGroupTestFixtureGen, self).setUp()
        if not self._parent_fixt:
            self._parent_fixt = self.useFixture(ProjectTestFixtureGen(self._conn_drv, 'default-project'))

        self._obj = vnc_api.SecurityGroup(self._name, self._parent_fixt.getObj ())
        try:
            self._obj = self._conn_drv.security_group_read (fq_name=self._obj.get_fq_name())
            self._update_links (update_server=True)
        except NoIdError:
            self._update_links (update_server=False)
            if self._auto_prop_val:
                self.populate ()
            else:
                self._obj.security_group_id = self.security_group_id
                self._obj.configured_security_group_id = self.configured_security_group_id
                self._obj.security_group_entries = self.security_group_entries
                self._obj.id_perms = self.id_perms
                self._obj.display_name = self.display_name
            self._conn_drv.security_group_create(self._obj)
            # read back for server allocated values
            self._obj = self._conn_drv.security_group_read(id = self._obj.uuid)
    #end setUp

    def cleanUp(self):
        if self._obj.get_virtual_machine_interface_back_refs():
            return
        parent_fixt = getattr(self, '_parent_fixt', None)
        if parent_fixt:
            # non config-root child
            parent_obj = self._parent_fixt.getObj()
            # remove child from parent obj
            for child_obj in parent_obj.get_security_groups():
                if type(child_obj) == dict:
                    child_uuid = child_obj['uuid']
                else:
                    child_uuid = child_obj.uuid
                if child_uuid == self._obj.uuid:
                    parent_obj.security_groups.remove(child_obj)
                    break

        self._conn_drv.security_group_delete(id = self._obj.uuid)
    #end cleanUp

    def getObj(self):
        return self._obj
    #end getObj

#end class SecurityGroupTestFixtureGen

class ProviderAttachmentTestFixtureGen(cfixture.ContrailFixture):
    """
    Fixture for :class:`.ProviderAttachment`
    """
    def __init__(self, conn_drv, provider_attachment_name=None, auto_prop_val=False, virtual_router_refs = None, id_perms=None, display_name=None):
        '''
        Create ProviderAttachmentTestFixtureGen object
        
        constructor

        Args:
            conn_drv (:class:`ConnectionDriver`): connection driver (eg. :class:`vnc_api.vnc_api.VncApi`, :class:`novaclient.client.Client`, etc)

        Kwargs:
            %s_name (str): Name of %s
            virtual_router (list): list of :class:`VirtualRouter` type
            id_perms (instance): instance of :class:`IdPermsType`
            display_name (instance): instance of :class:`xsd:string`

        '''
        super(ProviderAttachmentTestFixtureGen, self).__init__()
        self._conn_drv = conn_drv
        if not provider_attachment_name:
            self._name = 'default-provider-attachment'
        else:
            self._name = provider_attachment_name
        self._obj = None
        self._auto_prop_val = auto_prop_val
        if virtual_router_refs:
            for ln in virtual_router_refs:
                self.add_virtual_router (ln)
        self.id_perms = id_perms
        self.display_name = display_name
    #end __init__

    def _update_links (self, update_server):
        for ln in self.get_virtual_routers ():
            self.add_virtual_router (ln.fixture (), update_server = update_server, add_link = False)
        return None
    #end _update_links

    def add_virtual_router (self, lo, update_server = True, add_link = True):
        '''
        add :class:`VirtualRouter` link to :class:`ProviderAttachment`
        Args:
            lo (:class:`VirtualRouter`): obj to link
        '''
        if self._obj:
            self._obj.add_virtual_router (lo)
            if update_server:
                self._conn_drv.provider_attachment_update (self._obj)

        if add_link:
            self.add_link('virtual_router', cfixture.ConrtailLink('virtual_router', 'provider_attachment', 'virtual_router', ['ref'], lo))
    #end add_virtual_router_link

    def get_virtual_routers (self):
        return self.get_links ('virtual_router')
    #end get_virtual_routers

    def populate (self):
        self._obj.set_id_perms(self.id_perms or vnc_api.gen.resource_xsd.IdPermsType.populate())
        self._obj.set_display_name(self.display_name or GeneratedsSuper.populate_string("display_name"))
    #end populate

    def setUp(self):
        super(ProviderAttachmentTestFixtureGen, self).setUp()
        self._obj = vnc_api.ProviderAttachment(self._name)
        try:
            self._obj = self._conn_drv.provider_attachment_read (fq_name=self._obj.get_fq_name())
            self._update_links (update_server=True)
        except NoIdError:
            self._update_links (update_server=False)
            if self._auto_prop_val:
                self.populate ()
            else:
                self._obj.id_perms = self.id_perms
                self._obj.display_name = self.display_name
            self._conn_drv.provider_attachment_create(self._obj)
            # read back for server allocated values
            self._obj = self._conn_drv.provider_attachment_read(id = self._obj.uuid)
    #end setUp

    def cleanUp(self):
        self._conn_drv.provider_attachment_delete(id = self._obj.uuid)
    #end cleanUp

    def getObj(self):
        return self._obj
    #end getObj

#end class ProviderAttachmentTestFixtureGen

class VirtualMachineInterfaceTestFixtureGen(cfixture.ContrailFixture):
    """
    Fixture for :class:`.VirtualMachineInterface`
    """
    def __init__(self, conn_drv, virtual_machine_interface_name=None, parent_fixt=None, auto_prop_val=False, qos_forwarding_class_refs = None, security_group_refs = None, virtual_machine_interface_refs = None, virtual_machine_refs = None, virtual_network_refs = None, routing_instance_ref_infos = None, interface_route_table_refs = None, virtual_machine_interface_mac_addresses=None, virtual_machine_interface_dhcp_option_list=None, virtual_machine_interface_host_routes=None, virtual_machine_interface_allowed_address_pairs=None, vrf_assign_table=None, virtual_machine_interface_device_owner=None, virtual_machine_interface_properties=None, id_perms=None, display_name=None):
        '''
        Create VirtualMachineInterfaceTestFixtureGen object
        
        constructor

        Args:
            conn_drv (:class:`ConnectionDriver`): connection driver (eg. :class:`vnc_api.vnc_api.VncApi`, :class:`novaclient.client.Client`, etc)

        Kwargs:
            virtual_machine_interface_name (str): Name of virtual_machine_interface
            parent_fixt (:class:`.VirtualMachineTestFixtureGen`): Parent fixture
            qos_forwarding_class (list): list of :class:`QosForwardingClass` type
            security_group (list): list of :class:`SecurityGroup` type
            virtual_machine_interface (list): list of :class:`VirtualMachineInterface` type
            virtual_machine (list): list of :class:`VirtualMachine` type
            virtual_network (list): list of :class:`VirtualNetwork` type
            routing_instance (list): list of tuple (:class:`RoutingInstance`, :class: `PolicyBasedForwardingRuleType`) type
            interface_route_table (list): list of :class:`InterfaceRouteTable` type
            virtual_machine_interface_mac_addresses (instance): instance of :class:`MacAddressesType`
            virtual_machine_interface_dhcp_option_list (instance): instance of :class:`DhcpOptionsListType`
            virtual_machine_interface_host_routes (instance): instance of :class:`RouteTableType`
            virtual_machine_interface_allowed_address_pairs (instance): instance of :class:`AllowedAddressPairs`
            vrf_assign_table (instance): instance of :class:`VrfAssignTableType`
            virtual_machine_interface_device_owner (instance): instance of :class:`xsd:string`
            virtual_machine_interface_properties (instance): instance of :class:`VirtualMachineInterfacePropertiesType`
            id_perms (instance): instance of :class:`IdPermsType`
            display_name (instance): instance of :class:`xsd:string`

        '''
        super(VirtualMachineInterfaceTestFixtureGen, self).__init__()
        self._conn_drv = conn_drv
        if not virtual_machine_interface_name:
            self._name = 'default-virtual-machine-interface'
        else:
            self._name = virtual_machine_interface_name
        self._obj = None
        self._parent_fixt = parent_fixt
        self._auto_prop_val = auto_prop_val
        if qos_forwarding_class_refs:
            for ln in qos_forwarding_class_refs:
                self.add_qos_forwarding_class (ln)
        if security_group_refs:
            for ln in security_group_refs:
                self.add_security_group (ln)
        if virtual_machine_interface_refs:
            for ln in virtual_machine_interface_refs:
                self.add_virtual_machine_interface (ln)
        if virtual_machine_refs:
            for ln in virtual_machine_refs:
                self.add_virtual_machine (ln)
        if virtual_network_refs:
            for ln in virtual_network_refs:
                self.add_virtual_network (ln)
        if routing_instance_ref_infos:
            for ln, ref in routing_instance_ref_infos:
                self.add_routing_instance (ln, ref)
        if interface_route_table_refs:
            for ln in interface_route_table_refs:
                self.add_interface_route_table (ln)
        self.virtual_machine_interface_mac_addresses = virtual_machine_interface_mac_addresses
        self.virtual_machine_interface_dhcp_option_list = virtual_machine_interface_dhcp_option_list
        self.virtual_machine_interface_host_routes = virtual_machine_interface_host_routes
        self.virtual_machine_interface_allowed_address_pairs = virtual_machine_interface_allowed_address_pairs
        self.vrf_assign_table = vrf_assign_table
        self.virtual_machine_interface_device_owner = virtual_machine_interface_device_owner
        self.virtual_machine_interface_properties = virtual_machine_interface_properties
        self.id_perms = id_perms
        self.display_name = display_name
    #end __init__

    def _update_links (self, update_server):
        for ln in self.get_qos_forwarding_classs ():
            self.add_qos_forwarding_class (ln.fixture (), update_server = update_server, add_link = False)
        for ln in self.get_security_groups ():
            self.add_security_group (ln.fixture (), update_server = update_server, add_link = False)
        for ln in self.get_virtual_machine_interfaces ():
            self.add_virtual_machine_interface (ln.fixture (), update_server = update_server, add_link = False)
        for ln in self.get_virtual_machines ():
            self.add_virtual_machine (ln.fixture (), update_server = update_server, add_link = False)
        for ln in self.get_virtual_networks ():
            self.add_virtual_network (ln.fixture (), update_server = update_server, add_link = False)
        for ln in self.get_routing_instances ():
            self.add_routing_instance (*ln.fixture (), update_server = update_server, add_link = False)
        for ln in self.get_interface_route_tables ():
            self.add_interface_route_table (ln.fixture (), update_server = update_server, add_link = False)
        return None
    #end _update_links

    def add_qos_forwarding_class (self, lo, update_server = True, add_link = True):
        '''
        add :class:`QosForwardingClass` link to :class:`VirtualMachineInterface`
        Args:
            lo (:class:`QosForwardingClass`): obj to link
        '''
        if self._obj:
            self._obj.add_qos_forwarding_class (lo)
            if update_server:
                self._conn_drv.virtual_machine_interface_update (self._obj)

        if add_link:
            self.add_link('qos_forwarding_class', cfixture.ConrtailLink('qos_forwarding_class', 'virtual_machine_interface', 'qos_forwarding_class', ['ref'], lo))
    #end add_qos_forwarding_class_link

    def get_qos_forwarding_classs (self):
        return self.get_links ('qos_forwarding_class')
    #end get_qos_forwarding_classs
    def add_security_group (self, lo, update_server = True, add_link = True):
        '''
        add :class:`SecurityGroup` link to :class:`VirtualMachineInterface`
        Args:
            lo (:class:`SecurityGroup`): obj to link
        '''
        if self._obj:
            self._obj.add_security_group (lo)
            if update_server:
                self._conn_drv.virtual_machine_interface_update (self._obj)

        if add_link:
            self.add_link('security_group', cfixture.ConrtailLink('security_group', 'virtual_machine_interface', 'security_group', ['ref'], lo))
    #end add_security_group_link

    def get_security_groups (self):
        return self.get_links ('security_group')
    #end get_security_groups
    def add_virtual_machine_interface (self, lo, update_server = True, add_link = True):
        '''
        add :class:`VirtualMachineInterface` link to :class:`VirtualMachineInterface`
        Args:
            lo (:class:`VirtualMachineInterface`): obj to link
        '''
        if self._obj:
            self._obj.add_virtual_machine_interface (lo)
            if update_server:
                self._conn_drv.virtual_machine_interface_update (self._obj)

        if add_link:
            self.add_link('virtual_machine_interface', cfixture.ConrtailLink('virtual_machine_interface', 'virtual_machine_interface', 'virtual_machine_interface', ['ref'], lo))
    #end add_virtual_machine_interface_link

    def get_virtual_machine_interfaces (self):
        return self.get_links ('virtual_machine_interface')
    #end get_virtual_machine_interfaces
    def add_virtual_machine (self, lo, update_server = True, add_link = True):
        '''
        add :class:`VirtualMachine` link to :class:`VirtualMachineInterface`
        Args:
            lo (:class:`VirtualMachine`): obj to link
        '''
        if self._obj:
            self._obj.add_virtual_machine (lo)
            if update_server:
                self._conn_drv.virtual_machine_interface_update (self._obj)

        if add_link:
            self.add_link('virtual_machine', cfixture.ConrtailLink('virtual_machine', 'virtual_machine_interface', 'virtual_machine', ['ref'], lo))
    #end add_virtual_machine_link

    def get_virtual_machines (self):
        return self.get_links ('virtual_machine')
    #end get_virtual_machines
    def add_virtual_network (self, lo, update_server = True, add_link = True):
        '''
        add :class:`VirtualNetwork` link to :class:`VirtualMachineInterface`
        Args:
            lo (:class:`VirtualNetwork`): obj to link
        '''
        if self._obj:
            self._obj.add_virtual_network (lo)
            if update_server:
                self._conn_drv.virtual_machine_interface_update (self._obj)

        if add_link:
            self.add_link('virtual_network', cfixture.ConrtailLink('virtual_network', 'virtual_machine_interface', 'virtual_network', ['ref'], lo))
    #end add_virtual_network_link

    def get_virtual_networks (self):
        return self.get_links ('virtual_network')
    #end get_virtual_networks
    def add_routing_instance (self, lo, ref, update_server = True, add_link = True):
        '''
        add :class:`RoutingInstance` link to :class:`VirtualMachineInterface`
        Args:
            lo (:class:`RoutingInstance`): obj to link
            ref (:class:`PolicyBasedForwardingRuleType`): property of the link object
        '''
        if self._obj:
            self._obj.add_routing_instance (lo, ref)
            if update_server:
                self._conn_drv.virtual_machine_interface_update (self._obj)

        if add_link:
            self.add_link('routing_instance', cfixture.ConrtailLink('routing_instance', 'virtual_machine_interface', 'routing_instance', ['ref'], (lo, ref)))
    #end add_routing_instance_link

    def get_routing_instances (self):
        return self.get_links ('routing_instance')
    #end get_routing_instances
    def add_interface_route_table (self, lo, update_server = True, add_link = True):
        '''
        add :class:`InterfaceRouteTable` link to :class:`VirtualMachineInterface`
        Args:
            lo (:class:`InterfaceRouteTable`): obj to link
        '''
        if self._obj:
            self._obj.add_interface_route_table (lo)
            if update_server:
                self._conn_drv.virtual_machine_interface_update (self._obj)

        if add_link:
            self.add_link('interface_route_table', cfixture.ConrtailLink('interface_route_table', 'virtual_machine_interface', 'interface_route_table', ['ref'], lo))
    #end add_interface_route_table_link

    def get_interface_route_tables (self):
        return self.get_links ('interface_route_table')
    #end get_interface_route_tables

    def populate (self):
        self._obj.set_virtual_machine_interface_mac_addresses(self.virtual_machine_interface_mac_addresses or vnc_api.gen.resource_xsd.MacAddressesType.populate())
        self._obj.set_virtual_machine_interface_dhcp_option_list(self.virtual_machine_interface_dhcp_option_list or vnc_api.gen.resource_xsd.DhcpOptionsListType.populate())
        self._obj.set_virtual_machine_interface_host_routes(self.virtual_machine_interface_host_routes or vnc_api.gen.resource_xsd.RouteTableType.populate())
        self._obj.set_virtual_machine_interface_allowed_address_pairs(self.virtual_machine_interface_allowed_address_pairs or vnc_api.gen.resource_xsd.AllowedAddressPairs.populate())
        self._obj.set_vrf_assign_table(self.vrf_assign_table or vnc_api.gen.resource_xsd.VrfAssignTableType.populate())
        self._obj.set_virtual_machine_interface_device_owner(self.virtual_machine_interface_device_owner or GeneratedsSuper.populate_string("virtual_machine_interface_device_owner"))
        self._obj.set_virtual_machine_interface_properties(self.virtual_machine_interface_properties or vnc_api.gen.resource_xsd.VirtualMachineInterfacePropertiesType.populate())
        self._obj.set_id_perms(self.id_perms or vnc_api.gen.resource_xsd.IdPermsType.populate())
        self._obj.set_display_name(self.display_name or GeneratedsSuper.populate_string("display_name"))
    #end populate

    def setUp(self):
        super(VirtualMachineInterfaceTestFixtureGen, self).setUp()
        if not self._parent_fixt:
            raise AmbiguousParentError("[[u'default-virtual-machine'], [u'default-domain', u'default-project']]")

        self._obj = vnc_api.VirtualMachineInterface(self._name, self._parent_fixt.getObj ())
        try:
            self._obj = self._conn_drv.virtual_machine_interface_read (fq_name=self._obj.get_fq_name())
            self._update_links (update_server=True)
        except NoIdError:
            self._update_links (update_server=False)
            if self._auto_prop_val:
                self.populate ()
            else:
                self._obj.virtual_machine_interface_mac_addresses = self.virtual_machine_interface_mac_addresses
                self._obj.virtual_machine_interface_dhcp_option_list = self.virtual_machine_interface_dhcp_option_list
                self._obj.virtual_machine_interface_host_routes = self.virtual_machine_interface_host_routes
                self._obj.virtual_machine_interface_allowed_address_pairs = self.virtual_machine_interface_allowed_address_pairs
                self._obj.vrf_assign_table = self.vrf_assign_table
                self._obj.virtual_machine_interface_device_owner = self.virtual_machine_interface_device_owner
                self._obj.virtual_machine_interface_properties = self.virtual_machine_interface_properties
                self._obj.id_perms = self.id_perms
                self._obj.display_name = self.display_name
            self._conn_drv.virtual_machine_interface_create(self._obj)
            # read back for server allocated values
            self._obj = self._conn_drv.virtual_machine_interface_read(id = self._obj.uuid)
    #end setUp

    def cleanUp(self):
        if self._obj.get_virtual_machine_interface_back_refs() or self._obj.get_instance_ip_back_refs() or self._obj.get_subnet_back_refs() or self._obj.get_floating_ip_back_refs() or self._obj.get_logical_interface_back_refs() or self._obj.get_customer_attachment_back_refs() or self._obj.get_logical_router_back_refs() or self._obj.get_loadbalancer_pool_back_refs() or self._obj.get_virtual_ip_back_refs():
            return
        parent_fixt = getattr(self, '_parent_fixt', None)
        if parent_fixt:
            # non config-root child
            parent_obj = self._parent_fixt.getObj()
            # remove child from parent obj
            for child_obj in parent_obj.get_virtual_machine_interfaces():
                if type(child_obj) == dict:
                    child_uuid = child_obj['uuid']
                else:
                    child_uuid = child_obj.uuid
                if child_uuid == self._obj.uuid:
                    parent_obj.virtual_machine_interfaces.remove(child_obj)
                    break

        self._conn_drv.virtual_machine_interface_delete(id = self._obj.uuid)
    #end cleanUp

    def getObj(self):
        return self._obj
    #end getObj

#end class VirtualMachineInterfaceTestFixtureGen

class LoadbalancerHealthmonitorTestFixtureGen(cfixture.ContrailFixture):
    """
    Fixture for :class:`.LoadbalancerHealthmonitor`
    """
    def __init__(self, conn_drv, loadbalancer_healthmonitor_name=None, parent_fixt=None, auto_prop_val=False, loadbalancer_healthmonitor_properties=None, id_perms=None, display_name=None):
        '''
        Create LoadbalancerHealthmonitorTestFixtureGen object
        
        constructor

        Args:
            conn_drv (:class:`ConnectionDriver`): connection driver (eg. :class:`vnc_api.vnc_api.VncApi`, :class:`novaclient.client.Client`, etc)

        Kwargs:
            loadbalancer_healthmonitor_name (str): Name of loadbalancer_healthmonitor
            parent_fixt (:class:`.ProjectTestFixtureGen`): Parent fixture
            loadbalancer_healthmonitor_properties (instance): instance of :class:`LoadbalancerHealthmonitorType`
            id_perms (instance): instance of :class:`IdPermsType`
            display_name (instance): instance of :class:`xsd:string`

        '''
        super(LoadbalancerHealthmonitorTestFixtureGen, self).__init__()
        self._conn_drv = conn_drv
        if not loadbalancer_healthmonitor_name:
            self._name = 'default-loadbalancer-healthmonitor'
        else:
            self._name = loadbalancer_healthmonitor_name
        self._obj = None
        self._parent_fixt = parent_fixt
        self._auto_prop_val = auto_prop_val
        self.loadbalancer_healthmonitor_properties = loadbalancer_healthmonitor_properties
        self.id_perms = id_perms
        self.display_name = display_name
    #end __init__

    def _update_links (self, update_server):
        return None
    #end _update_links


    def populate (self):
        self._obj.set_loadbalancer_healthmonitor_properties(self.loadbalancer_healthmonitor_properties or vnc_api.gen.resource_xsd.LoadbalancerHealthmonitorType.populate())
        self._obj.set_id_perms(self.id_perms or vnc_api.gen.resource_xsd.IdPermsType.populate())
        self._obj.set_display_name(self.display_name or GeneratedsSuper.populate_string("display_name"))
    #end populate

    def setUp(self):
        super(LoadbalancerHealthmonitorTestFixtureGen, self).setUp()
        if not self._parent_fixt:
            self._parent_fixt = self.useFixture(ProjectTestFixtureGen(self._conn_drv, 'default-project'))

        self._obj = vnc_api.LoadbalancerHealthmonitor(self._name, self._parent_fixt.getObj ())
        try:
            self._obj = self._conn_drv.loadbalancer_healthmonitor_read (fq_name=self._obj.get_fq_name())
            self._update_links (update_server=True)
        except NoIdError:
            self._update_links (update_server=False)
            if self._auto_prop_val:
                self.populate ()
            else:
                self._obj.loadbalancer_healthmonitor_properties = self.loadbalancer_healthmonitor_properties
                self._obj.id_perms = self.id_perms
                self._obj.display_name = self.display_name
            self._conn_drv.loadbalancer_healthmonitor_create(self._obj)
            # read back for server allocated values
            self._obj = self._conn_drv.loadbalancer_healthmonitor_read(id = self._obj.uuid)
    #end setUp

    def cleanUp(self):
        if self._obj.get_loadbalancer_pool_back_refs():
            return
        parent_fixt = getattr(self, '_parent_fixt', None)
        if parent_fixt:
            # non config-root child
            parent_obj = self._parent_fixt.getObj()
            # remove child from parent obj
            for child_obj in parent_obj.get_loadbalancer_healthmonitors():
                if type(child_obj) == dict:
                    child_uuid = child_obj['uuid']
                else:
                    child_uuid = child_obj.uuid
                if child_uuid == self._obj.uuid:
                    parent_obj.loadbalancer_healthmonitors.remove(child_obj)
                    break

        self._conn_drv.loadbalancer_healthmonitor_delete(id = self._obj.uuid)
    #end cleanUp

    def getObj(self):
        return self._obj
    #end getObj

#end class LoadbalancerHealthmonitorTestFixtureGen

class VirtualNetworkTestFixtureGen(cfixture.ContrailFixture):
    """
    Fixture for :class:`.VirtualNetwork`
    """
    def __init__(self, conn_drv, virtual_network_name=None, parent_fixt=None, auto_prop_val=False, qos_forwarding_class_refs = None, network_ipam_ref_infos = None, network_policy_ref_infos = None, route_table_refs = None, virtual_network_properties=None, virtual_network_network_id=None, route_target_list=None, router_external=None, is_shared=None, external_ipam=None, flood_unknown_unicast=None, id_perms=None, display_name=None):
        '''
        Create VirtualNetworkTestFixtureGen object
        
        constructor

        Args:
            conn_drv (:class:`ConnectionDriver`): connection driver (eg. :class:`vnc_api.vnc_api.VncApi`, :class:`novaclient.client.Client`, etc)

        Kwargs:
            virtual_network_name (str): Name of virtual_network
            parent_fixt (:class:`.ProjectTestFixtureGen`): Parent fixture
            qos_forwarding_class (list): list of :class:`QosForwardingClass` type
            network_ipam (list): list of tuple (:class:`NetworkIpam`, :class: `VnSubnetsType`) type
            network_policy (list): list of tuple (:class:`NetworkPolicy`, :class: `VirtualNetworkPolicyType`) type
            route_table (list): list of :class:`RouteTable` type
            virtual_network_properties (instance): instance of :class:`VirtualNetworkType`
            virtual_network_network_id (instance): instance of :class:`xsd:integer`
            route_target_list (instance): instance of :class:`RouteTargetList`
            router_external (instance): instance of :class:`xsd:boolean`
            is_shared (instance): instance of :class:`xsd:boolean`
            external_ipam (instance): instance of :class:`xsd:boolean`
            flood_unknown_unicast (instance): instance of :class:`xsd:boolean`
            id_perms (instance): instance of :class:`IdPermsType`
            display_name (instance): instance of :class:`xsd:string`

        '''
        super(VirtualNetworkTestFixtureGen, self).__init__()
        self._conn_drv = conn_drv
        if not virtual_network_name:
            self._name = 'default-virtual-network'
        else:
            self._name = virtual_network_name
        self._obj = None
        self._parent_fixt = parent_fixt
        self._auto_prop_val = auto_prop_val
        if qos_forwarding_class_refs:
            for ln in qos_forwarding_class_refs:
                self.add_qos_forwarding_class (ln)
        if network_ipam_ref_infos:
            for ln, ref in network_ipam_ref_infos:
                self.add_network_ipam (ln, ref)
        if network_policy_ref_infos:
            for ln, ref in network_policy_ref_infos:
                self.add_network_policy (ln, ref)
        if route_table_refs:
            for ln in route_table_refs:
                self.add_route_table (ln)
        self.virtual_network_properties = virtual_network_properties
        self.virtual_network_network_id = virtual_network_network_id
        self.route_target_list = route_target_list
        self.router_external = router_external
        self.is_shared = is_shared
        self.external_ipam = external_ipam
        self.flood_unknown_unicast = flood_unknown_unicast
        self.id_perms = id_perms
        self.display_name = display_name
    #end __init__

    def _update_links (self, update_server):
        for ln in self.get_qos_forwarding_classs ():
            self.add_qos_forwarding_class (ln.fixture (), update_server = update_server, add_link = False)
        for ln in self.get_network_ipams ():
            self.add_network_ipam (*ln.fixture (), update_server = update_server, add_link = False)
        for ln in self.get_network_policys ():
            self.add_network_policy (*ln.fixture (), update_server = update_server, add_link = False)
        for ln in self.get_route_tables ():
            self.add_route_table (ln.fixture (), update_server = update_server, add_link = False)
        return None
    #end _update_links

    def add_qos_forwarding_class (self, lo, update_server = True, add_link = True):
        '''
        add :class:`QosForwardingClass` link to :class:`VirtualNetwork`
        Args:
            lo (:class:`QosForwardingClass`): obj to link
        '''
        if self._obj:
            self._obj.add_qos_forwarding_class (lo)
            if update_server:
                self._conn_drv.virtual_network_update (self._obj)

        if add_link:
            self.add_link('qos_forwarding_class', cfixture.ConrtailLink('qos_forwarding_class', 'virtual_network', 'qos_forwarding_class', ['ref'], lo))
    #end add_qos_forwarding_class_link

    def get_qos_forwarding_classs (self):
        return self.get_links ('qos_forwarding_class')
    #end get_qos_forwarding_classs
    def add_network_ipam (self, lo, ref, update_server = True, add_link = True):
        '''
        add :class:`NetworkIpam` link to :class:`VirtualNetwork`
        Args:
            lo (:class:`NetworkIpam`): obj to link
            ref (:class:`VnSubnetsType`): property of the link object
        '''
        if self._obj:
            self._obj.add_network_ipam (lo, ref)
            if update_server:
                self._conn_drv.virtual_network_update (self._obj)

        if add_link:
            self.add_link('network_ipam', cfixture.ConrtailLink('network_ipam', 'virtual_network', 'network_ipam', ['ref'], (lo, ref)))
    #end add_network_ipam_link

    def get_network_ipams (self):
        return self.get_links ('network_ipam')
    #end get_network_ipams
    def add_network_policy (self, lo, ref, update_server = True, add_link = True):
        '''
        add :class:`NetworkPolicy` link to :class:`VirtualNetwork`
        Args:
            lo (:class:`NetworkPolicy`): obj to link
            ref (:class:`VirtualNetworkPolicyType`): property of the link object
        '''
        if self._obj:
            self._obj.add_network_policy (lo, ref)
            if update_server:
                self._conn_drv.virtual_network_update (self._obj)

        if add_link:
            self.add_link('network_policy', cfixture.ConrtailLink('network_policy', 'virtual_network', 'network_policy', ['ref'], (lo, ref)))
    #end add_network_policy_link

    def get_network_policys (self):
        return self.get_links ('network_policy')
    #end get_network_policys
    def add_route_table (self, lo, update_server = True, add_link = True):
        '''
        add :class:`RouteTable` link to :class:`VirtualNetwork`
        Args:
            lo (:class:`RouteTable`): obj to link
        '''
        if self._obj:
            self._obj.add_route_table (lo)
            if update_server:
                self._conn_drv.virtual_network_update (self._obj)

        if add_link:
            self.add_link('route_table', cfixture.ConrtailLink('route_table', 'virtual_network', 'route_table', ['ref'], lo))
    #end add_route_table_link

    def get_route_tables (self):
        return self.get_links ('route_table')
    #end get_route_tables

    def populate (self):
        self._obj.set_virtual_network_properties(self.virtual_network_properties or vnc_api.gen.resource_xsd.VirtualNetworkType.populate())
        self._obj.set_virtual_network_network_id(self.virtual_network_network_id or GeneratedsSuper.populate_integer("virtual_network_network_id"))
        self._obj.set_route_target_list(self.route_target_list or vnc_api.gen.resource_xsd.RouteTargetList.populate())
        self._obj.set_router_external(self.router_external or GeneratedsSuper.populate_boolean("router_external"))
        self._obj.set_is_shared(self.is_shared or GeneratedsSuper.populate_boolean("is_shared"))
        self._obj.set_external_ipam(self.external_ipam or GeneratedsSuper.populate_boolean("external_ipam"))
        self._obj.set_flood_unknown_unicast(self.flood_unknown_unicast or GeneratedsSuper.populate_boolean("flood_unknown_unicast"))
        self._obj.set_id_perms(self.id_perms or vnc_api.gen.resource_xsd.IdPermsType.populate())
        self._obj.set_display_name(self.display_name or GeneratedsSuper.populate_string("display_name"))
    #end populate

    def setUp(self):
        super(VirtualNetworkTestFixtureGen, self).setUp()
        if not self._parent_fixt:
            self._parent_fixt = self.useFixture(ProjectTestFixtureGen(self._conn_drv, 'default-project'))

        self._obj = vnc_api.VirtualNetwork(self._name, self._parent_fixt.getObj ())
        try:
            self._obj = self._conn_drv.virtual_network_read (fq_name=self._obj.get_fq_name())
            self._update_links (update_server=True)
        except NoIdError:
            self._update_links (update_server=False)
            if self._auto_prop_val:
                self.populate ()
            else:
                self._obj.virtual_network_properties = self.virtual_network_properties
                self._obj.virtual_network_network_id = self.virtual_network_network_id
                self._obj.route_target_list = self.route_target_list
                self._obj.router_external = self.router_external
                self._obj.is_shared = self.is_shared
                self._obj.external_ipam = self.external_ipam
                self._obj.flood_unknown_unicast = self.flood_unknown_unicast
                self._obj.id_perms = self.id_perms
                self._obj.display_name = self.display_name
            self._conn_drv.virtual_network_create(self._obj)
            # read back for server allocated values
            self._obj = self._conn_drv.virtual_network_read(id = self._obj.uuid)
    #end setUp

    def cleanUp(self):
        if self._obj.get_virtual_machine_interface_back_refs() or self._obj.get_instance_ip_back_refs() or self._obj.get_physical_router_back_refs() or self._obj.get_logical_router_back_refs():
            return
        if self._obj.get_floating_ip_pools():
            return
        parent_fixt = getattr(self, '_parent_fixt', None)
        if parent_fixt:
            # non config-root child
            parent_obj = self._parent_fixt.getObj()
            # remove child from parent obj
            for child_obj in parent_obj.get_virtual_networks():
                if type(child_obj) == dict:
                    child_uuid = child_obj['uuid']
                else:
                    child_uuid = child_obj.uuid
                if child_uuid == self._obj.uuid:
                    parent_obj.virtual_networks.remove(child_obj)
                    break

        self._conn_drv.virtual_network_delete(id = self._obj.uuid)
    #end cleanUp

    def getObj(self):
        return self._obj
    #end getObj

#end class VirtualNetworkTestFixtureGen

class ProjectTestFixtureGen(cfixture.ContrailFixture):
    """
    Fixture for :class:`.Project`
    """
    def __init__(self, conn_drv, project_name=None, parent_fixt=None, auto_prop_val=False, namespace_ref_infos = None, floating_ip_pool_refs = None, quota=None, id_perms=None, display_name=None):
        '''
        Create ProjectTestFixtureGen object
        
        constructor

        Args:
            conn_drv (:class:`ConnectionDriver`): connection driver (eg. :class:`vnc_api.vnc_api.VncApi`, :class:`novaclient.client.Client`, etc)

        Kwargs:
            project_name (str): Name of project
            parent_fixt (:class:`.DomainTestFixtureGen`): Parent fixture
            namespace (list): list of tuple (:class:`Namespace`, :class: `SubnetType`) type
            floating_ip_pool (list): list of :class:`FloatingIpPool` type
            quota (instance): instance of :class:`QuotaType`
            id_perms (instance): instance of :class:`IdPermsType`
            display_name (instance): instance of :class:`xsd:string`

        '''
        super(ProjectTestFixtureGen, self).__init__()
        self._conn_drv = conn_drv
        if not project_name:
            self._name = 'default-project'
        else:
            self._name = project_name
        self._obj = None
        self._parent_fixt = parent_fixt
        self._auto_prop_val = auto_prop_val
        if namespace_ref_infos:
            for ln, ref in namespace_ref_infos:
                self.add_namespace (ln, ref)
        if floating_ip_pool_refs:
            for ln in floating_ip_pool_refs:
                self.add_floating_ip_pool (ln)
        self.quota = quota
        self.id_perms = id_perms
        self.display_name = display_name
    #end __init__

    def _update_links (self, update_server):
        for ln in self.get_namespaces ():
            self.add_namespace (*ln.fixture (), update_server = update_server, add_link = False)
        for ln in self.get_floating_ip_pools ():
            self.add_floating_ip_pool (ln.fixture (), update_server = update_server, add_link = False)
        return None
    #end _update_links

    def add_namespace (self, lo, ref, update_server = True, add_link = True):
        '''
        add :class:`Namespace` link to :class:`Project`
        Args:
            lo (:class:`Namespace`): obj to link
            ref (:class:`SubnetType`): property of the link object
        '''
        if self._obj:
            self._obj.add_namespace (lo, ref)
            if update_server:
                self._conn_drv.project_update (self._obj)

        if add_link:
            self.add_link('namespace', cfixture.ConrtailLink('namespace', 'project', 'namespace', ['ref'], (lo, ref)))
    #end add_namespace_link

    def get_namespaces (self):
        return self.get_links ('namespace')
    #end get_namespaces
    def add_floating_ip_pool (self, lo, update_server = True, add_link = True):
        '''
        add :class:`FloatingIpPool` link to :class:`Project`
        Args:
            lo (:class:`FloatingIpPool`): obj to link
        '''
        if self._obj:
            self._obj.add_floating_ip_pool (lo)
            if update_server:
                self._conn_drv.project_update (self._obj)

        if add_link:
            self.add_link('floating_ip_pool', cfixture.ConrtailLink('floating_ip_pool', 'project', 'floating_ip_pool', ['ref'], lo))
    #end add_floating_ip_pool_link

    def get_floating_ip_pools (self):
        return self.get_links ('floating_ip_pool')
    #end get_floating_ip_pools

    def populate (self):
        self._obj.set_quota(self.quota or vnc_api.gen.resource_xsd.QuotaType.populate())
        self._obj.set_id_perms(self.id_perms or vnc_api.gen.resource_xsd.IdPermsType.populate())
        self._obj.set_display_name(self.display_name or GeneratedsSuper.populate_string("display_name"))
    #end populate

    def setUp(self):
        super(ProjectTestFixtureGen, self).setUp()
        if not self._parent_fixt:
            self._parent_fixt = self.useFixture(DomainTestFixtureGen(self._conn_drv, 'default-domain'))

        self._obj = vnc_api.Project(self._name, self._parent_fixt.getObj ())
        try:
            self._obj = self._conn_drv.project_read (fq_name=self._obj.get_fq_name())
            self._update_links (update_server=True)
        except NoIdError:
            self._update_links (update_server=False)
            if self._auto_prop_val:
                self.populate ()
            else:
                self._obj.quota = self.quota
                self._obj.id_perms = self.id_perms
                self._obj.display_name = self.display_name
            self._conn_drv.project_create(self._obj)
            # read back for server allocated values
            self._obj = self._conn_drv.project_read(id = self._obj.uuid)
    #end setUp

    def cleanUp(self):
        if self._obj.get_floating_ip_back_refs():
            return
        if self._obj.get_security_groups() or self._obj.get_virtual_networks() or self._obj.get_qos_queues() or self._obj.get_qos_forwarding_classs() or self._obj.get_network_ipams() or self._obj.get_network_policys() or self._obj.get_virtual_machine_interfaces() or self._obj.get_service_instances() or self._obj.get_route_tables() or self._obj.get_interface_route_tables() or self._obj.get_logical_routers() or self._obj.get_loadbalancer_pools() or self._obj.get_loadbalancer_healthmonitors() or self._obj.get_virtual_ips():
            return
        parent_fixt = getattr(self, '_parent_fixt', None)
        if parent_fixt:
            # non config-root child
            parent_obj = self._parent_fixt.getObj()
            # remove child from parent obj
            for child_obj in parent_obj.get_projects():
                if type(child_obj) == dict:
                    child_uuid = child_obj['uuid']
                else:
                    child_uuid = child_obj.uuid
                if child_uuid == self._obj.uuid:
                    parent_obj.projects.remove(child_obj)
                    break

        self._conn_drv.project_delete(id = self._obj.uuid)
    #end cleanUp

    def getObj(self):
        return self._obj
    #end getObj

#end class ProjectTestFixtureGen

class QosForwardingClassTestFixtureGen(cfixture.ContrailFixture):
    """
    Fixture for :class:`.QosForwardingClass`
    """
    def __init__(self, conn_drv, qos_forwarding_class_name=None, parent_fixt=None, auto_prop_val=False, qos_queue_refs = None, dscp=None, trusted=None, id_perms=None, display_name=None):
        '''
        Create QosForwardingClassTestFixtureGen object
        
        constructor

        Args:
            conn_drv (:class:`ConnectionDriver`): connection driver (eg. :class:`vnc_api.vnc_api.VncApi`, :class:`novaclient.client.Client`, etc)

        Kwargs:
            qos_forwarding_class_name (str): Name of qos_forwarding_class
            parent_fixt (:class:`.ProjectTestFixtureGen`): Parent fixture
            qos_queue (list): list of :class:`QosQueue` type
            dscp (instance): instance of :class:`xsd:integer`
            trusted (instance): instance of :class:`xsd:boolean`
            id_perms (instance): instance of :class:`IdPermsType`
            display_name (instance): instance of :class:`xsd:string`

        '''
        super(QosForwardingClassTestFixtureGen, self).__init__()
        self._conn_drv = conn_drv
        if not qos_forwarding_class_name:
            self._name = 'default-qos-forwarding-class'
        else:
            self._name = qos_forwarding_class_name
        self._obj = None
        self._parent_fixt = parent_fixt
        self._auto_prop_val = auto_prop_val
        if qos_queue_refs:
            for ln in qos_queue_refs:
                self.add_qos_queue (ln)
        self.dscp = dscp
        self.trusted = trusted
        self.id_perms = id_perms
        self.display_name = display_name
    #end __init__

    def _update_links (self, update_server):
        for ln in self.get_qos_queues ():
            self.add_qos_queue (ln.fixture (), update_server = update_server, add_link = False)
        return None
    #end _update_links

    def add_qos_queue (self, lo, update_server = True, add_link = True):
        '''
        add :class:`QosQueue` link to :class:`QosForwardingClass`
        Args:
            lo (:class:`QosQueue`): obj to link
        '''
        if self._obj:
            self._obj.add_qos_queue (lo)
            if update_server:
                self._conn_drv.qos_forwarding_class_update (self._obj)

        if add_link:
            self.add_link('qos_queue', cfixture.ConrtailLink('qos_queue', 'qos_forwarding_class', 'qos_queue', ['ref'], lo))
    #end add_qos_queue_link

    def get_qos_queues (self):
        return self.get_links ('qos_queue')
    #end get_qos_queues

    def populate (self):
        self._obj.set_dscp(self.dscp or GeneratedsSuper.populate_integer("dscp"))
        self._obj.set_trusted(self.trusted or GeneratedsSuper.populate_boolean("trusted"))
        self._obj.set_id_perms(self.id_perms or vnc_api.gen.resource_xsd.IdPermsType.populate())
        self._obj.set_display_name(self.display_name or GeneratedsSuper.populate_string("display_name"))
    #end populate

    def setUp(self):
        super(QosForwardingClassTestFixtureGen, self).setUp()
        if not self._parent_fixt:
            self._parent_fixt = self.useFixture(ProjectTestFixtureGen(self._conn_drv, 'default-project'))

        self._obj = vnc_api.QosForwardingClass(self._name, self._parent_fixt.getObj ())
        try:
            self._obj = self._conn_drv.qos_forwarding_class_read (fq_name=self._obj.get_fq_name())
            self._update_links (update_server=True)
        except NoIdError:
            self._update_links (update_server=False)
            if self._auto_prop_val:
                self.populate ()
            else:
                self._obj.dscp = self.dscp
                self._obj.trusted = self.trusted
                self._obj.id_perms = self.id_perms
                self._obj.display_name = self.display_name
            self._conn_drv.qos_forwarding_class_create(self._obj)
            # read back for server allocated values
            self._obj = self._conn_drv.qos_forwarding_class_read(id = self._obj.uuid)
    #end setUp

    def cleanUp(self):
        if self._obj.get_virtual_network_back_refs() or self._obj.get_virtual_machine_interface_back_refs():
            return
        parent_fixt = getattr(self, '_parent_fixt', None)
        if parent_fixt:
            # non config-root child
            parent_obj = self._parent_fixt.getObj()
            # remove child from parent obj
            for child_obj in parent_obj.get_qos_forwarding_classs():
                if type(child_obj) == dict:
                    child_uuid = child_obj['uuid']
                else:
                    child_uuid = child_obj.uuid
                if child_uuid == self._obj.uuid:
                    parent_obj.qos_forwarding_classs.remove(child_obj)
                    break

        self._conn_drv.qos_forwarding_class_delete(id = self._obj.uuid)
    #end cleanUp

    def getObj(self):
        return self._obj
    #end getObj

#end class QosForwardingClassTestFixtureGen

class DatabaseNodeTestFixtureGen(cfixture.ContrailFixture):
    """
    Fixture for :class:`.DatabaseNode`
    """
    def __init__(self, conn_drv, database_node_name=None, parent_fixt=None, auto_prop_val=False, database_node_ip_address=None, id_perms=None, display_name=None):
        '''
        Create DatabaseNodeTestFixtureGen object
        
        constructor

        Args:
            conn_drv (:class:`ConnectionDriver`): connection driver (eg. :class:`vnc_api.vnc_api.VncApi`, :class:`novaclient.client.Client`, etc)

        Kwargs:
            database_node_name (str): Name of database_node
            parent_fixt (:class:`.GlobalSystemConfigTestFixtureGen`): Parent fixture
            database_node_ip_address (instance): instance of :class:`xsd:string`
            id_perms (instance): instance of :class:`IdPermsType`
            display_name (instance): instance of :class:`xsd:string`

        '''
        super(DatabaseNodeTestFixtureGen, self).__init__()
        self._conn_drv = conn_drv
        if not database_node_name:
            self._name = 'default-database-node'
        else:
            self._name = database_node_name
        self._obj = None
        self._parent_fixt = parent_fixt
        self._auto_prop_val = auto_prop_val
        self.database_node_ip_address = database_node_ip_address
        self.id_perms = id_perms
        self.display_name = display_name
    #end __init__

    def _update_links (self, update_server):
        return None
    #end _update_links


    def populate (self):
        self._obj.set_database_node_ip_address(self.database_node_ip_address or GeneratedsSuper.populate_string("database_node_ip_address"))
        self._obj.set_id_perms(self.id_perms or vnc_api.gen.resource_xsd.IdPermsType.populate())
        self._obj.set_display_name(self.display_name or GeneratedsSuper.populate_string("display_name"))
    #end populate

    def setUp(self):
        super(DatabaseNodeTestFixtureGen, self).setUp()
        if not self._parent_fixt:
            self._parent_fixt = self.useFixture(GlobalSystemConfigTestFixtureGen(self._conn_drv, 'default-global-system-config'))

        self._obj = vnc_api.DatabaseNode(self._name, self._parent_fixt.getObj ())
        try:
            self._obj = self._conn_drv.database_node_read (fq_name=self._obj.get_fq_name())
            self._update_links (update_server=True)
        except NoIdError:
            self._update_links (update_server=False)
            if self._auto_prop_val:
                self.populate ()
            else:
                self._obj.database_node_ip_address = self.database_node_ip_address
                self._obj.id_perms = self.id_perms
                self._obj.display_name = self.display_name
            self._conn_drv.database_node_create(self._obj)
            # read back for server allocated values
            self._obj = self._conn_drv.database_node_read(id = self._obj.uuid)
    #end setUp

    def cleanUp(self):
        parent_fixt = getattr(self, '_parent_fixt', None)
        if parent_fixt:
            # non config-root child
            parent_obj = self._parent_fixt.getObj()
            # remove child from parent obj
            for child_obj in parent_obj.get_database_nodes():
                if type(child_obj) == dict:
                    child_uuid = child_obj['uuid']
                else:
                    child_uuid = child_obj.uuid
                if child_uuid == self._obj.uuid:
                    parent_obj.database_nodes.remove(child_obj)
                    break

        self._conn_drv.database_node_delete(id = self._obj.uuid)
    #end cleanUp

    def getObj(self):
        return self._obj
    #end getObj

#end class DatabaseNodeTestFixtureGen

class RoutingInstanceTestFixtureGen(cfixture.ContrailFixture):
    """
    Fixture for :class:`.RoutingInstance`
    """
    def __init__(self, conn_drv, routing_instance_name=None, parent_fixt=None, auto_prop_val=False, routing_instance_ref_infos = None, route_target_ref_infos = None, service_chain_information=None, routing_instance_is_default=None, static_route_entries=None, default_ce_protocol=None, id_perms=None, display_name=None):
        '''
        Create RoutingInstanceTestFixtureGen object
        
        constructor

        Args:
            conn_drv (:class:`ConnectionDriver`): connection driver (eg. :class:`vnc_api.vnc_api.VncApi`, :class:`novaclient.client.Client`, etc)

        Kwargs:
            routing_instance_name (str): Name of routing_instance
            parent_fixt (:class:`.VirtualNetworkTestFixtureGen`): Parent fixture
            routing_instance (list): list of tuple (:class:`RoutingInstance`, :class: `ConnectionType`) type
            route_target (list): list of tuple (:class:`RouteTarget`, :class: `InstanceTargetType`) type
            service_chain_information (instance): instance of :class:`ServiceChainInfo`
            routing_instance_is_default (instance): instance of :class:`xsd:boolean`
            static_route_entries (instance): instance of :class:`StaticRouteEntriesType`
            default_ce_protocol (instance): instance of :class:`DefaultProtocolType`
            id_perms (instance): instance of :class:`IdPermsType`
            display_name (instance): instance of :class:`xsd:string`

        '''
        super(RoutingInstanceTestFixtureGen, self).__init__()
        self._conn_drv = conn_drv
        if not routing_instance_name:
            self._name = 'default-routing-instance'
        else:
            self._name = routing_instance_name
        self._obj = None
        self._parent_fixt = parent_fixt
        self._auto_prop_val = auto_prop_val
        if routing_instance_ref_infos:
            for ln, ref in routing_instance_ref_infos:
                self.add_routing_instance (ln, ref)
        if route_target_ref_infos:
            for ln, ref in route_target_ref_infos:
                self.add_route_target (ln, ref)
        self.service_chain_information = service_chain_information
        self.routing_instance_is_default = routing_instance_is_default
        self.static_route_entries = static_route_entries
        self.default_ce_protocol = default_ce_protocol
        self.id_perms = id_perms
        self.display_name = display_name
    #end __init__

    def _update_links (self, update_server):
        for ln in self.get_routing_instances ():
            self.add_routing_instance (*ln.fixture (), update_server = update_server, add_link = False)
        for ln in self.get_route_targets ():
            self.add_route_target (*ln.fixture (), update_server = update_server, add_link = False)
        return None
    #end _update_links

    def add_routing_instance (self, lo, ref, update_server = True, add_link = True):
        '''
        add :class:`RoutingInstance` link to :class:`RoutingInstance`
        Args:
            lo (:class:`RoutingInstance`): obj to link
            ref (:class:`ConnectionType`): property of the link object
        '''
        if self._obj:
            self._obj.add_routing_instance (lo, ref)
            if update_server:
                self._conn_drv.routing_instance_update (self._obj)

        if add_link:
            self.add_link('routing_instance', cfixture.ConrtailLink('routing_instance', 'routing_instance', 'routing_instance', ['ref'], (lo, ref)))
    #end add_routing_instance_link

    def get_routing_instances (self):
        return self.get_links ('routing_instance')
    #end get_routing_instances
    def add_route_target (self, lo, ref, update_server = True, add_link = True):
        '''
        add :class:`RouteTarget` link to :class:`RoutingInstance`
        Args:
            lo (:class:`RouteTarget`): obj to link
            ref (:class:`InstanceTargetType`): property of the link object
        '''
        if self._obj:
            self._obj.add_route_target (lo, ref)
            if update_server:
                self._conn_drv.routing_instance_update (self._obj)

        if add_link:
            self.add_link('route_target', cfixture.ConrtailLink('route_target', 'routing_instance', 'route_target', ['ref'], (lo, ref)))
    #end add_route_target_link

    def get_route_targets (self):
        return self.get_links ('route_target')
    #end get_route_targets

    def populate (self):
        self._obj.set_service_chain_information(self.service_chain_information or vnc_api.gen.resource_xsd.ServiceChainInfo.populate())
        self._obj.set_routing_instance_is_default(self.routing_instance_is_default or GeneratedsSuper.populate_boolean("routing_instance_is_default"))
        self._obj.set_static_route_entries(self.static_route_entries or vnc_api.gen.resource_xsd.StaticRouteEntriesType.populate())
        self._obj.set_default_ce_protocol(self.default_ce_protocol or vnc_api.gen.resource_xsd.DefaultProtocolType.populate())
        self._obj.set_id_perms(self.id_perms or vnc_api.gen.resource_xsd.IdPermsType.populate())
        self._obj.set_display_name(self.display_name or GeneratedsSuper.populate_string("display_name"))
    #end populate

    def setUp(self):
        super(RoutingInstanceTestFixtureGen, self).setUp()
        if not self._parent_fixt:
            self._parent_fixt = self.useFixture(VirtualNetworkTestFixtureGen(self._conn_drv, 'default-virtual-network'))

        self._obj = vnc_api.RoutingInstance(self._name, self._parent_fixt.getObj ())
        try:
            self._obj = self._conn_drv.routing_instance_read (fq_name=self._obj.get_fq_name())
            self._update_links (update_server=True)
        except NoIdError:
            self._update_links (update_server=False)
            if self._auto_prop_val:
                self.populate ()
            else:
                self._obj.service_chain_information = self.service_chain_information
                self._obj.routing_instance_is_default = self.routing_instance_is_default
                self._obj.static_route_entries = self.static_route_entries
                self._obj.default_ce_protocol = self.default_ce_protocol
                self._obj.id_perms = self.id_perms
                self._obj.display_name = self.display_name
            self._conn_drv.routing_instance_create(self._obj)
            # read back for server allocated values
            self._obj = self._conn_drv.routing_instance_read(id = self._obj.uuid)
    #end setUp

    def cleanUp(self):
        if self._obj.get_virtual_machine_interface_back_refs() or self._obj.get_routing_instance_back_refs():
            return
        if self._obj.get_bgp_routers():
            return
        parent_fixt = getattr(self, '_parent_fixt', None)
        if parent_fixt:
            # non config-root child
            parent_obj = self._parent_fixt.getObj()
            # remove child from parent obj
            for child_obj in parent_obj.get_routing_instances():
                if type(child_obj) == dict:
                    child_uuid = child_obj['uuid']
                else:
                    child_uuid = child_obj.uuid
                if child_uuid == self._obj.uuid:
                    parent_obj.routing_instances.remove(child_obj)
                    break

        self._conn_drv.routing_instance_delete(id = self._obj.uuid)
    #end cleanUp

    def getObj(self):
        return self._obj
    #end getObj

#end class RoutingInstanceTestFixtureGen

class NetworkIpamTestFixtureGen(cfixture.ContrailFixture):
    """
    Fixture for :class:`.NetworkIpam`
    """
    def __init__(self, conn_drv, network_ipam_name=None, parent_fixt=None, auto_prop_val=False, virtual_DNS_refs = None, network_ipam_mgmt=None, id_perms=None, display_name=None):
        '''
        Create NetworkIpamTestFixtureGen object
        
        constructor

        Args:
            conn_drv (:class:`ConnectionDriver`): connection driver (eg. :class:`vnc_api.vnc_api.VncApi`, :class:`novaclient.client.Client`, etc)

        Kwargs:
            network_ipam_name (str): Name of network_ipam
            parent_fixt (:class:`.ProjectTestFixtureGen`): Parent fixture
            virtual_DNS (list): list of :class:`VirtualDns` type
            network_ipam_mgmt (instance): instance of :class:`IpamType`
            id_perms (instance): instance of :class:`IdPermsType`
            display_name (instance): instance of :class:`xsd:string`

        '''
        super(NetworkIpamTestFixtureGen, self).__init__()
        self._conn_drv = conn_drv
        if not network_ipam_name:
            self._name = 'default-network-ipam'
        else:
            self._name = network_ipam_name
        self._obj = None
        self._parent_fixt = parent_fixt
        self._auto_prop_val = auto_prop_val
        if virtual_DNS_refs:
            for ln in virtual_DNS_refs:
                self.add_virtual_DNS (ln)
        self.network_ipam_mgmt = network_ipam_mgmt
        self.id_perms = id_perms
        self.display_name = display_name
    #end __init__

    def _update_links (self, update_server):
        for ln in self.get_virtual_DNSs ():
            self.add_virtual_DNS (ln.fixture (), update_server = update_server, add_link = False)
        return None
    #end _update_links

    def add_virtual_DNS (self, lo, update_server = True, add_link = True):
        '''
        add :class:`VirtualDns` link to :class:`NetworkIpam`
        Args:
            lo (:class:`VirtualDns`): obj to link
        '''
        if self._obj:
            self._obj.add_virtual_DNS (lo)
            if update_server:
                self._conn_drv.network_ipam_update (self._obj)

        if add_link:
            self.add_link('virtual_DNS', cfixture.ConrtailLink('virtual_DNS', 'network_ipam', 'virtual_DNS', ['ref'], lo))
    #end add_virtual_DNS_link

    def get_virtual_DNSs (self):
        return self.get_links ('virtual_DNS')
    #end get_virtual_DNSs

    def populate (self):
        self._obj.set_network_ipam_mgmt(self.network_ipam_mgmt or vnc_api.gen.resource_xsd.IpamType.populate())
        self._obj.set_id_perms(self.id_perms or vnc_api.gen.resource_xsd.IdPermsType.populate())
        self._obj.set_display_name(self.display_name or GeneratedsSuper.populate_string("display_name"))
    #end populate

    def setUp(self):
        super(NetworkIpamTestFixtureGen, self).setUp()
        if not self._parent_fixt:
            self._parent_fixt = self.useFixture(ProjectTestFixtureGen(self._conn_drv, 'default-project'))

        self._obj = vnc_api.NetworkIpam(self._name, self._parent_fixt.getObj ())
        try:
            self._obj = self._conn_drv.network_ipam_read (fq_name=self._obj.get_fq_name())
            self._update_links (update_server=True)
        except NoIdError:
            self._update_links (update_server=False)
            if self._auto_prop_val:
                self.populate ()
            else:
                self._obj.network_ipam_mgmt = self.network_ipam_mgmt
                self._obj.id_perms = self.id_perms
                self._obj.display_name = self.display_name
            self._conn_drv.network_ipam_create(self._obj)
            # read back for server allocated values
            self._obj = self._conn_drv.network_ipam_read(id = self._obj.uuid)
    #end setUp

    def cleanUp(self):
        if self._obj.get_virtual_network_back_refs():
            return
        parent_fixt = getattr(self, '_parent_fixt', None)
        if parent_fixt:
            # non config-root child
            parent_obj = self._parent_fixt.getObj()
            # remove child from parent obj
            for child_obj in parent_obj.get_network_ipams():
                if type(child_obj) == dict:
                    child_uuid = child_obj['uuid']
                else:
                    child_uuid = child_obj.uuid
                if child_uuid == self._obj.uuid:
                    parent_obj.network_ipams.remove(child_obj)
                    break

        self._conn_drv.network_ipam_delete(id = self._obj.uuid)
    #end cleanUp

    def getObj(self):
        return self._obj
    #end getObj

#end class NetworkIpamTestFixtureGen

class LogicalRouterTestFixtureGen(cfixture.ContrailFixture):
    """
    Fixture for :class:`.LogicalRouter`
    """
    def __init__(self, conn_drv, logical_router_name=None, parent_fixt=None, auto_prop_val=False, virtual_machine_interface_refs = None, route_target_refs = None, virtual_network_refs = None, service_instance_refs = None, id_perms=None, display_name=None):
        '''
        Create LogicalRouterTestFixtureGen object
        
        constructor

        Args:
            conn_drv (:class:`ConnectionDriver`): connection driver (eg. :class:`vnc_api.vnc_api.VncApi`, :class:`novaclient.client.Client`, etc)

        Kwargs:
            logical_router_name (str): Name of logical_router
            parent_fixt (:class:`.ProjectTestFixtureGen`): Parent fixture
            virtual_machine_interface (list): list of :class:`VirtualMachineInterface` type
            route_target (list): list of :class:`RouteTarget` type
            virtual_network (list): list of :class:`VirtualNetwork` type
            service_instance (list): list of :class:`ServiceInstance` type
            id_perms (instance): instance of :class:`IdPermsType`
            display_name (instance): instance of :class:`xsd:string`

        '''
        super(LogicalRouterTestFixtureGen, self).__init__()
        self._conn_drv = conn_drv
        if not logical_router_name:
            self._name = 'default-logical-router'
        else:
            self._name = logical_router_name
        self._obj = None
        self._parent_fixt = parent_fixt
        self._auto_prop_val = auto_prop_val
        if virtual_machine_interface_refs:
            for ln in virtual_machine_interface_refs:
                self.add_virtual_machine_interface (ln)
        if route_target_refs:
            for ln in route_target_refs:
                self.add_route_target (ln)
        if virtual_network_refs:
            for ln in virtual_network_refs:
                self.add_virtual_network (ln)
        if service_instance_refs:
            for ln in service_instance_refs:
                self.add_service_instance (ln)
        self.id_perms = id_perms
        self.display_name = display_name
    #end __init__

    def _update_links (self, update_server):
        for ln in self.get_virtual_machine_interfaces ():
            self.add_virtual_machine_interface (ln.fixture (), update_server = update_server, add_link = False)
        for ln in self.get_route_targets ():
            self.add_route_target (ln.fixture (), update_server = update_server, add_link = False)
        for ln in self.get_virtual_networks ():
            self.add_virtual_network (ln.fixture (), update_server = update_server, add_link = False)
        for ln in self.get_service_instances ():
            self.add_service_instance (ln.fixture (), update_server = update_server, add_link = False)
        return None
    #end _update_links

    def add_virtual_machine_interface (self, lo, update_server = True, add_link = True):
        '''
        add :class:`VirtualMachineInterface` link to :class:`LogicalRouter`
        Args:
            lo (:class:`VirtualMachineInterface`): obj to link
        '''
        if self._obj:
            self._obj.add_virtual_machine_interface (lo)
            if update_server:
                self._conn_drv.logical_router_update (self._obj)

        if add_link:
            self.add_link('virtual_machine_interface', cfixture.ConrtailLink('virtual_machine_interface', 'logical_router', 'virtual_machine_interface', ['ref'], lo))
    #end add_virtual_machine_interface_link

    def get_virtual_machine_interfaces (self):
        return self.get_links ('virtual_machine_interface')
    #end get_virtual_machine_interfaces
    def add_route_target (self, lo, update_server = True, add_link = True):
        '''
        add :class:`RouteTarget` link to :class:`LogicalRouter`
        Args:
            lo (:class:`RouteTarget`): obj to link
        '''
        if self._obj:
            self._obj.add_route_target (lo)
            if update_server:
                self._conn_drv.logical_router_update (self._obj)

        if add_link:
            self.add_link('route_target', cfixture.ConrtailLink('route_target', 'logical_router', 'route_target', ['ref'], lo))
    #end add_route_target_link

    def get_route_targets (self):
        return self.get_links ('route_target')
    #end get_route_targets
    def add_virtual_network (self, lo, update_server = True, add_link = True):
        '''
        add :class:`VirtualNetwork` link to :class:`LogicalRouter`
        Args:
            lo (:class:`VirtualNetwork`): obj to link
        '''
        if self._obj:
            self._obj.add_virtual_network (lo)
            if update_server:
                self._conn_drv.logical_router_update (self._obj)

        if add_link:
            self.add_link('virtual_network', cfixture.ConrtailLink('virtual_network', 'logical_router', 'virtual_network', ['ref'], lo))
    #end add_virtual_network_link

    def get_virtual_networks (self):
        return self.get_links ('virtual_network')
    #end get_virtual_networks
    def add_service_instance (self, lo, update_server = True, add_link = True):
        '''
        add :class:`ServiceInstance` link to :class:`LogicalRouter`
        Args:
            lo (:class:`ServiceInstance`): obj to link
        '''
        if self._obj:
            self._obj.add_service_instance (lo)
            if update_server:
                self._conn_drv.logical_router_update (self._obj)

        if add_link:
            self.add_link('service_instance', cfixture.ConrtailLink('service_instance', 'logical_router', 'service_instance', ['ref'], lo))
    #end add_service_instance_link

    def get_service_instances (self):
        return self.get_links ('service_instance')
    #end get_service_instances

    def populate (self):
        self._obj.set_id_perms(self.id_perms or vnc_api.gen.resource_xsd.IdPermsType.populate())
        self._obj.set_display_name(self.display_name or GeneratedsSuper.populate_string("display_name"))
    #end populate

    def setUp(self):
        super(LogicalRouterTestFixtureGen, self).setUp()
        if not self._parent_fixt:
            self._parent_fixt = self.useFixture(ProjectTestFixtureGen(self._conn_drv, 'default-project'))

        self._obj = vnc_api.LogicalRouter(self._name, self._parent_fixt.getObj ())
        try:
            self._obj = self._conn_drv.logical_router_read (fq_name=self._obj.get_fq_name())
            self._update_links (update_server=True)
        except NoIdError:
            self._update_links (update_server=False)
            if self._auto_prop_val:
                self.populate ()
            else:
                self._obj.id_perms = self.id_perms
                self._obj.display_name = self.display_name
            self._conn_drv.logical_router_create(self._obj)
            # read back for server allocated values
            self._obj = self._conn_drv.logical_router_read(id = self._obj.uuid)
    #end setUp

    def cleanUp(self):
        parent_fixt = getattr(self, '_parent_fixt', None)
        if parent_fixt:
            # non config-root child
            parent_obj = self._parent_fixt.getObj()
            # remove child from parent obj
            for child_obj in parent_obj.get_logical_routers():
                if type(child_obj) == dict:
                    child_uuid = child_obj['uuid']
                else:
                    child_uuid = child_obj.uuid
                if child_uuid == self._obj.uuid:
                    parent_obj.logical_routers.remove(child_obj)
                    break

        self._conn_drv.logical_router_delete(id = self._obj.uuid)
    #end cleanUp

    def getObj(self):
        return self._obj
    #end getObj

#end class LogicalRouterTestFixtureGen

