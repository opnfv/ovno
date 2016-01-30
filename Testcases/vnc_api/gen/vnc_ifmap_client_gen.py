
# AUTO-GENERATED file from IFMapApiGenerator. Do Not Edit!

import re
import json
import cStringIO
from lxml import etree

from cfgm_common.ifmap.client import client, namespaces
from cfgm_common.ifmap.request import NewSessionRequest, RenewSessionRequest
from cfgm_common.ifmap.request import EndSessionRequest, PublishRequest
from cfgm_common.ifmap.request import SearchRequest, SubscribeRequest, PurgeRequest, PollRequest
from cfgm_common.ifmap.id import IPAddress, MACAddress, Device, AccessRequest, Identity, CustomIdentity
from cfgm_common.ifmap.operations import PublishUpdateOperation, PublishNotifyOperation
from cfgm_common.ifmap.operations import PublishDeleteOperation, SubscribeUpdateOperation, SubscribeDeleteOperation
from cfgm_common.ifmap.util import attr, link_ids
from cfgm_common.ifmap.response import Response, newSessionResult
from cfgm_common.ifmap.metadata import Metadata

import cfgm_common.imid
import cfgm_common.exceptions
from cfgm_common.imid import escape
from resource_xsd import *

class VncIfmapClientGen(object):
    def __init__(self):
        self._parent_metas = {}
        self._parent_metas['domain'] = {}
        self._parent_metas['domain']['project'] = 'domain-project'
        self._parent_metas['domain']['namespace'] = 'domain-namespace'
        self._parent_metas['domain']['service-template'] = 'domain-service-template'
        self._parent_metas['domain']['virtual-DNS'] = 'domain-virtual-DNS'
        self._parent_metas['global-vrouter-config'] = {}
        self._parent_metas['instance-ip'] = {}
        self._parent_metas['network-policy'] = {}
        self._parent_metas['loadbalancer-pool'] = {}
        self._parent_metas['loadbalancer-pool']['loadbalancer-member'] = 'loadbalancer-pool-loadbalancer-member'
        self._parent_metas['virtual-DNS-record'] = {}
        self._parent_metas['route-target'] = {}
        self._parent_metas['floating-ip'] = {}
        self._parent_metas['floating-ip-pool'] = {}
        self._parent_metas['floating-ip-pool']['floating-ip'] = 'floating-ip-pool-floating-ip'
        self._parent_metas['physical-router'] = {}
        self._parent_metas['physical-router']['physical-interface'] = 'physical-router-physical-interface'
        self._parent_metas['physical-router']['logical-interface'] = 'physical-router-logical-interface'
        self._parent_metas['bgp-router'] = {}
        self._parent_metas['virtual-router'] = {}
        self._parent_metas['config-root'] = {}
        self._parent_metas['config-root']['global-system-config'] = 'config-root-global-system-config'
        self._parent_metas['config-root']['domain'] = 'config-root-domain'
        self._parent_metas['subnet'] = {}
        self._parent_metas['global-system-config'] = {}
        self._parent_metas['global-system-config']['global-vrouter-config'] = 'global-system-config-global-vrouter-config'
        self._parent_metas['global-system-config']['physical-router'] = 'global-system-config-physical-router'
        self._parent_metas['global-system-config']['virtual-router'] = 'global-system-config-virtual-router'
        self._parent_metas['global-system-config']['config-node'] = 'global-system-config-config-node'
        self._parent_metas['global-system-config']['analytics-node'] = 'global-system-config-analytics-node'
        self._parent_metas['global-system-config']['database-node'] = 'global-system-config-database-node'
        self._parent_metas['global-system-config']['service-appliance-set'] = 'global-system-config-service-appliance-set'
        self._parent_metas['service-appliance'] = {}
        self._parent_metas['service-instance'] = {}
        self._parent_metas['namespace'] = {}
        self._parent_metas['logical-interface'] = {}
        self._parent_metas['route-table'] = {}
        self._parent_metas['physical-interface'] = {}
        self._parent_metas['physical-interface']['logical-interface'] = 'physical-interface-logical-interface'
        self._parent_metas['access-control-list'] = {}
        self._parent_metas['analytics-node'] = {}
        self._parent_metas['virtual-DNS'] = {}
        self._parent_metas['virtual-DNS']['virtual-DNS-record'] = 'virtual-DNS-virtual-DNS-record'
        self._parent_metas['customer-attachment'] = {}
        self._parent_metas['service-appliance-set'] = {}
        self._parent_metas['service-appliance-set']['service-appliance'] = 'service-appliance-set-service-appliance'
        self._parent_metas['config-node'] = {}
        self._parent_metas['qos-queue'] = {}
        self._parent_metas['virtual-machine'] = {}
        self._parent_metas['virtual-machine']['virtual-machine-interface'] = 'virtual-machine-virtual-machine-interface'
        self._parent_metas['interface-route-table'] = {}
        self._parent_metas['service-template'] = {}
        self._parent_metas['virtual-ip'] = {}
        self._parent_metas['loadbalancer-member'] = {}
        self._parent_metas['security-group'] = {}
        self._parent_metas['security-group']['access-control-list'] = 'security-group-access-control-list'
        self._parent_metas['provider-attachment'] = {}
        self._parent_metas['virtual-machine-interface'] = {}
        self._parent_metas['loadbalancer-healthmonitor'] = {}
        self._parent_metas['virtual-network'] = {}
        self._parent_metas['virtual-network']['access-control-list'] = 'virtual-network-access-control-list'
        self._parent_metas['virtual-network']['floating-ip-pool'] = 'virtual-network-floating-ip-pool'
        self._parent_metas['virtual-network']['routing-instance'] = 'virtual-network-routing-instance'
        self._parent_metas['project'] = {}
        self._parent_metas['project']['security-group'] = 'project-security-group'
        self._parent_metas['project']['virtual-network'] = 'project-virtual-network'
        self._parent_metas['project']['qos-queue'] = 'project-qos-queue'
        self._parent_metas['project']['qos-forwarding-class'] = 'project-qos-forwarding-class'
        self._parent_metas['project']['network-ipam'] = 'project-network-ipam'
        self._parent_metas['project']['network-policy'] = 'project-network-policy'
        self._parent_metas['project']['virtual-machine-interface'] = 'project-virtual-machine-interface'
        self._parent_metas['project']['service-instance'] = 'project-service-instance'
        self._parent_metas['project']['route-table'] = 'project-route-table'
        self._parent_metas['project']['interface-route-table'] = 'project-interface-route-table'
        self._parent_metas['project']['logical-router'] = 'project-logical-router'
        self._parent_metas['project']['loadbalancer-pool'] = 'project-loadbalancer-pool'
        self._parent_metas['project']['loadbalancer-healthmonitor'] = 'project-loadbalancer-healthmonitor'
        self._parent_metas['project']['virtual-ip'] = 'project-virtual-ip'
        self._parent_metas['qos-forwarding-class'] = {}
        self._parent_metas['database-node'] = {}
        self._parent_metas['routing-instance'] = {}
        self._parent_metas['routing-instance']['bgp-router'] = 'instance-bgp-router'
        self._parent_metas['network-ipam'] = {}
        self._parent_metas['logical-router'] = {}
    #end __init__

    def _ifmap_domain_alloc(self, parent_type, fq_name):
        imid = self._imid_handler
        (my_imid, parent_imid) = \
            imid.domain_alloc_ifmap_id(parent_type, fq_name)
        if my_imid is None or parent_imid is None:
            return (False, (my_imid, parent_imid))
        return (True, (my_imid, parent_imid))
    #end _ifmap_domain_alloc

    def _ifmap_domain_set(self, my_imid, existing_metas, obj_dict):
        # Properties Meta
        update = {}
        field = obj_dict.get('domain_limits', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['domain_limits']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                DomainLimitsType(**field).exportChildren(buf, level = 1, name_ = 'domain-limits', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'domain-limits', pretty_print = False)
            domain_limits_xml = buf.getvalue()
            buf.close()
            meta = Metadata('domain-limits' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = domain_limits_xml)

            if (existing_metas and 'domain-limits' in existing_metas and
                str(existing_metas['domain-limits'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('api_access_list', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['api_access_list']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                ApiAccessListType(**field).exportChildren(buf, level = 1, name_ = 'api-access-list', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'api-access-list', pretty_print = False)
            api_access_list_xml = buf.getvalue()
            buf.close()
            meta = Metadata('api-access-list' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = api_access_list_xml)

            if (existing_metas and 'api-access-list' in existing_metas and
                str(existing_metas['api-access-list'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['id_perms']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                IdPermsType(**field).exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            id_perms_xml = buf.getvalue()
            buf.close()
            meta = Metadata('id-perms' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = id_perms_xml)

            if (existing_metas and 'id-perms' in existing_metas and
                str(existing_metas['id-perms'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('display_name', None)
        if field is not None:
            norm_str = escape(str(obj_dict['display_name']))
            meta = Metadata('display-name', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'display-name' in existing_metas and
                str(existing_metas['display-name'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        # Ref Link Metas
        imid = self._imid_handler

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('project_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'project'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                domain_project_xml = ''
                meta = Metadata('domain-project' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = domain_project_xml)
                self._update_id_pair_meta(update, to_imid, meta)

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('namespace_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'namespace'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                domain_namespace_xml = ''
                meta = Metadata('domain-namespace' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = domain_namespace_xml)
                self._update_id_pair_meta(update, to_imid, meta)

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('service_template_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'service-template'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                domain_service_template_xml = ''
                meta = Metadata('domain-service-template' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = domain_service_template_xml)
                self._update_id_pair_meta(update, to_imid, meta)

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('virtual_DNS_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'virtual-DNS'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                domain_virtual_DNS_xml = ''
                meta = Metadata('domain-virtual-DNS' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = domain_virtual_DNS_xml)
                self._update_id_pair_meta(update, to_imid, meta)


        self._publish_update(my_imid, update)
        return (True, '')
    #end _ifmap_domain_set

    def _ifmap_domain_create(self, obj_ids, obj_dict):
        if not 'parent_type' in obj_dict:
            # parent is config-root
            parent_type = 'config-root'
            parent_imid = 'contrail:config-root:root'
        else:
            parent_type = obj_dict['parent_type']
            parent_imid = obj_ids.get('parent_imid', None)

        # Parent Link Meta
        update = {}
        parent_link_meta = self._parent_metas[parent_type]['domain']
        meta = Metadata(parent_link_meta, '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')
        self._update_id_pair_meta(update, obj_ids['imid'], meta)
        self._publish_update(parent_imid, update)

        (ok, result) = self._ifmap_domain_set(obj_ids['imid'], None, obj_dict)
        return (ok, result)
    #end _ifmap_domain_create


    def _ifmap_domain_read_to_meta_index(self, ifmap_id, field_names = None):
        # field_names = None means all fields will be read
        imid = self._imid_handler
        start_id = str(Identity(name = ifmap_id, type = 'other',
                        other_type = 'extended'))
        # if id-perms missing, identity doesn't exist

        all_metas = [u'domain-limits', u'api-access-list', u'id-perms', u'display-name', u'domain-project', u'domain-namespace', u'domain-service-template', u'domain-virtual-DNS']
        if not field_names:
            metas_to_read = all_metas
        else: # read only requested fields
            metas_to_read = set(all_metas) & set(field_names.keys())

        # metas is a dict where key is meta-name and val is list of dict
        # of form [{'meta':meta}, {'id':id1, 'meta':meta}, {'id':id2, 'meta':meta}]
        metas = {}
        for meta_name in metas_to_read:
            if meta_name in self._id_to_metas[ifmap_id]:
                metas[meta_name] = self._id_to_metas[ifmap_id][meta_name]
        return metas
    #end _ifmap_domain_read_to_meta_index

    def _ifmap_domain_update(self, ifmap_id, new_obj_dict):
        # read in refs from ifmap to determine which ones become inactive after update
        existing_metas = self._ifmap_domain_read_to_meta_index(ifmap_id)

        # remove properties that are no longer active
        props = ['domain-limits', 'api-access-list', 'id-perms', 'display-name']
        for prop in props:
            prop_m = prop.replace('-', '_')
            if prop in existing_metas and prop_m not in new_obj_dict:
                self._delete_id_self_meta(ifmap_id, 'contrail:'+prop)
        # remove refs that are no longer active
        delete_list = []
        refs = {}
        for meta, to_name in refs.items():
            old_set = set([m['id'] for m in existing_metas.get(meta, [])])
            new_set = set()
            to_name_m = to_name.replace('-', '_')
            for ref in new_obj_dict.get(to_name_m+'_refs', []):
                to_imid = self.fq_name_to_ifmap_id(to_name, ref['to'])
                new_set.add(to_imid)

            for inact_ref in old_set - new_set:
                delete_list.append((inact_ref, 'contrail:'+meta))

        if delete_list:
            self._delete_id_pair_meta_list(ifmap_id, delete_list)

        (ok, result) = self._ifmap_domain_set(ifmap_id, existing_metas, new_obj_dict)
        return (ok, result)
    #end _ifmap_domain_update

    def _ifmap_domain_delete(self, obj_ids):
        ifmap_id = obj_ids['imid']
        parent_imid = obj_ids.get('parent_imid', None)
        existing_metas = self._ifmap_domain_read_to_meta_index(ifmap_id)
        meta_list = []
        for meta_name, meta_infos in existing_metas.items():
            for meta_info in meta_infos:
                ref_imid = meta_info.get('id')
                if ref_imid is None:
                    continue
                meta_list.append((ref_imid, 'contrail:'+meta_name))

        if parent_imid:
            # Remove link from parent
            meta_list.append((parent_imid, None))

        if meta_list:
            self._delete_id_pair_meta_list(ifmap_id, meta_list)

        # Remove all property metadata associated with this ident
        self._delete_id_self_meta(ifmap_id, None)

        return (True, '')
    #end _ifmap_domain_delete

    def _ifmap_global_vrouter_config_alloc(self, parent_type, fq_name):
        imid = self._imid_handler
        (my_imid, parent_imid) = \
            imid.global_vrouter_config_alloc_ifmap_id(parent_type, fq_name)
        if my_imid is None or parent_imid is None:
            return (False, (my_imid, parent_imid))
        return (True, (my_imid, parent_imid))
    #end _ifmap_global_vrouter_config_alloc

    def _ifmap_global_vrouter_config_set(self, my_imid, existing_metas, obj_dict):
        # Properties Meta
        update = {}
        field = obj_dict.get('linklocal_services', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['linklocal_services']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                LinklocalServicesTypes(**field).exportChildren(buf, level = 1, name_ = 'linklocal-services', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'linklocal-services', pretty_print = False)
            linklocal_services_xml = buf.getvalue()
            buf.close()
            meta = Metadata('linklocal-services' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = linklocal_services_xml)

            if (existing_metas and 'linklocal-services' in existing_metas and
                str(existing_metas['linklocal-services'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('encapsulation_priorities', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['encapsulation_priorities']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                EncapsulationPrioritiesType(**field).exportChildren(buf, level = 1, name_ = 'encapsulation-priorities', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'encapsulation-priorities', pretty_print = False)
            encapsulation_priorities_xml = buf.getvalue()
            buf.close()
            meta = Metadata('encapsulation-priorities' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = encapsulation_priorities_xml)

            if (existing_metas and 'encapsulation-priorities' in existing_metas and
                str(existing_metas['encapsulation-priorities'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('vxlan_network_identifier_mode', None)
        if field is not None:
            norm_str = escape(str(obj_dict['vxlan_network_identifier_mode']))
            meta = Metadata('vxlan-network-identifier-mode', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'vxlan-network-identifier-mode' in existing_metas and
                str(existing_metas['vxlan-network-identifier-mode'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['id_perms']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                IdPermsType(**field).exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            id_perms_xml = buf.getvalue()
            buf.close()
            meta = Metadata('id-perms' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = id_perms_xml)

            if (existing_metas and 'id-perms' in existing_metas and
                str(existing_metas['id-perms'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('display_name', None)
        if field is not None:
            norm_str = escape(str(obj_dict['display_name']))
            meta = Metadata('display-name', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'display-name' in existing_metas and
                str(existing_metas['display-name'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        # Ref Link Metas
        imid = self._imid_handler


        self._publish_update(my_imid, update)
        return (True, '')
    #end _ifmap_global_vrouter_config_set

    def _ifmap_global_vrouter_config_create(self, obj_ids, obj_dict):
        if not 'parent_type' in obj_dict:
            # parent is config-root
            parent_type = 'config-root'
            parent_imid = 'contrail:config-root:root'
        else:
            parent_type = obj_dict['parent_type']
            parent_imid = obj_ids.get('parent_imid', None)

        # Parent Link Meta
        update = {}
        parent_link_meta = self._parent_metas[parent_type]['global-vrouter-config']
        meta = Metadata(parent_link_meta, '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')
        self._update_id_pair_meta(update, obj_ids['imid'], meta)
        self._publish_update(parent_imid, update)

        (ok, result) = self._ifmap_global_vrouter_config_set(obj_ids['imid'], None, obj_dict)
        return (ok, result)
    #end _ifmap_global_vrouter_config_create


    def _ifmap_global_vrouter_config_read_to_meta_index(self, ifmap_id, field_names = None):
        # field_names = None means all fields will be read
        imid = self._imid_handler
        start_id = str(Identity(name = ifmap_id, type = 'other',
                        other_type = 'extended'))
        # if id-perms missing, identity doesn't exist

        all_metas = [u'linklocal-services', u'encapsulation-priorities', u'vxlan-network-identifier-mode', u'id-perms', u'display-name']
        if not field_names:
            metas_to_read = all_metas
        else: # read only requested fields
            metas_to_read = set(all_metas) & set(field_names.keys())

        # metas is a dict where key is meta-name and val is list of dict
        # of form [{'meta':meta}, {'id':id1, 'meta':meta}, {'id':id2, 'meta':meta}]
        metas = {}
        for meta_name in metas_to_read:
            if meta_name in self._id_to_metas[ifmap_id]:
                metas[meta_name] = self._id_to_metas[ifmap_id][meta_name]
        return metas
    #end _ifmap_global-vrouter-config_read_to_meta_index

    def _ifmap_global_vrouter_config_update(self, ifmap_id, new_obj_dict):
        # read in refs from ifmap to determine which ones become inactive after update
        existing_metas = self._ifmap_global_vrouter_config_read_to_meta_index(ifmap_id)

        # remove properties that are no longer active
        props = ['linklocal-services', 'encapsulation-priorities', 'vxlan-network-identifier-mode', 'id-perms', 'display-name']
        for prop in props:
            prop_m = prop.replace('-', '_')
            if prop in existing_metas and prop_m not in new_obj_dict:
                self._delete_id_self_meta(ifmap_id, 'contrail:'+prop)
        # remove refs that are no longer active
        delete_list = []
        refs = {}
        for meta, to_name in refs.items():
            old_set = set([m['id'] for m in existing_metas.get(meta, [])])
            new_set = set()
            to_name_m = to_name.replace('-', '_')
            for ref in new_obj_dict.get(to_name_m+'_refs', []):
                to_imid = self.fq_name_to_ifmap_id(to_name, ref['to'])
                new_set.add(to_imid)

            for inact_ref in old_set - new_set:
                delete_list.append((inact_ref, 'contrail:'+meta))

        if delete_list:
            self._delete_id_pair_meta_list(ifmap_id, delete_list)

        (ok, result) = self._ifmap_global_vrouter_config_set(ifmap_id, existing_metas, new_obj_dict)
        return (ok, result)
    #end _ifmap_global_vrouter_config_update

    def _ifmap_global_vrouter_config_delete(self, obj_ids):
        ifmap_id = obj_ids['imid']
        parent_imid = obj_ids.get('parent_imid', None)
        existing_metas = self._ifmap_global_vrouter_config_read_to_meta_index(ifmap_id)
        meta_list = []
        for meta_name, meta_infos in existing_metas.items():
            for meta_info in meta_infos:
                ref_imid = meta_info.get('id')
                if ref_imid is None:
                    continue
                meta_list.append((ref_imid, 'contrail:'+meta_name))

        if parent_imid:
            # Remove link from parent
            meta_list.append((parent_imid, None))

        if meta_list:
            self._delete_id_pair_meta_list(ifmap_id, meta_list)

        # Remove all property metadata associated with this ident
        self._delete_id_self_meta(ifmap_id, None)

        return (True, '')
    #end _ifmap_global_vrouter_config_delete

    def _ifmap_instance_ip_alloc(self, parent_type, fq_name):
        imid = self._imid_handler
        (my_imid, parent_imid) = \
            imid.instance_ip_alloc_ifmap_id(parent_type, fq_name)
        if my_imid is None or parent_imid is None:
            return (False, (my_imid, parent_imid))
        return (True, (my_imid, parent_imid))
    #end _ifmap_instance_ip_alloc

    def _ifmap_instance_ip_set(self, my_imid, existing_metas, obj_dict):
        # Properties Meta
        update = {}
        field = obj_dict.get('instance_ip_address', None)
        if field is not None:
            norm_str = escape(str(obj_dict['instance_ip_address']))
            meta = Metadata('instance-ip-address', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'instance-ip-address' in existing_metas and
                str(existing_metas['instance-ip-address'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('instance_ip_family', None)
        if field is not None:
            norm_str = escape(str(obj_dict['instance_ip_family']))
            meta = Metadata('instance-ip-family', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'instance-ip-family' in existing_metas and
                str(existing_metas['instance-ip-family'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('instance_ip_mode', None)
        if field is not None:
            norm_str = escape(str(obj_dict['instance_ip_mode']))
            meta = Metadata('instance-ip-mode', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'instance-ip-mode' in existing_metas and
                str(existing_metas['instance-ip-mode'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('subnet_uuid', None)
        if field is not None:
            norm_str = escape(str(obj_dict['subnet_uuid']))
            meta = Metadata('subnet-uuid', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'subnet-uuid' in existing_metas and
                str(existing_metas['subnet-uuid'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['id_perms']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                IdPermsType(**field).exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            id_perms_xml = buf.getvalue()
            buf.close()
            meta = Metadata('id-perms' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = id_perms_xml)

            if (existing_metas and 'id-perms' in existing_metas and
                str(existing_metas['id-perms'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('display_name', None)
        if field is not None:
            norm_str = escape(str(obj_dict['display_name']))
            meta = Metadata('display-name', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'display-name' in existing_metas and
                str(existing_metas['display-name'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        # Ref Link Metas
        imid = self._imid_handler

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('virtual_network_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'virtual-network'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                instance_ip_virtual_network_xml = ''
                meta = Metadata('instance-ip-virtual-network' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = instance_ip_virtual_network_xml)
                self._update_id_pair_meta(update, to_imid, meta)

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('virtual_machine_interface_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'virtual-machine-interface'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                instance_ip_virtual_machine_interface_xml = ''
                meta = Metadata('instance-ip-virtual-machine-interface' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = instance_ip_virtual_machine_interface_xml)
                self._update_id_pair_meta(update, to_imid, meta)


        self._publish_update(my_imid, update)
        return (True, '')
    #end _ifmap_instance_ip_set

    def _ifmap_instance_ip_create(self, obj_ids, obj_dict):
        (ok, result) = self._ifmap_instance_ip_set(obj_ids['imid'], None, obj_dict)
        return (ok, result)
    #end _ifmap_instance_ip_create


    def _ifmap_instance_ip_read_to_meta_index(self, ifmap_id, field_names = None):
        # field_names = None means all fields will be read
        imid = self._imid_handler
        start_id = str(Identity(name = ifmap_id, type = 'other',
                        other_type = 'extended'))
        # if id-perms missing, identity doesn't exist

        all_metas = [u'instance-ip-virtual-network', u'instance-ip-virtual-machine-interface', u'instance-ip-address', u'instance-ip-family', u'instance-ip-mode', u'subnet-uuid', u'id-perms', u'display-name']
        if not field_names:
            metas_to_read = all_metas
        else: # read only requested fields
            metas_to_read = set(all_metas) & set(field_names.keys())

        # metas is a dict where key is meta-name and val is list of dict
        # of form [{'meta':meta}, {'id':id1, 'meta':meta}, {'id':id2, 'meta':meta}]
        metas = {}
        for meta_name in metas_to_read:
            if meta_name in self._id_to_metas[ifmap_id]:
                metas[meta_name] = self._id_to_metas[ifmap_id][meta_name]
        return metas
    #end _ifmap_instance-ip_read_to_meta_index

    def _ifmap_instance_ip_update(self, ifmap_id, new_obj_dict):
        # read in refs from ifmap to determine which ones become inactive after update
        existing_metas = self._ifmap_instance_ip_read_to_meta_index(ifmap_id)

        # remove properties that are no longer active
        props = ['instance-ip-address', 'instance-ip-family', 'instance-ip-mode', 'subnet-uuid', 'id-perms', 'display-name']
        for prop in props:
            prop_m = prop.replace('-', '_')
            if prop in existing_metas and prop_m not in new_obj_dict:
                self._delete_id_self_meta(ifmap_id, 'contrail:'+prop)
        # remove refs that are no longer active
        delete_list = []
        refs = {'instance-ip-virtual-network': 'virtual-network',
                'instance-ip-virtual-machine-interface': 'virtual-machine-interface'}
        for meta, to_name in refs.items():
            old_set = set([m['id'] for m in existing_metas.get(meta, [])])
            new_set = set()
            to_name_m = to_name.replace('-', '_')
            for ref in new_obj_dict.get(to_name_m+'_refs', []):
                to_imid = self.fq_name_to_ifmap_id(to_name, ref['to'])
                new_set.add(to_imid)

            for inact_ref in old_set - new_set:
                delete_list.append((inact_ref, 'contrail:'+meta))

        if delete_list:
            self._delete_id_pair_meta_list(ifmap_id, delete_list)

        (ok, result) = self._ifmap_instance_ip_set(ifmap_id, existing_metas, new_obj_dict)
        return (ok, result)
    #end _ifmap_instance_ip_update

    def _ifmap_instance_ip_delete(self, obj_ids):
        ifmap_id = obj_ids['imid']
        parent_imid = obj_ids.get('parent_imid', None)
        existing_metas = self._ifmap_instance_ip_read_to_meta_index(ifmap_id)
        meta_list = []
        for meta_name, meta_infos in existing_metas.items():
            for meta_info in meta_infos:
                ref_imid = meta_info.get('id')
                if ref_imid is None:
                    continue
                meta_list.append((ref_imid, 'contrail:'+meta_name))

        if parent_imid:
            # Remove link from parent
            meta_list.append((parent_imid, None))

        if meta_list:
            self._delete_id_pair_meta_list(ifmap_id, meta_list)

        # Remove all property metadata associated with this ident
        self._delete_id_self_meta(ifmap_id, None)

        return (True, '')
    #end _ifmap_instance_ip_delete

    def _ifmap_network_policy_alloc(self, parent_type, fq_name):
        imid = self._imid_handler
        (my_imid, parent_imid) = \
            imid.network_policy_alloc_ifmap_id(parent_type, fq_name)
        if my_imid is None or parent_imid is None:
            return (False, (my_imid, parent_imid))
        return (True, (my_imid, parent_imid))
    #end _ifmap_network_policy_alloc

    def _ifmap_network_policy_set(self, my_imid, existing_metas, obj_dict):
        # Properties Meta
        update = {}
        field = obj_dict.get('network_policy_entries', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['network_policy_entries']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                PolicyEntriesType(**field).exportChildren(buf, level = 1, name_ = 'network-policy-entries', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'network-policy-entries', pretty_print = False)
            network_policy_entries_xml = buf.getvalue()
            buf.close()
            meta = Metadata('network-policy-entries' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = network_policy_entries_xml)

            if (existing_metas and 'network-policy-entries' in existing_metas and
                str(existing_metas['network-policy-entries'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['id_perms']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                IdPermsType(**field).exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            id_perms_xml = buf.getvalue()
            buf.close()
            meta = Metadata('id-perms' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = id_perms_xml)

            if (existing_metas and 'id-perms' in existing_metas and
                str(existing_metas['id-perms'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('display_name', None)
        if field is not None:
            norm_str = escape(str(obj_dict['display_name']))
            meta = Metadata('display-name', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'display-name' in existing_metas and
                str(existing_metas['display-name'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        # Ref Link Metas
        imid = self._imid_handler


        self._publish_update(my_imid, update)
        return (True, '')
    #end _ifmap_network_policy_set

    def _ifmap_network_policy_create(self, obj_ids, obj_dict):
        if not 'parent_type' in obj_dict:
            # parent is config-root
            parent_type = 'config-root'
            parent_imid = 'contrail:config-root:root'
        else:
            parent_type = obj_dict['parent_type']
            parent_imid = obj_ids.get('parent_imid', None)

        # Parent Link Meta
        update = {}
        parent_link_meta = self._parent_metas[parent_type]['network-policy']
        meta = Metadata(parent_link_meta, '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')
        self._update_id_pair_meta(update, obj_ids['imid'], meta)
        self._publish_update(parent_imid, update)

        (ok, result) = self._ifmap_network_policy_set(obj_ids['imid'], None, obj_dict)
        return (ok, result)
    #end _ifmap_network_policy_create


    def _ifmap_network_policy_read_to_meta_index(self, ifmap_id, field_names = None):
        # field_names = None means all fields will be read
        imid = self._imid_handler
        start_id = str(Identity(name = ifmap_id, type = 'other',
                        other_type = 'extended'))
        # if id-perms missing, identity doesn't exist

        all_metas = [u'virtual-network-network-policy', u'network-policy-entries', u'id-perms', u'display-name']
        if not field_names:
            metas_to_read = all_metas
        else: # read only requested fields
            metas_to_read = set(all_metas) & set(field_names.keys())

        # metas is a dict where key is meta-name and val is list of dict
        # of form [{'meta':meta}, {'id':id1, 'meta':meta}, {'id':id2, 'meta':meta}]
        metas = {}
        for meta_name in metas_to_read:
            if meta_name in self._id_to_metas[ifmap_id]:
                metas[meta_name] = self._id_to_metas[ifmap_id][meta_name]
        return metas
    #end _ifmap_network-policy_read_to_meta_index

    def _ifmap_network_policy_update(self, ifmap_id, new_obj_dict):
        # read in refs from ifmap to determine which ones become inactive after update
        existing_metas = self._ifmap_network_policy_read_to_meta_index(ifmap_id)

        # remove properties that are no longer active
        props = ['network-policy-entries', 'id-perms', 'display-name']
        for prop in props:
            prop_m = prop.replace('-', '_')
            if prop in existing_metas and prop_m not in new_obj_dict:
                self._delete_id_self_meta(ifmap_id, 'contrail:'+prop)
        # remove refs that are no longer active
        delete_list = []
        refs = {}
        for meta, to_name in refs.items():
            old_set = set([m['id'] for m in existing_metas.get(meta, [])])
            new_set = set()
            to_name_m = to_name.replace('-', '_')
            for ref in new_obj_dict.get(to_name_m+'_refs', []):
                to_imid = self.fq_name_to_ifmap_id(to_name, ref['to'])
                new_set.add(to_imid)

            for inact_ref in old_set - new_set:
                delete_list.append((inact_ref, 'contrail:'+meta))

        if delete_list:
            self._delete_id_pair_meta_list(ifmap_id, delete_list)

        (ok, result) = self._ifmap_network_policy_set(ifmap_id, existing_metas, new_obj_dict)
        return (ok, result)
    #end _ifmap_network_policy_update

    def _ifmap_network_policy_delete(self, obj_ids):
        ifmap_id = obj_ids['imid']
        parent_imid = obj_ids.get('parent_imid', None)
        existing_metas = self._ifmap_network_policy_read_to_meta_index(ifmap_id)
        meta_list = []
        for meta_name, meta_infos in existing_metas.items():
            for meta_info in meta_infos:
                ref_imid = meta_info.get('id')
                if ref_imid is None:
                    continue
                meta_list.append((ref_imid, 'contrail:'+meta_name))

        if parent_imid:
            # Remove link from parent
            meta_list.append((parent_imid, None))

        if meta_list:
            self._delete_id_pair_meta_list(ifmap_id, meta_list)

        # Remove all property metadata associated with this ident
        self._delete_id_self_meta(ifmap_id, None)

        return (True, '')
    #end _ifmap_network_policy_delete

    def _ifmap_loadbalancer_pool_alloc(self, parent_type, fq_name):
        imid = self._imid_handler
        (my_imid, parent_imid) = \
            imid.loadbalancer_pool_alloc_ifmap_id(parent_type, fq_name)
        if my_imid is None or parent_imid is None:
            return (False, (my_imid, parent_imid))
        return (True, (my_imid, parent_imid))
    #end _ifmap_loadbalancer_pool_alloc

    def _ifmap_loadbalancer_pool_set(self, my_imid, existing_metas, obj_dict):
        # Properties Meta
        update = {}
        field = obj_dict.get('loadbalancer_pool_properties', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['loadbalancer_pool_properties']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                LoadbalancerPoolType(**field).exportChildren(buf, level = 1, name_ = 'loadbalancer-pool-properties', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'loadbalancer-pool-properties', pretty_print = False)
            loadbalancer_pool_properties_xml = buf.getvalue()
            buf.close()
            meta = Metadata('loadbalancer-pool-properties' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = loadbalancer_pool_properties_xml)

            if (existing_metas and 'loadbalancer-pool-properties' in existing_metas and
                str(existing_metas['loadbalancer-pool-properties'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('loadbalancer_pool_provider', None)
        if field is not None:
            norm_str = escape(str(obj_dict['loadbalancer_pool_provider']))
            meta = Metadata('loadbalancer-pool-provider', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'loadbalancer-pool-provider' in existing_metas and
                str(existing_metas['loadbalancer-pool-provider'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['id_perms']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                IdPermsType(**field).exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            id_perms_xml = buf.getvalue()
            buf.close()
            meta = Metadata('id-perms' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = id_perms_xml)

            if (existing_metas and 'id-perms' in existing_metas and
                str(existing_metas['id-perms'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('display_name', None)
        if field is not None:
            norm_str = escape(str(obj_dict['display_name']))
            meta = Metadata('display-name', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'display-name' in existing_metas and
                str(existing_metas['display-name'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        # Ref Link Metas
        imid = self._imid_handler

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('service_instance_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'service-instance'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                loadbalancer_pool_service_instance_xml = ''
                meta = Metadata('loadbalancer-pool-service-instance' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = loadbalancer_pool_service_instance_xml)
                self._update_id_pair_meta(update, to_imid, meta)

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('virtual_machine_interface_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'virtual-machine-interface'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                loadbalancer_pool_virtual_machine_interface_xml = ''
                meta = Metadata('loadbalancer-pool-virtual-machine-interface' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = loadbalancer_pool_virtual_machine_interface_xml)
                self._update_id_pair_meta(update, to_imid, meta)

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('service_appliance_set_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'service-appliance-set'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                loadbalancer_pool_service_appliance_set_xml = ''
                meta = Metadata('loadbalancer-pool-service-appliance-set' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = loadbalancer_pool_service_appliance_set_xml)
                self._update_id_pair_meta(update, to_imid, meta)

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('loadbalancer_member_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'loadbalancer-member'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                loadbalancer_pool_loadbalancer_member_xml = ''
                meta = Metadata('loadbalancer-pool-loadbalancer-member' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = loadbalancer_pool_loadbalancer_member_xml)
                self._update_id_pair_meta(update, to_imid, meta)

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('loadbalancer_healthmonitor_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'loadbalancer-healthmonitor'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                loadbalancer_pool_loadbalancer_healthmonitor_xml = ''
                meta = Metadata('loadbalancer-pool-loadbalancer-healthmonitor' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = loadbalancer_pool_loadbalancer_healthmonitor_xml)
                self._update_id_pair_meta(update, to_imid, meta)


        self._publish_update(my_imid, update)
        return (True, '')
    #end _ifmap_loadbalancer_pool_set

    def _ifmap_loadbalancer_pool_create(self, obj_ids, obj_dict):
        if not 'parent_type' in obj_dict:
            # parent is config-root
            parent_type = 'config-root'
            parent_imid = 'contrail:config-root:root'
        else:
            parent_type = obj_dict['parent_type']
            parent_imid = obj_ids.get('parent_imid', None)

        # Parent Link Meta
        update = {}
        parent_link_meta = self._parent_metas[parent_type]['loadbalancer-pool']
        meta = Metadata(parent_link_meta, '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')
        self._update_id_pair_meta(update, obj_ids['imid'], meta)
        self._publish_update(parent_imid, update)

        (ok, result) = self._ifmap_loadbalancer_pool_set(obj_ids['imid'], None, obj_dict)
        return (ok, result)
    #end _ifmap_loadbalancer_pool_create


    def _ifmap_loadbalancer_pool_read_to_meta_index(self, ifmap_id, field_names = None):
        # field_names = None means all fields will be read
        imid = self._imid_handler
        start_id = str(Identity(name = ifmap_id, type = 'other',
                        other_type = 'extended'))
        # if id-perms missing, identity doesn't exist

        all_metas = [u'loadbalancer-pool-service-instance', u'loadbalancer-pool-virtual-machine-interface', u'loadbalancer-pool-service-appliance-set', u'loadbalancer-pool-loadbalancer-healthmonitor', u'virtual-ip-loadbalancer-pool', u'loadbalancer-pool-properties', u'loadbalancer-pool-provider', u'id-perms', u'display-name', u'loadbalancer-pool-loadbalancer-member']
        if not field_names:
            metas_to_read = all_metas
        else: # read only requested fields
            metas_to_read = set(all_metas) & set(field_names.keys())

        # metas is a dict where key is meta-name and val is list of dict
        # of form [{'meta':meta}, {'id':id1, 'meta':meta}, {'id':id2, 'meta':meta}]
        metas = {}
        for meta_name in metas_to_read:
            if meta_name in self._id_to_metas[ifmap_id]:
                metas[meta_name] = self._id_to_metas[ifmap_id][meta_name]
        return metas
    #end _ifmap_loadbalancer-pool_read_to_meta_index

    def _ifmap_loadbalancer_pool_update(self, ifmap_id, new_obj_dict):
        # read in refs from ifmap to determine which ones become inactive after update
        existing_metas = self._ifmap_loadbalancer_pool_read_to_meta_index(ifmap_id)

        # remove properties that are no longer active
        props = ['loadbalancer-pool-properties', 'loadbalancer-pool-provider', 'id-perms', 'display-name']
        for prop in props:
            prop_m = prop.replace('-', '_')
            if prop in existing_metas and prop_m not in new_obj_dict:
                self._delete_id_self_meta(ifmap_id, 'contrail:'+prop)
        # remove refs that are no longer active
        delete_list = []
        refs = {'loadbalancer-pool-service-instance': 'service-instance',
                'loadbalancer-pool-virtual-machine-interface': 'virtual-machine-interface',
                'loadbalancer-pool-service-appliance-set': 'service-appliance-set',
                'loadbalancer-pool-loadbalancer-healthmonitor': 'loadbalancer-healthmonitor'}
        for meta, to_name in refs.items():
            old_set = set([m['id'] for m in existing_metas.get(meta, [])])
            new_set = set()
            to_name_m = to_name.replace('-', '_')
            for ref in new_obj_dict.get(to_name_m+'_refs', []):
                to_imid = self.fq_name_to_ifmap_id(to_name, ref['to'])
                new_set.add(to_imid)

            for inact_ref in old_set - new_set:
                delete_list.append((inact_ref, 'contrail:'+meta))

        if delete_list:
            self._delete_id_pair_meta_list(ifmap_id, delete_list)

        (ok, result) = self._ifmap_loadbalancer_pool_set(ifmap_id, existing_metas, new_obj_dict)
        return (ok, result)
    #end _ifmap_loadbalancer_pool_update

    def _ifmap_loadbalancer_pool_delete(self, obj_ids):
        ifmap_id = obj_ids['imid']
        parent_imid = obj_ids.get('parent_imid', None)
        existing_metas = self._ifmap_loadbalancer_pool_read_to_meta_index(ifmap_id)
        meta_list = []
        for meta_name, meta_infos in existing_metas.items():
            for meta_info in meta_infos:
                ref_imid = meta_info.get('id')
                if ref_imid is None:
                    continue
                meta_list.append((ref_imid, 'contrail:'+meta_name))

        if parent_imid:
            # Remove link from parent
            meta_list.append((parent_imid, None))

        if meta_list:
            self._delete_id_pair_meta_list(ifmap_id, meta_list)

        # Remove all property metadata associated with this ident
        self._delete_id_self_meta(ifmap_id, None)

        return (True, '')
    #end _ifmap_loadbalancer_pool_delete

    def _ifmap_virtual_DNS_record_alloc(self, parent_type, fq_name):
        imid = self._imid_handler
        (my_imid, parent_imid) = \
            imid.virtual_DNS_record_alloc_ifmap_id(parent_type, fq_name)
        if my_imid is None or parent_imid is None:
            return (False, (my_imid, parent_imid))
        return (True, (my_imid, parent_imid))
    #end _ifmap_virtual_DNS_record_alloc

    def _ifmap_virtual_DNS_record_set(self, my_imid, existing_metas, obj_dict):
        # Properties Meta
        update = {}
        field = obj_dict.get('virtual_DNS_record_data', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['virtual_DNS_record_data']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                VirtualDnsRecordType(**field).exportChildren(buf, level = 1, name_ = 'virtual-DNS-record-data', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'virtual-DNS-record-data', pretty_print = False)
            virtual_DNS_record_data_xml = buf.getvalue()
            buf.close()
            meta = Metadata('virtual-DNS-record-data' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = virtual_DNS_record_data_xml)

            if (existing_metas and 'virtual-DNS-record-data' in existing_metas and
                str(existing_metas['virtual-DNS-record-data'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['id_perms']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                IdPermsType(**field).exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            id_perms_xml = buf.getvalue()
            buf.close()
            meta = Metadata('id-perms' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = id_perms_xml)

            if (existing_metas and 'id-perms' in existing_metas and
                str(existing_metas['id-perms'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('display_name', None)
        if field is not None:
            norm_str = escape(str(obj_dict['display_name']))
            meta = Metadata('display-name', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'display-name' in existing_metas and
                str(existing_metas['display-name'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        # Ref Link Metas
        imid = self._imid_handler


        self._publish_update(my_imid, update)
        return (True, '')
    #end _ifmap_virtual_DNS_record_set

    def _ifmap_virtual_DNS_record_create(self, obj_ids, obj_dict):
        if not 'parent_type' in obj_dict:
            # parent is config-root
            parent_type = 'config-root'
            parent_imid = 'contrail:config-root:root'
        else:
            parent_type = obj_dict['parent_type']
            parent_imid = obj_ids.get('parent_imid', None)

        # Parent Link Meta
        update = {}
        parent_link_meta = self._parent_metas[parent_type]['virtual-DNS-record']
        meta = Metadata(parent_link_meta, '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')
        self._update_id_pair_meta(update, obj_ids['imid'], meta)
        self._publish_update(parent_imid, update)

        (ok, result) = self._ifmap_virtual_DNS_record_set(obj_ids['imid'], None, obj_dict)
        return (ok, result)
    #end _ifmap_virtual_DNS_record_create


    def _ifmap_virtual_DNS_record_read_to_meta_index(self, ifmap_id, field_names = None):
        # field_names = None means all fields will be read
        imid = self._imid_handler
        start_id = str(Identity(name = ifmap_id, type = 'other',
                        other_type = 'extended'))
        # if id-perms missing, identity doesn't exist

        all_metas = [u'virtual-DNS-record-data', u'id-perms', u'display-name']
        if not field_names:
            metas_to_read = all_metas
        else: # read only requested fields
            metas_to_read = set(all_metas) & set(field_names.keys())

        # metas is a dict where key is meta-name and val is list of dict
        # of form [{'meta':meta}, {'id':id1, 'meta':meta}, {'id':id2, 'meta':meta}]
        metas = {}
        for meta_name in metas_to_read:
            if meta_name in self._id_to_metas[ifmap_id]:
                metas[meta_name] = self._id_to_metas[ifmap_id][meta_name]
        return metas
    #end _ifmap_virtual-DNS-record_read_to_meta_index

    def _ifmap_virtual_DNS_record_update(self, ifmap_id, new_obj_dict):
        # read in refs from ifmap to determine which ones become inactive after update
        existing_metas = self._ifmap_virtual_DNS_record_read_to_meta_index(ifmap_id)

        # remove properties that are no longer active
        props = ['virtual-DNS-record-data', 'id-perms', 'display-name']
        for prop in props:
            prop_m = prop.replace('-', '_')
            if prop in existing_metas and prop_m not in new_obj_dict:
                self._delete_id_self_meta(ifmap_id, 'contrail:'+prop)
        # remove refs that are no longer active
        delete_list = []
        refs = {}
        for meta, to_name in refs.items():
            old_set = set([m['id'] for m in existing_metas.get(meta, [])])
            new_set = set()
            to_name_m = to_name.replace('-', '_')
            for ref in new_obj_dict.get(to_name_m+'_refs', []):
                to_imid = self.fq_name_to_ifmap_id(to_name, ref['to'])
                new_set.add(to_imid)

            for inact_ref in old_set - new_set:
                delete_list.append((inact_ref, 'contrail:'+meta))

        if delete_list:
            self._delete_id_pair_meta_list(ifmap_id, delete_list)

        (ok, result) = self._ifmap_virtual_DNS_record_set(ifmap_id, existing_metas, new_obj_dict)
        return (ok, result)
    #end _ifmap_virtual_DNS_record_update

    def _ifmap_virtual_DNS_record_delete(self, obj_ids):
        ifmap_id = obj_ids['imid']
        parent_imid = obj_ids.get('parent_imid', None)
        existing_metas = self._ifmap_virtual_DNS_record_read_to_meta_index(ifmap_id)
        meta_list = []
        for meta_name, meta_infos in existing_metas.items():
            for meta_info in meta_infos:
                ref_imid = meta_info.get('id')
                if ref_imid is None:
                    continue
                meta_list.append((ref_imid, 'contrail:'+meta_name))

        if parent_imid:
            # Remove link from parent
            meta_list.append((parent_imid, None))

        if meta_list:
            self._delete_id_pair_meta_list(ifmap_id, meta_list)

        # Remove all property metadata associated with this ident
        self._delete_id_self_meta(ifmap_id, None)

        return (True, '')
    #end _ifmap_virtual_DNS_record_delete

    def _ifmap_route_target_alloc(self, parent_type, fq_name):
        imid = self._imid_handler
        (my_imid, parent_imid) = \
            imid.route_target_alloc_ifmap_id(parent_type, fq_name)
        if my_imid is None or parent_imid is None:
            return (False, (my_imid, parent_imid))
        return (True, (my_imid, parent_imid))
    #end _ifmap_route_target_alloc

    def _ifmap_route_target_set(self, my_imid, existing_metas, obj_dict):
        # Properties Meta
        update = {}
        field = obj_dict.get('id_perms', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['id_perms']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                IdPermsType(**field).exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            id_perms_xml = buf.getvalue()
            buf.close()
            meta = Metadata('id-perms' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = id_perms_xml)

            if (existing_metas and 'id-perms' in existing_metas and
                str(existing_metas['id-perms'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('display_name', None)
        if field is not None:
            norm_str = escape(str(obj_dict['display_name']))
            meta = Metadata('display-name', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'display-name' in existing_metas and
                str(existing_metas['display-name'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        # Ref Link Metas
        imid = self._imid_handler


        self._publish_update(my_imid, update)
        return (True, '')
    #end _ifmap_route_target_set

    def _ifmap_route_target_create(self, obj_ids, obj_dict):
        (ok, result) = self._ifmap_route_target_set(obj_ids['imid'], None, obj_dict)
        return (ok, result)
    #end _ifmap_route_target_create


    def _ifmap_route_target_read_to_meta_index(self, ifmap_id, field_names = None):
        # field_names = None means all fields will be read
        imid = self._imid_handler
        start_id = str(Identity(name = ifmap_id, type = 'other',
                        other_type = 'extended'))
        # if id-perms missing, identity doesn't exist

        all_metas = [u'logical-router-target', u'instance-target', u'id-perms', u'display-name']
        if not field_names:
            metas_to_read = all_metas
        else: # read only requested fields
            metas_to_read = set(all_metas) & set(field_names.keys())

        # metas is a dict where key is meta-name and val is list of dict
        # of form [{'meta':meta}, {'id':id1, 'meta':meta}, {'id':id2, 'meta':meta}]
        metas = {}
        for meta_name in metas_to_read:
            if meta_name in self._id_to_metas[ifmap_id]:
                metas[meta_name] = self._id_to_metas[ifmap_id][meta_name]
        return metas
    #end _ifmap_route-target_read_to_meta_index

    def _ifmap_route_target_update(self, ifmap_id, new_obj_dict):
        # read in refs from ifmap to determine which ones become inactive after update
        existing_metas = self._ifmap_route_target_read_to_meta_index(ifmap_id)

        # remove properties that are no longer active
        props = ['id-perms', 'display-name']
        for prop in props:
            prop_m = prop.replace('-', '_')
            if prop in existing_metas and prop_m not in new_obj_dict:
                self._delete_id_self_meta(ifmap_id, 'contrail:'+prop)
        # remove refs that are no longer active
        delete_list = []
        refs = {}
        for meta, to_name in refs.items():
            old_set = set([m['id'] for m in existing_metas.get(meta, [])])
            new_set = set()
            to_name_m = to_name.replace('-', '_')
            for ref in new_obj_dict.get(to_name_m+'_refs', []):
                to_imid = self.fq_name_to_ifmap_id(to_name, ref['to'])
                new_set.add(to_imid)

            for inact_ref in old_set - new_set:
                delete_list.append((inact_ref, 'contrail:'+meta))

        if delete_list:
            self._delete_id_pair_meta_list(ifmap_id, delete_list)

        (ok, result) = self._ifmap_route_target_set(ifmap_id, existing_metas, new_obj_dict)
        return (ok, result)
    #end _ifmap_route_target_update

    def _ifmap_route_target_delete(self, obj_ids):
        ifmap_id = obj_ids['imid']
        parent_imid = obj_ids.get('parent_imid', None)
        existing_metas = self._ifmap_route_target_read_to_meta_index(ifmap_id)
        meta_list = []
        for meta_name, meta_infos in existing_metas.items():
            for meta_info in meta_infos:
                ref_imid = meta_info.get('id')
                if ref_imid is None:
                    continue
                meta_list.append((ref_imid, 'contrail:'+meta_name))

        if parent_imid:
            # Remove link from parent
            meta_list.append((parent_imid, None))

        if meta_list:
            self._delete_id_pair_meta_list(ifmap_id, meta_list)

        # Remove all property metadata associated with this ident
        self._delete_id_self_meta(ifmap_id, None)

        return (True, '')
    #end _ifmap_route_target_delete

    def _ifmap_floating_ip_alloc(self, parent_type, fq_name):
        imid = self._imid_handler
        (my_imid, parent_imid) = \
            imid.floating_ip_alloc_ifmap_id(parent_type, fq_name)
        if my_imid is None or parent_imid is None:
            return (False, (my_imid, parent_imid))
        return (True, (my_imid, parent_imid))
    #end _ifmap_floating_ip_alloc

    def _ifmap_floating_ip_set(self, my_imid, existing_metas, obj_dict):
        # Properties Meta
        update = {}
        field = obj_dict.get('floating_ip_address', None)
        if field is not None:
            norm_str = escape(str(obj_dict['floating_ip_address']))
            meta = Metadata('floating-ip-address', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'floating-ip-address' in existing_metas and
                str(existing_metas['floating-ip-address'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('floating_ip_is_virtual_ip', None)
        if field is not None:
            norm_str = escape(str(obj_dict['floating_ip_is_virtual_ip']))
            meta = Metadata('floating-ip-is-virtual-ip', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'floating-ip-is-virtual-ip' in existing_metas and
                str(existing_metas['floating-ip-is-virtual-ip'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('floating_ip_fixed_ip_address', None)
        if field is not None:
            norm_str = escape(str(obj_dict['floating_ip_fixed_ip_address']))
            meta = Metadata('floating-ip-fixed-ip-address', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'floating-ip-fixed-ip-address' in existing_metas and
                str(existing_metas['floating-ip-fixed-ip-address'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('floating_ip_address_family', None)
        if field is not None:
            norm_str = escape(str(obj_dict['floating_ip_address_family']))
            meta = Metadata('floating-ip-address-family', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'floating-ip-address-family' in existing_metas and
                str(existing_metas['floating-ip-address-family'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['id_perms']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                IdPermsType(**field).exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            id_perms_xml = buf.getvalue()
            buf.close()
            meta = Metadata('id-perms' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = id_perms_xml)

            if (existing_metas and 'id-perms' in existing_metas and
                str(existing_metas['id-perms'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('display_name', None)
        if field is not None:
            norm_str = escape(str(obj_dict['display_name']))
            meta = Metadata('display-name', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'display-name' in existing_metas and
                str(existing_metas['display-name'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        # Ref Link Metas
        imid = self._imid_handler

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('project_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'project'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                floating_ip_project_xml = ''
                meta = Metadata('floating-ip-project' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = floating_ip_project_xml)
                self._update_id_pair_meta(update, to_imid, meta)

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('virtual_machine_interface_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'virtual-machine-interface'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                floating_ip_virtual_machine_interface_xml = ''
                meta = Metadata('floating-ip-virtual-machine-interface' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = floating_ip_virtual_machine_interface_xml)
                self._update_id_pair_meta(update, to_imid, meta)


        self._publish_update(my_imid, update)
        return (True, '')
    #end _ifmap_floating_ip_set

    def _ifmap_floating_ip_create(self, obj_ids, obj_dict):
        if not 'parent_type' in obj_dict:
            # parent is config-root
            parent_type = 'config-root'
            parent_imid = 'contrail:config-root:root'
        else:
            parent_type = obj_dict['parent_type']
            parent_imid = obj_ids.get('parent_imid', None)

        # Parent Link Meta
        update = {}
        parent_link_meta = self._parent_metas[parent_type]['floating-ip']
        meta = Metadata(parent_link_meta, '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')
        self._update_id_pair_meta(update, obj_ids['imid'], meta)
        self._publish_update(parent_imid, update)

        (ok, result) = self._ifmap_floating_ip_set(obj_ids['imid'], None, obj_dict)
        return (ok, result)
    #end _ifmap_floating_ip_create


    def _ifmap_floating_ip_read_to_meta_index(self, ifmap_id, field_names = None):
        # field_names = None means all fields will be read
        imid = self._imid_handler
        start_id = str(Identity(name = ifmap_id, type = 'other',
                        other_type = 'extended'))
        # if id-perms missing, identity doesn't exist

        all_metas = [u'floating-ip-project', u'floating-ip-virtual-machine-interface', u'customer-attachment-floating-ip', u'floating-ip-address', u'floating-ip-is-virtual-ip', u'floating-ip-fixed-ip-address', u'floating-ip-address-family', u'id-perms', u'display-name']
        if not field_names:
            metas_to_read = all_metas
        else: # read only requested fields
            metas_to_read = set(all_metas) & set(field_names.keys())

        # metas is a dict where key is meta-name and val is list of dict
        # of form [{'meta':meta}, {'id':id1, 'meta':meta}, {'id':id2, 'meta':meta}]
        metas = {}
        for meta_name in metas_to_read:
            if meta_name in self._id_to_metas[ifmap_id]:
                metas[meta_name] = self._id_to_metas[ifmap_id][meta_name]
        return metas
    #end _ifmap_floating-ip_read_to_meta_index

    def _ifmap_floating_ip_update(self, ifmap_id, new_obj_dict):
        # read in refs from ifmap to determine which ones become inactive after update
        existing_metas = self._ifmap_floating_ip_read_to_meta_index(ifmap_id)

        # remove properties that are no longer active
        props = ['floating-ip-address', 'floating-ip-is-virtual-ip', 'floating-ip-fixed-ip-address', 'floating-ip-address-family', 'id-perms', 'display-name']
        for prop in props:
            prop_m = prop.replace('-', '_')
            if prop in existing_metas and prop_m not in new_obj_dict:
                self._delete_id_self_meta(ifmap_id, 'contrail:'+prop)
        # remove refs that are no longer active
        delete_list = []
        refs = {'floating-ip-project': 'project',
                'floating-ip-virtual-machine-interface': 'virtual-machine-interface'}
        for meta, to_name in refs.items():
            old_set = set([m['id'] for m in existing_metas.get(meta, [])])
            new_set = set()
            to_name_m = to_name.replace('-', '_')
            for ref in new_obj_dict.get(to_name_m+'_refs', []):
                to_imid = self.fq_name_to_ifmap_id(to_name, ref['to'])
                new_set.add(to_imid)

            for inact_ref in old_set - new_set:
                delete_list.append((inact_ref, 'contrail:'+meta))

        if delete_list:
            self._delete_id_pair_meta_list(ifmap_id, delete_list)

        (ok, result) = self._ifmap_floating_ip_set(ifmap_id, existing_metas, new_obj_dict)
        return (ok, result)
    #end _ifmap_floating_ip_update

    def _ifmap_floating_ip_delete(self, obj_ids):
        ifmap_id = obj_ids['imid']
        parent_imid = obj_ids.get('parent_imid', None)
        existing_metas = self._ifmap_floating_ip_read_to_meta_index(ifmap_id)
        meta_list = []
        for meta_name, meta_infos in existing_metas.items():
            for meta_info in meta_infos:
                ref_imid = meta_info.get('id')
                if ref_imid is None:
                    continue
                meta_list.append((ref_imid, 'contrail:'+meta_name))

        if parent_imid:
            # Remove link from parent
            meta_list.append((parent_imid, None))

        if meta_list:
            self._delete_id_pair_meta_list(ifmap_id, meta_list)

        # Remove all property metadata associated with this ident
        self._delete_id_self_meta(ifmap_id, None)

        return (True, '')
    #end _ifmap_floating_ip_delete

    def _ifmap_floating_ip_pool_alloc(self, parent_type, fq_name):
        imid = self._imid_handler
        (my_imid, parent_imid) = \
            imid.floating_ip_pool_alloc_ifmap_id(parent_type, fq_name)
        if my_imid is None or parent_imid is None:
            return (False, (my_imid, parent_imid))
        return (True, (my_imid, parent_imid))
    #end _ifmap_floating_ip_pool_alloc

    def _ifmap_floating_ip_pool_set(self, my_imid, existing_metas, obj_dict):
        # Properties Meta
        update = {}
        field = obj_dict.get('floating_ip_pool_prefixes', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['floating_ip_pool_prefixes']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                FloatingIpPoolType(**field).exportChildren(buf, level = 1, name_ = 'floating-ip-pool-prefixes', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'floating-ip-pool-prefixes', pretty_print = False)
            floating_ip_pool_prefixes_xml = buf.getvalue()
            buf.close()
            meta = Metadata('floating-ip-pool-prefixes' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = floating_ip_pool_prefixes_xml)

            if (existing_metas and 'floating-ip-pool-prefixes' in existing_metas and
                str(existing_metas['floating-ip-pool-prefixes'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['id_perms']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                IdPermsType(**field).exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            id_perms_xml = buf.getvalue()
            buf.close()
            meta = Metadata('id-perms' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = id_perms_xml)

            if (existing_metas and 'id-perms' in existing_metas and
                str(existing_metas['id-perms'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('display_name', None)
        if field is not None:
            norm_str = escape(str(obj_dict['display_name']))
            meta = Metadata('display-name', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'display-name' in existing_metas and
                str(existing_metas['display-name'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        # Ref Link Metas
        imid = self._imid_handler

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('floating_ip_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'floating-ip'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                floating_ip_pool_floating_ip_xml = ''
                meta = Metadata('floating-ip-pool-floating-ip' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = floating_ip_pool_floating_ip_xml)
                self._update_id_pair_meta(update, to_imid, meta)


        self._publish_update(my_imid, update)
        return (True, '')
    #end _ifmap_floating_ip_pool_set

    def _ifmap_floating_ip_pool_create(self, obj_ids, obj_dict):
        if not 'parent_type' in obj_dict:
            # parent is config-root
            parent_type = 'config-root'
            parent_imid = 'contrail:config-root:root'
        else:
            parent_type = obj_dict['parent_type']
            parent_imid = obj_ids.get('parent_imid', None)

        # Parent Link Meta
        update = {}
        parent_link_meta = self._parent_metas[parent_type]['floating-ip-pool']
        meta = Metadata(parent_link_meta, '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')
        self._update_id_pair_meta(update, obj_ids['imid'], meta)
        self._publish_update(parent_imid, update)

        (ok, result) = self._ifmap_floating_ip_pool_set(obj_ids['imid'], None, obj_dict)
        return (ok, result)
    #end _ifmap_floating_ip_pool_create


    def _ifmap_floating_ip_pool_read_to_meta_index(self, ifmap_id, field_names = None):
        # field_names = None means all fields will be read
        imid = self._imid_handler
        start_id = str(Identity(name = ifmap_id, type = 'other',
                        other_type = 'extended'))
        # if id-perms missing, identity doesn't exist

        all_metas = [u'project-floating-ip-pool', u'floating-ip-pool-prefixes', u'id-perms', u'display-name', u'floating-ip-pool-floating-ip']
        if not field_names:
            metas_to_read = all_metas
        else: # read only requested fields
            metas_to_read = set(all_metas) & set(field_names.keys())

        # metas is a dict where key is meta-name and val is list of dict
        # of form [{'meta':meta}, {'id':id1, 'meta':meta}, {'id':id2, 'meta':meta}]
        metas = {}
        for meta_name in metas_to_read:
            if meta_name in self._id_to_metas[ifmap_id]:
                metas[meta_name] = self._id_to_metas[ifmap_id][meta_name]
        return metas
    #end _ifmap_floating-ip-pool_read_to_meta_index

    def _ifmap_floating_ip_pool_update(self, ifmap_id, new_obj_dict):
        # read in refs from ifmap to determine which ones become inactive after update
        existing_metas = self._ifmap_floating_ip_pool_read_to_meta_index(ifmap_id)

        # remove properties that are no longer active
        props = ['floating-ip-pool-prefixes', 'id-perms', 'display-name']
        for prop in props:
            prop_m = prop.replace('-', '_')
            if prop in existing_metas and prop_m not in new_obj_dict:
                self._delete_id_self_meta(ifmap_id, 'contrail:'+prop)
        # remove refs that are no longer active
        delete_list = []
        refs = {}
        for meta, to_name in refs.items():
            old_set = set([m['id'] for m in existing_metas.get(meta, [])])
            new_set = set()
            to_name_m = to_name.replace('-', '_')
            for ref in new_obj_dict.get(to_name_m+'_refs', []):
                to_imid = self.fq_name_to_ifmap_id(to_name, ref['to'])
                new_set.add(to_imid)

            for inact_ref in old_set - new_set:
                delete_list.append((inact_ref, 'contrail:'+meta))

        if delete_list:
            self._delete_id_pair_meta_list(ifmap_id, delete_list)

        (ok, result) = self._ifmap_floating_ip_pool_set(ifmap_id, existing_metas, new_obj_dict)
        return (ok, result)
    #end _ifmap_floating_ip_pool_update

    def _ifmap_floating_ip_pool_delete(self, obj_ids):
        ifmap_id = obj_ids['imid']
        parent_imid = obj_ids.get('parent_imid', None)
        existing_metas = self._ifmap_floating_ip_pool_read_to_meta_index(ifmap_id)
        meta_list = []
        for meta_name, meta_infos in existing_metas.items():
            for meta_info in meta_infos:
                ref_imid = meta_info.get('id')
                if ref_imid is None:
                    continue
                meta_list.append((ref_imid, 'contrail:'+meta_name))

        if parent_imid:
            # Remove link from parent
            meta_list.append((parent_imid, None))

        if meta_list:
            self._delete_id_pair_meta_list(ifmap_id, meta_list)

        # Remove all property metadata associated with this ident
        self._delete_id_self_meta(ifmap_id, None)

        return (True, '')
    #end _ifmap_floating_ip_pool_delete

    def _ifmap_physical_router_alloc(self, parent_type, fq_name):
        imid = self._imid_handler
        (my_imid, parent_imid) = \
            imid.physical_router_alloc_ifmap_id(parent_type, fq_name)
        if my_imid is None or parent_imid is None:
            return (False, (my_imid, parent_imid))
        return (True, (my_imid, parent_imid))
    #end _ifmap_physical_router_alloc

    def _ifmap_physical_router_set(self, my_imid, existing_metas, obj_dict):
        # Properties Meta
        update = {}
        field = obj_dict.get('physical_router_management_ip', None)
        if field is not None:
            norm_str = escape(str(obj_dict['physical_router_management_ip']))
            meta = Metadata('physical-router-management-ip', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'physical-router-management-ip' in existing_metas and
                str(existing_metas['physical-router-management-ip'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('physical_router_dataplane_ip', None)
        if field is not None:
            norm_str = escape(str(obj_dict['physical_router_dataplane_ip']))
            meta = Metadata('physical-router-dataplane-ip', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'physical-router-dataplane-ip' in existing_metas and
                str(existing_metas['physical-router-dataplane-ip'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('physical_router_vendor_name', None)
        if field is not None:
            norm_str = escape(str(obj_dict['physical_router_vendor_name']))
            meta = Metadata('physical-router-vendor-name', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'physical-router-vendor-name' in existing_metas and
                str(existing_metas['physical-router-vendor-name'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('physical_router_product_name', None)
        if field is not None:
            norm_str = escape(str(obj_dict['physical_router_product_name']))
            meta = Metadata('physical-router-product-name', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'physical-router-product-name' in existing_metas and
                str(existing_metas['physical-router-product-name'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('physical_router_vnc_managed', None)
        if field is not None:
            norm_str = escape(str(obj_dict['physical_router_vnc_managed']))
            meta = Metadata('physical-router-vnc-managed', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'physical-router-vnc-managed' in existing_metas and
                str(existing_metas['physical-router-vnc-managed'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('physical_router_user_credentials', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['physical_router_user_credentials']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                UserCredentials(**field).exportChildren(buf, level = 1, name_ = 'physical-router-user-credentials', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'physical-router-user-credentials', pretty_print = False)
            physical_router_user_credentials_xml = buf.getvalue()
            buf.close()
            meta = Metadata('physical-router-user-credentials' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = physical_router_user_credentials_xml)

            if (existing_metas and 'physical-router-user-credentials' in existing_metas and
                str(existing_metas['physical-router-user-credentials'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('physical_router_snmp_credentials', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['physical_router_snmp_credentials']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                SNMPCredentials(**field).exportChildren(buf, level = 1, name_ = 'physical-router-snmp-credentials', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'physical-router-snmp-credentials', pretty_print = False)
            physical_router_snmp_credentials_xml = buf.getvalue()
            buf.close()
            meta = Metadata('physical-router-snmp-credentials' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = physical_router_snmp_credentials_xml)

            if (existing_metas and 'physical-router-snmp-credentials' in existing_metas and
                str(existing_metas['physical-router-snmp-credentials'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('physical_router_junos_service_ports', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['physical_router_junos_service_ports']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                JunosServicePorts(**field).exportChildren(buf, level = 1, name_ = 'physical-router-junos-service-ports', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'physical-router-junos-service-ports', pretty_print = False)
            physical_router_junos_service_ports_xml = buf.getvalue()
            buf.close()
            meta = Metadata('physical-router-junos-service-ports' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = physical_router_junos_service_ports_xml)

            if (existing_metas and 'physical-router-junos-service-ports' in existing_metas and
                str(existing_metas['physical-router-junos-service-ports'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['id_perms']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                IdPermsType(**field).exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            id_perms_xml = buf.getvalue()
            buf.close()
            meta = Metadata('id-perms' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = id_perms_xml)

            if (existing_metas and 'id-perms' in existing_metas and
                str(existing_metas['id-perms'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('display_name', None)
        if field is not None:
            norm_str = escape(str(obj_dict['display_name']))
            meta = Metadata('display-name', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'display-name' in existing_metas and
                str(existing_metas['display-name'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        # Ref Link Metas
        imid = self._imid_handler

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('virtual_router_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'virtual-router'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                physical_router_virtual_router_xml = ''
                meta = Metadata('physical-router-virtual-router' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = physical_router_virtual_router_xml)
                self._update_id_pair_meta(update, to_imid, meta)

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('bgp_router_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'bgp-router'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                physical_router_bgp_router_xml = ''
                meta = Metadata('physical-router-bgp-router' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = physical_router_bgp_router_xml)
                self._update_id_pair_meta(update, to_imid, meta)

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('virtual_network_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'virtual-network'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                physical_router_virtual_network_xml = ''
                meta = Metadata('physical-router-virtual-network' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = physical_router_virtual_network_xml)
                self._update_id_pair_meta(update, to_imid, meta)

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('physical_interface_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'physical-interface'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                physical_router_physical_interface_xml = ''
                meta = Metadata('physical-router-physical-interface' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = physical_router_physical_interface_xml)
                self._update_id_pair_meta(update, to_imid, meta)

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('logical_interface_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'logical-interface'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                physical_router_logical_interface_xml = ''
                meta = Metadata('physical-router-logical-interface' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = physical_router_logical_interface_xml)
                self._update_id_pair_meta(update, to_imid, meta)


        self._publish_update(my_imid, update)
        return (True, '')
    #end _ifmap_physical_router_set

    def _ifmap_physical_router_create(self, obj_ids, obj_dict):
        if not 'parent_type' in obj_dict:
            # parent is config-root
            parent_type = 'config-root'
            parent_imid = 'contrail:config-root:root'
        else:
            parent_type = obj_dict['parent_type']
            parent_imid = obj_ids.get('parent_imid', None)

        # Parent Link Meta
        update = {}
        parent_link_meta = self._parent_metas[parent_type]['physical-router']
        meta = Metadata(parent_link_meta, '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')
        self._update_id_pair_meta(update, obj_ids['imid'], meta)
        self._publish_update(parent_imid, update)

        (ok, result) = self._ifmap_physical_router_set(obj_ids['imid'], None, obj_dict)
        return (ok, result)
    #end _ifmap_physical_router_create


    def _ifmap_physical_router_read_to_meta_index(self, ifmap_id, field_names = None):
        # field_names = None means all fields will be read
        imid = self._imid_handler
        start_id = str(Identity(name = ifmap_id, type = 'other',
                        other_type = 'extended'))
        # if id-perms missing, identity doesn't exist

        all_metas = [u'physical-router-virtual-router', u'physical-router-bgp-router', u'physical-router-virtual-network', u'physical-router-management-ip', u'physical-router-dataplane-ip', u'physical-router-vendor-name', u'physical-router-product-name', u'physical-router-vnc-managed', u'physical-router-user-credentials', u'physical-router-snmp-credentials', u'physical-router-junos-service-ports', u'id-perms', u'display-name', u'physical-router-physical-interface', u'physical-router-logical-interface']
        if not field_names:
            metas_to_read = all_metas
        else: # read only requested fields
            metas_to_read = set(all_metas) & set(field_names.keys())

        # metas is a dict where key is meta-name and val is list of dict
        # of form [{'meta':meta}, {'id':id1, 'meta':meta}, {'id':id2, 'meta':meta}]
        metas = {}
        for meta_name in metas_to_read:
            if meta_name in self._id_to_metas[ifmap_id]:
                metas[meta_name] = self._id_to_metas[ifmap_id][meta_name]
        return metas
    #end _ifmap_physical-router_read_to_meta_index

    def _ifmap_physical_router_update(self, ifmap_id, new_obj_dict):
        # read in refs from ifmap to determine which ones become inactive after update
        existing_metas = self._ifmap_physical_router_read_to_meta_index(ifmap_id)

        # remove properties that are no longer active
        props = ['physical-router-management-ip', 'physical-router-dataplane-ip', 'physical-router-vendor-name', 'physical-router-product-name', 'physical-router-vnc-managed', 'physical-router-user-credentials', 'physical-router-snmp-credentials', 'physical-router-junos-service-ports', 'id-perms', 'display-name']
        for prop in props:
            prop_m = prop.replace('-', '_')
            if prop in existing_metas and prop_m not in new_obj_dict:
                self._delete_id_self_meta(ifmap_id, 'contrail:'+prop)
        # remove refs that are no longer active
        delete_list = []
        refs = {'physical-router-virtual-router': 'virtual-router',
                'physical-router-bgp-router': 'bgp-router',
                'physical-router-virtual-network': 'virtual-network'}
        for meta, to_name in refs.items():
            old_set = set([m['id'] for m in existing_metas.get(meta, [])])
            new_set = set()
            to_name_m = to_name.replace('-', '_')
            for ref in new_obj_dict.get(to_name_m+'_refs', []):
                to_imid = self.fq_name_to_ifmap_id(to_name, ref['to'])
                new_set.add(to_imid)

            for inact_ref in old_set - new_set:
                delete_list.append((inact_ref, 'contrail:'+meta))

        if delete_list:
            self._delete_id_pair_meta_list(ifmap_id, delete_list)

        (ok, result) = self._ifmap_physical_router_set(ifmap_id, existing_metas, new_obj_dict)
        return (ok, result)
    #end _ifmap_physical_router_update

    def _ifmap_physical_router_delete(self, obj_ids):
        ifmap_id = obj_ids['imid']
        parent_imid = obj_ids.get('parent_imid', None)
        existing_metas = self._ifmap_physical_router_read_to_meta_index(ifmap_id)
        meta_list = []
        for meta_name, meta_infos in existing_metas.items():
            for meta_info in meta_infos:
                ref_imid = meta_info.get('id')
                if ref_imid is None:
                    continue
                meta_list.append((ref_imid, 'contrail:'+meta_name))

        if parent_imid:
            # Remove link from parent
            meta_list.append((parent_imid, None))

        if meta_list:
            self._delete_id_pair_meta_list(ifmap_id, meta_list)

        # Remove all property metadata associated with this ident
        self._delete_id_self_meta(ifmap_id, None)

        return (True, '')
    #end _ifmap_physical_router_delete

    def _ifmap_bgp_router_alloc(self, parent_type, fq_name):
        imid = self._imid_handler
        (my_imid, parent_imid) = \
            imid.bgp_router_alloc_ifmap_id(parent_type, fq_name)
        if my_imid is None or parent_imid is None:
            return (False, (my_imid, parent_imid))
        return (True, (my_imid, parent_imid))
    #end _ifmap_bgp_router_alloc

    def _ifmap_bgp_router_set(self, my_imid, existing_metas, obj_dict):
        # Properties Meta
        update = {}
        field = obj_dict.get('bgp_router_parameters', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['bgp_router_parameters']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                BgpRouterParams(**field).exportChildren(buf, level = 1, name_ = 'bgp-router-parameters', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'bgp-router-parameters', pretty_print = False)
            bgp_router_parameters_xml = buf.getvalue()
            buf.close()
            meta = Metadata('bgp-router-parameters' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = bgp_router_parameters_xml)

            if (existing_metas and 'bgp-router-parameters' in existing_metas and
                str(existing_metas['bgp-router-parameters'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['id_perms']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                IdPermsType(**field).exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            id_perms_xml = buf.getvalue()
            buf.close()
            meta = Metadata('id-perms' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = id_perms_xml)

            if (existing_metas and 'id-perms' in existing_metas and
                str(existing_metas['id-perms'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('display_name', None)
        if field is not None:
            norm_str = escape(str(obj_dict['display_name']))
            meta = Metadata('display-name', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'display-name' in existing_metas and
                str(existing_metas['display-name'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        # Ref Link Metas
        imid = self._imid_handler

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('bgp_router_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'bgp-router'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                bgp_peering_xml = ''
                ref_data = ref['attr']
                if ref_data:
                    buf = cStringIO.StringIO()
                    BgpPeeringAttributes(**ref_data).exportChildren(buf, level = 1, name_ = 'bgp-peering', pretty_print = False)
                    bgp_peering_xml = bgp_peering_xml + buf.getvalue()
                    buf.close()
                meta = Metadata('bgp-peering' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = bgp_peering_xml)
                self._update_id_pair_meta(update, to_imid, meta)


        self._publish_update(my_imid, update)
        return (True, '')
    #end _ifmap_bgp_router_set

    def _ifmap_bgp_router_create(self, obj_ids, obj_dict):
        if not 'parent_type' in obj_dict:
            # parent is config-root
            parent_type = 'config-root'
            parent_imid = 'contrail:config-root:root'
        else:
            parent_type = obj_dict['parent_type']
            parent_imid = obj_ids.get('parent_imid', None)

        # Parent Link Meta
        update = {}
        parent_link_meta = self._parent_metas[parent_type]['bgp-router']
        meta = Metadata(parent_link_meta, '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')
        self._update_id_pair_meta(update, obj_ids['imid'], meta)
        self._publish_update(parent_imid, update)

        (ok, result) = self._ifmap_bgp_router_set(obj_ids['imid'], None, obj_dict)
        return (ok, result)
    #end _ifmap_bgp_router_create


    def _ifmap_bgp_router_read_to_meta_index(self, ifmap_id, field_names = None):
        # field_names = None means all fields will be read
        imid = self._imid_handler
        start_id = str(Identity(name = ifmap_id, type = 'other',
                        other_type = 'extended'))
        # if id-perms missing, identity doesn't exist

        all_metas = [u'bgp-peering', u'global-system-config-bgp-router', u'physical-router-bgp-router', u'virtual-router-bgp-router', u'bgp-peering', u'bgp-router-parameters', u'id-perms', u'display-name']
        if not field_names:
            metas_to_read = all_metas
        else: # read only requested fields
            metas_to_read = set(all_metas) & set(field_names.keys())

        # metas is a dict where key is meta-name and val is list of dict
        # of form [{'meta':meta}, {'id':id1, 'meta':meta}, {'id':id2, 'meta':meta}]
        metas = {}
        for meta_name in metas_to_read:
            if meta_name in self._id_to_metas[ifmap_id]:
                metas[meta_name] = self._id_to_metas[ifmap_id][meta_name]
        return metas
    #end _ifmap_bgp-router_read_to_meta_index

    def _ifmap_bgp_router_update(self, ifmap_id, new_obj_dict):
        # read in refs from ifmap to determine which ones become inactive after update
        existing_metas = self._ifmap_bgp_router_read_to_meta_index(ifmap_id)

        # remove properties that are no longer active
        props = ['bgp-router-parameters', 'id-perms', 'display-name']
        for prop in props:
            prop_m = prop.replace('-', '_')
            if prop in existing_metas and prop_m not in new_obj_dict:
                self._delete_id_self_meta(ifmap_id, 'contrail:'+prop)
        # remove refs that are no longer active
        delete_list = []
        refs = {'bgp-peering': 'bgp-router'}
        for meta, to_name in refs.items():
            old_set = set([m['id'] for m in existing_metas.get(meta, [])])
            new_set = set()
            to_name_m = to_name.replace('-', '_')
            for ref in new_obj_dict.get(to_name_m+'_refs', []):
                to_imid = self.fq_name_to_ifmap_id(to_name, ref['to'])
                new_set.add(to_imid)

            for inact_ref in old_set - new_set:
                delete_list.append((inact_ref, 'contrail:'+meta))

        if delete_list:
            self._delete_id_pair_meta_list(ifmap_id, delete_list)

        (ok, result) = self._ifmap_bgp_router_set(ifmap_id, existing_metas, new_obj_dict)
        return (ok, result)
    #end _ifmap_bgp_router_update

    def _ifmap_bgp_router_delete(self, obj_ids):
        ifmap_id = obj_ids['imid']
        parent_imid = obj_ids.get('parent_imid', None)
        existing_metas = self._ifmap_bgp_router_read_to_meta_index(ifmap_id)
        meta_list = []
        for meta_name, meta_infos in existing_metas.items():
            for meta_info in meta_infos:
                ref_imid = meta_info.get('id')
                if ref_imid is None:
                    continue
                meta_list.append((ref_imid, 'contrail:'+meta_name))

        if parent_imid:
            # Remove link from parent
            meta_list.append((parent_imid, None))

        if meta_list:
            self._delete_id_pair_meta_list(ifmap_id, meta_list)

        # Remove all property metadata associated with this ident
        self._delete_id_self_meta(ifmap_id, None)

        return (True, '')
    #end _ifmap_bgp_router_delete

    def _ifmap_virtual_router_alloc(self, parent_type, fq_name):
        imid = self._imid_handler
        (my_imid, parent_imid) = \
            imid.virtual_router_alloc_ifmap_id(parent_type, fq_name)
        if my_imid is None or parent_imid is None:
            return (False, (my_imid, parent_imid))
        return (True, (my_imid, parent_imid))
    #end _ifmap_virtual_router_alloc

    def _ifmap_virtual_router_set(self, my_imid, existing_metas, obj_dict):
        # Properties Meta
        update = {}
        field = obj_dict.get('virtual_router_type', None)
        if field is not None:
            norm_str = escape(str(obj_dict['virtual_router_type']))
            meta = Metadata('virtual-router-type', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'virtual-router-type' in existing_metas and
                str(existing_metas['virtual-router-type'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('virtual_router_ip_address', None)
        if field is not None:
            norm_str = escape(str(obj_dict['virtual_router_ip_address']))
            meta = Metadata('virtual-router-ip-address', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'virtual-router-ip-address' in existing_metas and
                str(existing_metas['virtual-router-ip-address'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['id_perms']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                IdPermsType(**field).exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            id_perms_xml = buf.getvalue()
            buf.close()
            meta = Metadata('id-perms' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = id_perms_xml)

            if (existing_metas and 'id-perms' in existing_metas and
                str(existing_metas['id-perms'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('display_name', None)
        if field is not None:
            norm_str = escape(str(obj_dict['display_name']))
            meta = Metadata('display-name', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'display-name' in existing_metas and
                str(existing_metas['display-name'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        # Ref Link Metas
        imid = self._imid_handler

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('bgp_router_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'bgp-router'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                virtual_router_bgp_router_xml = ''
                meta = Metadata('virtual-router-bgp-router' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = virtual_router_bgp_router_xml)
                self._update_id_pair_meta(update, to_imid, meta)

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('virtual_machine_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'virtual-machine'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                virtual_router_virtual_machine_xml = ''
                meta = Metadata('virtual-router-virtual-machine' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = virtual_router_virtual_machine_xml)
                self._update_id_pair_meta(update, to_imid, meta)


        self._publish_update(my_imid, update)
        return (True, '')
    #end _ifmap_virtual_router_set

    def _ifmap_virtual_router_create(self, obj_ids, obj_dict):
        if not 'parent_type' in obj_dict:
            # parent is config-root
            parent_type = 'config-root'
            parent_imid = 'contrail:config-root:root'
        else:
            parent_type = obj_dict['parent_type']
            parent_imid = obj_ids.get('parent_imid', None)

        # Parent Link Meta
        update = {}
        parent_link_meta = self._parent_metas[parent_type]['virtual-router']
        meta = Metadata(parent_link_meta, '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')
        self._update_id_pair_meta(update, obj_ids['imid'], meta)
        self._publish_update(parent_imid, update)

        (ok, result) = self._ifmap_virtual_router_set(obj_ids['imid'], None, obj_dict)
        return (ok, result)
    #end _ifmap_virtual_router_create


    def _ifmap_virtual_router_read_to_meta_index(self, ifmap_id, field_names = None):
        # field_names = None means all fields will be read
        imid = self._imid_handler
        start_id = str(Identity(name = ifmap_id, type = 'other',
                        other_type = 'extended'))
        # if id-perms missing, identity doesn't exist

        all_metas = [u'virtual-router-bgp-router', u'virtual-router-virtual-machine', u'physical-router-virtual-router', u'provider-attachment-virtual-router', u'virtual-router-type', u'virtual-router-ip-address', u'id-perms', u'display-name']
        if not field_names:
            metas_to_read = all_metas
        else: # read only requested fields
            metas_to_read = set(all_metas) & set(field_names.keys())

        # metas is a dict where key is meta-name and val is list of dict
        # of form [{'meta':meta}, {'id':id1, 'meta':meta}, {'id':id2, 'meta':meta}]
        metas = {}
        for meta_name in metas_to_read:
            if meta_name in self._id_to_metas[ifmap_id]:
                metas[meta_name] = self._id_to_metas[ifmap_id][meta_name]
        return metas
    #end _ifmap_virtual-router_read_to_meta_index

    def _ifmap_virtual_router_update(self, ifmap_id, new_obj_dict):
        # read in refs from ifmap to determine which ones become inactive after update
        existing_metas = self._ifmap_virtual_router_read_to_meta_index(ifmap_id)

        # remove properties that are no longer active
        props = ['virtual-router-type', 'virtual-router-ip-address', 'id-perms', 'display-name']
        for prop in props:
            prop_m = prop.replace('-', '_')
            if prop in existing_metas and prop_m not in new_obj_dict:
                self._delete_id_self_meta(ifmap_id, 'contrail:'+prop)
        # remove refs that are no longer active
        delete_list = []
        refs = {'virtual-router-bgp-router': 'bgp-router',
                'virtual-router-virtual-machine': 'virtual-machine'}
        for meta, to_name in refs.items():
            old_set = set([m['id'] for m in existing_metas.get(meta, [])])
            new_set = set()
            to_name_m = to_name.replace('-', '_')
            for ref in new_obj_dict.get(to_name_m+'_refs', []):
                to_imid = self.fq_name_to_ifmap_id(to_name, ref['to'])
                new_set.add(to_imid)

            for inact_ref in old_set - new_set:
                delete_list.append((inact_ref, 'contrail:'+meta))

        if delete_list:
            self._delete_id_pair_meta_list(ifmap_id, delete_list)

        (ok, result) = self._ifmap_virtual_router_set(ifmap_id, existing_metas, new_obj_dict)
        return (ok, result)
    #end _ifmap_virtual_router_update

    def _ifmap_virtual_router_delete(self, obj_ids):
        ifmap_id = obj_ids['imid']
        parent_imid = obj_ids.get('parent_imid', None)
        existing_metas = self._ifmap_virtual_router_read_to_meta_index(ifmap_id)
        meta_list = []
        for meta_name, meta_infos in existing_metas.items():
            for meta_info in meta_infos:
                ref_imid = meta_info.get('id')
                if ref_imid is None:
                    continue
                meta_list.append((ref_imid, 'contrail:'+meta_name))

        if parent_imid:
            # Remove link from parent
            meta_list.append((parent_imid, None))

        if meta_list:
            self._delete_id_pair_meta_list(ifmap_id, meta_list)

        # Remove all property metadata associated with this ident
        self._delete_id_self_meta(ifmap_id, None)

        return (True, '')
    #end _ifmap_virtual_router_delete

    def _ifmap_config_root_alloc(self, parent_type, fq_name):
        imid = self._imid_handler
        (my_imid, parent_imid) = \
            imid.config_root_alloc_ifmap_id(parent_type, fq_name)
        if my_imid is None or parent_imid is None:
            return (False, (my_imid, parent_imid))
        return (True, (my_imid, parent_imid))
    #end _ifmap_config_root_alloc

    def _ifmap_config_root_set(self, my_imid, existing_metas, obj_dict):
        # Properties Meta
        update = {}
        field = obj_dict.get('id_perms', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['id_perms']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                IdPermsType(**field).exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            id_perms_xml = buf.getvalue()
            buf.close()
            meta = Metadata('id-perms' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = id_perms_xml)

            if (existing_metas and 'id-perms' in existing_metas and
                str(existing_metas['id-perms'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('display_name', None)
        if field is not None:
            norm_str = escape(str(obj_dict['display_name']))
            meta = Metadata('display-name', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'display-name' in existing_metas and
                str(existing_metas['display-name'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        # Ref Link Metas
        imid = self._imid_handler

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('global_system_config_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'global-system-config'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                config_root_global_system_config_xml = ''
                meta = Metadata('config-root-global-system-config' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = config_root_global_system_config_xml)
                self._update_id_pair_meta(update, to_imid, meta)

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('domain_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'domain'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                config_root_domain_xml = ''
                meta = Metadata('config-root-domain' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = config_root_domain_xml)
                self._update_id_pair_meta(update, to_imid, meta)


        self._publish_update(my_imid, update)
        return (True, '')
    #end _ifmap_config_root_set

    def _ifmap_config_root_create(self, obj_ids, obj_dict):
        (ok, result) = self._ifmap_config_root_set(obj_ids['imid'], None, obj_dict)
        return (ok, result)
    #end _ifmap_config_root_create


    def _ifmap_config_root_read_to_meta_index(self, ifmap_id, field_names = None):
        # field_names = None means all fields will be read
        imid = self._imid_handler
        start_id = str(Identity(name = ifmap_id, type = 'other',
                        other_type = 'extended'))
        # if id-perms missing, identity doesn't exist

        all_metas = [u'id-perms', u'display-name', u'config-root-global-system-config', u'config-root-domain']
        if not field_names:
            metas_to_read = all_metas
        else: # read only requested fields
            metas_to_read = set(all_metas) & set(field_names.keys())

        # metas is a dict where key is meta-name and val is list of dict
        # of form [{'meta':meta}, {'id':id1, 'meta':meta}, {'id':id2, 'meta':meta}]
        metas = {}
        for meta_name in metas_to_read:
            if meta_name in self._id_to_metas[ifmap_id]:
                metas[meta_name] = self._id_to_metas[ifmap_id][meta_name]
        return metas
    #end _ifmap_config-root_read_to_meta_index

    def _ifmap_config_root_update(self, ifmap_id, new_obj_dict):
        # read in refs from ifmap to determine which ones become inactive after update
        existing_metas = self._ifmap_config_root_read_to_meta_index(ifmap_id)

        # remove properties that are no longer active
        props = ['id-perms', 'display-name']
        for prop in props:
            prop_m = prop.replace('-', '_')
            if prop in existing_metas and prop_m not in new_obj_dict:
                self._delete_id_self_meta(ifmap_id, 'contrail:'+prop)
        # remove refs that are no longer active
        delete_list = []
        refs = {}
        for meta, to_name in refs.items():
            old_set = set([m['id'] for m in existing_metas.get(meta, [])])
            new_set = set()
            to_name_m = to_name.replace('-', '_')
            for ref in new_obj_dict.get(to_name_m+'_refs', []):
                to_imid = self.fq_name_to_ifmap_id(to_name, ref['to'])
                new_set.add(to_imid)

            for inact_ref in old_set - new_set:
                delete_list.append((inact_ref, 'contrail:'+meta))

        if delete_list:
            self._delete_id_pair_meta_list(ifmap_id, delete_list)

        (ok, result) = self._ifmap_config_root_set(ifmap_id, existing_metas, new_obj_dict)
        return (ok, result)
    #end _ifmap_config_root_update

    def _ifmap_config_root_delete(self, obj_ids):
        ifmap_id = obj_ids['imid']
        parent_imid = obj_ids.get('parent_imid', None)
        existing_metas = self._ifmap_config_root_read_to_meta_index(ifmap_id)
        meta_list = []
        for meta_name, meta_infos in existing_metas.items():
            for meta_info in meta_infos:
                ref_imid = meta_info.get('id')
                if ref_imid is None:
                    continue
                meta_list.append((ref_imid, 'contrail:'+meta_name))

        if parent_imid:
            # Remove link from parent
            meta_list.append((parent_imid, None))

        if meta_list:
            self._delete_id_pair_meta_list(ifmap_id, meta_list)

        # Remove all property metadata associated with this ident
        self._delete_id_self_meta(ifmap_id, None)

        return (True, '')
    #end _ifmap_config_root_delete

    def _ifmap_subnet_alloc(self, parent_type, fq_name):
        imid = self._imid_handler
        (my_imid, parent_imid) = \
            imid.subnet_alloc_ifmap_id(parent_type, fq_name)
        if my_imid is None or parent_imid is None:
            return (False, (my_imid, parent_imid))
        return (True, (my_imid, parent_imid))
    #end _ifmap_subnet_alloc

    def _ifmap_subnet_set(self, my_imid, existing_metas, obj_dict):
        # Properties Meta
        update = {}
        field = obj_dict.get('subnet_ip_prefix', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['subnet_ip_prefix']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                SubnetType(**field).exportChildren(buf, level = 1, name_ = 'subnet-ip-prefix', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'subnet-ip-prefix', pretty_print = False)
            subnet_ip_prefix_xml = buf.getvalue()
            buf.close()
            meta = Metadata('subnet-ip-prefix' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = subnet_ip_prefix_xml)

            if (existing_metas and 'subnet-ip-prefix' in existing_metas and
                str(existing_metas['subnet-ip-prefix'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['id_perms']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                IdPermsType(**field).exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            id_perms_xml = buf.getvalue()
            buf.close()
            meta = Metadata('id-perms' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = id_perms_xml)

            if (existing_metas and 'id-perms' in existing_metas and
                str(existing_metas['id-perms'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('display_name', None)
        if field is not None:
            norm_str = escape(str(obj_dict['display_name']))
            meta = Metadata('display-name', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'display-name' in existing_metas and
                str(existing_metas['display-name'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        # Ref Link Metas
        imid = self._imid_handler

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('virtual_machine_interface_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'virtual-machine-interface'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                subnet_virtual_machine_interface_xml = ''
                meta = Metadata('subnet-virtual-machine-interface' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = subnet_virtual_machine_interface_xml)
                self._update_id_pair_meta(update, to_imid, meta)


        self._publish_update(my_imid, update)
        return (True, '')
    #end _ifmap_subnet_set

    def _ifmap_subnet_create(self, obj_ids, obj_dict):
        (ok, result) = self._ifmap_subnet_set(obj_ids['imid'], None, obj_dict)
        return (ok, result)
    #end _ifmap_subnet_create


    def _ifmap_subnet_read_to_meta_index(self, ifmap_id, field_names = None):
        # field_names = None means all fields will be read
        imid = self._imid_handler
        start_id = str(Identity(name = ifmap_id, type = 'other',
                        other_type = 'extended'))
        # if id-perms missing, identity doesn't exist

        all_metas = [u'subnet-virtual-machine-interface', u'subnet-ip-prefix', u'id-perms', u'display-name']
        if not field_names:
            metas_to_read = all_metas
        else: # read only requested fields
            metas_to_read = set(all_metas) & set(field_names.keys())

        # metas is a dict where key is meta-name and val is list of dict
        # of form [{'meta':meta}, {'id':id1, 'meta':meta}, {'id':id2, 'meta':meta}]
        metas = {}
        for meta_name in metas_to_read:
            if meta_name in self._id_to_metas[ifmap_id]:
                metas[meta_name] = self._id_to_metas[ifmap_id][meta_name]
        return metas
    #end _ifmap_subnet_read_to_meta_index

    def _ifmap_subnet_update(self, ifmap_id, new_obj_dict):
        # read in refs from ifmap to determine which ones become inactive after update
        existing_metas = self._ifmap_subnet_read_to_meta_index(ifmap_id)

        # remove properties that are no longer active
        props = ['subnet-ip-prefix', 'id-perms', 'display-name']
        for prop in props:
            prop_m = prop.replace('-', '_')
            if prop in existing_metas and prop_m not in new_obj_dict:
                self._delete_id_self_meta(ifmap_id, 'contrail:'+prop)
        # remove refs that are no longer active
        delete_list = []
        refs = {'subnet-virtual-machine-interface': 'virtual-machine-interface'}
        for meta, to_name in refs.items():
            old_set = set([m['id'] for m in existing_metas.get(meta, [])])
            new_set = set()
            to_name_m = to_name.replace('-', '_')
            for ref in new_obj_dict.get(to_name_m+'_refs', []):
                to_imid = self.fq_name_to_ifmap_id(to_name, ref['to'])
                new_set.add(to_imid)

            for inact_ref in old_set - new_set:
                delete_list.append((inact_ref, 'contrail:'+meta))

        if delete_list:
            self._delete_id_pair_meta_list(ifmap_id, delete_list)

        (ok, result) = self._ifmap_subnet_set(ifmap_id, existing_metas, new_obj_dict)
        return (ok, result)
    #end _ifmap_subnet_update

    def _ifmap_subnet_delete(self, obj_ids):
        ifmap_id = obj_ids['imid']
        parent_imid = obj_ids.get('parent_imid', None)
        existing_metas = self._ifmap_subnet_read_to_meta_index(ifmap_id)
        meta_list = []
        for meta_name, meta_infos in existing_metas.items():
            for meta_info in meta_infos:
                ref_imid = meta_info.get('id')
                if ref_imid is None:
                    continue
                meta_list.append((ref_imid, 'contrail:'+meta_name))

        if parent_imid:
            # Remove link from parent
            meta_list.append((parent_imid, None))

        if meta_list:
            self._delete_id_pair_meta_list(ifmap_id, meta_list)

        # Remove all property metadata associated with this ident
        self._delete_id_self_meta(ifmap_id, None)

        return (True, '')
    #end _ifmap_subnet_delete

    def _ifmap_global_system_config_alloc(self, parent_type, fq_name):
        imid = self._imid_handler
        (my_imid, parent_imid) = \
            imid.global_system_config_alloc_ifmap_id(parent_type, fq_name)
        if my_imid is None or parent_imid is None:
            return (False, (my_imid, parent_imid))
        return (True, (my_imid, parent_imid))
    #end _ifmap_global_system_config_alloc

    def _ifmap_global_system_config_set(self, my_imid, existing_metas, obj_dict):
        # Properties Meta
        update = {}
        field = obj_dict.get('autonomous_system', None)
        if field is not None:
            norm_str = escape(str(obj_dict['autonomous_system']))
            meta = Metadata('autonomous-system', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'autonomous-system' in existing_metas and
                str(existing_metas['autonomous-system'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('config_version', None)
        if field is not None:
            norm_str = escape(str(obj_dict['config_version']))
            meta = Metadata('config-version', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'config-version' in existing_metas and
                str(existing_metas['config-version'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('plugin_tuning', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['plugin_tuning']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                PluginProperties(**field).exportChildren(buf, level = 1, name_ = 'plugin-tuning', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'plugin-tuning', pretty_print = False)
            plugin_tuning_xml = buf.getvalue()
            buf.close()
            meta = Metadata('plugin-tuning' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = plugin_tuning_xml)

            if (existing_metas and 'plugin-tuning' in existing_metas and
                str(existing_metas['plugin-tuning'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('ibgp_auto_mesh', None)
        if field is not None:
            norm_str = escape(str(obj_dict['ibgp_auto_mesh']))
            meta = Metadata('ibgp-auto-mesh', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'ibgp-auto-mesh' in existing_metas and
                str(existing_metas['ibgp-auto-mesh'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('ip_fabric_subnets', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['ip_fabric_subnets']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                SubnetListType(**field).exportChildren(buf, level = 1, name_ = 'ip-fabric-subnets', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'ip-fabric-subnets', pretty_print = False)
            ip_fabric_subnets_xml = buf.getvalue()
            buf.close()
            meta = Metadata('ip-fabric-subnets' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = ip_fabric_subnets_xml)

            if (existing_metas and 'ip-fabric-subnets' in existing_metas and
                str(existing_metas['ip-fabric-subnets'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['id_perms']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                IdPermsType(**field).exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            id_perms_xml = buf.getvalue()
            buf.close()
            meta = Metadata('id-perms' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = id_perms_xml)

            if (existing_metas and 'id-perms' in existing_metas and
                str(existing_metas['id-perms'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('display_name', None)
        if field is not None:
            norm_str = escape(str(obj_dict['display_name']))
            meta = Metadata('display-name', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'display-name' in existing_metas and
                str(existing_metas['display-name'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        # Ref Link Metas
        imid = self._imid_handler

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('bgp_router_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'bgp-router'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                global_system_config_bgp_router_xml = ''
                meta = Metadata('global-system-config-bgp-router' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = global_system_config_bgp_router_xml)
                self._update_id_pair_meta(update, to_imid, meta)

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('global_vrouter_config_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'global-vrouter-config'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                global_system_config_global_vrouter_config_xml = ''
                meta = Metadata('global-system-config-global-vrouter-config' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = global_system_config_global_vrouter_config_xml)
                self._update_id_pair_meta(update, to_imid, meta)

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('physical_router_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'physical-router'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                global_system_config_physical_router_xml = ''
                meta = Metadata('global-system-config-physical-router' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = global_system_config_physical_router_xml)
                self._update_id_pair_meta(update, to_imid, meta)

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('virtual_router_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'virtual-router'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                global_system_config_virtual_router_xml = ''
                meta = Metadata('global-system-config-virtual-router' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = global_system_config_virtual_router_xml)
                self._update_id_pair_meta(update, to_imid, meta)

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('config_node_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'config-node'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                global_system_config_config_node_xml = ''
                meta = Metadata('global-system-config-config-node' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = global_system_config_config_node_xml)
                self._update_id_pair_meta(update, to_imid, meta)

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('analytics_node_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'analytics-node'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                global_system_config_analytics_node_xml = ''
                meta = Metadata('global-system-config-analytics-node' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = global_system_config_analytics_node_xml)
                self._update_id_pair_meta(update, to_imid, meta)

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('database_node_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'database-node'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                global_system_config_database_node_xml = ''
                meta = Metadata('global-system-config-database-node' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = global_system_config_database_node_xml)
                self._update_id_pair_meta(update, to_imid, meta)

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('service_appliance_set_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'service-appliance-set'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                global_system_config_service_appliance_set_xml = ''
                meta = Metadata('global-system-config-service-appliance-set' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = global_system_config_service_appliance_set_xml)
                self._update_id_pair_meta(update, to_imid, meta)


        self._publish_update(my_imid, update)
        return (True, '')
    #end _ifmap_global_system_config_set

    def _ifmap_global_system_config_create(self, obj_ids, obj_dict):
        if not 'parent_type' in obj_dict:
            # parent is config-root
            parent_type = 'config-root'
            parent_imid = 'contrail:config-root:root'
        else:
            parent_type = obj_dict['parent_type']
            parent_imid = obj_ids.get('parent_imid', None)

        # Parent Link Meta
        update = {}
        parent_link_meta = self._parent_metas[parent_type]['global-system-config']
        meta = Metadata(parent_link_meta, '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')
        self._update_id_pair_meta(update, obj_ids['imid'], meta)
        self._publish_update(parent_imid, update)

        (ok, result) = self._ifmap_global_system_config_set(obj_ids['imid'], None, obj_dict)
        return (ok, result)
    #end _ifmap_global_system_config_create


    def _ifmap_global_system_config_read_to_meta_index(self, ifmap_id, field_names = None):
        # field_names = None means all fields will be read
        imid = self._imid_handler
        start_id = str(Identity(name = ifmap_id, type = 'other',
                        other_type = 'extended'))
        # if id-perms missing, identity doesn't exist

        all_metas = [u'global-system-config-bgp-router', u'autonomous-system', u'config-version', u'plugin-tuning', u'ibgp-auto-mesh', u'ip-fabric-subnets', u'id-perms', u'display-name', u'global-system-config-global-vrouter-config', u'global-system-config-physical-router', u'global-system-config-virtual-router', u'global-system-config-config-node', u'global-system-config-analytics-node', u'global-system-config-database-node', u'global-system-config-service-appliance-set']
        if not field_names:
            metas_to_read = all_metas
        else: # read only requested fields
            metas_to_read = set(all_metas) & set(field_names.keys())

        # metas is a dict where key is meta-name and val is list of dict
        # of form [{'meta':meta}, {'id':id1, 'meta':meta}, {'id':id2, 'meta':meta}]
        metas = {}
        for meta_name in metas_to_read:
            if meta_name in self._id_to_metas[ifmap_id]:
                metas[meta_name] = self._id_to_metas[ifmap_id][meta_name]
        return metas
    #end _ifmap_global-system-config_read_to_meta_index

    def _ifmap_global_system_config_update(self, ifmap_id, new_obj_dict):
        # read in refs from ifmap to determine which ones become inactive after update
        existing_metas = self._ifmap_global_system_config_read_to_meta_index(ifmap_id)

        # remove properties that are no longer active
        props = ['autonomous-system', 'config-version', 'plugin-tuning', 'ibgp-auto-mesh', 'ip-fabric-subnets', 'id-perms', 'display-name']
        for prop in props:
            prop_m = prop.replace('-', '_')
            if prop in existing_metas and prop_m not in new_obj_dict:
                self._delete_id_self_meta(ifmap_id, 'contrail:'+prop)
        # remove refs that are no longer active
        delete_list = []
        refs = {'global-system-config-bgp-router': 'bgp-router'}
        for meta, to_name in refs.items():
            old_set = set([m['id'] for m in existing_metas.get(meta, [])])
            new_set = set()
            to_name_m = to_name.replace('-', '_')
            for ref in new_obj_dict.get(to_name_m+'_refs', []):
                to_imid = self.fq_name_to_ifmap_id(to_name, ref['to'])
                new_set.add(to_imid)

            for inact_ref in old_set - new_set:
                delete_list.append((inact_ref, 'contrail:'+meta))

        if delete_list:
            self._delete_id_pair_meta_list(ifmap_id, delete_list)

        (ok, result) = self._ifmap_global_system_config_set(ifmap_id, existing_metas, new_obj_dict)
        return (ok, result)
    #end _ifmap_global_system_config_update

    def _ifmap_global_system_config_delete(self, obj_ids):
        ifmap_id = obj_ids['imid']
        parent_imid = obj_ids.get('parent_imid', None)
        existing_metas = self._ifmap_global_system_config_read_to_meta_index(ifmap_id)
        meta_list = []
        for meta_name, meta_infos in existing_metas.items():
            for meta_info in meta_infos:
                ref_imid = meta_info.get('id')
                if ref_imid is None:
                    continue
                meta_list.append((ref_imid, 'contrail:'+meta_name))

        if parent_imid:
            # Remove link from parent
            meta_list.append((parent_imid, None))

        if meta_list:
            self._delete_id_pair_meta_list(ifmap_id, meta_list)

        # Remove all property metadata associated with this ident
        self._delete_id_self_meta(ifmap_id, None)

        return (True, '')
    #end _ifmap_global_system_config_delete

    def _ifmap_service_appliance_alloc(self, parent_type, fq_name):
        imid = self._imid_handler
        (my_imid, parent_imid) = \
            imid.service_appliance_alloc_ifmap_id(parent_type, fq_name)
        if my_imid is None or parent_imid is None:
            return (False, (my_imid, parent_imid))
        return (True, (my_imid, parent_imid))
    #end _ifmap_service_appliance_alloc

    def _ifmap_service_appliance_set(self, my_imid, existing_metas, obj_dict):
        # Properties Meta
        update = {}
        field = obj_dict.get('service_appliance_user_credentials', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['service_appliance_user_credentials']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                UserCredentials(**field).exportChildren(buf, level = 1, name_ = 'service-appliance-user-credentials', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'service-appliance-user-credentials', pretty_print = False)
            service_appliance_user_credentials_xml = buf.getvalue()
            buf.close()
            meta = Metadata('service-appliance-user-credentials' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = service_appliance_user_credentials_xml)

            if (existing_metas and 'service-appliance-user-credentials' in existing_metas and
                str(existing_metas['service-appliance-user-credentials'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('service_appliance_ip_address', None)
        if field is not None:
            norm_str = escape(str(obj_dict['service_appliance_ip_address']))
            meta = Metadata('service-appliance-ip-address', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'service-appliance-ip-address' in existing_metas and
                str(existing_metas['service-appliance-ip-address'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('service_appliance_properties', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['service_appliance_properties']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                KeyValuePairs(**field).exportChildren(buf, level = 1, name_ = 'service-appliance-properties', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'service-appliance-properties', pretty_print = False)
            service_appliance_properties_xml = buf.getvalue()
            buf.close()
            meta = Metadata('service-appliance-properties' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = service_appliance_properties_xml)

            if (existing_metas and 'service-appliance-properties' in existing_metas and
                str(existing_metas['service-appliance-properties'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['id_perms']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                IdPermsType(**field).exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            id_perms_xml = buf.getvalue()
            buf.close()
            meta = Metadata('id-perms' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = id_perms_xml)

            if (existing_metas and 'id-perms' in existing_metas and
                str(existing_metas['id-perms'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('display_name', None)
        if field is not None:
            norm_str = escape(str(obj_dict['display_name']))
            meta = Metadata('display-name', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'display-name' in existing_metas and
                str(existing_metas['display-name'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        # Ref Link Metas
        imid = self._imid_handler


        self._publish_update(my_imid, update)
        return (True, '')
    #end _ifmap_service_appliance_set

    def _ifmap_service_appliance_create(self, obj_ids, obj_dict):
        if not 'parent_type' in obj_dict:
            # parent is config-root
            parent_type = 'config-root'
            parent_imid = 'contrail:config-root:root'
        else:
            parent_type = obj_dict['parent_type']
            parent_imid = obj_ids.get('parent_imid', None)

        # Parent Link Meta
        update = {}
        parent_link_meta = self._parent_metas[parent_type]['service-appliance']
        meta = Metadata(parent_link_meta, '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')
        self._update_id_pair_meta(update, obj_ids['imid'], meta)
        self._publish_update(parent_imid, update)

        (ok, result) = self._ifmap_service_appliance_set(obj_ids['imid'], None, obj_dict)
        return (ok, result)
    #end _ifmap_service_appliance_create


    def _ifmap_service_appliance_read_to_meta_index(self, ifmap_id, field_names = None):
        # field_names = None means all fields will be read
        imid = self._imid_handler
        start_id = str(Identity(name = ifmap_id, type = 'other',
                        other_type = 'extended'))
        # if id-perms missing, identity doesn't exist

        all_metas = [u'service-appliance-user-credentials', u'service-appliance-ip-address', u'service-appliance-properties', u'id-perms', u'display-name']
        if not field_names:
            metas_to_read = all_metas
        else: # read only requested fields
            metas_to_read = set(all_metas) & set(field_names.keys())

        # metas is a dict where key is meta-name and val is list of dict
        # of form [{'meta':meta}, {'id':id1, 'meta':meta}, {'id':id2, 'meta':meta}]
        metas = {}
        for meta_name in metas_to_read:
            if meta_name in self._id_to_metas[ifmap_id]:
                metas[meta_name] = self._id_to_metas[ifmap_id][meta_name]
        return metas
    #end _ifmap_service-appliance_read_to_meta_index

    def _ifmap_service_appliance_update(self, ifmap_id, new_obj_dict):
        # read in refs from ifmap to determine which ones become inactive after update
        existing_metas = self._ifmap_service_appliance_read_to_meta_index(ifmap_id)

        # remove properties that are no longer active
        props = ['service-appliance-user-credentials', 'service-appliance-ip-address', 'service-appliance-properties', 'id-perms', 'display-name']
        for prop in props:
            prop_m = prop.replace('-', '_')
            if prop in existing_metas and prop_m not in new_obj_dict:
                self._delete_id_self_meta(ifmap_id, 'contrail:'+prop)
        # remove refs that are no longer active
        delete_list = []
        refs = {}
        for meta, to_name in refs.items():
            old_set = set([m['id'] for m in existing_metas.get(meta, [])])
            new_set = set()
            to_name_m = to_name.replace('-', '_')
            for ref in new_obj_dict.get(to_name_m+'_refs', []):
                to_imid = self.fq_name_to_ifmap_id(to_name, ref['to'])
                new_set.add(to_imid)

            for inact_ref in old_set - new_set:
                delete_list.append((inact_ref, 'contrail:'+meta))

        if delete_list:
            self._delete_id_pair_meta_list(ifmap_id, delete_list)

        (ok, result) = self._ifmap_service_appliance_set(ifmap_id, existing_metas, new_obj_dict)
        return (ok, result)
    #end _ifmap_service_appliance_update

    def _ifmap_service_appliance_delete(self, obj_ids):
        ifmap_id = obj_ids['imid']
        parent_imid = obj_ids.get('parent_imid', None)
        existing_metas = self._ifmap_service_appliance_read_to_meta_index(ifmap_id)
        meta_list = []
        for meta_name, meta_infos in existing_metas.items():
            for meta_info in meta_infos:
                ref_imid = meta_info.get('id')
                if ref_imid is None:
                    continue
                meta_list.append((ref_imid, 'contrail:'+meta_name))

        if parent_imid:
            # Remove link from parent
            meta_list.append((parent_imid, None))

        if meta_list:
            self._delete_id_pair_meta_list(ifmap_id, meta_list)

        # Remove all property metadata associated with this ident
        self._delete_id_self_meta(ifmap_id, None)

        return (True, '')
    #end _ifmap_service_appliance_delete

    def _ifmap_service_instance_alloc(self, parent_type, fq_name):
        imid = self._imid_handler
        (my_imid, parent_imid) = \
            imid.service_instance_alloc_ifmap_id(parent_type, fq_name)
        if my_imid is None or parent_imid is None:
            return (False, (my_imid, parent_imid))
        return (True, (my_imid, parent_imid))
    #end _ifmap_service_instance_alloc

    def _ifmap_service_instance_set(self, my_imid, existing_metas, obj_dict):
        # Properties Meta
        update = {}
        field = obj_dict.get('service_instance_properties', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['service_instance_properties']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                ServiceInstanceType(**field).exportChildren(buf, level = 1, name_ = 'service-instance-properties', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'service-instance-properties', pretty_print = False)
            service_instance_properties_xml = buf.getvalue()
            buf.close()
            meta = Metadata('service-instance-properties' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = service_instance_properties_xml)

            if (existing_metas and 'service-instance-properties' in existing_metas and
                str(existing_metas['service-instance-properties'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['id_perms']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                IdPermsType(**field).exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            id_perms_xml = buf.getvalue()
            buf.close()
            meta = Metadata('id-perms' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = id_perms_xml)

            if (existing_metas and 'id-perms' in existing_metas and
                str(existing_metas['id-perms'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('display_name', None)
        if field is not None:
            norm_str = escape(str(obj_dict['display_name']))
            meta = Metadata('display-name', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'display-name' in existing_metas and
                str(existing_metas['display-name'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        # Ref Link Metas
        imid = self._imid_handler

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('service_template_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'service-template'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                service_instance_service_template_xml = ''
                meta = Metadata('service-instance-service-template' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = service_instance_service_template_xml)
                self._update_id_pair_meta(update, to_imid, meta)


        self._publish_update(my_imid, update)
        return (True, '')
    #end _ifmap_service_instance_set

    def _ifmap_service_instance_create(self, obj_ids, obj_dict):
        if not 'parent_type' in obj_dict:
            # parent is config-root
            parent_type = 'config-root'
            parent_imid = 'contrail:config-root:root'
        else:
            parent_type = obj_dict['parent_type']
            parent_imid = obj_ids.get('parent_imid', None)

        # Parent Link Meta
        update = {}
        parent_link_meta = self._parent_metas[parent_type]['service-instance']
        meta = Metadata(parent_link_meta, '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')
        self._update_id_pair_meta(update, obj_ids['imid'], meta)
        self._publish_update(parent_imid, update)

        (ok, result) = self._ifmap_service_instance_set(obj_ids['imid'], None, obj_dict)
        return (ok, result)
    #end _ifmap_service_instance_create


    def _ifmap_service_instance_read_to_meta_index(self, ifmap_id, field_names = None):
        # field_names = None means all fields will be read
        imid = self._imid_handler
        start_id = str(Identity(name = ifmap_id, type = 'other',
                        other_type = 'extended'))
        # if id-perms missing, identity doesn't exist

        all_metas = [u'service-instance-service-template', u'virtual-machine-service-instance', u'logical-router-service-instance', u'loadbalancer-pool-service-instance', u'service-instance-properties', u'id-perms', u'display-name']
        if not field_names:
            metas_to_read = all_metas
        else: # read only requested fields
            metas_to_read = set(all_metas) & set(field_names.keys())

        # metas is a dict where key is meta-name and val is list of dict
        # of form [{'meta':meta}, {'id':id1, 'meta':meta}, {'id':id2, 'meta':meta}]
        metas = {}
        for meta_name in metas_to_read:
            if meta_name in self._id_to_metas[ifmap_id]:
                metas[meta_name] = self._id_to_metas[ifmap_id][meta_name]
        return metas
    #end _ifmap_service-instance_read_to_meta_index

    def _ifmap_service_instance_update(self, ifmap_id, new_obj_dict):
        # read in refs from ifmap to determine which ones become inactive after update
        existing_metas = self._ifmap_service_instance_read_to_meta_index(ifmap_id)

        # remove properties that are no longer active
        props = ['service-instance-properties', 'id-perms', 'display-name']
        for prop in props:
            prop_m = prop.replace('-', '_')
            if prop in existing_metas and prop_m not in new_obj_dict:
                self._delete_id_self_meta(ifmap_id, 'contrail:'+prop)
        # remove refs that are no longer active
        delete_list = []
        refs = {'service-instance-service-template': 'service-template'}
        for meta, to_name in refs.items():
            old_set = set([m['id'] for m in existing_metas.get(meta, [])])
            new_set = set()
            to_name_m = to_name.replace('-', '_')
            for ref in new_obj_dict.get(to_name_m+'_refs', []):
                to_imid = self.fq_name_to_ifmap_id(to_name, ref['to'])
                new_set.add(to_imid)

            for inact_ref in old_set - new_set:
                delete_list.append((inact_ref, 'contrail:'+meta))

        if delete_list:
            self._delete_id_pair_meta_list(ifmap_id, delete_list)

        (ok, result) = self._ifmap_service_instance_set(ifmap_id, existing_metas, new_obj_dict)
        return (ok, result)
    #end _ifmap_service_instance_update

    def _ifmap_service_instance_delete(self, obj_ids):
        ifmap_id = obj_ids['imid']
        parent_imid = obj_ids.get('parent_imid', None)
        existing_metas = self._ifmap_service_instance_read_to_meta_index(ifmap_id)
        meta_list = []
        for meta_name, meta_infos in existing_metas.items():
            for meta_info in meta_infos:
                ref_imid = meta_info.get('id')
                if ref_imid is None:
                    continue
                meta_list.append((ref_imid, 'contrail:'+meta_name))

        if parent_imid:
            # Remove link from parent
            meta_list.append((parent_imid, None))

        if meta_list:
            self._delete_id_pair_meta_list(ifmap_id, meta_list)

        # Remove all property metadata associated with this ident
        self._delete_id_self_meta(ifmap_id, None)

        return (True, '')
    #end _ifmap_service_instance_delete

    def _ifmap_namespace_alloc(self, parent_type, fq_name):
        imid = self._imid_handler
        (my_imid, parent_imid) = \
            imid.namespace_alloc_ifmap_id(parent_type, fq_name)
        if my_imid is None or parent_imid is None:
            return (False, (my_imid, parent_imid))
        return (True, (my_imid, parent_imid))
    #end _ifmap_namespace_alloc

    def _ifmap_namespace_set(self, my_imid, existing_metas, obj_dict):
        # Properties Meta
        update = {}
        field = obj_dict.get('namespace_cidr', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['namespace_cidr']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                SubnetType(**field).exportChildren(buf, level = 1, name_ = 'namespace-cidr', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'namespace-cidr', pretty_print = False)
            namespace_cidr_xml = buf.getvalue()
            buf.close()
            meta = Metadata('namespace-cidr' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = namespace_cidr_xml)

            if (existing_metas and 'namespace-cidr' in existing_metas and
                str(existing_metas['namespace-cidr'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['id_perms']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                IdPermsType(**field).exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            id_perms_xml = buf.getvalue()
            buf.close()
            meta = Metadata('id-perms' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = id_perms_xml)

            if (existing_metas and 'id-perms' in existing_metas and
                str(existing_metas['id-perms'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('display_name', None)
        if field is not None:
            norm_str = escape(str(obj_dict['display_name']))
            meta = Metadata('display-name', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'display-name' in existing_metas and
                str(existing_metas['display-name'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        # Ref Link Metas
        imid = self._imid_handler


        self._publish_update(my_imid, update)
        return (True, '')
    #end _ifmap_namespace_set

    def _ifmap_namespace_create(self, obj_ids, obj_dict):
        if not 'parent_type' in obj_dict:
            # parent is config-root
            parent_type = 'config-root'
            parent_imid = 'contrail:config-root:root'
        else:
            parent_type = obj_dict['parent_type']
            parent_imid = obj_ids.get('parent_imid', None)

        # Parent Link Meta
        update = {}
        parent_link_meta = self._parent_metas[parent_type]['namespace']
        meta = Metadata(parent_link_meta, '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')
        self._update_id_pair_meta(update, obj_ids['imid'], meta)
        self._publish_update(parent_imid, update)

        (ok, result) = self._ifmap_namespace_set(obj_ids['imid'], None, obj_dict)
        return (ok, result)
    #end _ifmap_namespace_create


    def _ifmap_namespace_read_to_meta_index(self, ifmap_id, field_names = None):
        # field_names = None means all fields will be read
        imid = self._imid_handler
        start_id = str(Identity(name = ifmap_id, type = 'other',
                        other_type = 'extended'))
        # if id-perms missing, identity doesn't exist

        all_metas = [u'project-namespace', u'namespace-cidr', u'id-perms', u'display-name']
        if not field_names:
            metas_to_read = all_metas
        else: # read only requested fields
            metas_to_read = set(all_metas) & set(field_names.keys())

        # metas is a dict where key is meta-name and val is list of dict
        # of form [{'meta':meta}, {'id':id1, 'meta':meta}, {'id':id2, 'meta':meta}]
        metas = {}
        for meta_name in metas_to_read:
            if meta_name in self._id_to_metas[ifmap_id]:
                metas[meta_name] = self._id_to_metas[ifmap_id][meta_name]
        return metas
    #end _ifmap_namespace_read_to_meta_index

    def _ifmap_namespace_update(self, ifmap_id, new_obj_dict):
        # read in refs from ifmap to determine which ones become inactive after update
        existing_metas = self._ifmap_namespace_read_to_meta_index(ifmap_id)

        # remove properties that are no longer active
        props = ['namespace-cidr', 'id-perms', 'display-name']
        for prop in props:
            prop_m = prop.replace('-', '_')
            if prop in existing_metas and prop_m not in new_obj_dict:
                self._delete_id_self_meta(ifmap_id, 'contrail:'+prop)
        # remove refs that are no longer active
        delete_list = []
        refs = {}
        for meta, to_name in refs.items():
            old_set = set([m['id'] for m in existing_metas.get(meta, [])])
            new_set = set()
            to_name_m = to_name.replace('-', '_')
            for ref in new_obj_dict.get(to_name_m+'_refs', []):
                to_imid = self.fq_name_to_ifmap_id(to_name, ref['to'])
                new_set.add(to_imid)

            for inact_ref in old_set - new_set:
                delete_list.append((inact_ref, 'contrail:'+meta))

        if delete_list:
            self._delete_id_pair_meta_list(ifmap_id, delete_list)

        (ok, result) = self._ifmap_namespace_set(ifmap_id, existing_metas, new_obj_dict)
        return (ok, result)
    #end _ifmap_namespace_update

    def _ifmap_namespace_delete(self, obj_ids):
        ifmap_id = obj_ids['imid']
        parent_imid = obj_ids.get('parent_imid', None)
        existing_metas = self._ifmap_namespace_read_to_meta_index(ifmap_id)
        meta_list = []
        for meta_name, meta_infos in existing_metas.items():
            for meta_info in meta_infos:
                ref_imid = meta_info.get('id')
                if ref_imid is None:
                    continue
                meta_list.append((ref_imid, 'contrail:'+meta_name))

        if parent_imid:
            # Remove link from parent
            meta_list.append((parent_imid, None))

        if meta_list:
            self._delete_id_pair_meta_list(ifmap_id, meta_list)

        # Remove all property metadata associated with this ident
        self._delete_id_self_meta(ifmap_id, None)

        return (True, '')
    #end _ifmap_namespace_delete

    def _ifmap_logical_interface_alloc(self, parent_type, fq_name):
        imid = self._imid_handler
        (my_imid, parent_imid) = \
            imid.logical_interface_alloc_ifmap_id(parent_type, fq_name)
        if my_imid is None or parent_imid is None:
            return (False, (my_imid, parent_imid))
        return (True, (my_imid, parent_imid))
    #end _ifmap_logical_interface_alloc

    def _ifmap_logical_interface_set(self, my_imid, existing_metas, obj_dict):
        # Properties Meta
        update = {}
        field = obj_dict.get('logical_interface_vlan_tag', None)
        if field is not None:
            norm_str = escape(str(obj_dict['logical_interface_vlan_tag']))
            meta = Metadata('logical-interface-vlan-tag', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'logical-interface-vlan-tag' in existing_metas and
                str(existing_metas['logical-interface-vlan-tag'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('logical_interface_type', None)
        if field is not None:
            norm_str = escape(str(obj_dict['logical_interface_type']))
            meta = Metadata('logical-interface-type', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'logical-interface-type' in existing_metas and
                str(existing_metas['logical-interface-type'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['id_perms']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                IdPermsType(**field).exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            id_perms_xml = buf.getvalue()
            buf.close()
            meta = Metadata('id-perms' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = id_perms_xml)

            if (existing_metas and 'id-perms' in existing_metas and
                str(existing_metas['id-perms'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('display_name', None)
        if field is not None:
            norm_str = escape(str(obj_dict['display_name']))
            meta = Metadata('display-name', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'display-name' in existing_metas and
                str(existing_metas['display-name'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        # Ref Link Metas
        imid = self._imid_handler

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('virtual_machine_interface_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'virtual-machine-interface'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                logical_interface_virtual_machine_interface_xml = ''
                meta = Metadata('logical-interface-virtual-machine-interface' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = logical_interface_virtual_machine_interface_xml)
                self._update_id_pair_meta(update, to_imid, meta)


        self._publish_update(my_imid, update)
        return (True, '')
    #end _ifmap_logical_interface_set

    def _ifmap_logical_interface_create(self, obj_ids, obj_dict):
        if not 'parent_type' in obj_dict:
            # parent is config-root
            parent_type = 'config-root'
            parent_imid = 'contrail:config-root:root'
        else:
            parent_type = obj_dict['parent_type']
            parent_imid = obj_ids.get('parent_imid', None)

        # Parent Link Meta
        update = {}
        parent_link_meta = self._parent_metas[parent_type]['logical-interface']
        meta = Metadata(parent_link_meta, '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')
        self._update_id_pair_meta(update, obj_ids['imid'], meta)
        self._publish_update(parent_imid, update)

        (ok, result) = self._ifmap_logical_interface_set(obj_ids['imid'], None, obj_dict)
        return (ok, result)
    #end _ifmap_logical_interface_create


    def _ifmap_logical_interface_read_to_meta_index(self, ifmap_id, field_names = None):
        # field_names = None means all fields will be read
        imid = self._imid_handler
        start_id = str(Identity(name = ifmap_id, type = 'other',
                        other_type = 'extended'))
        # if id-perms missing, identity doesn't exist

        all_metas = [u'logical-interface-virtual-machine-interface', u'logical-interface-vlan-tag', u'logical-interface-type', u'id-perms', u'display-name']
        if not field_names:
            metas_to_read = all_metas
        else: # read only requested fields
            metas_to_read = set(all_metas) & set(field_names.keys())

        # metas is a dict where key is meta-name and val is list of dict
        # of form [{'meta':meta}, {'id':id1, 'meta':meta}, {'id':id2, 'meta':meta}]
        metas = {}
        for meta_name in metas_to_read:
            if meta_name in self._id_to_metas[ifmap_id]:
                metas[meta_name] = self._id_to_metas[ifmap_id][meta_name]
        return metas
    #end _ifmap_logical-interface_read_to_meta_index

    def _ifmap_logical_interface_update(self, ifmap_id, new_obj_dict):
        # read in refs from ifmap to determine which ones become inactive after update
        existing_metas = self._ifmap_logical_interface_read_to_meta_index(ifmap_id)

        # remove properties that are no longer active
        props = ['logical-interface-vlan-tag', 'logical-interface-type', 'id-perms', 'display-name']
        for prop in props:
            prop_m = prop.replace('-', '_')
            if prop in existing_metas and prop_m not in new_obj_dict:
                self._delete_id_self_meta(ifmap_id, 'contrail:'+prop)
        # remove refs that are no longer active
        delete_list = []
        refs = {'logical-interface-virtual-machine-interface': 'virtual-machine-interface'}
        for meta, to_name in refs.items():
            old_set = set([m['id'] for m in existing_metas.get(meta, [])])
            new_set = set()
            to_name_m = to_name.replace('-', '_')
            for ref in new_obj_dict.get(to_name_m+'_refs', []):
                to_imid = self.fq_name_to_ifmap_id(to_name, ref['to'])
                new_set.add(to_imid)

            for inact_ref in old_set - new_set:
                delete_list.append((inact_ref, 'contrail:'+meta))

        if delete_list:
            self._delete_id_pair_meta_list(ifmap_id, delete_list)

        (ok, result) = self._ifmap_logical_interface_set(ifmap_id, existing_metas, new_obj_dict)
        return (ok, result)
    #end _ifmap_logical_interface_update

    def _ifmap_logical_interface_delete(self, obj_ids):
        ifmap_id = obj_ids['imid']
        parent_imid = obj_ids.get('parent_imid', None)
        existing_metas = self._ifmap_logical_interface_read_to_meta_index(ifmap_id)
        meta_list = []
        for meta_name, meta_infos in existing_metas.items():
            for meta_info in meta_infos:
                ref_imid = meta_info.get('id')
                if ref_imid is None:
                    continue
                meta_list.append((ref_imid, 'contrail:'+meta_name))

        if parent_imid:
            # Remove link from parent
            meta_list.append((parent_imid, None))

        if meta_list:
            self._delete_id_pair_meta_list(ifmap_id, meta_list)

        # Remove all property metadata associated with this ident
        self._delete_id_self_meta(ifmap_id, None)

        return (True, '')
    #end _ifmap_logical_interface_delete

    def _ifmap_route_table_alloc(self, parent_type, fq_name):
        imid = self._imid_handler
        (my_imid, parent_imid) = \
            imid.route_table_alloc_ifmap_id(parent_type, fq_name)
        if my_imid is None or parent_imid is None:
            return (False, (my_imid, parent_imid))
        return (True, (my_imid, parent_imid))
    #end _ifmap_route_table_alloc

    def _ifmap_route_table_set(self, my_imid, existing_metas, obj_dict):
        # Properties Meta
        update = {}
        field = obj_dict.get('routes', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['routes']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                RouteTableType(**field).exportChildren(buf, level = 1, name_ = 'routes', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'routes', pretty_print = False)
            routes_xml = buf.getvalue()
            buf.close()
            meta = Metadata('routes' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = routes_xml)

            if (existing_metas and 'routes' in existing_metas and
                str(existing_metas['routes'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['id_perms']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                IdPermsType(**field).exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            id_perms_xml = buf.getvalue()
            buf.close()
            meta = Metadata('id-perms' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = id_perms_xml)

            if (existing_metas and 'id-perms' in existing_metas and
                str(existing_metas['id-perms'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('display_name', None)
        if field is not None:
            norm_str = escape(str(obj_dict['display_name']))
            meta = Metadata('display-name', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'display-name' in existing_metas and
                str(existing_metas['display-name'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        # Ref Link Metas
        imid = self._imid_handler


        self._publish_update(my_imid, update)
        return (True, '')
    #end _ifmap_route_table_set

    def _ifmap_route_table_create(self, obj_ids, obj_dict):
        if not 'parent_type' in obj_dict:
            # parent is config-root
            parent_type = 'config-root'
            parent_imid = 'contrail:config-root:root'
        else:
            parent_type = obj_dict['parent_type']
            parent_imid = obj_ids.get('parent_imid', None)

        # Parent Link Meta
        update = {}
        parent_link_meta = self._parent_metas[parent_type]['route-table']
        meta = Metadata(parent_link_meta, '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')
        self._update_id_pair_meta(update, obj_ids['imid'], meta)
        self._publish_update(parent_imid, update)

        (ok, result) = self._ifmap_route_table_set(obj_ids['imid'], None, obj_dict)
        return (ok, result)
    #end _ifmap_route_table_create


    def _ifmap_route_table_read_to_meta_index(self, ifmap_id, field_names = None):
        # field_names = None means all fields will be read
        imid = self._imid_handler
        start_id = str(Identity(name = ifmap_id, type = 'other',
                        other_type = 'extended'))
        # if id-perms missing, identity doesn't exist

        all_metas = [u'virtual-network-route-table', u'routes', u'id-perms', u'display-name']
        if not field_names:
            metas_to_read = all_metas
        else: # read only requested fields
            metas_to_read = set(all_metas) & set(field_names.keys())

        # metas is a dict where key is meta-name and val is list of dict
        # of form [{'meta':meta}, {'id':id1, 'meta':meta}, {'id':id2, 'meta':meta}]
        metas = {}
        for meta_name in metas_to_read:
            if meta_name in self._id_to_metas[ifmap_id]:
                metas[meta_name] = self._id_to_metas[ifmap_id][meta_name]
        return metas
    #end _ifmap_route-table_read_to_meta_index

    def _ifmap_route_table_update(self, ifmap_id, new_obj_dict):
        # read in refs from ifmap to determine which ones become inactive after update
        existing_metas = self._ifmap_route_table_read_to_meta_index(ifmap_id)

        # remove properties that are no longer active
        props = ['routes', 'id-perms', 'display-name']
        for prop in props:
            prop_m = prop.replace('-', '_')
            if prop in existing_metas and prop_m not in new_obj_dict:
                self._delete_id_self_meta(ifmap_id, 'contrail:'+prop)
        # remove refs that are no longer active
        delete_list = []
        refs = {}
        for meta, to_name in refs.items():
            old_set = set([m['id'] for m in existing_metas.get(meta, [])])
            new_set = set()
            to_name_m = to_name.replace('-', '_')
            for ref in new_obj_dict.get(to_name_m+'_refs', []):
                to_imid = self.fq_name_to_ifmap_id(to_name, ref['to'])
                new_set.add(to_imid)

            for inact_ref in old_set - new_set:
                delete_list.append((inact_ref, 'contrail:'+meta))

        if delete_list:
            self._delete_id_pair_meta_list(ifmap_id, delete_list)

        (ok, result) = self._ifmap_route_table_set(ifmap_id, existing_metas, new_obj_dict)
        return (ok, result)
    #end _ifmap_route_table_update

    def _ifmap_route_table_delete(self, obj_ids):
        ifmap_id = obj_ids['imid']
        parent_imid = obj_ids.get('parent_imid', None)
        existing_metas = self._ifmap_route_table_read_to_meta_index(ifmap_id)
        meta_list = []
        for meta_name, meta_infos in existing_metas.items():
            for meta_info in meta_infos:
                ref_imid = meta_info.get('id')
                if ref_imid is None:
                    continue
                meta_list.append((ref_imid, 'contrail:'+meta_name))

        if parent_imid:
            # Remove link from parent
            meta_list.append((parent_imid, None))

        if meta_list:
            self._delete_id_pair_meta_list(ifmap_id, meta_list)

        # Remove all property metadata associated with this ident
        self._delete_id_self_meta(ifmap_id, None)

        return (True, '')
    #end _ifmap_route_table_delete

    def _ifmap_physical_interface_alloc(self, parent_type, fq_name):
        imid = self._imid_handler
        (my_imid, parent_imid) = \
            imid.physical_interface_alloc_ifmap_id(parent_type, fq_name)
        if my_imid is None or parent_imid is None:
            return (False, (my_imid, parent_imid))
        return (True, (my_imid, parent_imid))
    #end _ifmap_physical_interface_alloc

    def _ifmap_physical_interface_set(self, my_imid, existing_metas, obj_dict):
        # Properties Meta
        update = {}
        field = obj_dict.get('id_perms', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['id_perms']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                IdPermsType(**field).exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            id_perms_xml = buf.getvalue()
            buf.close()
            meta = Metadata('id-perms' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = id_perms_xml)

            if (existing_metas and 'id-perms' in existing_metas and
                str(existing_metas['id-perms'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('display_name', None)
        if field is not None:
            norm_str = escape(str(obj_dict['display_name']))
            meta = Metadata('display-name', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'display-name' in existing_metas and
                str(existing_metas['display-name'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        # Ref Link Metas
        imid = self._imid_handler

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('logical_interface_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'logical-interface'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                physical_interface_logical_interface_xml = ''
                meta = Metadata('physical-interface-logical-interface' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = physical_interface_logical_interface_xml)
                self._update_id_pair_meta(update, to_imid, meta)


        self._publish_update(my_imid, update)
        return (True, '')
    #end _ifmap_physical_interface_set

    def _ifmap_physical_interface_create(self, obj_ids, obj_dict):
        if not 'parent_type' in obj_dict:
            # parent is config-root
            parent_type = 'config-root'
            parent_imid = 'contrail:config-root:root'
        else:
            parent_type = obj_dict['parent_type']
            parent_imid = obj_ids.get('parent_imid', None)

        # Parent Link Meta
        update = {}
        parent_link_meta = self._parent_metas[parent_type]['physical-interface']
        meta = Metadata(parent_link_meta, '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')
        self._update_id_pair_meta(update, obj_ids['imid'], meta)
        self._publish_update(parent_imid, update)

        (ok, result) = self._ifmap_physical_interface_set(obj_ids['imid'], None, obj_dict)
        return (ok, result)
    #end _ifmap_physical_interface_create


    def _ifmap_physical_interface_read_to_meta_index(self, ifmap_id, field_names = None):
        # field_names = None means all fields will be read
        imid = self._imid_handler
        start_id = str(Identity(name = ifmap_id, type = 'other',
                        other_type = 'extended'))
        # if id-perms missing, identity doesn't exist

        all_metas = [u'id-perms', u'display-name', u'physical-interface-logical-interface']
        if not field_names:
            metas_to_read = all_metas
        else: # read only requested fields
            metas_to_read = set(all_metas) & set(field_names.keys())

        # metas is a dict where key is meta-name and val is list of dict
        # of form [{'meta':meta}, {'id':id1, 'meta':meta}, {'id':id2, 'meta':meta}]
        metas = {}
        for meta_name in metas_to_read:
            if meta_name in self._id_to_metas[ifmap_id]:
                metas[meta_name] = self._id_to_metas[ifmap_id][meta_name]
        return metas
    #end _ifmap_physical-interface_read_to_meta_index

    def _ifmap_physical_interface_update(self, ifmap_id, new_obj_dict):
        # read in refs from ifmap to determine which ones become inactive after update
        existing_metas = self._ifmap_physical_interface_read_to_meta_index(ifmap_id)

        # remove properties that are no longer active
        props = ['id-perms', 'display-name']
        for prop in props:
            prop_m = prop.replace('-', '_')
            if prop in existing_metas and prop_m not in new_obj_dict:
                self._delete_id_self_meta(ifmap_id, 'contrail:'+prop)
        # remove refs that are no longer active
        delete_list = []
        refs = {}
        for meta, to_name in refs.items():
            old_set = set([m['id'] for m in existing_metas.get(meta, [])])
            new_set = set()
            to_name_m = to_name.replace('-', '_')
            for ref in new_obj_dict.get(to_name_m+'_refs', []):
                to_imid = self.fq_name_to_ifmap_id(to_name, ref['to'])
                new_set.add(to_imid)

            for inact_ref in old_set - new_set:
                delete_list.append((inact_ref, 'contrail:'+meta))

        if delete_list:
            self._delete_id_pair_meta_list(ifmap_id, delete_list)

        (ok, result) = self._ifmap_physical_interface_set(ifmap_id, existing_metas, new_obj_dict)
        return (ok, result)
    #end _ifmap_physical_interface_update

    def _ifmap_physical_interface_delete(self, obj_ids):
        ifmap_id = obj_ids['imid']
        parent_imid = obj_ids.get('parent_imid', None)
        existing_metas = self._ifmap_physical_interface_read_to_meta_index(ifmap_id)
        meta_list = []
        for meta_name, meta_infos in existing_metas.items():
            for meta_info in meta_infos:
                ref_imid = meta_info.get('id')
                if ref_imid is None:
                    continue
                meta_list.append((ref_imid, 'contrail:'+meta_name))

        if parent_imid:
            # Remove link from parent
            meta_list.append((parent_imid, None))

        if meta_list:
            self._delete_id_pair_meta_list(ifmap_id, meta_list)

        # Remove all property metadata associated with this ident
        self._delete_id_self_meta(ifmap_id, None)

        return (True, '')
    #end _ifmap_physical_interface_delete

    def _ifmap_access_control_list_alloc(self, parent_type, fq_name):
        imid = self._imid_handler
        (my_imid, parent_imid) = \
            imid.access_control_list_alloc_ifmap_id(parent_type, fq_name)
        if my_imid is None or parent_imid is None:
            return (False, (my_imid, parent_imid))
        return (True, (my_imid, parent_imid))
    #end _ifmap_access_control_list_alloc

    def _ifmap_access_control_list_set(self, my_imid, existing_metas, obj_dict):
        # Properties Meta
        update = {}
        field = obj_dict.get('access_control_list_entries', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['access_control_list_entries']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                AclEntriesType(**field).exportChildren(buf, level = 1, name_ = 'access-control-list-entries', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'access-control-list-entries', pretty_print = False)
            access_control_list_entries_xml = buf.getvalue()
            buf.close()
            meta = Metadata('access-control-list-entries' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = access_control_list_entries_xml)

            if (existing_metas and 'access-control-list-entries' in existing_metas and
                str(existing_metas['access-control-list-entries'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['id_perms']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                IdPermsType(**field).exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            id_perms_xml = buf.getvalue()
            buf.close()
            meta = Metadata('id-perms' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = id_perms_xml)

            if (existing_metas and 'id-perms' in existing_metas and
                str(existing_metas['id-perms'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('display_name', None)
        if field is not None:
            norm_str = escape(str(obj_dict['display_name']))
            meta = Metadata('display-name', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'display-name' in existing_metas and
                str(existing_metas['display-name'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        # Ref Link Metas
        imid = self._imid_handler


        self._publish_update(my_imid, update)
        return (True, '')
    #end _ifmap_access_control_list_set

    def _ifmap_access_control_list_create(self, obj_ids, obj_dict):
        if not 'parent_type' in obj_dict:
            # parent is config-root
            parent_type = 'config-root'
            parent_imid = 'contrail:config-root:root'
        else:
            parent_type = obj_dict['parent_type']
            parent_imid = obj_ids.get('parent_imid', None)

        # Parent Link Meta
        update = {}
        parent_link_meta = self._parent_metas[parent_type]['access-control-list']
        meta = Metadata(parent_link_meta, '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')
        self._update_id_pair_meta(update, obj_ids['imid'], meta)
        self._publish_update(parent_imid, update)

        (ok, result) = self._ifmap_access_control_list_set(obj_ids['imid'], None, obj_dict)
        return (ok, result)
    #end _ifmap_access_control_list_create


    def _ifmap_access_control_list_read_to_meta_index(self, ifmap_id, field_names = None):
        # field_names = None means all fields will be read
        imid = self._imid_handler
        start_id = str(Identity(name = ifmap_id, type = 'other',
                        other_type = 'extended'))
        # if id-perms missing, identity doesn't exist

        all_metas = [u'access-control-list-entries', u'id-perms', u'display-name']
        if not field_names:
            metas_to_read = all_metas
        else: # read only requested fields
            metas_to_read = set(all_metas) & set(field_names.keys())

        # metas is a dict where key is meta-name and val is list of dict
        # of form [{'meta':meta}, {'id':id1, 'meta':meta}, {'id':id2, 'meta':meta}]
        metas = {}
        for meta_name in metas_to_read:
            if meta_name in self._id_to_metas[ifmap_id]:
                metas[meta_name] = self._id_to_metas[ifmap_id][meta_name]
        return metas
    #end _ifmap_access-control-list_read_to_meta_index

    def _ifmap_access_control_list_update(self, ifmap_id, new_obj_dict):
        # read in refs from ifmap to determine which ones become inactive after update
        existing_metas = self._ifmap_access_control_list_read_to_meta_index(ifmap_id)

        # remove properties that are no longer active
        props = ['access-control-list-entries', 'id-perms', 'display-name']
        for prop in props:
            prop_m = prop.replace('-', '_')
            if prop in existing_metas and prop_m not in new_obj_dict:
                self._delete_id_self_meta(ifmap_id, 'contrail:'+prop)
        # remove refs that are no longer active
        delete_list = []
        refs = {}
        for meta, to_name in refs.items():
            old_set = set([m['id'] for m in existing_metas.get(meta, [])])
            new_set = set()
            to_name_m = to_name.replace('-', '_')
            for ref in new_obj_dict.get(to_name_m+'_refs', []):
                to_imid = self.fq_name_to_ifmap_id(to_name, ref['to'])
                new_set.add(to_imid)

            for inact_ref in old_set - new_set:
                delete_list.append((inact_ref, 'contrail:'+meta))

        if delete_list:
            self._delete_id_pair_meta_list(ifmap_id, delete_list)

        (ok, result) = self._ifmap_access_control_list_set(ifmap_id, existing_metas, new_obj_dict)
        return (ok, result)
    #end _ifmap_access_control_list_update

    def _ifmap_access_control_list_delete(self, obj_ids):
        ifmap_id = obj_ids['imid']
        parent_imid = obj_ids.get('parent_imid', None)
        existing_metas = self._ifmap_access_control_list_read_to_meta_index(ifmap_id)
        meta_list = []
        for meta_name, meta_infos in existing_metas.items():
            for meta_info in meta_infos:
                ref_imid = meta_info.get('id')
                if ref_imid is None:
                    continue
                meta_list.append((ref_imid, 'contrail:'+meta_name))

        if parent_imid:
            # Remove link from parent
            meta_list.append((parent_imid, None))

        if meta_list:
            self._delete_id_pair_meta_list(ifmap_id, meta_list)

        # Remove all property metadata associated with this ident
        self._delete_id_self_meta(ifmap_id, None)

        return (True, '')
    #end _ifmap_access_control_list_delete

    def _ifmap_analytics_node_alloc(self, parent_type, fq_name):
        imid = self._imid_handler
        (my_imid, parent_imid) = \
            imid.analytics_node_alloc_ifmap_id(parent_type, fq_name)
        if my_imid is None or parent_imid is None:
            return (False, (my_imid, parent_imid))
        return (True, (my_imid, parent_imid))
    #end _ifmap_analytics_node_alloc

    def _ifmap_analytics_node_set(self, my_imid, existing_metas, obj_dict):
        # Properties Meta
        update = {}
        field = obj_dict.get('analytics_node_ip_address', None)
        if field is not None:
            norm_str = escape(str(obj_dict['analytics_node_ip_address']))
            meta = Metadata('analytics-node-ip-address', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'analytics-node-ip-address' in existing_metas and
                str(existing_metas['analytics-node-ip-address'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['id_perms']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                IdPermsType(**field).exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            id_perms_xml = buf.getvalue()
            buf.close()
            meta = Metadata('id-perms' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = id_perms_xml)

            if (existing_metas and 'id-perms' in existing_metas and
                str(existing_metas['id-perms'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('display_name', None)
        if field is not None:
            norm_str = escape(str(obj_dict['display_name']))
            meta = Metadata('display-name', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'display-name' in existing_metas and
                str(existing_metas['display-name'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        # Ref Link Metas
        imid = self._imid_handler


        self._publish_update(my_imid, update)
        return (True, '')
    #end _ifmap_analytics_node_set

    def _ifmap_analytics_node_create(self, obj_ids, obj_dict):
        if not 'parent_type' in obj_dict:
            # parent is config-root
            parent_type = 'config-root'
            parent_imid = 'contrail:config-root:root'
        else:
            parent_type = obj_dict['parent_type']
            parent_imid = obj_ids.get('parent_imid', None)

        # Parent Link Meta
        update = {}
        parent_link_meta = self._parent_metas[parent_type]['analytics-node']
        meta = Metadata(parent_link_meta, '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')
        self._update_id_pair_meta(update, obj_ids['imid'], meta)
        self._publish_update(parent_imid, update)

        (ok, result) = self._ifmap_analytics_node_set(obj_ids['imid'], None, obj_dict)
        return (ok, result)
    #end _ifmap_analytics_node_create


    def _ifmap_analytics_node_read_to_meta_index(self, ifmap_id, field_names = None):
        # field_names = None means all fields will be read
        imid = self._imid_handler
        start_id = str(Identity(name = ifmap_id, type = 'other',
                        other_type = 'extended'))
        # if id-perms missing, identity doesn't exist

        all_metas = [u'analytics-node-ip-address', u'id-perms', u'display-name']
        if not field_names:
            metas_to_read = all_metas
        else: # read only requested fields
            metas_to_read = set(all_metas) & set(field_names.keys())

        # metas is a dict where key is meta-name and val is list of dict
        # of form [{'meta':meta}, {'id':id1, 'meta':meta}, {'id':id2, 'meta':meta}]
        metas = {}
        for meta_name in metas_to_read:
            if meta_name in self._id_to_metas[ifmap_id]:
                metas[meta_name] = self._id_to_metas[ifmap_id][meta_name]
        return metas
    #end _ifmap_analytics-node_read_to_meta_index

    def _ifmap_analytics_node_update(self, ifmap_id, new_obj_dict):
        # read in refs from ifmap to determine which ones become inactive after update
        existing_metas = self._ifmap_analytics_node_read_to_meta_index(ifmap_id)

        # remove properties that are no longer active
        props = ['analytics-node-ip-address', 'id-perms', 'display-name']
        for prop in props:
            prop_m = prop.replace('-', '_')
            if prop in existing_metas and prop_m not in new_obj_dict:
                self._delete_id_self_meta(ifmap_id, 'contrail:'+prop)
        # remove refs that are no longer active
        delete_list = []
        refs = {}
        for meta, to_name in refs.items():
            old_set = set([m['id'] for m in existing_metas.get(meta, [])])
            new_set = set()
            to_name_m = to_name.replace('-', '_')
            for ref in new_obj_dict.get(to_name_m+'_refs', []):
                to_imid = self.fq_name_to_ifmap_id(to_name, ref['to'])
                new_set.add(to_imid)

            for inact_ref in old_set - new_set:
                delete_list.append((inact_ref, 'contrail:'+meta))

        if delete_list:
            self._delete_id_pair_meta_list(ifmap_id, delete_list)

        (ok, result) = self._ifmap_analytics_node_set(ifmap_id, existing_metas, new_obj_dict)
        return (ok, result)
    #end _ifmap_analytics_node_update

    def _ifmap_analytics_node_delete(self, obj_ids):
        ifmap_id = obj_ids['imid']
        parent_imid = obj_ids.get('parent_imid', None)
        existing_metas = self._ifmap_analytics_node_read_to_meta_index(ifmap_id)
        meta_list = []
        for meta_name, meta_infos in existing_metas.items():
            for meta_info in meta_infos:
                ref_imid = meta_info.get('id')
                if ref_imid is None:
                    continue
                meta_list.append((ref_imid, 'contrail:'+meta_name))

        if parent_imid:
            # Remove link from parent
            meta_list.append((parent_imid, None))

        if meta_list:
            self._delete_id_pair_meta_list(ifmap_id, meta_list)

        # Remove all property metadata associated with this ident
        self._delete_id_self_meta(ifmap_id, None)

        return (True, '')
    #end _ifmap_analytics_node_delete

    def _ifmap_virtual_DNS_alloc(self, parent_type, fq_name):
        imid = self._imid_handler
        (my_imid, parent_imid) = \
            imid.virtual_DNS_alloc_ifmap_id(parent_type, fq_name)
        if my_imid is None or parent_imid is None:
            return (False, (my_imid, parent_imid))
        return (True, (my_imid, parent_imid))
    #end _ifmap_virtual_DNS_alloc

    def _ifmap_virtual_DNS_set(self, my_imid, existing_metas, obj_dict):
        # Properties Meta
        update = {}
        field = obj_dict.get('virtual_DNS_data', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['virtual_DNS_data']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                VirtualDnsType(**field).exportChildren(buf, level = 1, name_ = 'virtual-DNS-data', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'virtual-DNS-data', pretty_print = False)
            virtual_DNS_data_xml = buf.getvalue()
            buf.close()
            meta = Metadata('virtual-DNS-data' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = virtual_DNS_data_xml)

            if (existing_metas and 'virtual-DNS-data' in existing_metas and
                str(existing_metas['virtual-DNS-data'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['id_perms']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                IdPermsType(**field).exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            id_perms_xml = buf.getvalue()
            buf.close()
            meta = Metadata('id-perms' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = id_perms_xml)

            if (existing_metas and 'id-perms' in existing_metas and
                str(existing_metas['id-perms'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('display_name', None)
        if field is not None:
            norm_str = escape(str(obj_dict['display_name']))
            meta = Metadata('display-name', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'display-name' in existing_metas and
                str(existing_metas['display-name'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        # Ref Link Metas
        imid = self._imid_handler

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('virtual_DNS_record_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'virtual-DNS-record'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                virtual_DNS_virtual_DNS_record_xml = ''
                meta = Metadata('virtual-DNS-virtual-DNS-record' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = virtual_DNS_virtual_DNS_record_xml)
                self._update_id_pair_meta(update, to_imid, meta)


        self._publish_update(my_imid, update)
        return (True, '')
    #end _ifmap_virtual_DNS_set

    def _ifmap_virtual_DNS_create(self, obj_ids, obj_dict):
        if not 'parent_type' in obj_dict:
            # parent is config-root
            parent_type = 'config-root'
            parent_imid = 'contrail:config-root:root'
        else:
            parent_type = obj_dict['parent_type']
            parent_imid = obj_ids.get('parent_imid', None)

        # Parent Link Meta
        update = {}
        parent_link_meta = self._parent_metas[parent_type]['virtual-DNS']
        meta = Metadata(parent_link_meta, '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')
        self._update_id_pair_meta(update, obj_ids['imid'], meta)
        self._publish_update(parent_imid, update)

        (ok, result) = self._ifmap_virtual_DNS_set(obj_ids['imid'], None, obj_dict)
        return (ok, result)
    #end _ifmap_virtual_DNS_create


    def _ifmap_virtual_DNS_read_to_meta_index(self, ifmap_id, field_names = None):
        # field_names = None means all fields will be read
        imid = self._imid_handler
        start_id = str(Identity(name = ifmap_id, type = 'other',
                        other_type = 'extended'))
        # if id-perms missing, identity doesn't exist

        all_metas = [u'network-ipam-virtual-DNS', u'virtual-DNS-data', u'id-perms', u'display-name', u'virtual-DNS-virtual-DNS-record']
        if not field_names:
            metas_to_read = all_metas
        else: # read only requested fields
            metas_to_read = set(all_metas) & set(field_names.keys())

        # metas is a dict where key is meta-name and val is list of dict
        # of form [{'meta':meta}, {'id':id1, 'meta':meta}, {'id':id2, 'meta':meta}]
        metas = {}
        for meta_name in metas_to_read:
            if meta_name in self._id_to_metas[ifmap_id]:
                metas[meta_name] = self._id_to_metas[ifmap_id][meta_name]
        return metas
    #end _ifmap_virtual-DNS_read_to_meta_index

    def _ifmap_virtual_DNS_update(self, ifmap_id, new_obj_dict):
        # read in refs from ifmap to determine which ones become inactive after update
        existing_metas = self._ifmap_virtual_DNS_read_to_meta_index(ifmap_id)

        # remove properties that are no longer active
        props = ['virtual-DNS-data', 'id-perms', 'display-name']
        for prop in props:
            prop_m = prop.replace('-', '_')
            if prop in existing_metas and prop_m not in new_obj_dict:
                self._delete_id_self_meta(ifmap_id, 'contrail:'+prop)
        # remove refs that are no longer active
        delete_list = []
        refs = {}
        for meta, to_name in refs.items():
            old_set = set([m['id'] for m in existing_metas.get(meta, [])])
            new_set = set()
            to_name_m = to_name.replace('-', '_')
            for ref in new_obj_dict.get(to_name_m+'_refs', []):
                to_imid = self.fq_name_to_ifmap_id(to_name, ref['to'])
                new_set.add(to_imid)

            for inact_ref in old_set - new_set:
                delete_list.append((inact_ref, 'contrail:'+meta))

        if delete_list:
            self._delete_id_pair_meta_list(ifmap_id, delete_list)

        (ok, result) = self._ifmap_virtual_DNS_set(ifmap_id, existing_metas, new_obj_dict)
        return (ok, result)
    #end _ifmap_virtual_DNS_update

    def _ifmap_virtual_DNS_delete(self, obj_ids):
        ifmap_id = obj_ids['imid']
        parent_imid = obj_ids.get('parent_imid', None)
        existing_metas = self._ifmap_virtual_DNS_read_to_meta_index(ifmap_id)
        meta_list = []
        for meta_name, meta_infos in existing_metas.items():
            for meta_info in meta_infos:
                ref_imid = meta_info.get('id')
                if ref_imid is None:
                    continue
                meta_list.append((ref_imid, 'contrail:'+meta_name))

        if parent_imid:
            # Remove link from parent
            meta_list.append((parent_imid, None))

        if meta_list:
            self._delete_id_pair_meta_list(ifmap_id, meta_list)

        # Remove all property metadata associated with this ident
        self._delete_id_self_meta(ifmap_id, None)

        return (True, '')
    #end _ifmap_virtual_DNS_delete

    def _ifmap_customer_attachment_alloc(self, parent_type, fq_name):
        imid = self._imid_handler
        (my_imid, parent_imid) = \
            imid.customer_attachment_alloc_ifmap_id(parent_type, fq_name)
        if my_imid is None or parent_imid is None:
            return (False, (my_imid, parent_imid))
        return (True, (my_imid, parent_imid))
    #end _ifmap_customer_attachment_alloc

    def _ifmap_customer_attachment_set(self, my_imid, existing_metas, obj_dict):
        # Properties Meta
        update = {}
        field = obj_dict.get('attachment_address', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['attachment_address']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                AttachmentAddressType(**field).exportChildren(buf, level = 1, name_ = 'attachment-address', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'attachment-address', pretty_print = False)
            attachment_address_xml = buf.getvalue()
            buf.close()
            meta = Metadata('attachment-address' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = attachment_address_xml)

            if (existing_metas and 'attachment-address' in existing_metas and
                str(existing_metas['attachment-address'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['id_perms']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                IdPermsType(**field).exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            id_perms_xml = buf.getvalue()
            buf.close()
            meta = Metadata('id-perms' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = id_perms_xml)

            if (existing_metas and 'id-perms' in existing_metas and
                str(existing_metas['id-perms'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('display_name', None)
        if field is not None:
            norm_str = escape(str(obj_dict['display_name']))
            meta = Metadata('display-name', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'display-name' in existing_metas and
                str(existing_metas['display-name'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        # Ref Link Metas
        imid = self._imid_handler

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('virtual_machine_interface_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'virtual-machine-interface'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                customer_attachment_virtual_machine_interface_xml = ''
                meta = Metadata('customer-attachment-virtual-machine-interface' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = customer_attachment_virtual_machine_interface_xml)
                self._update_id_pair_meta(update, to_imid, meta)

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('floating_ip_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'floating-ip'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                customer_attachment_floating_ip_xml = ''
                meta = Metadata('customer-attachment-floating-ip' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = customer_attachment_floating_ip_xml)
                self._update_id_pair_meta(update, to_imid, meta)

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('routing_instance_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'routing-instance'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                binding_xml = ''
                ref_data = ref['attr']
                if ref_data:
                    buf = cStringIO.StringIO()
                    BindingType(**ref_data).exportChildren(buf, level = 1, name_ = 'binding', pretty_print = False)
                    binding_xml = binding_xml + buf.getvalue()
                    buf.close()
                meta = Metadata('binding' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = binding_xml)
                self._update_id_pair_meta(update, to_imid, meta)

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('provider_attachment_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'provider-attachment'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                attachment_info_xml = ''
                ref_data = ref['attr']
                if ref_data:
                    buf = cStringIO.StringIO()
                    AttachmentInfoType(**ref_data).exportChildren(buf, level = 1, name_ = 'attachment-info', pretty_print = False)
                    attachment_info_xml = attachment_info_xml + buf.getvalue()
                    buf.close()
                meta = Metadata('attachment-info' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = attachment_info_xml)
                self._update_id_pair_meta(update, to_imid, meta)


        self._publish_update(my_imid, update)
        return (True, '')
    #end _ifmap_customer_attachment_set

    def _ifmap_customer_attachment_create(self, obj_ids, obj_dict):
        (ok, result) = self._ifmap_customer_attachment_set(obj_ids['imid'], None, obj_dict)
        return (ok, result)
    #end _ifmap_customer_attachment_create


    def _ifmap_customer_attachment_read_to_meta_index(self, ifmap_id, field_names = None):
        # field_names = None means all fields will be read
        imid = self._imid_handler
        start_id = str(Identity(name = ifmap_id, type = 'other',
                        other_type = 'extended'))
        # if id-perms missing, identity doesn't exist

        all_metas = [u'customer-attachment-virtual-machine-interface', u'customer-attachment-floating-ip', u'attachment-address', u'id-perms', u'display-name', u'binding', u'attachment-info']
        if not field_names:
            metas_to_read = all_metas
        else: # read only requested fields
            metas_to_read = set(all_metas) & set(field_names.keys())

        # metas is a dict where key is meta-name and val is list of dict
        # of form [{'meta':meta}, {'id':id1, 'meta':meta}, {'id':id2, 'meta':meta}]
        metas = {}
        for meta_name in metas_to_read:
            if meta_name in self._id_to_metas[ifmap_id]:
                metas[meta_name] = self._id_to_metas[ifmap_id][meta_name]
        return metas
    #end _ifmap_customer-attachment_read_to_meta_index

    def _ifmap_customer_attachment_update(self, ifmap_id, new_obj_dict):
        # read in refs from ifmap to determine which ones become inactive after update
        existing_metas = self._ifmap_customer_attachment_read_to_meta_index(ifmap_id)

        # remove properties that are no longer active
        props = ['attachment-address', 'id-perms', 'display-name']
        for prop in props:
            prop_m = prop.replace('-', '_')
            if prop in existing_metas and prop_m not in new_obj_dict:
                self._delete_id_self_meta(ifmap_id, 'contrail:'+prop)
        # remove refs that are no longer active
        delete_list = []
        refs = {'customer-attachment-virtual-machine-interface': 'virtual-machine-interface',
                'customer-attachment-floating-ip': 'floating-ip'}
        for meta, to_name in refs.items():
            old_set = set([m['id'] for m in existing_metas.get(meta, [])])
            new_set = set()
            to_name_m = to_name.replace('-', '_')
            for ref in new_obj_dict.get(to_name_m+'_refs', []):
                to_imid = self.fq_name_to_ifmap_id(to_name, ref['to'])
                new_set.add(to_imid)

            for inact_ref in old_set - new_set:
                delete_list.append((inact_ref, 'contrail:'+meta))

        if delete_list:
            self._delete_id_pair_meta_list(ifmap_id, delete_list)

        (ok, result) = self._ifmap_customer_attachment_set(ifmap_id, existing_metas, new_obj_dict)
        return (ok, result)
    #end _ifmap_customer_attachment_update

    def _ifmap_customer_attachment_delete(self, obj_ids):
        ifmap_id = obj_ids['imid']
        parent_imid = obj_ids.get('parent_imid', None)
        existing_metas = self._ifmap_customer_attachment_read_to_meta_index(ifmap_id)
        meta_list = []
        for meta_name, meta_infos in existing_metas.items():
            for meta_info in meta_infos:
                ref_imid = meta_info.get('id')
                if ref_imid is None:
                    continue
                meta_list.append((ref_imid, 'contrail:'+meta_name))

        if parent_imid:
            # Remove link from parent
            meta_list.append((parent_imid, None))

        if meta_list:
            self._delete_id_pair_meta_list(ifmap_id, meta_list)

        # Remove all property metadata associated with this ident
        self._delete_id_self_meta(ifmap_id, None)

        return (True, '')
    #end _ifmap_customer_attachment_delete

    def _ifmap_service_appliance_set_alloc(self, parent_type, fq_name):
        imid = self._imid_handler
        (my_imid, parent_imid) = \
            imid.service_appliance_set_alloc_ifmap_id(parent_type, fq_name)
        if my_imid is None or parent_imid is None:
            return (False, (my_imid, parent_imid))
        return (True, (my_imid, parent_imid))
    #end _ifmap_service_appliance_set_alloc

    def _ifmap_service_appliance_set_set(self, my_imid, existing_metas, obj_dict):
        # Properties Meta
        update = {}
        field = obj_dict.get('service_appliance_set_properties', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['service_appliance_set_properties']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                KeyValuePairs(**field).exportChildren(buf, level = 1, name_ = 'service-appliance-set-properties', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'service-appliance-set-properties', pretty_print = False)
            service_appliance_set_properties_xml = buf.getvalue()
            buf.close()
            meta = Metadata('service-appliance-set-properties' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = service_appliance_set_properties_xml)

            if (existing_metas and 'service-appliance-set-properties' in existing_metas and
                str(existing_metas['service-appliance-set-properties'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('service_appliance_driver', None)
        if field is not None:
            norm_str = escape(str(obj_dict['service_appliance_driver']))
            meta = Metadata('service-appliance-driver', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'service-appliance-driver' in existing_metas and
                str(existing_metas['service-appliance-driver'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('service_appliance_ha_mode', None)
        if field is not None:
            norm_str = escape(str(obj_dict['service_appliance_ha_mode']))
            meta = Metadata('service-appliance-ha-mode', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'service-appliance-ha-mode' in existing_metas and
                str(existing_metas['service-appliance-ha-mode'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['id_perms']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                IdPermsType(**field).exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            id_perms_xml = buf.getvalue()
            buf.close()
            meta = Metadata('id-perms' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = id_perms_xml)

            if (existing_metas and 'id-perms' in existing_metas and
                str(existing_metas['id-perms'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('display_name', None)
        if field is not None:
            norm_str = escape(str(obj_dict['display_name']))
            meta = Metadata('display-name', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'display-name' in existing_metas and
                str(existing_metas['display-name'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        # Ref Link Metas
        imid = self._imid_handler

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('service_appliance_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'service-appliance'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                service_appliance_set_service_appliance_xml = ''
                meta = Metadata('service-appliance-set-service-appliance' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = service_appliance_set_service_appliance_xml)
                self._update_id_pair_meta(update, to_imid, meta)


        self._publish_update(my_imid, update)
        return (True, '')
    #end _ifmap_service_appliance_set_set

    def _ifmap_service_appliance_set_create(self, obj_ids, obj_dict):
        if not 'parent_type' in obj_dict:
            # parent is config-root
            parent_type = 'config-root'
            parent_imid = 'contrail:config-root:root'
        else:
            parent_type = obj_dict['parent_type']
            parent_imid = obj_ids.get('parent_imid', None)

        # Parent Link Meta
        update = {}
        parent_link_meta = self._parent_metas[parent_type]['service-appliance-set']
        meta = Metadata(parent_link_meta, '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')
        self._update_id_pair_meta(update, obj_ids['imid'], meta)
        self._publish_update(parent_imid, update)

        (ok, result) = self._ifmap_service_appliance_set_set(obj_ids['imid'], None, obj_dict)
        return (ok, result)
    #end _ifmap_service_appliance_set_create


    def _ifmap_service_appliance_set_read_to_meta_index(self, ifmap_id, field_names = None):
        # field_names = None means all fields will be read
        imid = self._imid_handler
        start_id = str(Identity(name = ifmap_id, type = 'other',
                        other_type = 'extended'))
        # if id-perms missing, identity doesn't exist

        all_metas = [u'loadbalancer-pool-service-appliance-set', u'service-appliance-set-properties', u'service-appliance-driver', u'service-appliance-ha-mode', u'id-perms', u'display-name', u'service-appliance-set-service-appliance']
        if not field_names:
            metas_to_read = all_metas
        else: # read only requested fields
            metas_to_read = set(all_metas) & set(field_names.keys())

        # metas is a dict where key is meta-name and val is list of dict
        # of form [{'meta':meta}, {'id':id1, 'meta':meta}, {'id':id2, 'meta':meta}]
        metas = {}
        for meta_name in metas_to_read:
            if meta_name in self._id_to_metas[ifmap_id]:
                metas[meta_name] = self._id_to_metas[ifmap_id][meta_name]
        return metas
    #end _ifmap_service-appliance-set_read_to_meta_index

    def _ifmap_service_appliance_set_update(self, ifmap_id, new_obj_dict):
        # read in refs from ifmap to determine which ones become inactive after update
        existing_metas = self._ifmap_service_appliance_set_read_to_meta_index(ifmap_id)

        # remove properties that are no longer active
        props = ['service-appliance-set-properties', 'service-appliance-driver', 'service-appliance-ha-mode', 'id-perms', 'display-name']
        for prop in props:
            prop_m = prop.replace('-', '_')
            if prop in existing_metas and prop_m not in new_obj_dict:
                self._delete_id_self_meta(ifmap_id, 'contrail:'+prop)
        # remove refs that are no longer active
        delete_list = []
        refs = {}
        for meta, to_name in refs.items():
            old_set = set([m['id'] for m in existing_metas.get(meta, [])])
            new_set = set()
            to_name_m = to_name.replace('-', '_')
            for ref in new_obj_dict.get(to_name_m+'_refs', []):
                to_imid = self.fq_name_to_ifmap_id(to_name, ref['to'])
                new_set.add(to_imid)

            for inact_ref in old_set - new_set:
                delete_list.append((inact_ref, 'contrail:'+meta))

        if delete_list:
            self._delete_id_pair_meta_list(ifmap_id, delete_list)

        (ok, result) = self._ifmap_service_appliance_set_set(ifmap_id, existing_metas, new_obj_dict)
        return (ok, result)
    #end _ifmap_service_appliance_set_update

    def _ifmap_service_appliance_set_delete(self, obj_ids):
        ifmap_id = obj_ids['imid']
        parent_imid = obj_ids.get('parent_imid', None)
        existing_metas = self._ifmap_service_appliance_set_read_to_meta_index(ifmap_id)
        meta_list = []
        for meta_name, meta_infos in existing_metas.items():
            for meta_info in meta_infos:
                ref_imid = meta_info.get('id')
                if ref_imid is None:
                    continue
                meta_list.append((ref_imid, 'contrail:'+meta_name))

        if parent_imid:
            # Remove link from parent
            meta_list.append((parent_imid, None))

        if meta_list:
            self._delete_id_pair_meta_list(ifmap_id, meta_list)

        # Remove all property metadata associated with this ident
        self._delete_id_self_meta(ifmap_id, None)

        return (True, '')
    #end _ifmap_service_appliance_set_delete

    def _ifmap_config_node_alloc(self, parent_type, fq_name):
        imid = self._imid_handler
        (my_imid, parent_imid) = \
            imid.config_node_alloc_ifmap_id(parent_type, fq_name)
        if my_imid is None or parent_imid is None:
            return (False, (my_imid, parent_imid))
        return (True, (my_imid, parent_imid))
    #end _ifmap_config_node_alloc

    def _ifmap_config_node_set(self, my_imid, existing_metas, obj_dict):
        # Properties Meta
        update = {}
        field = obj_dict.get('config_node_ip_address', None)
        if field is not None:
            norm_str = escape(str(obj_dict['config_node_ip_address']))
            meta = Metadata('config-node-ip-address', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'config-node-ip-address' in existing_metas and
                str(existing_metas['config-node-ip-address'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['id_perms']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                IdPermsType(**field).exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            id_perms_xml = buf.getvalue()
            buf.close()
            meta = Metadata('id-perms' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = id_perms_xml)

            if (existing_metas and 'id-perms' in existing_metas and
                str(existing_metas['id-perms'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('display_name', None)
        if field is not None:
            norm_str = escape(str(obj_dict['display_name']))
            meta = Metadata('display-name', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'display-name' in existing_metas and
                str(existing_metas['display-name'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        # Ref Link Metas
        imid = self._imid_handler


        self._publish_update(my_imid, update)
        return (True, '')
    #end _ifmap_config_node_set

    def _ifmap_config_node_create(self, obj_ids, obj_dict):
        if not 'parent_type' in obj_dict:
            # parent is config-root
            parent_type = 'config-root'
            parent_imid = 'contrail:config-root:root'
        else:
            parent_type = obj_dict['parent_type']
            parent_imid = obj_ids.get('parent_imid', None)

        # Parent Link Meta
        update = {}
        parent_link_meta = self._parent_metas[parent_type]['config-node']
        meta = Metadata(parent_link_meta, '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')
        self._update_id_pair_meta(update, obj_ids['imid'], meta)
        self._publish_update(parent_imid, update)

        (ok, result) = self._ifmap_config_node_set(obj_ids['imid'], None, obj_dict)
        return (ok, result)
    #end _ifmap_config_node_create


    def _ifmap_config_node_read_to_meta_index(self, ifmap_id, field_names = None):
        # field_names = None means all fields will be read
        imid = self._imid_handler
        start_id = str(Identity(name = ifmap_id, type = 'other',
                        other_type = 'extended'))
        # if id-perms missing, identity doesn't exist

        all_metas = [u'config-node-ip-address', u'id-perms', u'display-name']
        if not field_names:
            metas_to_read = all_metas
        else: # read only requested fields
            metas_to_read = set(all_metas) & set(field_names.keys())

        # metas is a dict where key is meta-name and val is list of dict
        # of form [{'meta':meta}, {'id':id1, 'meta':meta}, {'id':id2, 'meta':meta}]
        metas = {}
        for meta_name in metas_to_read:
            if meta_name in self._id_to_metas[ifmap_id]:
                metas[meta_name] = self._id_to_metas[ifmap_id][meta_name]
        return metas
    #end _ifmap_config-node_read_to_meta_index

    def _ifmap_config_node_update(self, ifmap_id, new_obj_dict):
        # read in refs from ifmap to determine which ones become inactive after update
        existing_metas = self._ifmap_config_node_read_to_meta_index(ifmap_id)

        # remove properties that are no longer active
        props = ['config-node-ip-address', 'id-perms', 'display-name']
        for prop in props:
            prop_m = prop.replace('-', '_')
            if prop in existing_metas and prop_m not in new_obj_dict:
                self._delete_id_self_meta(ifmap_id, 'contrail:'+prop)
        # remove refs that are no longer active
        delete_list = []
        refs = {}
        for meta, to_name in refs.items():
            old_set = set([m['id'] for m in existing_metas.get(meta, [])])
            new_set = set()
            to_name_m = to_name.replace('-', '_')
            for ref in new_obj_dict.get(to_name_m+'_refs', []):
                to_imid = self.fq_name_to_ifmap_id(to_name, ref['to'])
                new_set.add(to_imid)

            for inact_ref in old_set - new_set:
                delete_list.append((inact_ref, 'contrail:'+meta))

        if delete_list:
            self._delete_id_pair_meta_list(ifmap_id, delete_list)

        (ok, result) = self._ifmap_config_node_set(ifmap_id, existing_metas, new_obj_dict)
        return (ok, result)
    #end _ifmap_config_node_update

    def _ifmap_config_node_delete(self, obj_ids):
        ifmap_id = obj_ids['imid']
        parent_imid = obj_ids.get('parent_imid', None)
        existing_metas = self._ifmap_config_node_read_to_meta_index(ifmap_id)
        meta_list = []
        for meta_name, meta_infos in existing_metas.items():
            for meta_info in meta_infos:
                ref_imid = meta_info.get('id')
                if ref_imid is None:
                    continue
                meta_list.append((ref_imid, 'contrail:'+meta_name))

        if parent_imid:
            # Remove link from parent
            meta_list.append((parent_imid, None))

        if meta_list:
            self._delete_id_pair_meta_list(ifmap_id, meta_list)

        # Remove all property metadata associated with this ident
        self._delete_id_self_meta(ifmap_id, None)

        return (True, '')
    #end _ifmap_config_node_delete

    def _ifmap_qos_queue_alloc(self, parent_type, fq_name):
        imid = self._imid_handler
        (my_imid, parent_imid) = \
            imid.qos_queue_alloc_ifmap_id(parent_type, fq_name)
        if my_imid is None or parent_imid is None:
            return (False, (my_imid, parent_imid))
        return (True, (my_imid, parent_imid))
    #end _ifmap_qos_queue_alloc

    def _ifmap_qos_queue_set(self, my_imid, existing_metas, obj_dict):
        # Properties Meta
        update = {}
        field = obj_dict.get('min_bandwidth', None)
        if field is not None:
            norm_str = escape(str(obj_dict['min_bandwidth']))
            meta = Metadata('min-bandwidth', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'min-bandwidth' in existing_metas and
                str(existing_metas['min-bandwidth'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('max_bandwidth', None)
        if field is not None:
            norm_str = escape(str(obj_dict['max_bandwidth']))
            meta = Metadata('max-bandwidth', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'max-bandwidth' in existing_metas and
                str(existing_metas['max-bandwidth'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['id_perms']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                IdPermsType(**field).exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            id_perms_xml = buf.getvalue()
            buf.close()
            meta = Metadata('id-perms' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = id_perms_xml)

            if (existing_metas and 'id-perms' in existing_metas and
                str(existing_metas['id-perms'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('display_name', None)
        if field is not None:
            norm_str = escape(str(obj_dict['display_name']))
            meta = Metadata('display-name', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'display-name' in existing_metas and
                str(existing_metas['display-name'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        # Ref Link Metas
        imid = self._imid_handler


        self._publish_update(my_imid, update)
        return (True, '')
    #end _ifmap_qos_queue_set

    def _ifmap_qos_queue_create(self, obj_ids, obj_dict):
        if not 'parent_type' in obj_dict:
            # parent is config-root
            parent_type = 'config-root'
            parent_imid = 'contrail:config-root:root'
        else:
            parent_type = obj_dict['parent_type']
            parent_imid = obj_ids.get('parent_imid', None)

        # Parent Link Meta
        update = {}
        parent_link_meta = self._parent_metas[parent_type]['qos-queue']
        meta = Metadata(parent_link_meta, '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')
        self._update_id_pair_meta(update, obj_ids['imid'], meta)
        self._publish_update(parent_imid, update)

        (ok, result) = self._ifmap_qos_queue_set(obj_ids['imid'], None, obj_dict)
        return (ok, result)
    #end _ifmap_qos_queue_create


    def _ifmap_qos_queue_read_to_meta_index(self, ifmap_id, field_names = None):
        # field_names = None means all fields will be read
        imid = self._imid_handler
        start_id = str(Identity(name = ifmap_id, type = 'other',
                        other_type = 'extended'))
        # if id-perms missing, identity doesn't exist

        all_metas = [u'qos-forwarding-class-qos-queue', u'min-bandwidth', u'max-bandwidth', u'id-perms', u'display-name']
        if not field_names:
            metas_to_read = all_metas
        else: # read only requested fields
            metas_to_read = set(all_metas) & set(field_names.keys())

        # metas is a dict where key is meta-name and val is list of dict
        # of form [{'meta':meta}, {'id':id1, 'meta':meta}, {'id':id2, 'meta':meta}]
        metas = {}
        for meta_name in metas_to_read:
            if meta_name in self._id_to_metas[ifmap_id]:
                metas[meta_name] = self._id_to_metas[ifmap_id][meta_name]
        return metas
    #end _ifmap_qos-queue_read_to_meta_index

    def _ifmap_qos_queue_update(self, ifmap_id, new_obj_dict):
        # read in refs from ifmap to determine which ones become inactive after update
        existing_metas = self._ifmap_qos_queue_read_to_meta_index(ifmap_id)

        # remove properties that are no longer active
        props = ['min-bandwidth', 'max-bandwidth', 'id-perms', 'display-name']
        for prop in props:
            prop_m = prop.replace('-', '_')
            if prop in existing_metas and prop_m not in new_obj_dict:
                self._delete_id_self_meta(ifmap_id, 'contrail:'+prop)
        # remove refs that are no longer active
        delete_list = []
        refs = {}
        for meta, to_name in refs.items():
            old_set = set([m['id'] for m in existing_metas.get(meta, [])])
            new_set = set()
            to_name_m = to_name.replace('-', '_')
            for ref in new_obj_dict.get(to_name_m+'_refs', []):
                to_imid = self.fq_name_to_ifmap_id(to_name, ref['to'])
                new_set.add(to_imid)

            for inact_ref in old_set - new_set:
                delete_list.append((inact_ref, 'contrail:'+meta))

        if delete_list:
            self._delete_id_pair_meta_list(ifmap_id, delete_list)

        (ok, result) = self._ifmap_qos_queue_set(ifmap_id, existing_metas, new_obj_dict)
        return (ok, result)
    #end _ifmap_qos_queue_update

    def _ifmap_qos_queue_delete(self, obj_ids):
        ifmap_id = obj_ids['imid']
        parent_imid = obj_ids.get('parent_imid', None)
        existing_metas = self._ifmap_qos_queue_read_to_meta_index(ifmap_id)
        meta_list = []
        for meta_name, meta_infos in existing_metas.items():
            for meta_info in meta_infos:
                ref_imid = meta_info.get('id')
                if ref_imid is None:
                    continue
                meta_list.append((ref_imid, 'contrail:'+meta_name))

        if parent_imid:
            # Remove link from parent
            meta_list.append((parent_imid, None))

        if meta_list:
            self._delete_id_pair_meta_list(ifmap_id, meta_list)

        # Remove all property metadata associated with this ident
        self._delete_id_self_meta(ifmap_id, None)

        return (True, '')
    #end _ifmap_qos_queue_delete

    def _ifmap_virtual_machine_alloc(self, parent_type, fq_name):
        imid = self._imid_handler
        (my_imid, parent_imid) = \
            imid.virtual_machine_alloc_ifmap_id(parent_type, fq_name)
        if my_imid is None or parent_imid is None:
            return (False, (my_imid, parent_imid))
        return (True, (my_imid, parent_imid))
    #end _ifmap_virtual_machine_alloc

    def _ifmap_virtual_machine_set(self, my_imid, existing_metas, obj_dict):
        # Properties Meta
        update = {}
        field = obj_dict.get('id_perms', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['id_perms']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                IdPermsType(**field).exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            id_perms_xml = buf.getvalue()
            buf.close()
            meta = Metadata('id-perms' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = id_perms_xml)

            if (existing_metas and 'id-perms' in existing_metas and
                str(existing_metas['id-perms'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('display_name', None)
        if field is not None:
            norm_str = escape(str(obj_dict['display_name']))
            meta = Metadata('display-name', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'display-name' in existing_metas and
                str(existing_metas['display-name'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        # Ref Link Metas
        imid = self._imid_handler

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('virtual_machine_interface_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'virtual-machine-interface'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                virtual_machine_virtual_machine_interface_xml = ''
                meta = Metadata('virtual-machine-virtual-machine-interface' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = virtual_machine_virtual_machine_interface_xml)
                self._update_id_pair_meta(update, to_imid, meta)

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('service_instance_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'service-instance'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                virtual_machine_service_instance_xml = ''
                meta = Metadata('virtual-machine-service-instance' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = virtual_machine_service_instance_xml)
                self._update_id_pair_meta(update, to_imid, meta)


        self._publish_update(my_imid, update)
        return (True, '')
    #end _ifmap_virtual_machine_set

    def _ifmap_virtual_machine_create(self, obj_ids, obj_dict):
        (ok, result) = self._ifmap_virtual_machine_set(obj_ids['imid'], None, obj_dict)
        return (ok, result)
    #end _ifmap_virtual_machine_create


    def _ifmap_virtual_machine_read_to_meta_index(self, ifmap_id, field_names = None):
        # field_names = None means all fields will be read
        imid = self._imid_handler
        start_id = str(Identity(name = ifmap_id, type = 'other',
                        other_type = 'extended'))
        # if id-perms missing, identity doesn't exist

        all_metas = [u'virtual-machine-service-instance', u'virtual-machine-interface-virtual-machine', u'virtual-router-virtual-machine', u'id-perms', u'display-name', u'virtual-machine-virtual-machine-interface']
        if not field_names:
            metas_to_read = all_metas
        else: # read only requested fields
            metas_to_read = set(all_metas) & set(field_names.keys())

        # metas is a dict where key is meta-name and val is list of dict
        # of form [{'meta':meta}, {'id':id1, 'meta':meta}, {'id':id2, 'meta':meta}]
        metas = {}
        for meta_name in metas_to_read:
            if meta_name in self._id_to_metas[ifmap_id]:
                metas[meta_name] = self._id_to_metas[ifmap_id][meta_name]
        return metas
    #end _ifmap_virtual-machine_read_to_meta_index

    def _ifmap_virtual_machine_update(self, ifmap_id, new_obj_dict):
        # read in refs from ifmap to determine which ones become inactive after update
        existing_metas = self._ifmap_virtual_machine_read_to_meta_index(ifmap_id)

        # remove properties that are no longer active
        props = ['id-perms', 'display-name']
        for prop in props:
            prop_m = prop.replace('-', '_')
            if prop in existing_metas and prop_m not in new_obj_dict:
                self._delete_id_self_meta(ifmap_id, 'contrail:'+prop)
        # remove refs that are no longer active
        delete_list = []
        refs = {'virtual-machine-service-instance': 'service-instance'}
        for meta, to_name in refs.items():
            old_set = set([m['id'] for m in existing_metas.get(meta, [])])
            new_set = set()
            to_name_m = to_name.replace('-', '_')
            for ref in new_obj_dict.get(to_name_m+'_refs', []):
                to_imid = self.fq_name_to_ifmap_id(to_name, ref['to'])
                new_set.add(to_imid)

            for inact_ref in old_set - new_set:
                delete_list.append((inact_ref, 'contrail:'+meta))

        if delete_list:
            self._delete_id_pair_meta_list(ifmap_id, delete_list)

        (ok, result) = self._ifmap_virtual_machine_set(ifmap_id, existing_metas, new_obj_dict)
        return (ok, result)
    #end _ifmap_virtual_machine_update

    def _ifmap_virtual_machine_delete(self, obj_ids):
        ifmap_id = obj_ids['imid']
        parent_imid = obj_ids.get('parent_imid', None)
        existing_metas = self._ifmap_virtual_machine_read_to_meta_index(ifmap_id)
        meta_list = []
        for meta_name, meta_infos in existing_metas.items():
            for meta_info in meta_infos:
                ref_imid = meta_info.get('id')
                if ref_imid is None:
                    continue
                meta_list.append((ref_imid, 'contrail:'+meta_name))

        if parent_imid:
            # Remove link from parent
            meta_list.append((parent_imid, None))

        if meta_list:
            self._delete_id_pair_meta_list(ifmap_id, meta_list)

        # Remove all property metadata associated with this ident
        self._delete_id_self_meta(ifmap_id, None)

        return (True, '')
    #end _ifmap_virtual_machine_delete

    def _ifmap_interface_route_table_alloc(self, parent_type, fq_name):
        imid = self._imid_handler
        (my_imid, parent_imid) = \
            imid.interface_route_table_alloc_ifmap_id(parent_type, fq_name)
        if my_imid is None or parent_imid is None:
            return (False, (my_imid, parent_imid))
        return (True, (my_imid, parent_imid))
    #end _ifmap_interface_route_table_alloc

    def _ifmap_interface_route_table_set(self, my_imid, existing_metas, obj_dict):
        # Properties Meta
        update = {}
        field = obj_dict.get('interface_route_table_routes', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['interface_route_table_routes']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                RouteTableType(**field).exportChildren(buf, level = 1, name_ = 'interface-route-table-routes', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'interface-route-table-routes', pretty_print = False)
            interface_route_table_routes_xml = buf.getvalue()
            buf.close()
            meta = Metadata('interface-route-table-routes' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = interface_route_table_routes_xml)

            if (existing_metas and 'interface-route-table-routes' in existing_metas and
                str(existing_metas['interface-route-table-routes'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['id_perms']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                IdPermsType(**field).exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            id_perms_xml = buf.getvalue()
            buf.close()
            meta = Metadata('id-perms' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = id_perms_xml)

            if (existing_metas and 'id-perms' in existing_metas and
                str(existing_metas['id-perms'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('display_name', None)
        if field is not None:
            norm_str = escape(str(obj_dict['display_name']))
            meta = Metadata('display-name', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'display-name' in existing_metas and
                str(existing_metas['display-name'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        # Ref Link Metas
        imid = self._imid_handler


        self._publish_update(my_imid, update)
        return (True, '')
    #end _ifmap_interface_route_table_set

    def _ifmap_interface_route_table_create(self, obj_ids, obj_dict):
        if not 'parent_type' in obj_dict:
            # parent is config-root
            parent_type = 'config-root'
            parent_imid = 'contrail:config-root:root'
        else:
            parent_type = obj_dict['parent_type']
            parent_imid = obj_ids.get('parent_imid', None)

        # Parent Link Meta
        update = {}
        parent_link_meta = self._parent_metas[parent_type]['interface-route-table']
        meta = Metadata(parent_link_meta, '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')
        self._update_id_pair_meta(update, obj_ids['imid'], meta)
        self._publish_update(parent_imid, update)

        (ok, result) = self._ifmap_interface_route_table_set(obj_ids['imid'], None, obj_dict)
        return (ok, result)
    #end _ifmap_interface_route_table_create


    def _ifmap_interface_route_table_read_to_meta_index(self, ifmap_id, field_names = None):
        # field_names = None means all fields will be read
        imid = self._imid_handler
        start_id = str(Identity(name = ifmap_id, type = 'other',
                        other_type = 'extended'))
        # if id-perms missing, identity doesn't exist

        all_metas = [u'virtual-machine-interface-route-table', u'interface-route-table-routes', u'id-perms', u'display-name']
        if not field_names:
            metas_to_read = all_metas
        else: # read only requested fields
            metas_to_read = set(all_metas) & set(field_names.keys())

        # metas is a dict where key is meta-name and val is list of dict
        # of form [{'meta':meta}, {'id':id1, 'meta':meta}, {'id':id2, 'meta':meta}]
        metas = {}
        for meta_name in metas_to_read:
            if meta_name in self._id_to_metas[ifmap_id]:
                metas[meta_name] = self._id_to_metas[ifmap_id][meta_name]
        return metas
    #end _ifmap_interface-route-table_read_to_meta_index

    def _ifmap_interface_route_table_update(self, ifmap_id, new_obj_dict):
        # read in refs from ifmap to determine which ones become inactive after update
        existing_metas = self._ifmap_interface_route_table_read_to_meta_index(ifmap_id)

        # remove properties that are no longer active
        props = ['interface-route-table-routes', 'id-perms', 'display-name']
        for prop in props:
            prop_m = prop.replace('-', '_')
            if prop in existing_metas and prop_m not in new_obj_dict:
                self._delete_id_self_meta(ifmap_id, 'contrail:'+prop)
        # remove refs that are no longer active
        delete_list = []
        refs = {}
        for meta, to_name in refs.items():
            old_set = set([m['id'] for m in existing_metas.get(meta, [])])
            new_set = set()
            to_name_m = to_name.replace('-', '_')
            for ref in new_obj_dict.get(to_name_m+'_refs', []):
                to_imid = self.fq_name_to_ifmap_id(to_name, ref['to'])
                new_set.add(to_imid)

            for inact_ref in old_set - new_set:
                delete_list.append((inact_ref, 'contrail:'+meta))

        if delete_list:
            self._delete_id_pair_meta_list(ifmap_id, delete_list)

        (ok, result) = self._ifmap_interface_route_table_set(ifmap_id, existing_metas, new_obj_dict)
        return (ok, result)
    #end _ifmap_interface_route_table_update

    def _ifmap_interface_route_table_delete(self, obj_ids):
        ifmap_id = obj_ids['imid']
        parent_imid = obj_ids.get('parent_imid', None)
        existing_metas = self._ifmap_interface_route_table_read_to_meta_index(ifmap_id)
        meta_list = []
        for meta_name, meta_infos in existing_metas.items():
            for meta_info in meta_infos:
                ref_imid = meta_info.get('id')
                if ref_imid is None:
                    continue
                meta_list.append((ref_imid, 'contrail:'+meta_name))

        if parent_imid:
            # Remove link from parent
            meta_list.append((parent_imid, None))

        if meta_list:
            self._delete_id_pair_meta_list(ifmap_id, meta_list)

        # Remove all property metadata associated with this ident
        self._delete_id_self_meta(ifmap_id, None)

        return (True, '')
    #end _ifmap_interface_route_table_delete

    def _ifmap_service_template_alloc(self, parent_type, fq_name):
        imid = self._imid_handler
        (my_imid, parent_imid) = \
            imid.service_template_alloc_ifmap_id(parent_type, fq_name)
        if my_imid is None or parent_imid is None:
            return (False, (my_imid, parent_imid))
        return (True, (my_imid, parent_imid))
    #end _ifmap_service_template_alloc

    def _ifmap_service_template_set(self, my_imid, existing_metas, obj_dict):
        # Properties Meta
        update = {}
        field = obj_dict.get('service_template_properties', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['service_template_properties']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                ServiceTemplateType(**field).exportChildren(buf, level = 1, name_ = 'service-template-properties', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'service-template-properties', pretty_print = False)
            service_template_properties_xml = buf.getvalue()
            buf.close()
            meta = Metadata('service-template-properties' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = service_template_properties_xml)

            if (existing_metas and 'service-template-properties' in existing_metas and
                str(existing_metas['service-template-properties'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['id_perms']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                IdPermsType(**field).exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            id_perms_xml = buf.getvalue()
            buf.close()
            meta = Metadata('id-perms' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = id_perms_xml)

            if (existing_metas and 'id-perms' in existing_metas and
                str(existing_metas['id-perms'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('display_name', None)
        if field is not None:
            norm_str = escape(str(obj_dict['display_name']))
            meta = Metadata('display-name', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'display-name' in existing_metas and
                str(existing_metas['display-name'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        # Ref Link Metas
        imid = self._imid_handler


        self._publish_update(my_imid, update)
        return (True, '')
    #end _ifmap_service_template_set

    def _ifmap_service_template_create(self, obj_ids, obj_dict):
        if not 'parent_type' in obj_dict:
            # parent is config-root
            parent_type = 'config-root'
            parent_imid = 'contrail:config-root:root'
        else:
            parent_type = obj_dict['parent_type']
            parent_imid = obj_ids.get('parent_imid', None)

        # Parent Link Meta
        update = {}
        parent_link_meta = self._parent_metas[parent_type]['service-template']
        meta = Metadata(parent_link_meta, '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')
        self._update_id_pair_meta(update, obj_ids['imid'], meta)
        self._publish_update(parent_imid, update)

        (ok, result) = self._ifmap_service_template_set(obj_ids['imid'], None, obj_dict)
        return (ok, result)
    #end _ifmap_service_template_create


    def _ifmap_service_template_read_to_meta_index(self, ifmap_id, field_names = None):
        # field_names = None means all fields will be read
        imid = self._imid_handler
        start_id = str(Identity(name = ifmap_id, type = 'other',
                        other_type = 'extended'))
        # if id-perms missing, identity doesn't exist

        all_metas = [u'service-instance-service-template', u'service-template-properties', u'id-perms', u'display-name']
        if not field_names:
            metas_to_read = all_metas
        else: # read only requested fields
            metas_to_read = set(all_metas) & set(field_names.keys())

        # metas is a dict where key is meta-name and val is list of dict
        # of form [{'meta':meta}, {'id':id1, 'meta':meta}, {'id':id2, 'meta':meta}]
        metas = {}
        for meta_name in metas_to_read:
            if meta_name in self._id_to_metas[ifmap_id]:
                metas[meta_name] = self._id_to_metas[ifmap_id][meta_name]
        return metas
    #end _ifmap_service-template_read_to_meta_index

    def _ifmap_service_template_update(self, ifmap_id, new_obj_dict):
        # read in refs from ifmap to determine which ones become inactive after update
        existing_metas = self._ifmap_service_template_read_to_meta_index(ifmap_id)

        # remove properties that are no longer active
        props = ['service-template-properties', 'id-perms', 'display-name']
        for prop in props:
            prop_m = prop.replace('-', '_')
            if prop in existing_metas and prop_m not in new_obj_dict:
                self._delete_id_self_meta(ifmap_id, 'contrail:'+prop)
        # remove refs that are no longer active
        delete_list = []
        refs = {}
        for meta, to_name in refs.items():
            old_set = set([m['id'] for m in existing_metas.get(meta, [])])
            new_set = set()
            to_name_m = to_name.replace('-', '_')
            for ref in new_obj_dict.get(to_name_m+'_refs', []):
                to_imid = self.fq_name_to_ifmap_id(to_name, ref['to'])
                new_set.add(to_imid)

            for inact_ref in old_set - new_set:
                delete_list.append((inact_ref, 'contrail:'+meta))

        if delete_list:
            self._delete_id_pair_meta_list(ifmap_id, delete_list)

        (ok, result) = self._ifmap_service_template_set(ifmap_id, existing_metas, new_obj_dict)
        return (ok, result)
    #end _ifmap_service_template_update

    def _ifmap_service_template_delete(self, obj_ids):
        ifmap_id = obj_ids['imid']
        parent_imid = obj_ids.get('parent_imid', None)
        existing_metas = self._ifmap_service_template_read_to_meta_index(ifmap_id)
        meta_list = []
        for meta_name, meta_infos in existing_metas.items():
            for meta_info in meta_infos:
                ref_imid = meta_info.get('id')
                if ref_imid is None:
                    continue
                meta_list.append((ref_imid, 'contrail:'+meta_name))

        if parent_imid:
            # Remove link from parent
            meta_list.append((parent_imid, None))

        if meta_list:
            self._delete_id_pair_meta_list(ifmap_id, meta_list)

        # Remove all property metadata associated with this ident
        self._delete_id_self_meta(ifmap_id, None)

        return (True, '')
    #end _ifmap_service_template_delete

    def _ifmap_virtual_ip_alloc(self, parent_type, fq_name):
        imid = self._imid_handler
        (my_imid, parent_imid) = \
            imid.virtual_ip_alloc_ifmap_id(parent_type, fq_name)
        if my_imid is None or parent_imid is None:
            return (False, (my_imid, parent_imid))
        return (True, (my_imid, parent_imid))
    #end _ifmap_virtual_ip_alloc

    def _ifmap_virtual_ip_set(self, my_imid, existing_metas, obj_dict):
        # Properties Meta
        update = {}
        field = obj_dict.get('virtual_ip_properties', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['virtual_ip_properties']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                VirtualIpType(**field).exportChildren(buf, level = 1, name_ = 'virtual-ip-properties', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'virtual-ip-properties', pretty_print = False)
            virtual_ip_properties_xml = buf.getvalue()
            buf.close()
            meta = Metadata('virtual-ip-properties' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = virtual_ip_properties_xml)

            if (existing_metas and 'virtual-ip-properties' in existing_metas and
                str(existing_metas['virtual-ip-properties'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['id_perms']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                IdPermsType(**field).exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            id_perms_xml = buf.getvalue()
            buf.close()
            meta = Metadata('id-perms' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = id_perms_xml)

            if (existing_metas and 'id-perms' in existing_metas and
                str(existing_metas['id-perms'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('display_name', None)
        if field is not None:
            norm_str = escape(str(obj_dict['display_name']))
            meta = Metadata('display-name', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'display-name' in existing_metas and
                str(existing_metas['display-name'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        # Ref Link Metas
        imid = self._imid_handler

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('loadbalancer_pool_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'loadbalancer-pool'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                virtual_ip_loadbalancer_pool_xml = ''
                meta = Metadata('virtual-ip-loadbalancer-pool' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = virtual_ip_loadbalancer_pool_xml)
                self._update_id_pair_meta(update, to_imid, meta)

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('virtual_machine_interface_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'virtual-machine-interface'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                virtual_ip_virtual_machine_interface_xml = ''
                meta = Metadata('virtual-ip-virtual-machine-interface' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = virtual_ip_virtual_machine_interface_xml)
                self._update_id_pair_meta(update, to_imid, meta)


        self._publish_update(my_imid, update)
        return (True, '')
    #end _ifmap_virtual_ip_set

    def _ifmap_virtual_ip_create(self, obj_ids, obj_dict):
        if not 'parent_type' in obj_dict:
            # parent is config-root
            parent_type = 'config-root'
            parent_imid = 'contrail:config-root:root'
        else:
            parent_type = obj_dict['parent_type']
            parent_imid = obj_ids.get('parent_imid', None)

        # Parent Link Meta
        update = {}
        parent_link_meta = self._parent_metas[parent_type]['virtual-ip']
        meta = Metadata(parent_link_meta, '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')
        self._update_id_pair_meta(update, obj_ids['imid'], meta)
        self._publish_update(parent_imid, update)

        (ok, result) = self._ifmap_virtual_ip_set(obj_ids['imid'], None, obj_dict)
        return (ok, result)
    #end _ifmap_virtual_ip_create


    def _ifmap_virtual_ip_read_to_meta_index(self, ifmap_id, field_names = None):
        # field_names = None means all fields will be read
        imid = self._imid_handler
        start_id = str(Identity(name = ifmap_id, type = 'other',
                        other_type = 'extended'))
        # if id-perms missing, identity doesn't exist

        all_metas = [u'virtual-ip-loadbalancer-pool', u'virtual-ip-virtual-machine-interface', u'virtual-ip-properties', u'id-perms', u'display-name']
        if not field_names:
            metas_to_read = all_metas
        else: # read only requested fields
            metas_to_read = set(all_metas) & set(field_names.keys())

        # metas is a dict where key is meta-name and val is list of dict
        # of form [{'meta':meta}, {'id':id1, 'meta':meta}, {'id':id2, 'meta':meta}]
        metas = {}
        for meta_name in metas_to_read:
            if meta_name in self._id_to_metas[ifmap_id]:
                metas[meta_name] = self._id_to_metas[ifmap_id][meta_name]
        return metas
    #end _ifmap_virtual-ip_read_to_meta_index

    def _ifmap_virtual_ip_update(self, ifmap_id, new_obj_dict):
        # read in refs from ifmap to determine which ones become inactive after update
        existing_metas = self._ifmap_virtual_ip_read_to_meta_index(ifmap_id)

        # remove properties that are no longer active
        props = ['virtual-ip-properties', 'id-perms', 'display-name']
        for prop in props:
            prop_m = prop.replace('-', '_')
            if prop in existing_metas and prop_m not in new_obj_dict:
                self._delete_id_self_meta(ifmap_id, 'contrail:'+prop)
        # remove refs that are no longer active
        delete_list = []
        refs = {'virtual-ip-loadbalancer-pool': 'loadbalancer-pool',
                'virtual-ip-virtual-machine-interface': 'virtual-machine-interface'}
        for meta, to_name in refs.items():
            old_set = set([m['id'] for m in existing_metas.get(meta, [])])
            new_set = set()
            to_name_m = to_name.replace('-', '_')
            for ref in new_obj_dict.get(to_name_m+'_refs', []):
                to_imid = self.fq_name_to_ifmap_id(to_name, ref['to'])
                new_set.add(to_imid)

            for inact_ref in old_set - new_set:
                delete_list.append((inact_ref, 'contrail:'+meta))

        if delete_list:
            self._delete_id_pair_meta_list(ifmap_id, delete_list)

        (ok, result) = self._ifmap_virtual_ip_set(ifmap_id, existing_metas, new_obj_dict)
        return (ok, result)
    #end _ifmap_virtual_ip_update

    def _ifmap_virtual_ip_delete(self, obj_ids):
        ifmap_id = obj_ids['imid']
        parent_imid = obj_ids.get('parent_imid', None)
        existing_metas = self._ifmap_virtual_ip_read_to_meta_index(ifmap_id)
        meta_list = []
        for meta_name, meta_infos in existing_metas.items():
            for meta_info in meta_infos:
                ref_imid = meta_info.get('id')
                if ref_imid is None:
                    continue
                meta_list.append((ref_imid, 'contrail:'+meta_name))

        if parent_imid:
            # Remove link from parent
            meta_list.append((parent_imid, None))

        if meta_list:
            self._delete_id_pair_meta_list(ifmap_id, meta_list)

        # Remove all property metadata associated with this ident
        self._delete_id_self_meta(ifmap_id, None)

        return (True, '')
    #end _ifmap_virtual_ip_delete

    def _ifmap_loadbalancer_member_alloc(self, parent_type, fq_name):
        imid = self._imid_handler
        (my_imid, parent_imid) = \
            imid.loadbalancer_member_alloc_ifmap_id(parent_type, fq_name)
        if my_imid is None or parent_imid is None:
            return (False, (my_imid, parent_imid))
        return (True, (my_imid, parent_imid))
    #end _ifmap_loadbalancer_member_alloc

    def _ifmap_loadbalancer_member_set(self, my_imid, existing_metas, obj_dict):
        # Properties Meta
        update = {}
        field = obj_dict.get('loadbalancer_member_properties', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['loadbalancer_member_properties']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                LoadbalancerMemberType(**field).exportChildren(buf, level = 1, name_ = 'loadbalancer-member-properties', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'loadbalancer-member-properties', pretty_print = False)
            loadbalancer_member_properties_xml = buf.getvalue()
            buf.close()
            meta = Metadata('loadbalancer-member-properties' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = loadbalancer_member_properties_xml)

            if (existing_metas and 'loadbalancer-member-properties' in existing_metas and
                str(existing_metas['loadbalancer-member-properties'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['id_perms']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                IdPermsType(**field).exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            id_perms_xml = buf.getvalue()
            buf.close()
            meta = Metadata('id-perms' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = id_perms_xml)

            if (existing_metas and 'id-perms' in existing_metas and
                str(existing_metas['id-perms'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('display_name', None)
        if field is not None:
            norm_str = escape(str(obj_dict['display_name']))
            meta = Metadata('display-name', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'display-name' in existing_metas and
                str(existing_metas['display-name'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        # Ref Link Metas
        imid = self._imid_handler


        self._publish_update(my_imid, update)
        return (True, '')
    #end _ifmap_loadbalancer_member_set

    def _ifmap_loadbalancer_member_create(self, obj_ids, obj_dict):
        if not 'parent_type' in obj_dict:
            # parent is config-root
            parent_type = 'config-root'
            parent_imid = 'contrail:config-root:root'
        else:
            parent_type = obj_dict['parent_type']
            parent_imid = obj_ids.get('parent_imid', None)

        # Parent Link Meta
        update = {}
        parent_link_meta = self._parent_metas[parent_type]['loadbalancer-member']
        meta = Metadata(parent_link_meta, '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')
        self._update_id_pair_meta(update, obj_ids['imid'], meta)
        self._publish_update(parent_imid, update)

        (ok, result) = self._ifmap_loadbalancer_member_set(obj_ids['imid'], None, obj_dict)
        return (ok, result)
    #end _ifmap_loadbalancer_member_create


    def _ifmap_loadbalancer_member_read_to_meta_index(self, ifmap_id, field_names = None):
        # field_names = None means all fields will be read
        imid = self._imid_handler
        start_id = str(Identity(name = ifmap_id, type = 'other',
                        other_type = 'extended'))
        # if id-perms missing, identity doesn't exist

        all_metas = [u'loadbalancer-member-properties', u'id-perms', u'display-name']
        if not field_names:
            metas_to_read = all_metas
        else: # read only requested fields
            metas_to_read = set(all_metas) & set(field_names.keys())

        # metas is a dict where key is meta-name and val is list of dict
        # of form [{'meta':meta}, {'id':id1, 'meta':meta}, {'id':id2, 'meta':meta}]
        metas = {}
        for meta_name in metas_to_read:
            if meta_name in self._id_to_metas[ifmap_id]:
                metas[meta_name] = self._id_to_metas[ifmap_id][meta_name]
        return metas
    #end _ifmap_loadbalancer-member_read_to_meta_index

    def _ifmap_loadbalancer_member_update(self, ifmap_id, new_obj_dict):
        # read in refs from ifmap to determine which ones become inactive after update
        existing_metas = self._ifmap_loadbalancer_member_read_to_meta_index(ifmap_id)

        # remove properties that are no longer active
        props = ['loadbalancer-member-properties', 'id-perms', 'display-name']
        for prop in props:
            prop_m = prop.replace('-', '_')
            if prop in existing_metas and prop_m not in new_obj_dict:
                self._delete_id_self_meta(ifmap_id, 'contrail:'+prop)
        # remove refs that are no longer active
        delete_list = []
        refs = {}
        for meta, to_name in refs.items():
            old_set = set([m['id'] for m in existing_metas.get(meta, [])])
            new_set = set()
            to_name_m = to_name.replace('-', '_')
            for ref in new_obj_dict.get(to_name_m+'_refs', []):
                to_imid = self.fq_name_to_ifmap_id(to_name, ref['to'])
                new_set.add(to_imid)

            for inact_ref in old_set - new_set:
                delete_list.append((inact_ref, 'contrail:'+meta))

        if delete_list:
            self._delete_id_pair_meta_list(ifmap_id, delete_list)

        (ok, result) = self._ifmap_loadbalancer_member_set(ifmap_id, existing_metas, new_obj_dict)
        return (ok, result)
    #end _ifmap_loadbalancer_member_update

    def _ifmap_loadbalancer_member_delete(self, obj_ids):
        ifmap_id = obj_ids['imid']
        parent_imid = obj_ids.get('parent_imid', None)
        existing_metas = self._ifmap_loadbalancer_member_read_to_meta_index(ifmap_id)
        meta_list = []
        for meta_name, meta_infos in existing_metas.items():
            for meta_info in meta_infos:
                ref_imid = meta_info.get('id')
                if ref_imid is None:
                    continue
                meta_list.append((ref_imid, 'contrail:'+meta_name))

        if parent_imid:
            # Remove link from parent
            meta_list.append((parent_imid, None))

        if meta_list:
            self._delete_id_pair_meta_list(ifmap_id, meta_list)

        # Remove all property metadata associated with this ident
        self._delete_id_self_meta(ifmap_id, None)

        return (True, '')
    #end _ifmap_loadbalancer_member_delete

    def _ifmap_security_group_alloc(self, parent_type, fq_name):
        imid = self._imid_handler
        (my_imid, parent_imid) = \
            imid.security_group_alloc_ifmap_id(parent_type, fq_name)
        if my_imid is None or parent_imid is None:
            return (False, (my_imid, parent_imid))
        return (True, (my_imid, parent_imid))
    #end _ifmap_security_group_alloc

    def _ifmap_security_group_set(self, my_imid, existing_metas, obj_dict):
        # Properties Meta
        update = {}
        field = obj_dict.get('security_group_id', None)
        if field is not None:
            norm_str = escape(str(obj_dict['security_group_id']))
            meta = Metadata('security-group-id', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'security-group-id' in existing_metas and
                str(existing_metas['security-group-id'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('configured_security_group_id', None)
        if field is not None:
            norm_str = escape(str(obj_dict['configured_security_group_id']))
            meta = Metadata('configured-security-group-id', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'configured-security-group-id' in existing_metas and
                str(existing_metas['configured-security-group-id'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('security_group_entries', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['security_group_entries']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                PolicyEntriesType(**field).exportChildren(buf, level = 1, name_ = 'security-group-entries', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'security-group-entries', pretty_print = False)
            security_group_entries_xml = buf.getvalue()
            buf.close()
            meta = Metadata('security-group-entries' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = security_group_entries_xml)

            if (existing_metas and 'security-group-entries' in existing_metas and
                str(existing_metas['security-group-entries'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['id_perms']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                IdPermsType(**field).exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            id_perms_xml = buf.getvalue()
            buf.close()
            meta = Metadata('id-perms' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = id_perms_xml)

            if (existing_metas and 'id-perms' in existing_metas and
                str(existing_metas['id-perms'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('display_name', None)
        if field is not None:
            norm_str = escape(str(obj_dict['display_name']))
            meta = Metadata('display-name', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'display-name' in existing_metas and
                str(existing_metas['display-name'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        # Ref Link Metas
        imid = self._imid_handler

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('access_control_list_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'access-control-list'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                security_group_access_control_list_xml = ''
                meta = Metadata('security-group-access-control-list' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = security_group_access_control_list_xml)
                self._update_id_pair_meta(update, to_imid, meta)


        self._publish_update(my_imid, update)
        return (True, '')
    #end _ifmap_security_group_set

    def _ifmap_security_group_create(self, obj_ids, obj_dict):
        if not 'parent_type' in obj_dict:
            # parent is config-root
            parent_type = 'config-root'
            parent_imid = 'contrail:config-root:root'
        else:
            parent_type = obj_dict['parent_type']
            parent_imid = obj_ids.get('parent_imid', None)

        # Parent Link Meta
        update = {}
        parent_link_meta = self._parent_metas[parent_type]['security-group']
        meta = Metadata(parent_link_meta, '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')
        self._update_id_pair_meta(update, obj_ids['imid'], meta)
        self._publish_update(parent_imid, update)

        (ok, result) = self._ifmap_security_group_set(obj_ids['imid'], None, obj_dict)
        return (ok, result)
    #end _ifmap_security_group_create


    def _ifmap_security_group_read_to_meta_index(self, ifmap_id, field_names = None):
        # field_names = None means all fields will be read
        imid = self._imid_handler
        start_id = str(Identity(name = ifmap_id, type = 'other',
                        other_type = 'extended'))
        # if id-perms missing, identity doesn't exist

        all_metas = [u'virtual-machine-interface-security-group', u'security-group-id', u'configured-security-group-id', u'security-group-entries', u'id-perms', u'display-name', u'security-group-access-control-list']
        if not field_names:
            metas_to_read = all_metas
        else: # read only requested fields
            metas_to_read = set(all_metas) & set(field_names.keys())

        # metas is a dict where key is meta-name and val is list of dict
        # of form [{'meta':meta}, {'id':id1, 'meta':meta}, {'id':id2, 'meta':meta}]
        metas = {}
        for meta_name in metas_to_read:
            if meta_name in self._id_to_metas[ifmap_id]:
                metas[meta_name] = self._id_to_metas[ifmap_id][meta_name]
        return metas
    #end _ifmap_security-group_read_to_meta_index

    def _ifmap_security_group_update(self, ifmap_id, new_obj_dict):
        # read in refs from ifmap to determine which ones become inactive after update
        existing_metas = self._ifmap_security_group_read_to_meta_index(ifmap_id)

        # remove properties that are no longer active
        props = ['security-group-id', 'configured-security-group-id', 'security-group-entries', 'id-perms', 'display-name']
        for prop in props:
            prop_m = prop.replace('-', '_')
            if prop in existing_metas and prop_m not in new_obj_dict:
                self._delete_id_self_meta(ifmap_id, 'contrail:'+prop)
        # remove refs that are no longer active
        delete_list = []
        refs = {}
        for meta, to_name in refs.items():
            old_set = set([m['id'] for m in existing_metas.get(meta, [])])
            new_set = set()
            to_name_m = to_name.replace('-', '_')
            for ref in new_obj_dict.get(to_name_m+'_refs', []):
                to_imid = self.fq_name_to_ifmap_id(to_name, ref['to'])
                new_set.add(to_imid)

            for inact_ref in old_set - new_set:
                delete_list.append((inact_ref, 'contrail:'+meta))

        if delete_list:
            self._delete_id_pair_meta_list(ifmap_id, delete_list)

        (ok, result) = self._ifmap_security_group_set(ifmap_id, existing_metas, new_obj_dict)
        return (ok, result)
    #end _ifmap_security_group_update

    def _ifmap_security_group_delete(self, obj_ids):
        ifmap_id = obj_ids['imid']
        parent_imid = obj_ids.get('parent_imid', None)
        existing_metas = self._ifmap_security_group_read_to_meta_index(ifmap_id)
        meta_list = []
        for meta_name, meta_infos in existing_metas.items():
            for meta_info in meta_infos:
                ref_imid = meta_info.get('id')
                if ref_imid is None:
                    continue
                meta_list.append((ref_imid, 'contrail:'+meta_name))

        if parent_imid:
            # Remove link from parent
            meta_list.append((parent_imid, None))

        if meta_list:
            self._delete_id_pair_meta_list(ifmap_id, meta_list)

        # Remove all property metadata associated with this ident
        self._delete_id_self_meta(ifmap_id, None)

        return (True, '')
    #end _ifmap_security_group_delete

    def _ifmap_provider_attachment_alloc(self, parent_type, fq_name):
        imid = self._imid_handler
        (my_imid, parent_imid) = \
            imid.provider_attachment_alloc_ifmap_id(parent_type, fq_name)
        if my_imid is None or parent_imid is None:
            return (False, (my_imid, parent_imid))
        return (True, (my_imid, parent_imid))
    #end _ifmap_provider_attachment_alloc

    def _ifmap_provider_attachment_set(self, my_imid, existing_metas, obj_dict):
        # Properties Meta
        update = {}
        field = obj_dict.get('id_perms', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['id_perms']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                IdPermsType(**field).exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            id_perms_xml = buf.getvalue()
            buf.close()
            meta = Metadata('id-perms' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = id_perms_xml)

            if (existing_metas and 'id-perms' in existing_metas and
                str(existing_metas['id-perms'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('display_name', None)
        if field is not None:
            norm_str = escape(str(obj_dict['display_name']))
            meta = Metadata('display-name', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'display-name' in existing_metas and
                str(existing_metas['display-name'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        # Ref Link Metas
        imid = self._imid_handler

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('virtual_router_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'virtual-router'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                provider_attachment_virtual_router_xml = ''
                meta = Metadata('provider-attachment-virtual-router' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = provider_attachment_virtual_router_xml)
                self._update_id_pair_meta(update, to_imid, meta)


        self._publish_update(my_imid, update)
        return (True, '')
    #end _ifmap_provider_attachment_set

    def _ifmap_provider_attachment_create(self, obj_ids, obj_dict):
        (ok, result) = self._ifmap_provider_attachment_set(obj_ids['imid'], None, obj_dict)
        return (ok, result)
    #end _ifmap_provider_attachment_create


    def _ifmap_provider_attachment_read_to_meta_index(self, ifmap_id, field_names = None):
        # field_names = None means all fields will be read
        imid = self._imid_handler
        start_id = str(Identity(name = ifmap_id, type = 'other',
                        other_type = 'extended'))
        # if id-perms missing, identity doesn't exist

        all_metas = [u'provider-attachment-virtual-router', u'id-perms', u'display-name']
        if not field_names:
            metas_to_read = all_metas
        else: # read only requested fields
            metas_to_read = set(all_metas) & set(field_names.keys())

        # metas is a dict where key is meta-name and val is list of dict
        # of form [{'meta':meta}, {'id':id1, 'meta':meta}, {'id':id2, 'meta':meta}]
        metas = {}
        for meta_name in metas_to_read:
            if meta_name in self._id_to_metas[ifmap_id]:
                metas[meta_name] = self._id_to_metas[ifmap_id][meta_name]
        return metas
    #end _ifmap_provider-attachment_read_to_meta_index

    def _ifmap_provider_attachment_update(self, ifmap_id, new_obj_dict):
        # read in refs from ifmap to determine which ones become inactive after update
        existing_metas = self._ifmap_provider_attachment_read_to_meta_index(ifmap_id)

        # remove properties that are no longer active
        props = ['id-perms', 'display-name']
        for prop in props:
            prop_m = prop.replace('-', '_')
            if prop in existing_metas and prop_m not in new_obj_dict:
                self._delete_id_self_meta(ifmap_id, 'contrail:'+prop)
        # remove refs that are no longer active
        delete_list = []
        refs = {'provider-attachment-virtual-router': 'virtual-router'}
        for meta, to_name in refs.items():
            old_set = set([m['id'] for m in existing_metas.get(meta, [])])
            new_set = set()
            to_name_m = to_name.replace('-', '_')
            for ref in new_obj_dict.get(to_name_m+'_refs', []):
                to_imid = self.fq_name_to_ifmap_id(to_name, ref['to'])
                new_set.add(to_imid)

            for inact_ref in old_set - new_set:
                delete_list.append((inact_ref, 'contrail:'+meta))

        if delete_list:
            self._delete_id_pair_meta_list(ifmap_id, delete_list)

        (ok, result) = self._ifmap_provider_attachment_set(ifmap_id, existing_metas, new_obj_dict)
        return (ok, result)
    #end _ifmap_provider_attachment_update

    def _ifmap_provider_attachment_delete(self, obj_ids):
        ifmap_id = obj_ids['imid']
        parent_imid = obj_ids.get('parent_imid', None)
        existing_metas = self._ifmap_provider_attachment_read_to_meta_index(ifmap_id)
        meta_list = []
        for meta_name, meta_infos in existing_metas.items():
            for meta_info in meta_infos:
                ref_imid = meta_info.get('id')
                if ref_imid is None:
                    continue
                meta_list.append((ref_imid, 'contrail:'+meta_name))

        if parent_imid:
            # Remove link from parent
            meta_list.append((parent_imid, None))

        if meta_list:
            self._delete_id_pair_meta_list(ifmap_id, meta_list)

        # Remove all property metadata associated with this ident
        self._delete_id_self_meta(ifmap_id, None)

        return (True, '')
    #end _ifmap_provider_attachment_delete

    def _ifmap_virtual_machine_interface_alloc(self, parent_type, fq_name):
        imid = self._imid_handler
        (my_imid, parent_imid) = \
            imid.virtual_machine_interface_alloc_ifmap_id(parent_type, fq_name)
        if my_imid is None or parent_imid is None:
            return (False, (my_imid, parent_imid))
        return (True, (my_imid, parent_imid))
    #end _ifmap_virtual_machine_interface_alloc

    def _ifmap_virtual_machine_interface_set(self, my_imid, existing_metas, obj_dict):
        # Properties Meta
        update = {}
        field = obj_dict.get('virtual_machine_interface_mac_addresses', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['virtual_machine_interface_mac_addresses']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                MacAddressesType(**field).exportChildren(buf, level = 1, name_ = 'virtual-machine-interface-mac-addresses', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'virtual-machine-interface-mac-addresses', pretty_print = False)
            virtual_machine_interface_mac_addresses_xml = buf.getvalue()
            buf.close()
            meta = Metadata('virtual-machine-interface-mac-addresses' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = virtual_machine_interface_mac_addresses_xml)

            if (existing_metas and 'virtual-machine-interface-mac-addresses' in existing_metas and
                str(existing_metas['virtual-machine-interface-mac-addresses'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('virtual_machine_interface_dhcp_option_list', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['virtual_machine_interface_dhcp_option_list']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                DhcpOptionsListType(**field).exportChildren(buf, level = 1, name_ = 'virtual-machine-interface-dhcp-option-list', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'virtual-machine-interface-dhcp-option-list', pretty_print = False)
            virtual_machine_interface_dhcp_option_list_xml = buf.getvalue()
            buf.close()
            meta = Metadata('virtual-machine-interface-dhcp-option-list' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = virtual_machine_interface_dhcp_option_list_xml)

            if (existing_metas and 'virtual-machine-interface-dhcp-option-list' in existing_metas and
                str(existing_metas['virtual-machine-interface-dhcp-option-list'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('virtual_machine_interface_host_routes', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['virtual_machine_interface_host_routes']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                RouteTableType(**field).exportChildren(buf, level = 1, name_ = 'virtual-machine-interface-host-routes', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'virtual-machine-interface-host-routes', pretty_print = False)
            virtual_machine_interface_host_routes_xml = buf.getvalue()
            buf.close()
            meta = Metadata('virtual-machine-interface-host-routes' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = virtual_machine_interface_host_routes_xml)

            if (existing_metas and 'virtual-machine-interface-host-routes' in existing_metas and
                str(existing_metas['virtual-machine-interface-host-routes'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('virtual_machine_interface_allowed_address_pairs', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['virtual_machine_interface_allowed_address_pairs']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                AllowedAddressPairs(**field).exportChildren(buf, level = 1, name_ = 'virtual-machine-interface-allowed-address-pairs', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'virtual-machine-interface-allowed-address-pairs', pretty_print = False)
            virtual_machine_interface_allowed_address_pairs_xml = buf.getvalue()
            buf.close()
            meta = Metadata('virtual-machine-interface-allowed-address-pairs' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = virtual_machine_interface_allowed_address_pairs_xml)

            if (existing_metas and 'virtual-machine-interface-allowed-address-pairs' in existing_metas and
                str(existing_metas['virtual-machine-interface-allowed-address-pairs'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('vrf_assign_table', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['vrf_assign_table']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                VrfAssignTableType(**field).exportChildren(buf, level = 1, name_ = 'vrf-assign-table', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'vrf-assign-table', pretty_print = False)
            vrf_assign_table_xml = buf.getvalue()
            buf.close()
            meta = Metadata('vrf-assign-table' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = vrf_assign_table_xml)

            if (existing_metas and 'vrf-assign-table' in existing_metas and
                str(existing_metas['vrf-assign-table'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('virtual_machine_interface_device_owner', None)
        if field is not None:
            norm_str = escape(str(obj_dict['virtual_machine_interface_device_owner']))
            meta = Metadata('virtual-machine-interface-device-owner', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'virtual-machine-interface-device-owner' in existing_metas and
                str(existing_metas['virtual-machine-interface-device-owner'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('virtual_machine_interface_properties', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['virtual_machine_interface_properties']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                VirtualMachineInterfacePropertiesType(**field).exportChildren(buf, level = 1, name_ = 'virtual-machine-interface-properties', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'virtual-machine-interface-properties', pretty_print = False)
            virtual_machine_interface_properties_xml = buf.getvalue()
            buf.close()
            meta = Metadata('virtual-machine-interface-properties' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = virtual_machine_interface_properties_xml)

            if (existing_metas and 'virtual-machine-interface-properties' in existing_metas and
                str(existing_metas['virtual-machine-interface-properties'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['id_perms']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                IdPermsType(**field).exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            id_perms_xml = buf.getvalue()
            buf.close()
            meta = Metadata('id-perms' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = id_perms_xml)

            if (existing_metas and 'id-perms' in existing_metas and
                str(existing_metas['id-perms'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('display_name', None)
        if field is not None:
            norm_str = escape(str(obj_dict['display_name']))
            meta = Metadata('display-name', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'display-name' in existing_metas and
                str(existing_metas['display-name'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        # Ref Link Metas
        imid = self._imid_handler

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('qos_forwarding_class_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'qos-forwarding-class'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                virtual_machine_interface_qos_forwarding_class_xml = ''
                meta = Metadata('virtual-machine-interface-qos-forwarding-class' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = virtual_machine_interface_qos_forwarding_class_xml)
                self._update_id_pair_meta(update, to_imid, meta)

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('security_group_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'security-group'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                virtual_machine_interface_security_group_xml = ''
                meta = Metadata('virtual-machine-interface-security-group' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = virtual_machine_interface_security_group_xml)
                self._update_id_pair_meta(update, to_imid, meta)

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('virtual_machine_interface_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'virtual-machine-interface'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                virtual_machine_interface_sub_interface_xml = ''
                meta = Metadata('virtual-machine-interface-sub-interface' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = virtual_machine_interface_sub_interface_xml)
                self._update_id_pair_meta(update, to_imid, meta)

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('virtual_machine_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'virtual-machine'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                virtual_machine_interface_virtual_machine_xml = ''
                meta = Metadata('virtual-machine-interface-virtual-machine' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = virtual_machine_interface_virtual_machine_xml)
                self._update_id_pair_meta(update, to_imid, meta)

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('virtual_network_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'virtual-network'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                virtual_machine_interface_virtual_network_xml = ''
                meta = Metadata('virtual-machine-interface-virtual-network' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = virtual_machine_interface_virtual_network_xml)
                self._update_id_pair_meta(update, to_imid, meta)

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('routing_instance_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'routing-instance'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                virtual_machine_interface_routing_instance_xml = ''
                ref_data = ref['attr']
                if ref_data:
                    buf = cStringIO.StringIO()
                    PolicyBasedForwardingRuleType(**ref_data).exportChildren(buf, level = 1, name_ = 'virtual-machine-interface-routing-instance', pretty_print = False)
                    virtual_machine_interface_routing_instance_xml = virtual_machine_interface_routing_instance_xml + buf.getvalue()
                    buf.close()
                meta = Metadata('virtual-machine-interface-routing-instance' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = virtual_machine_interface_routing_instance_xml)
                self._update_id_pair_meta(update, to_imid, meta)

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('interface_route_table_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'interface-route-table'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                virtual_machine_interface_route_table_xml = ''
                meta = Metadata('virtual-machine-interface-route-table' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = virtual_machine_interface_route_table_xml)
                self._update_id_pair_meta(update, to_imid, meta)


        self._publish_update(my_imid, update)
        return (True, '')
    #end _ifmap_virtual_machine_interface_set

    def _ifmap_virtual_machine_interface_create(self, obj_ids, obj_dict):
        if not 'parent_type' in obj_dict:
            # parent is config-root
            parent_type = 'config-root'
            parent_imid = 'contrail:config-root:root'
        else:
            parent_type = obj_dict['parent_type']
            parent_imid = obj_ids.get('parent_imid', None)

        # Parent Link Meta
        update = {}
        parent_link_meta = self._parent_metas[parent_type]['virtual-machine-interface']
        meta = Metadata(parent_link_meta, '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')
        self._update_id_pair_meta(update, obj_ids['imid'], meta)
        self._publish_update(parent_imid, update)

        (ok, result) = self._ifmap_virtual_machine_interface_set(obj_ids['imid'], None, obj_dict)
        return (ok, result)
    #end _ifmap_virtual_machine_interface_create


    def _ifmap_virtual_machine_interface_read_to_meta_index(self, ifmap_id, field_names = None):
        # field_names = None means all fields will be read
        imid = self._imid_handler
        start_id = str(Identity(name = ifmap_id, type = 'other',
                        other_type = 'extended'))
        # if id-perms missing, identity doesn't exist

        all_metas = [u'virtual-machine-interface-qos-forwarding-class', u'virtual-machine-interface-security-group', u'virtual-machine-interface-sub-interface', u'virtual-machine-interface-virtual-machine', u'virtual-machine-interface-virtual-network', u'virtual-machine-interface-routing-instance', u'virtual-machine-interface-route-table', u'virtual-machine-interface-sub-interface', u'instance-ip-virtual-machine-interface', u'subnet-virtual-machine-interface', u'floating-ip-virtual-machine-interface', u'logical-interface-virtual-machine-interface', u'customer-attachment-virtual-machine-interface', u'logical-router-interface', u'loadbalancer-pool-virtual-machine-interface', u'virtual-ip-virtual-machine-interface', u'virtual-machine-interface-mac-addresses', u'virtual-machine-interface-dhcp-option-list', u'virtual-machine-interface-host-routes', u'virtual-machine-interface-allowed-address-pairs', u'vrf-assign-table', u'virtual-machine-interface-device-owner', u'virtual-machine-interface-properties', u'id-perms', u'display-name']
        if not field_names:
            metas_to_read = all_metas
        else: # read only requested fields
            metas_to_read = set(all_metas) & set(field_names.keys())

        # metas is a dict where key is meta-name and val is list of dict
        # of form [{'meta':meta}, {'id':id1, 'meta':meta}, {'id':id2, 'meta':meta}]
        metas = {}
        for meta_name in metas_to_read:
            if meta_name in self._id_to_metas[ifmap_id]:
                metas[meta_name] = self._id_to_metas[ifmap_id][meta_name]
        return metas
    #end _ifmap_virtual-machine-interface_read_to_meta_index

    def _ifmap_virtual_machine_interface_update(self, ifmap_id, new_obj_dict):
        # read in refs from ifmap to determine which ones become inactive after update
        existing_metas = self._ifmap_virtual_machine_interface_read_to_meta_index(ifmap_id)

        # remove properties that are no longer active
        props = ['virtual-machine-interface-mac-addresses', 'virtual-machine-interface-dhcp-option-list', 'virtual-machine-interface-host-routes', 'virtual-machine-interface-allowed-address-pairs', 'vrf-assign-table', 'virtual-machine-interface-device-owner', 'virtual-machine-interface-properties', 'id-perms', 'display-name']
        for prop in props:
            prop_m = prop.replace('-', '_')
            if prop in existing_metas and prop_m not in new_obj_dict:
                self._delete_id_self_meta(ifmap_id, 'contrail:'+prop)
        # remove refs that are no longer active
        delete_list = []
        refs = {'virtual-machine-interface-qos-forwarding-class': 'qos-forwarding-class',
                'virtual-machine-interface-security-group': 'security-group',
                'virtual-machine-interface-sub-interface': 'virtual-machine-interface',
                'virtual-machine-interface-virtual-machine': 'virtual-machine',
                'virtual-machine-interface-virtual-network': 'virtual-network',
                'virtual-machine-interface-routing-instance': 'routing-instance',
                'virtual-machine-interface-route-table': 'interface-route-table'}
        for meta, to_name in refs.items():
            old_set = set([m['id'] for m in existing_metas.get(meta, [])])
            new_set = set()
            to_name_m = to_name.replace('-', '_')
            for ref in new_obj_dict.get(to_name_m+'_refs', []):
                to_imid = self.fq_name_to_ifmap_id(to_name, ref['to'])
                new_set.add(to_imid)

            for inact_ref in old_set - new_set:
                delete_list.append((inact_ref, 'contrail:'+meta))

        if delete_list:
            self._delete_id_pair_meta_list(ifmap_id, delete_list)

        (ok, result) = self._ifmap_virtual_machine_interface_set(ifmap_id, existing_metas, new_obj_dict)
        return (ok, result)
    #end _ifmap_virtual_machine_interface_update

    def _ifmap_virtual_machine_interface_delete(self, obj_ids):
        ifmap_id = obj_ids['imid']
        parent_imid = obj_ids.get('parent_imid', None)
        existing_metas = self._ifmap_virtual_machine_interface_read_to_meta_index(ifmap_id)
        meta_list = []
        for meta_name, meta_infos in existing_metas.items():
            for meta_info in meta_infos:
                ref_imid = meta_info.get('id')
                if ref_imid is None:
                    continue
                meta_list.append((ref_imid, 'contrail:'+meta_name))

        if parent_imid:
            # Remove link from parent
            meta_list.append((parent_imid, None))

        if meta_list:
            self._delete_id_pair_meta_list(ifmap_id, meta_list)

        # Remove all property metadata associated with this ident
        self._delete_id_self_meta(ifmap_id, None)

        return (True, '')
    #end _ifmap_virtual_machine_interface_delete

    def _ifmap_loadbalancer_healthmonitor_alloc(self, parent_type, fq_name):
        imid = self._imid_handler
        (my_imid, parent_imid) = \
            imid.loadbalancer_healthmonitor_alloc_ifmap_id(parent_type, fq_name)
        if my_imid is None or parent_imid is None:
            return (False, (my_imid, parent_imid))
        return (True, (my_imid, parent_imid))
    #end _ifmap_loadbalancer_healthmonitor_alloc

    def _ifmap_loadbalancer_healthmonitor_set(self, my_imid, existing_metas, obj_dict):
        # Properties Meta
        update = {}
        field = obj_dict.get('loadbalancer_healthmonitor_properties', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['loadbalancer_healthmonitor_properties']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                LoadbalancerHealthmonitorType(**field).exportChildren(buf, level = 1, name_ = 'loadbalancer-healthmonitor-properties', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'loadbalancer-healthmonitor-properties', pretty_print = False)
            loadbalancer_healthmonitor_properties_xml = buf.getvalue()
            buf.close()
            meta = Metadata('loadbalancer-healthmonitor-properties' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = loadbalancer_healthmonitor_properties_xml)

            if (existing_metas and 'loadbalancer-healthmonitor-properties' in existing_metas and
                str(existing_metas['loadbalancer-healthmonitor-properties'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['id_perms']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                IdPermsType(**field).exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            id_perms_xml = buf.getvalue()
            buf.close()
            meta = Metadata('id-perms' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = id_perms_xml)

            if (existing_metas and 'id-perms' in existing_metas and
                str(existing_metas['id-perms'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('display_name', None)
        if field is not None:
            norm_str = escape(str(obj_dict['display_name']))
            meta = Metadata('display-name', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'display-name' in existing_metas and
                str(existing_metas['display-name'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        # Ref Link Metas
        imid = self._imid_handler


        self._publish_update(my_imid, update)
        return (True, '')
    #end _ifmap_loadbalancer_healthmonitor_set

    def _ifmap_loadbalancer_healthmonitor_create(self, obj_ids, obj_dict):
        if not 'parent_type' in obj_dict:
            # parent is config-root
            parent_type = 'config-root'
            parent_imid = 'contrail:config-root:root'
        else:
            parent_type = obj_dict['parent_type']
            parent_imid = obj_ids.get('parent_imid', None)

        # Parent Link Meta
        update = {}
        parent_link_meta = self._parent_metas[parent_type]['loadbalancer-healthmonitor']
        meta = Metadata(parent_link_meta, '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')
        self._update_id_pair_meta(update, obj_ids['imid'], meta)
        self._publish_update(parent_imid, update)

        (ok, result) = self._ifmap_loadbalancer_healthmonitor_set(obj_ids['imid'], None, obj_dict)
        return (ok, result)
    #end _ifmap_loadbalancer_healthmonitor_create


    def _ifmap_loadbalancer_healthmonitor_read_to_meta_index(self, ifmap_id, field_names = None):
        # field_names = None means all fields will be read
        imid = self._imid_handler
        start_id = str(Identity(name = ifmap_id, type = 'other',
                        other_type = 'extended'))
        # if id-perms missing, identity doesn't exist

        all_metas = [u'loadbalancer-pool-loadbalancer-healthmonitor', u'loadbalancer-healthmonitor-properties', u'id-perms', u'display-name']
        if not field_names:
            metas_to_read = all_metas
        else: # read only requested fields
            metas_to_read = set(all_metas) & set(field_names.keys())

        # metas is a dict where key is meta-name and val is list of dict
        # of form [{'meta':meta}, {'id':id1, 'meta':meta}, {'id':id2, 'meta':meta}]
        metas = {}
        for meta_name in metas_to_read:
            if meta_name in self._id_to_metas[ifmap_id]:
                metas[meta_name] = self._id_to_metas[ifmap_id][meta_name]
        return metas
    #end _ifmap_loadbalancer-healthmonitor_read_to_meta_index

    def _ifmap_loadbalancer_healthmonitor_update(self, ifmap_id, new_obj_dict):
        # read in refs from ifmap to determine which ones become inactive after update
        existing_metas = self._ifmap_loadbalancer_healthmonitor_read_to_meta_index(ifmap_id)

        # remove properties that are no longer active
        props = ['loadbalancer-healthmonitor-properties', 'id-perms', 'display-name']
        for prop in props:
            prop_m = prop.replace('-', '_')
            if prop in existing_metas and prop_m not in new_obj_dict:
                self._delete_id_self_meta(ifmap_id, 'contrail:'+prop)
        # remove refs that are no longer active
        delete_list = []
        refs = {}
        for meta, to_name in refs.items():
            old_set = set([m['id'] for m in existing_metas.get(meta, [])])
            new_set = set()
            to_name_m = to_name.replace('-', '_')
            for ref in new_obj_dict.get(to_name_m+'_refs', []):
                to_imid = self.fq_name_to_ifmap_id(to_name, ref['to'])
                new_set.add(to_imid)

            for inact_ref in old_set - new_set:
                delete_list.append((inact_ref, 'contrail:'+meta))

        if delete_list:
            self._delete_id_pair_meta_list(ifmap_id, delete_list)

        (ok, result) = self._ifmap_loadbalancer_healthmonitor_set(ifmap_id, existing_metas, new_obj_dict)
        return (ok, result)
    #end _ifmap_loadbalancer_healthmonitor_update

    def _ifmap_loadbalancer_healthmonitor_delete(self, obj_ids):
        ifmap_id = obj_ids['imid']
        parent_imid = obj_ids.get('parent_imid', None)
        existing_metas = self._ifmap_loadbalancer_healthmonitor_read_to_meta_index(ifmap_id)
        meta_list = []
        for meta_name, meta_infos in existing_metas.items():
            for meta_info in meta_infos:
                ref_imid = meta_info.get('id')
                if ref_imid is None:
                    continue
                meta_list.append((ref_imid, 'contrail:'+meta_name))

        if parent_imid:
            # Remove link from parent
            meta_list.append((parent_imid, None))

        if meta_list:
            self._delete_id_pair_meta_list(ifmap_id, meta_list)

        # Remove all property metadata associated with this ident
        self._delete_id_self_meta(ifmap_id, None)

        return (True, '')
    #end _ifmap_loadbalancer_healthmonitor_delete

    def _ifmap_virtual_network_alloc(self, parent_type, fq_name):
        imid = self._imid_handler
        (my_imid, parent_imid) = \
            imid.virtual_network_alloc_ifmap_id(parent_type, fq_name)
        if my_imid is None or parent_imid is None:
            return (False, (my_imid, parent_imid))
        return (True, (my_imid, parent_imid))
    #end _ifmap_virtual_network_alloc

    def _ifmap_virtual_network_set(self, my_imid, existing_metas, obj_dict):
        # Properties Meta
        update = {}
        field = obj_dict.get('virtual_network_properties', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['virtual_network_properties']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                VirtualNetworkType(**field).exportChildren(buf, level = 1, name_ = 'virtual-network-properties', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'virtual-network-properties', pretty_print = False)
            virtual_network_properties_xml = buf.getvalue()
            buf.close()
            meta = Metadata('virtual-network-properties' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = virtual_network_properties_xml)

            if (existing_metas and 'virtual-network-properties' in existing_metas and
                str(existing_metas['virtual-network-properties'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('virtual_network_network_id', None)
        if field is not None:
            norm_str = escape(str(obj_dict['virtual_network_network_id']))
            meta = Metadata('virtual-network-network-id', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'virtual-network-network-id' in existing_metas and
                str(existing_metas['virtual-network-network-id'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('route_target_list', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['route_target_list']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                RouteTargetList(**field).exportChildren(buf, level = 1, name_ = 'route-target-list', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'route-target-list', pretty_print = False)
            route_target_list_xml = buf.getvalue()
            buf.close()
            meta = Metadata('route-target-list' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = route_target_list_xml)

            if (existing_metas and 'route-target-list' in existing_metas and
                str(existing_metas['route-target-list'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('router_external', None)
        if field is not None:
            norm_str = escape(str(obj_dict['router_external']))
            meta = Metadata('router-external', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'router-external' in existing_metas and
                str(existing_metas['router-external'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('is_shared', None)
        if field is not None:
            norm_str = escape(str(obj_dict['is_shared']))
            meta = Metadata('is-shared', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'is-shared' in existing_metas and
                str(existing_metas['is-shared'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('external_ipam', None)
        if field is not None:
            norm_str = escape(str(obj_dict['external_ipam']))
            meta = Metadata('external-ipam', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'external-ipam' in existing_metas and
                str(existing_metas['external-ipam'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('flood_unknown_unicast', None)
        if field is not None:
            norm_str = escape(str(obj_dict['flood_unknown_unicast']))
            meta = Metadata('flood-unknown-unicast', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'flood-unknown-unicast' in existing_metas and
                str(existing_metas['flood-unknown-unicast'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['id_perms']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                IdPermsType(**field).exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            id_perms_xml = buf.getvalue()
            buf.close()
            meta = Metadata('id-perms' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = id_perms_xml)

            if (existing_metas and 'id-perms' in existing_metas and
                str(existing_metas['id-perms'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('display_name', None)
        if field is not None:
            norm_str = escape(str(obj_dict['display_name']))
            meta = Metadata('display-name', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'display-name' in existing_metas and
                str(existing_metas['display-name'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        # Ref Link Metas
        imid = self._imid_handler

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('qos_forwarding_class_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'qos-forwarding-class'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                virtual_network_qos_forwarding_class_xml = ''
                meta = Metadata('virtual-network-qos-forwarding-class' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = virtual_network_qos_forwarding_class_xml)
                self._update_id_pair_meta(update, to_imid, meta)

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('network_ipam_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'network-ipam'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                virtual_network_network_ipam_xml = ''
                ref_data = ref['attr']
                if ref_data:
                    buf = cStringIO.StringIO()
                    VnSubnetsType(**ref_data).exportChildren(buf, level = 1, name_ = 'virtual-network-network-ipam', pretty_print = False)
                    virtual_network_network_ipam_xml = virtual_network_network_ipam_xml + buf.getvalue()
                    buf.close()
                meta = Metadata('virtual-network-network-ipam' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = virtual_network_network_ipam_xml)
                self._update_id_pair_meta(update, to_imid, meta)

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('network_policy_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'network-policy'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                virtual_network_network_policy_xml = ''
                ref_data = ref['attr']
                if ref_data:
                    buf = cStringIO.StringIO()
                    VirtualNetworkPolicyType(**ref_data).exportChildren(buf, level = 1, name_ = 'virtual-network-network-policy', pretty_print = False)
                    virtual_network_network_policy_xml = virtual_network_network_policy_xml + buf.getvalue()
                    buf.close()
                meta = Metadata('virtual-network-network-policy' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = virtual_network_network_policy_xml)
                self._update_id_pair_meta(update, to_imid, meta)

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('access_control_list_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'access-control-list'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                virtual_network_access_control_list_xml = ''
                meta = Metadata('virtual-network-access-control-list' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = virtual_network_access_control_list_xml)
                self._update_id_pair_meta(update, to_imid, meta)

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('floating_ip_pool_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'floating-ip-pool'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                virtual_network_floating_ip_pool_xml = ''
                meta = Metadata('virtual-network-floating-ip-pool' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = virtual_network_floating_ip_pool_xml)
                self._update_id_pair_meta(update, to_imid, meta)

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('routing_instance_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'routing-instance'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                virtual_network_routing_instance_xml = ''
                meta = Metadata('virtual-network-routing-instance' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = virtual_network_routing_instance_xml)
                self._update_id_pair_meta(update, to_imid, meta)

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('route_table_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'route-table'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                virtual_network_route_table_xml = ''
                meta = Metadata('virtual-network-route-table' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = virtual_network_route_table_xml)
                self._update_id_pair_meta(update, to_imid, meta)


        self._publish_update(my_imid, update)
        return (True, '')
    #end _ifmap_virtual_network_set

    def _ifmap_virtual_network_create(self, obj_ids, obj_dict):
        if not 'parent_type' in obj_dict:
            # parent is config-root
            parent_type = 'config-root'
            parent_imid = 'contrail:config-root:root'
        else:
            parent_type = obj_dict['parent_type']
            parent_imid = obj_ids.get('parent_imid', None)

        # Parent Link Meta
        update = {}
        parent_link_meta = self._parent_metas[parent_type]['virtual-network']
        meta = Metadata(parent_link_meta, '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')
        self._update_id_pair_meta(update, obj_ids['imid'], meta)
        self._publish_update(parent_imid, update)

        (ok, result) = self._ifmap_virtual_network_set(obj_ids['imid'], None, obj_dict)
        return (ok, result)
    #end _ifmap_virtual_network_create


    def _ifmap_virtual_network_read_to_meta_index(self, ifmap_id, field_names = None):
        # field_names = None means all fields will be read
        imid = self._imid_handler
        start_id = str(Identity(name = ifmap_id, type = 'other',
                        other_type = 'extended'))
        # if id-perms missing, identity doesn't exist

        all_metas = [u'virtual-network-qos-forwarding-class', u'virtual-network-network-ipam', u'virtual-network-network-policy', u'virtual-network-route-table', u'virtual-machine-interface-virtual-network', u'instance-ip-virtual-network', u'physical-router-virtual-network', u'logical-router-gateway', u'virtual-network-properties', u'virtual-network-network-id', u'route-target-list', u'router-external', u'is-shared', u'external-ipam', u'flood-unknown-unicast', u'id-perms', u'display-name', u'virtual-network-access-control-list', u'virtual-network-floating-ip-pool', u'virtual-network-routing-instance']
        if not field_names:
            metas_to_read = all_metas
        else: # read only requested fields
            metas_to_read = set(all_metas) & set(field_names.keys())

        # metas is a dict where key is meta-name and val is list of dict
        # of form [{'meta':meta}, {'id':id1, 'meta':meta}, {'id':id2, 'meta':meta}]
        metas = {}
        for meta_name in metas_to_read:
            if meta_name in self._id_to_metas[ifmap_id]:
                metas[meta_name] = self._id_to_metas[ifmap_id][meta_name]
        return metas
    #end _ifmap_virtual-network_read_to_meta_index

    def _ifmap_virtual_network_update(self, ifmap_id, new_obj_dict):
        # read in refs from ifmap to determine which ones become inactive after update
        existing_metas = self._ifmap_virtual_network_read_to_meta_index(ifmap_id)

        # remove properties that are no longer active
        props = ['virtual-network-properties', 'virtual-network-network-id', 'route-target-list', 'router-external', 'is-shared', 'external-ipam', 'flood-unknown-unicast', 'id-perms', 'display-name']
        for prop in props:
            prop_m = prop.replace('-', '_')
            if prop in existing_metas and prop_m not in new_obj_dict:
                self._delete_id_self_meta(ifmap_id, 'contrail:'+prop)
        # remove refs that are no longer active
        delete_list = []
        refs = {'virtual-network-qos-forwarding-class': 'qos-forwarding-class',
                'virtual-network-network-ipam': 'network-ipam',
                'virtual-network-network-policy': 'network-policy',
                'virtual-network-route-table': 'route-table'}
        for meta, to_name in refs.items():
            old_set = set([m['id'] for m in existing_metas.get(meta, [])])
            new_set = set()
            to_name_m = to_name.replace('-', '_')
            for ref in new_obj_dict.get(to_name_m+'_refs', []):
                to_imid = self.fq_name_to_ifmap_id(to_name, ref['to'])
                new_set.add(to_imid)

            for inact_ref in old_set - new_set:
                delete_list.append((inact_ref, 'contrail:'+meta))

        if delete_list:
            self._delete_id_pair_meta_list(ifmap_id, delete_list)

        (ok, result) = self._ifmap_virtual_network_set(ifmap_id, existing_metas, new_obj_dict)
        return (ok, result)
    #end _ifmap_virtual_network_update

    def _ifmap_virtual_network_delete(self, obj_ids):
        ifmap_id = obj_ids['imid']
        parent_imid = obj_ids.get('parent_imid', None)
        existing_metas = self._ifmap_virtual_network_read_to_meta_index(ifmap_id)
        meta_list = []
        for meta_name, meta_infos in existing_metas.items():
            for meta_info in meta_infos:
                ref_imid = meta_info.get('id')
                if ref_imid is None:
                    continue
                meta_list.append((ref_imid, 'contrail:'+meta_name))

        if parent_imid:
            # Remove link from parent
            meta_list.append((parent_imid, None))

        if meta_list:
            self._delete_id_pair_meta_list(ifmap_id, meta_list)

        # Remove all property metadata associated with this ident
        self._delete_id_self_meta(ifmap_id, None)

        return (True, '')
    #end _ifmap_virtual_network_delete

    def _ifmap_project_alloc(self, parent_type, fq_name):
        imid = self._imid_handler
        (my_imid, parent_imid) = \
            imid.project_alloc_ifmap_id(parent_type, fq_name)
        if my_imid is None or parent_imid is None:
            return (False, (my_imid, parent_imid))
        return (True, (my_imid, parent_imid))
    #end _ifmap_project_alloc

    def _ifmap_project_set(self, my_imid, existing_metas, obj_dict):
        # Properties Meta
        update = {}
        field = obj_dict.get('quota', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['quota']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                QuotaType(**field).exportChildren(buf, level = 1, name_ = 'quota', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'quota', pretty_print = False)
            quota_xml = buf.getvalue()
            buf.close()
            meta = Metadata('quota' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = quota_xml)

            if (existing_metas and 'quota' in existing_metas and
                str(existing_metas['quota'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['id_perms']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                IdPermsType(**field).exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            id_perms_xml = buf.getvalue()
            buf.close()
            meta = Metadata('id-perms' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = id_perms_xml)

            if (existing_metas and 'id-perms' in existing_metas and
                str(existing_metas['id-perms'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('display_name', None)
        if field is not None:
            norm_str = escape(str(obj_dict['display_name']))
            meta = Metadata('display-name', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'display-name' in existing_metas and
                str(existing_metas['display-name'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        # Ref Link Metas
        imid = self._imid_handler

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('namespace_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'namespace'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                project_namespace_xml = ''
                ref_data = ref['attr']
                if ref_data:
                    buf = cStringIO.StringIO()
                    SubnetType(**ref_data).exportChildren(buf, level = 1, name_ = 'project-namespace', pretty_print = False)
                    project_namespace_xml = project_namespace_xml + buf.getvalue()
                    buf.close()
                meta = Metadata('project-namespace' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = project_namespace_xml)
                self._update_id_pair_meta(update, to_imid, meta)

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('security_group_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'security-group'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                project_security_group_xml = ''
                meta = Metadata('project-security-group' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = project_security_group_xml)
                self._update_id_pair_meta(update, to_imid, meta)

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('virtual_network_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'virtual-network'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                project_virtual_network_xml = ''
                meta = Metadata('project-virtual-network' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = project_virtual_network_xml)
                self._update_id_pair_meta(update, to_imid, meta)

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('qos_queue_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'qos-queue'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                project_qos_queue_xml = ''
                meta = Metadata('project-qos-queue' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = project_qos_queue_xml)
                self._update_id_pair_meta(update, to_imid, meta)

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('qos_forwarding_class_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'qos-forwarding-class'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                project_qos_forwarding_class_xml = ''
                meta = Metadata('project-qos-forwarding-class' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = project_qos_forwarding_class_xml)
                self._update_id_pair_meta(update, to_imid, meta)

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('network_ipam_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'network-ipam'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                project_network_ipam_xml = ''
                meta = Metadata('project-network-ipam' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = project_network_ipam_xml)
                self._update_id_pair_meta(update, to_imid, meta)

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('network_policy_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'network-policy'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                project_network_policy_xml = ''
                meta = Metadata('project-network-policy' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = project_network_policy_xml)
                self._update_id_pair_meta(update, to_imid, meta)

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('virtual_machine_interface_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'virtual-machine-interface'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                project_virtual_machine_interface_xml = ''
                meta = Metadata('project-virtual-machine-interface' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = project_virtual_machine_interface_xml)
                self._update_id_pair_meta(update, to_imid, meta)

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('floating_ip_pool_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'floating-ip-pool'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                project_floating_ip_pool_xml = ''
                meta = Metadata('project-floating-ip-pool' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = project_floating_ip_pool_xml)
                self._update_id_pair_meta(update, to_imid, meta)

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('service_instance_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'service-instance'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                project_service_instance_xml = ''
                meta = Metadata('project-service-instance' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = project_service_instance_xml)
                self._update_id_pair_meta(update, to_imid, meta)

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('route_table_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'route-table'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                project_route_table_xml = ''
                meta = Metadata('project-route-table' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = project_route_table_xml)
                self._update_id_pair_meta(update, to_imid, meta)

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('interface_route_table_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'interface-route-table'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                project_interface_route_table_xml = ''
                meta = Metadata('project-interface-route-table' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = project_interface_route_table_xml)
                self._update_id_pair_meta(update, to_imid, meta)

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('logical_router_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'logical-router'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                project_logical_router_xml = ''
                meta = Metadata('project-logical-router' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = project_logical_router_xml)
                self._update_id_pair_meta(update, to_imid, meta)

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('loadbalancer_pool_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'loadbalancer-pool'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                project_loadbalancer_pool_xml = ''
                meta = Metadata('project-loadbalancer-pool' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = project_loadbalancer_pool_xml)
                self._update_id_pair_meta(update, to_imid, meta)

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('loadbalancer_healthmonitor_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'loadbalancer-healthmonitor'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                project_loadbalancer_healthmonitor_xml = ''
                meta = Metadata('project-loadbalancer-healthmonitor' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = project_loadbalancer_healthmonitor_xml)
                self._update_id_pair_meta(update, to_imid, meta)

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('virtual_ip_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'virtual-ip'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                project_virtual_ip_xml = ''
                meta = Metadata('project-virtual-ip' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = project_virtual_ip_xml)
                self._update_id_pair_meta(update, to_imid, meta)


        self._publish_update(my_imid, update)
        return (True, '')
    #end _ifmap_project_set

    def _ifmap_project_create(self, obj_ids, obj_dict):
        if not 'parent_type' in obj_dict:
            # parent is config-root
            parent_type = 'config-root'
            parent_imid = 'contrail:config-root:root'
        else:
            parent_type = obj_dict['parent_type']
            parent_imid = obj_ids.get('parent_imid', None)

        # Parent Link Meta
        update = {}
        parent_link_meta = self._parent_metas[parent_type]['project']
        meta = Metadata(parent_link_meta, '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')
        self._update_id_pair_meta(update, obj_ids['imid'], meta)
        self._publish_update(parent_imid, update)

        (ok, result) = self._ifmap_project_set(obj_ids['imid'], None, obj_dict)
        return (ok, result)
    #end _ifmap_project_create


    def _ifmap_project_read_to_meta_index(self, ifmap_id, field_names = None):
        # field_names = None means all fields will be read
        imid = self._imid_handler
        start_id = str(Identity(name = ifmap_id, type = 'other',
                        other_type = 'extended'))
        # if id-perms missing, identity doesn't exist

        all_metas = [u'project-namespace', u'project-floating-ip-pool', u'floating-ip-project', u'quota', u'id-perms', u'display-name', u'project-security-group', u'project-virtual-network', u'project-qos-queue', u'project-qos-forwarding-class', u'project-network-ipam', u'project-network-policy', u'project-virtual-machine-interface', u'project-service-instance', u'project-route-table', u'project-interface-route-table', u'project-logical-router', u'project-loadbalancer-pool', u'project-loadbalancer-healthmonitor', u'project-virtual-ip']
        if not field_names:
            metas_to_read = all_metas
        else: # read only requested fields
            metas_to_read = set(all_metas) & set(field_names.keys())

        # metas is a dict where key is meta-name and val is list of dict
        # of form [{'meta':meta}, {'id':id1, 'meta':meta}, {'id':id2, 'meta':meta}]
        metas = {}
        for meta_name in metas_to_read:
            if meta_name in self._id_to_metas[ifmap_id]:
                metas[meta_name] = self._id_to_metas[ifmap_id][meta_name]
        return metas
    #end _ifmap_project_read_to_meta_index

    def _ifmap_project_update(self, ifmap_id, new_obj_dict):
        # read in refs from ifmap to determine which ones become inactive after update
        existing_metas = self._ifmap_project_read_to_meta_index(ifmap_id)

        # remove properties that are no longer active
        props = ['quota', 'id-perms', 'display-name']
        for prop in props:
            prop_m = prop.replace('-', '_')
            if prop in existing_metas and prop_m not in new_obj_dict:
                self._delete_id_self_meta(ifmap_id, 'contrail:'+prop)
        # remove refs that are no longer active
        delete_list = []
        refs = {'project-namespace': 'namespace',
                'project-floating-ip-pool': 'floating-ip-pool'}
        for meta, to_name in refs.items():
            old_set = set([m['id'] for m in existing_metas.get(meta, [])])
            new_set = set()
            to_name_m = to_name.replace('-', '_')
            for ref in new_obj_dict.get(to_name_m+'_refs', []):
                to_imid = self.fq_name_to_ifmap_id(to_name, ref['to'])
                new_set.add(to_imid)

            for inact_ref in old_set - new_set:
                delete_list.append((inact_ref, 'contrail:'+meta))

        if delete_list:
            self._delete_id_pair_meta_list(ifmap_id, delete_list)

        (ok, result) = self._ifmap_project_set(ifmap_id, existing_metas, new_obj_dict)
        return (ok, result)
    #end _ifmap_project_update

    def _ifmap_project_delete(self, obj_ids):
        ifmap_id = obj_ids['imid']
        parent_imid = obj_ids.get('parent_imid', None)
        existing_metas = self._ifmap_project_read_to_meta_index(ifmap_id)
        meta_list = []
        for meta_name, meta_infos in existing_metas.items():
            for meta_info in meta_infos:
                ref_imid = meta_info.get('id')
                if ref_imid is None:
                    continue
                meta_list.append((ref_imid, 'contrail:'+meta_name))

        if parent_imid:
            # Remove link from parent
            meta_list.append((parent_imid, None))

        if meta_list:
            self._delete_id_pair_meta_list(ifmap_id, meta_list)

        # Remove all property metadata associated with this ident
        self._delete_id_self_meta(ifmap_id, None)

        return (True, '')
    #end _ifmap_project_delete

    def _ifmap_qos_forwarding_class_alloc(self, parent_type, fq_name):
        imid = self._imid_handler
        (my_imid, parent_imid) = \
            imid.qos_forwarding_class_alloc_ifmap_id(parent_type, fq_name)
        if my_imid is None or parent_imid is None:
            return (False, (my_imid, parent_imid))
        return (True, (my_imid, parent_imid))
    #end _ifmap_qos_forwarding_class_alloc

    def _ifmap_qos_forwarding_class_set(self, my_imid, existing_metas, obj_dict):
        # Properties Meta
        update = {}
        field = obj_dict.get('dscp', None)
        if field is not None:
            norm_str = escape(str(obj_dict['dscp']))
            meta = Metadata('dscp', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'dscp' in existing_metas and
                str(existing_metas['dscp'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('trusted', None)
        if field is not None:
            norm_str = escape(str(obj_dict['trusted']))
            meta = Metadata('trusted', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'trusted' in existing_metas and
                str(existing_metas['trusted'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['id_perms']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                IdPermsType(**field).exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            id_perms_xml = buf.getvalue()
            buf.close()
            meta = Metadata('id-perms' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = id_perms_xml)

            if (existing_metas and 'id-perms' in existing_metas and
                str(existing_metas['id-perms'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('display_name', None)
        if field is not None:
            norm_str = escape(str(obj_dict['display_name']))
            meta = Metadata('display-name', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'display-name' in existing_metas and
                str(existing_metas['display-name'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        # Ref Link Metas
        imid = self._imid_handler

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('qos_queue_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'qos-queue'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                qos_forwarding_class_qos_queue_xml = ''
                meta = Metadata('qos-forwarding-class-qos-queue' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = qos_forwarding_class_qos_queue_xml)
                self._update_id_pair_meta(update, to_imid, meta)


        self._publish_update(my_imid, update)
        return (True, '')
    #end _ifmap_qos_forwarding_class_set

    def _ifmap_qos_forwarding_class_create(self, obj_ids, obj_dict):
        if not 'parent_type' in obj_dict:
            # parent is config-root
            parent_type = 'config-root'
            parent_imid = 'contrail:config-root:root'
        else:
            parent_type = obj_dict['parent_type']
            parent_imid = obj_ids.get('parent_imid', None)

        # Parent Link Meta
        update = {}
        parent_link_meta = self._parent_metas[parent_type]['qos-forwarding-class']
        meta = Metadata(parent_link_meta, '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')
        self._update_id_pair_meta(update, obj_ids['imid'], meta)
        self._publish_update(parent_imid, update)

        (ok, result) = self._ifmap_qos_forwarding_class_set(obj_ids['imid'], None, obj_dict)
        return (ok, result)
    #end _ifmap_qos_forwarding_class_create


    def _ifmap_qos_forwarding_class_read_to_meta_index(self, ifmap_id, field_names = None):
        # field_names = None means all fields will be read
        imid = self._imid_handler
        start_id = str(Identity(name = ifmap_id, type = 'other',
                        other_type = 'extended'))
        # if id-perms missing, identity doesn't exist

        all_metas = [u'qos-forwarding-class-qos-queue', u'virtual-network-qos-forwarding-class', u'virtual-machine-interface-qos-forwarding-class', u'dscp', u'trusted', u'id-perms', u'display-name']
        if not field_names:
            metas_to_read = all_metas
        else: # read only requested fields
            metas_to_read = set(all_metas) & set(field_names.keys())

        # metas is a dict where key is meta-name and val is list of dict
        # of form [{'meta':meta}, {'id':id1, 'meta':meta}, {'id':id2, 'meta':meta}]
        metas = {}
        for meta_name in metas_to_read:
            if meta_name in self._id_to_metas[ifmap_id]:
                metas[meta_name] = self._id_to_metas[ifmap_id][meta_name]
        return metas
    #end _ifmap_qos-forwarding-class_read_to_meta_index

    def _ifmap_qos_forwarding_class_update(self, ifmap_id, new_obj_dict):
        # read in refs from ifmap to determine which ones become inactive after update
        existing_metas = self._ifmap_qos_forwarding_class_read_to_meta_index(ifmap_id)

        # remove properties that are no longer active
        props = ['dscp', 'trusted', 'id-perms', 'display-name']
        for prop in props:
            prop_m = prop.replace('-', '_')
            if prop in existing_metas and prop_m not in new_obj_dict:
                self._delete_id_self_meta(ifmap_id, 'contrail:'+prop)
        # remove refs that are no longer active
        delete_list = []
        refs = {'qos-forwarding-class-qos-queue': 'qos-queue'}
        for meta, to_name in refs.items():
            old_set = set([m['id'] for m in existing_metas.get(meta, [])])
            new_set = set()
            to_name_m = to_name.replace('-', '_')
            for ref in new_obj_dict.get(to_name_m+'_refs', []):
                to_imid = self.fq_name_to_ifmap_id(to_name, ref['to'])
                new_set.add(to_imid)

            for inact_ref in old_set - new_set:
                delete_list.append((inact_ref, 'contrail:'+meta))

        if delete_list:
            self._delete_id_pair_meta_list(ifmap_id, delete_list)

        (ok, result) = self._ifmap_qos_forwarding_class_set(ifmap_id, existing_metas, new_obj_dict)
        return (ok, result)
    #end _ifmap_qos_forwarding_class_update

    def _ifmap_qos_forwarding_class_delete(self, obj_ids):
        ifmap_id = obj_ids['imid']
        parent_imid = obj_ids.get('parent_imid', None)
        existing_metas = self._ifmap_qos_forwarding_class_read_to_meta_index(ifmap_id)
        meta_list = []
        for meta_name, meta_infos in existing_metas.items():
            for meta_info in meta_infos:
                ref_imid = meta_info.get('id')
                if ref_imid is None:
                    continue
                meta_list.append((ref_imid, 'contrail:'+meta_name))

        if parent_imid:
            # Remove link from parent
            meta_list.append((parent_imid, None))

        if meta_list:
            self._delete_id_pair_meta_list(ifmap_id, meta_list)

        # Remove all property metadata associated with this ident
        self._delete_id_self_meta(ifmap_id, None)

        return (True, '')
    #end _ifmap_qos_forwarding_class_delete

    def _ifmap_database_node_alloc(self, parent_type, fq_name):
        imid = self._imid_handler
        (my_imid, parent_imid) = \
            imid.database_node_alloc_ifmap_id(parent_type, fq_name)
        if my_imid is None or parent_imid is None:
            return (False, (my_imid, parent_imid))
        return (True, (my_imid, parent_imid))
    #end _ifmap_database_node_alloc

    def _ifmap_database_node_set(self, my_imid, existing_metas, obj_dict):
        # Properties Meta
        update = {}
        field = obj_dict.get('database_node_ip_address', None)
        if field is not None:
            norm_str = escape(str(obj_dict['database_node_ip_address']))
            meta = Metadata('database-node-ip-address', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'database-node-ip-address' in existing_metas and
                str(existing_metas['database-node-ip-address'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['id_perms']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                IdPermsType(**field).exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            id_perms_xml = buf.getvalue()
            buf.close()
            meta = Metadata('id-perms' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = id_perms_xml)

            if (existing_metas and 'id-perms' in existing_metas and
                str(existing_metas['id-perms'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('display_name', None)
        if field is not None:
            norm_str = escape(str(obj_dict['display_name']))
            meta = Metadata('display-name', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'display-name' in existing_metas and
                str(existing_metas['display-name'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        # Ref Link Metas
        imid = self._imid_handler


        self._publish_update(my_imid, update)
        return (True, '')
    #end _ifmap_database_node_set

    def _ifmap_database_node_create(self, obj_ids, obj_dict):
        if not 'parent_type' in obj_dict:
            # parent is config-root
            parent_type = 'config-root'
            parent_imid = 'contrail:config-root:root'
        else:
            parent_type = obj_dict['parent_type']
            parent_imid = obj_ids.get('parent_imid', None)

        # Parent Link Meta
        update = {}
        parent_link_meta = self._parent_metas[parent_type]['database-node']
        meta = Metadata(parent_link_meta, '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')
        self._update_id_pair_meta(update, obj_ids['imid'], meta)
        self._publish_update(parent_imid, update)

        (ok, result) = self._ifmap_database_node_set(obj_ids['imid'], None, obj_dict)
        return (ok, result)
    #end _ifmap_database_node_create


    def _ifmap_database_node_read_to_meta_index(self, ifmap_id, field_names = None):
        # field_names = None means all fields will be read
        imid = self._imid_handler
        start_id = str(Identity(name = ifmap_id, type = 'other',
                        other_type = 'extended'))
        # if id-perms missing, identity doesn't exist

        all_metas = [u'database-node-ip-address', u'id-perms', u'display-name']
        if not field_names:
            metas_to_read = all_metas
        else: # read only requested fields
            metas_to_read = set(all_metas) & set(field_names.keys())

        # metas is a dict where key is meta-name and val is list of dict
        # of form [{'meta':meta}, {'id':id1, 'meta':meta}, {'id':id2, 'meta':meta}]
        metas = {}
        for meta_name in metas_to_read:
            if meta_name in self._id_to_metas[ifmap_id]:
                metas[meta_name] = self._id_to_metas[ifmap_id][meta_name]
        return metas
    #end _ifmap_database-node_read_to_meta_index

    def _ifmap_database_node_update(self, ifmap_id, new_obj_dict):
        # read in refs from ifmap to determine which ones become inactive after update
        existing_metas = self._ifmap_database_node_read_to_meta_index(ifmap_id)

        # remove properties that are no longer active
        props = ['database-node-ip-address', 'id-perms', 'display-name']
        for prop in props:
            prop_m = prop.replace('-', '_')
            if prop in existing_metas and prop_m not in new_obj_dict:
                self._delete_id_self_meta(ifmap_id, 'contrail:'+prop)
        # remove refs that are no longer active
        delete_list = []
        refs = {}
        for meta, to_name in refs.items():
            old_set = set([m['id'] for m in existing_metas.get(meta, [])])
            new_set = set()
            to_name_m = to_name.replace('-', '_')
            for ref in new_obj_dict.get(to_name_m+'_refs', []):
                to_imid = self.fq_name_to_ifmap_id(to_name, ref['to'])
                new_set.add(to_imid)

            for inact_ref in old_set - new_set:
                delete_list.append((inact_ref, 'contrail:'+meta))

        if delete_list:
            self._delete_id_pair_meta_list(ifmap_id, delete_list)

        (ok, result) = self._ifmap_database_node_set(ifmap_id, existing_metas, new_obj_dict)
        return (ok, result)
    #end _ifmap_database_node_update

    def _ifmap_database_node_delete(self, obj_ids):
        ifmap_id = obj_ids['imid']
        parent_imid = obj_ids.get('parent_imid', None)
        existing_metas = self._ifmap_database_node_read_to_meta_index(ifmap_id)
        meta_list = []
        for meta_name, meta_infos in existing_metas.items():
            for meta_info in meta_infos:
                ref_imid = meta_info.get('id')
                if ref_imid is None:
                    continue
                meta_list.append((ref_imid, 'contrail:'+meta_name))

        if parent_imid:
            # Remove link from parent
            meta_list.append((parent_imid, None))

        if meta_list:
            self._delete_id_pair_meta_list(ifmap_id, meta_list)

        # Remove all property metadata associated with this ident
        self._delete_id_self_meta(ifmap_id, None)

        return (True, '')
    #end _ifmap_database_node_delete

    def _ifmap_routing_instance_alloc(self, parent_type, fq_name):
        imid = self._imid_handler
        (my_imid, parent_imid) = \
            imid.routing_instance_alloc_ifmap_id(parent_type, fq_name)
        if my_imid is None or parent_imid is None:
            return (False, (my_imid, parent_imid))
        return (True, (my_imid, parent_imid))
    #end _ifmap_routing_instance_alloc

    def _ifmap_routing_instance_set(self, my_imid, existing_metas, obj_dict):
        # Properties Meta
        update = {}
        field = obj_dict.get('service_chain_information', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['service_chain_information']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                ServiceChainInfo(**field).exportChildren(buf, level = 1, name_ = 'service-chain-information', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'service-chain-information', pretty_print = False)
            service_chain_information_xml = buf.getvalue()
            buf.close()
            meta = Metadata('service-chain-information' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = service_chain_information_xml)

            if (existing_metas and 'service-chain-information' in existing_metas and
                str(existing_metas['service-chain-information'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('routing_instance_is_default', None)
        if field is not None:
            norm_str = escape(str(obj_dict['routing_instance_is_default']))
            meta = Metadata('routing-instance-is-default', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'routing-instance-is-default' in existing_metas and
                str(existing_metas['routing-instance-is-default'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('static_route_entries', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['static_route_entries']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                StaticRouteEntriesType(**field).exportChildren(buf, level = 1, name_ = 'static-route-entries', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'static-route-entries', pretty_print = False)
            static_route_entries_xml = buf.getvalue()
            buf.close()
            meta = Metadata('static-route-entries' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = static_route_entries_xml)

            if (existing_metas and 'static-route-entries' in existing_metas and
                str(existing_metas['static-route-entries'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('default_ce_protocol', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['default_ce_protocol']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                DefaultProtocolType(**field).exportChildren(buf, level = 1, name_ = 'default-ce-protocol', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'default-ce-protocol', pretty_print = False)
            default_ce_protocol_xml = buf.getvalue()
            buf.close()
            meta = Metadata('default-ce-protocol' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = default_ce_protocol_xml)

            if (existing_metas and 'default-ce-protocol' in existing_metas and
                str(existing_metas['default-ce-protocol'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['id_perms']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                IdPermsType(**field).exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            id_perms_xml = buf.getvalue()
            buf.close()
            meta = Metadata('id-perms' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = id_perms_xml)

            if (existing_metas and 'id-perms' in existing_metas and
                str(existing_metas['id-perms'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('display_name', None)
        if field is not None:
            norm_str = escape(str(obj_dict['display_name']))
            meta = Metadata('display-name', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'display-name' in existing_metas and
                str(existing_metas['display-name'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        # Ref Link Metas
        imid = self._imid_handler

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('bgp_router_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'bgp-router'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                instance_bgp_router_xml = ''
                meta = Metadata('instance-bgp-router' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = instance_bgp_router_xml)
                self._update_id_pair_meta(update, to_imid, meta)

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('routing_instance_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'routing-instance'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                connection_xml = ''
                ref_data = ref['attr']
                if ref_data:
                    buf = cStringIO.StringIO()
                    ConnectionType(**ref_data).exportChildren(buf, level = 1, name_ = 'connection', pretty_print = False)
                    connection_xml = connection_xml + buf.getvalue()
                    buf.close()
                meta = Metadata('connection' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = connection_xml)
                self._update_id_pair_meta(update, to_imid, meta)

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('route_target_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'route-target'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                instance_target_xml = ''
                ref_data = ref['attr']
                if ref_data:
                    buf = cStringIO.StringIO()
                    InstanceTargetType(**ref_data).exportChildren(buf, level = 1, name_ = 'instance-target', pretty_print = False)
                    instance_target_xml = instance_target_xml + buf.getvalue()
                    buf.close()
                meta = Metadata('instance-target' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = instance_target_xml)
                self._update_id_pair_meta(update, to_imid, meta)


        self._publish_update(my_imid, update)
        return (True, '')
    #end _ifmap_routing_instance_set

    def _ifmap_routing_instance_create(self, obj_ids, obj_dict):
        if not 'parent_type' in obj_dict:
            # parent is config-root
            parent_type = 'config-root'
            parent_imid = 'contrail:config-root:root'
        else:
            parent_type = obj_dict['parent_type']
            parent_imid = obj_ids.get('parent_imid', None)

        # Parent Link Meta
        update = {}
        parent_link_meta = self._parent_metas[parent_type]['routing-instance']
        meta = Metadata(parent_link_meta, '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')
        self._update_id_pair_meta(update, obj_ids['imid'], meta)
        self._publish_update(parent_imid, update)

        (ok, result) = self._ifmap_routing_instance_set(obj_ids['imid'], None, obj_dict)
        return (ok, result)
    #end _ifmap_routing_instance_create


    def _ifmap_routing_instance_read_to_meta_index(self, ifmap_id, field_names = None):
        # field_names = None means all fields will be read
        imid = self._imid_handler
        start_id = str(Identity(name = ifmap_id, type = 'other',
                        other_type = 'extended'))
        # if id-perms missing, identity doesn't exist

        all_metas = [u'connection', u'instance-target', u'virtual-machine-interface-routing-instance', u'connection', u'service-chain-information', u'routing-instance-is-default', u'static-route-entries', u'default-ce-protocol', u'id-perms', u'display-name', u'instance-bgp-router']
        if not field_names:
            metas_to_read = all_metas
        else: # read only requested fields
            metas_to_read = set(all_metas) & set(field_names.keys())

        # metas is a dict where key is meta-name and val is list of dict
        # of form [{'meta':meta}, {'id':id1, 'meta':meta}, {'id':id2, 'meta':meta}]
        metas = {}
        for meta_name in metas_to_read:
            if meta_name in self._id_to_metas[ifmap_id]:
                metas[meta_name] = self._id_to_metas[ifmap_id][meta_name]
        return metas
    #end _ifmap_routing-instance_read_to_meta_index

    def _ifmap_routing_instance_update(self, ifmap_id, new_obj_dict):
        # read in refs from ifmap to determine which ones become inactive after update
        existing_metas = self._ifmap_routing_instance_read_to_meta_index(ifmap_id)

        # remove properties that are no longer active
        props = ['service-chain-information', 'routing-instance-is-default', 'static-route-entries', 'default-ce-protocol', 'id-perms', 'display-name']
        for prop in props:
            prop_m = prop.replace('-', '_')
            if prop in existing_metas and prop_m not in new_obj_dict:
                self._delete_id_self_meta(ifmap_id, 'contrail:'+prop)
        # remove refs that are no longer active
        delete_list = []
        refs = {'connection': 'routing-instance',
                'instance-target': 'route-target'}
        for meta, to_name in refs.items():
            old_set = set([m['id'] for m in existing_metas.get(meta, [])])
            new_set = set()
            to_name_m = to_name.replace('-', '_')
            for ref in new_obj_dict.get(to_name_m+'_refs', []):
                to_imid = self.fq_name_to_ifmap_id(to_name, ref['to'])
                new_set.add(to_imid)

            for inact_ref in old_set - new_set:
                delete_list.append((inact_ref, 'contrail:'+meta))

        if delete_list:
            self._delete_id_pair_meta_list(ifmap_id, delete_list)

        (ok, result) = self._ifmap_routing_instance_set(ifmap_id, existing_metas, new_obj_dict)
        return (ok, result)
    #end _ifmap_routing_instance_update

    def _ifmap_routing_instance_delete(self, obj_ids):
        ifmap_id = obj_ids['imid']
        parent_imid = obj_ids.get('parent_imid', None)
        existing_metas = self._ifmap_routing_instance_read_to_meta_index(ifmap_id)
        meta_list = []
        for meta_name, meta_infos in existing_metas.items():
            for meta_info in meta_infos:
                ref_imid = meta_info.get('id')
                if ref_imid is None:
                    continue
                meta_list.append((ref_imid, 'contrail:'+meta_name))

        if parent_imid:
            # Remove link from parent
            meta_list.append((parent_imid, None))

        if meta_list:
            self._delete_id_pair_meta_list(ifmap_id, meta_list)

        # Remove all property metadata associated with this ident
        self._delete_id_self_meta(ifmap_id, None)

        return (True, '')
    #end _ifmap_routing_instance_delete

    def _ifmap_network_ipam_alloc(self, parent_type, fq_name):
        imid = self._imid_handler
        (my_imid, parent_imid) = \
            imid.network_ipam_alloc_ifmap_id(parent_type, fq_name)
        if my_imid is None or parent_imid is None:
            return (False, (my_imid, parent_imid))
        return (True, (my_imid, parent_imid))
    #end _ifmap_network_ipam_alloc

    def _ifmap_network_ipam_set(self, my_imid, existing_metas, obj_dict):
        # Properties Meta
        update = {}
        field = obj_dict.get('network_ipam_mgmt', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['network_ipam_mgmt']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                IpamType(**field).exportChildren(buf, level = 1, name_ = 'network-ipam-mgmt', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'network-ipam-mgmt', pretty_print = False)
            network_ipam_mgmt_xml = buf.getvalue()
            buf.close()
            meta = Metadata('network-ipam-mgmt' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = network_ipam_mgmt_xml)

            if (existing_metas and 'network-ipam-mgmt' in existing_metas and
                str(existing_metas['network-ipam-mgmt'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['id_perms']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                IdPermsType(**field).exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            id_perms_xml = buf.getvalue()
            buf.close()
            meta = Metadata('id-perms' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = id_perms_xml)

            if (existing_metas and 'id-perms' in existing_metas and
                str(existing_metas['id-perms'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('display_name', None)
        if field is not None:
            norm_str = escape(str(obj_dict['display_name']))
            meta = Metadata('display-name', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'display-name' in existing_metas and
                str(existing_metas['display-name'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        # Ref Link Metas
        imid = self._imid_handler

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('virtual_DNS_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'virtual-DNS'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                network_ipam_virtual_DNS_xml = ''
                meta = Metadata('network-ipam-virtual-DNS' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = network_ipam_virtual_DNS_xml)
                self._update_id_pair_meta(update, to_imid, meta)


        self._publish_update(my_imid, update)
        return (True, '')
    #end _ifmap_network_ipam_set

    def _ifmap_network_ipam_create(self, obj_ids, obj_dict):
        if not 'parent_type' in obj_dict:
            # parent is config-root
            parent_type = 'config-root'
            parent_imid = 'contrail:config-root:root'
        else:
            parent_type = obj_dict['parent_type']
            parent_imid = obj_ids.get('parent_imid', None)

        # Parent Link Meta
        update = {}
        parent_link_meta = self._parent_metas[parent_type]['network-ipam']
        meta = Metadata(parent_link_meta, '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')
        self._update_id_pair_meta(update, obj_ids['imid'], meta)
        self._publish_update(parent_imid, update)

        (ok, result) = self._ifmap_network_ipam_set(obj_ids['imid'], None, obj_dict)
        return (ok, result)
    #end _ifmap_network_ipam_create


    def _ifmap_network_ipam_read_to_meta_index(self, ifmap_id, field_names = None):
        # field_names = None means all fields will be read
        imid = self._imid_handler
        start_id = str(Identity(name = ifmap_id, type = 'other',
                        other_type = 'extended'))
        # if id-perms missing, identity doesn't exist

        all_metas = [u'network-ipam-virtual-DNS', u'virtual-network-network-ipam', u'network-ipam-mgmt', u'id-perms', u'display-name']
        if not field_names:
            metas_to_read = all_metas
        else: # read only requested fields
            metas_to_read = set(all_metas) & set(field_names.keys())

        # metas is a dict where key is meta-name and val is list of dict
        # of form [{'meta':meta}, {'id':id1, 'meta':meta}, {'id':id2, 'meta':meta}]
        metas = {}
        for meta_name in metas_to_read:
            if meta_name in self._id_to_metas[ifmap_id]:
                metas[meta_name] = self._id_to_metas[ifmap_id][meta_name]
        return metas
    #end _ifmap_network-ipam_read_to_meta_index

    def _ifmap_network_ipam_update(self, ifmap_id, new_obj_dict):
        # read in refs from ifmap to determine which ones become inactive after update
        existing_metas = self._ifmap_network_ipam_read_to_meta_index(ifmap_id)

        # remove properties that are no longer active
        props = ['network-ipam-mgmt', 'id-perms', 'display-name']
        for prop in props:
            prop_m = prop.replace('-', '_')
            if prop in existing_metas and prop_m not in new_obj_dict:
                self._delete_id_self_meta(ifmap_id, 'contrail:'+prop)
        # remove refs that are no longer active
        delete_list = []
        refs = {'network-ipam-virtual-DNS': 'virtual-DNS'}
        for meta, to_name in refs.items():
            old_set = set([m['id'] for m in existing_metas.get(meta, [])])
            new_set = set()
            to_name_m = to_name.replace('-', '_')
            for ref in new_obj_dict.get(to_name_m+'_refs', []):
                to_imid = self.fq_name_to_ifmap_id(to_name, ref['to'])
                new_set.add(to_imid)

            for inact_ref in old_set - new_set:
                delete_list.append((inact_ref, 'contrail:'+meta))

        if delete_list:
            self._delete_id_pair_meta_list(ifmap_id, delete_list)

        (ok, result) = self._ifmap_network_ipam_set(ifmap_id, existing_metas, new_obj_dict)
        return (ok, result)
    #end _ifmap_network_ipam_update

    def _ifmap_network_ipam_delete(self, obj_ids):
        ifmap_id = obj_ids['imid']
        parent_imid = obj_ids.get('parent_imid', None)
        existing_metas = self._ifmap_network_ipam_read_to_meta_index(ifmap_id)
        meta_list = []
        for meta_name, meta_infos in existing_metas.items():
            for meta_info in meta_infos:
                ref_imid = meta_info.get('id')
                if ref_imid is None:
                    continue
                meta_list.append((ref_imid, 'contrail:'+meta_name))

        if parent_imid:
            # Remove link from parent
            meta_list.append((parent_imid, None))

        if meta_list:
            self._delete_id_pair_meta_list(ifmap_id, meta_list)

        # Remove all property metadata associated with this ident
        self._delete_id_self_meta(ifmap_id, None)

        return (True, '')
    #end _ifmap_network_ipam_delete

    def _ifmap_logical_router_alloc(self, parent_type, fq_name):
        imid = self._imid_handler
        (my_imid, parent_imid) = \
            imid.logical_router_alloc_ifmap_id(parent_type, fq_name)
        if my_imid is None or parent_imid is None:
            return (False, (my_imid, parent_imid))
        return (True, (my_imid, parent_imid))
    #end _ifmap_logical_router_alloc

    def _ifmap_logical_router_set(self, my_imid, existing_metas, obj_dict):
        # Properties Meta
        update = {}
        field = obj_dict.get('id_perms', None)
        if field is not None:
            # construct object of xsd-type and get its xml repr
            field = obj_dict['id_perms']
            buf = cStringIO.StringIO()
            # perms might be inserted at server as obj.
            # obj construction diff from dict construction.
            if isinstance(field, dict):
                IdPermsType(**field).exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            else: # object
                field.exportChildren(buf, level = 1, name_ = 'id-perms', pretty_print = False)
            id_perms_xml = buf.getvalue()
            buf.close()
            meta = Metadata('id-perms' , '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                   elements = id_perms_xml)

            if (existing_metas and 'id-perms' in existing_metas and
                str(existing_metas['id-perms'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        field = obj_dict.get('display_name', None)
        if field is not None:
            norm_str = escape(str(obj_dict['display_name']))
            meta = Metadata('display-name', norm_str,
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')

            if (existing_metas and 'display-name' in existing_metas and
                str(existing_metas['display-name'][0]['meta']) == str(meta)):
                # no change
                pass
            else:
                self._update_id_self_meta(update, meta)

        # Ref Link Metas
        imid = self._imid_handler

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('virtual_machine_interface_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'virtual-machine-interface'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                logical_router_interface_xml = ''
                meta = Metadata('logical-router-interface' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = logical_router_interface_xml)
                self._update_id_pair_meta(update, to_imid, meta)

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('route_target_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'route-target'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                logical_router_target_xml = ''
                meta = Metadata('logical-router-target' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = logical_router_target_xml)
                self._update_id_pair_meta(update, to_imid, meta)

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('virtual_network_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'virtual-network'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                logical_router_gateway_xml = ''
                meta = Metadata('logical-router-gateway' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = logical_router_gateway_xml)
                self._update_id_pair_meta(update, to_imid, meta)

        # construct object of xsd-type and get its xml repr
        refs = obj_dict.get('service_instance_refs', None)
        if refs:
            for ref in refs:
                ref_fq_name = ref['to']
                obj_type = 'service-instance'
                to_imid = cfgm_common.imid.get_ifmap_id_from_fq_name(obj_type, ref_fq_name)
                logical_router_service_instance_xml = ''
                meta = Metadata('logical-router-service-instance' , '',
                       {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail',
                       elements = logical_router_service_instance_xml)
                self._update_id_pair_meta(update, to_imid, meta)


        self._publish_update(my_imid, update)
        return (True, '')
    #end _ifmap_logical_router_set

    def _ifmap_logical_router_create(self, obj_ids, obj_dict):
        if not 'parent_type' in obj_dict:
            # parent is config-root
            parent_type = 'config-root'
            parent_imid = 'contrail:config-root:root'
        else:
            parent_type = obj_dict['parent_type']
            parent_imid = obj_ids.get('parent_imid', None)

        # Parent Link Meta
        update = {}
        parent_link_meta = self._parent_metas[parent_type]['logical-router']
        meta = Metadata(parent_link_meta, '',
                   {'ifmap-cardinality':'singleValue'}, ns_prefix = 'contrail')
        self._update_id_pair_meta(update, obj_ids['imid'], meta)
        self._publish_update(parent_imid, update)

        (ok, result) = self._ifmap_logical_router_set(obj_ids['imid'], None, obj_dict)
        return (ok, result)
    #end _ifmap_logical_router_create


    def _ifmap_logical_router_read_to_meta_index(self, ifmap_id, field_names = None):
        # field_names = None means all fields will be read
        imid = self._imid_handler
        start_id = str(Identity(name = ifmap_id, type = 'other',
                        other_type = 'extended'))
        # if id-perms missing, identity doesn't exist

        all_metas = [u'logical-router-interface', u'logical-router-target', u'logical-router-gateway', u'logical-router-service-instance', u'id-perms', u'display-name']
        if not field_names:
            metas_to_read = all_metas
        else: # read only requested fields
            metas_to_read = set(all_metas) & set(field_names.keys())

        # metas is a dict where key is meta-name and val is list of dict
        # of form [{'meta':meta}, {'id':id1, 'meta':meta}, {'id':id2, 'meta':meta}]
        metas = {}
        for meta_name in metas_to_read:
            if meta_name in self._id_to_metas[ifmap_id]:
                metas[meta_name] = self._id_to_metas[ifmap_id][meta_name]
        return metas
    #end _ifmap_logical-router_read_to_meta_index

    def _ifmap_logical_router_update(self, ifmap_id, new_obj_dict):
        # read in refs from ifmap to determine which ones become inactive after update
        existing_metas = self._ifmap_logical_router_read_to_meta_index(ifmap_id)

        # remove properties that are no longer active
        props = ['id-perms', 'display-name']
        for prop in props:
            prop_m = prop.replace('-', '_')
            if prop in existing_metas and prop_m not in new_obj_dict:
                self._delete_id_self_meta(ifmap_id, 'contrail:'+prop)
        # remove refs that are no longer active
        delete_list = []
        refs = {'logical-router-interface': 'virtual-machine-interface',
                'logical-router-target': 'route-target',
                'logical-router-gateway': 'virtual-network',
                'logical-router-service-instance': 'service-instance'}
        for meta, to_name in refs.items():
            old_set = set([m['id'] for m in existing_metas.get(meta, [])])
            new_set = set()
            to_name_m = to_name.replace('-', '_')
            for ref in new_obj_dict.get(to_name_m+'_refs', []):
                to_imid = self.fq_name_to_ifmap_id(to_name, ref['to'])
                new_set.add(to_imid)

            for inact_ref in old_set - new_set:
                delete_list.append((inact_ref, 'contrail:'+meta))

        if delete_list:
            self._delete_id_pair_meta_list(ifmap_id, delete_list)

        (ok, result) = self._ifmap_logical_router_set(ifmap_id, existing_metas, new_obj_dict)
        return (ok, result)
    #end _ifmap_logical_router_update

    def _ifmap_logical_router_delete(self, obj_ids):
        ifmap_id = obj_ids['imid']
        parent_imid = obj_ids.get('parent_imid', None)
        existing_metas = self._ifmap_logical_router_read_to_meta_index(ifmap_id)
        meta_list = []
        for meta_name, meta_infos in existing_metas.items():
            for meta_info in meta_infos:
                ref_imid = meta_info.get('id')
                if ref_imid is None:
                    continue
                meta_list.append((ref_imid, 'contrail:'+meta_name))

        if parent_imid:
            # Remove link from parent
            meta_list.append((parent_imid, None))

        if meta_list:
            self._delete_id_pair_meta_list(ifmap_id, meta_list)

        # Remove all property metadata associated with this ident
        self._delete_id_self_meta(ifmap_id, None)

        return (True, '')
    #end _ifmap_logical_router_delete

#end class VncIfmapClientGen

class ImidGen(object):
    def domain_alloc_ifmap_id(self, parent_type, fq_name):
        my_fqn = ':'.join(fq_name)
        parent_fqn = ':'.join(fq_name[:-1])

        my_imid = 'contrail:domain:' + my_fqn
        if parent_fqn:
            if parent_type is None:
                return (None, None)
            parent_imid = 'contrail:' + parent_type + ':' + parent_fqn
        else: # parent is config-root
            parent_imid = 'contrail:config-root:root'

        # Normalize/escape special chars
        my_imid = escape(my_imid)
        parent_imid = escape(parent_imid)

        return (my_imid, parent_imid)
    #end domain_alloc_ifmap_id

    def global_vrouter_config_alloc_ifmap_id(self, parent_type, fq_name):
        my_fqn = ':'.join(fq_name)
        parent_fqn = ':'.join(fq_name[:-1])

        my_imid = 'contrail:global-vrouter-config:' + my_fqn
        if parent_fqn:
            if parent_type is None:
                return (None, None)
            parent_imid = 'contrail:' + parent_type + ':' + parent_fqn
        else: # parent is config-root
            parent_imid = 'contrail:config-root:root'

        # Normalize/escape special chars
        my_imid = escape(my_imid)
        parent_imid = escape(parent_imid)

        return (my_imid, parent_imid)
    #end global_vrouter_config_alloc_ifmap_id

    def instance_ip_alloc_ifmap_id(self, parent_type, fq_name):
        my_fqn = ':'.join(fq_name)
        parent_fqn = ':'.join(fq_name[:-1])

        my_imid = 'contrail:instance-ip:' + my_fqn
        if parent_fqn:
            if parent_type is None:
                return (None, None)
            parent_imid = 'contrail:' + parent_type + ':' + parent_fqn
        else: # parent is config-root
            parent_imid = 'contrail:config-root:root'

        # Normalize/escape special chars
        my_imid = escape(my_imid)
        parent_imid = escape(parent_imid)

        return (my_imid, parent_imid)
    #end instance_ip_alloc_ifmap_id

    def network_policy_alloc_ifmap_id(self, parent_type, fq_name):
        my_fqn = ':'.join(fq_name)
        parent_fqn = ':'.join(fq_name[:-1])

        my_imid = 'contrail:network-policy:' + my_fqn
        if parent_fqn:
            if parent_type is None:
                return (None, None)
            parent_imid = 'contrail:' + parent_type + ':' + parent_fqn
        else: # parent is config-root
            parent_imid = 'contrail:config-root:root'

        # Normalize/escape special chars
        my_imid = escape(my_imid)
        parent_imid = escape(parent_imid)

        return (my_imid, parent_imid)
    #end network_policy_alloc_ifmap_id

    def loadbalancer_pool_alloc_ifmap_id(self, parent_type, fq_name):
        my_fqn = ':'.join(fq_name)
        parent_fqn = ':'.join(fq_name[:-1])

        my_imid = 'contrail:loadbalancer-pool:' + my_fqn
        if parent_fqn:
            if parent_type is None:
                return (None, None)
            parent_imid = 'contrail:' + parent_type + ':' + parent_fqn
        else: # parent is config-root
            parent_imid = 'contrail:config-root:root'

        # Normalize/escape special chars
        my_imid = escape(my_imid)
        parent_imid = escape(parent_imid)

        return (my_imid, parent_imid)
    #end loadbalancer_pool_alloc_ifmap_id

    def virtual_DNS_record_alloc_ifmap_id(self, parent_type, fq_name):
        my_fqn = ':'.join(fq_name)
        parent_fqn = ':'.join(fq_name[:-1])

        my_imid = 'contrail:virtual-DNS-record:' + my_fqn
        if parent_fqn:
            if parent_type is None:
                return (None, None)
            parent_imid = 'contrail:' + parent_type + ':' + parent_fqn
        else: # parent is config-root
            parent_imid = 'contrail:config-root:root'

        # Normalize/escape special chars
        my_imid = escape(my_imid)
        parent_imid = escape(parent_imid)

        return (my_imid, parent_imid)
    #end virtual_DNS_record_alloc_ifmap_id

    def route_target_alloc_ifmap_id(self, parent_type, fq_name):
        my_fqn = ':'.join(fq_name)
        parent_fqn = ':'.join(fq_name[:-1])

        my_imid = 'contrail:route-target:' + my_fqn
        if parent_fqn:
            if parent_type is None:
                return (None, None)
            parent_imid = 'contrail:' + parent_type + ':' + parent_fqn
        else: # parent is config-root
            parent_imid = 'contrail:config-root:root'

        # Normalize/escape special chars
        my_imid = escape(my_imid)
        parent_imid = escape(parent_imid)

        return (my_imid, parent_imid)
    #end route_target_alloc_ifmap_id

    def floating_ip_alloc_ifmap_id(self, parent_type, fq_name):
        my_fqn = ':'.join(fq_name)
        parent_fqn = ':'.join(fq_name[:-1])

        my_imid = 'contrail:floating-ip:' + my_fqn
        if parent_fqn:
            if parent_type is None:
                return (None, None)
            parent_imid = 'contrail:' + parent_type + ':' + parent_fqn
        else: # parent is config-root
            parent_imid = 'contrail:config-root:root'

        # Normalize/escape special chars
        my_imid = escape(my_imid)
        parent_imid = escape(parent_imid)

        return (my_imid, parent_imid)
    #end floating_ip_alloc_ifmap_id

    def floating_ip_pool_alloc_ifmap_id(self, parent_type, fq_name):
        my_fqn = ':'.join(fq_name)
        parent_fqn = ':'.join(fq_name[:-1])

        my_imid = 'contrail:floating-ip-pool:' + my_fqn
        if parent_fqn:
            if parent_type is None:
                return (None, None)
            parent_imid = 'contrail:' + parent_type + ':' + parent_fqn
        else: # parent is config-root
            parent_imid = 'contrail:config-root:root'

        # Normalize/escape special chars
        my_imid = escape(my_imid)
        parent_imid = escape(parent_imid)

        return (my_imid, parent_imid)
    #end floating_ip_pool_alloc_ifmap_id

    def physical_router_alloc_ifmap_id(self, parent_type, fq_name):
        my_fqn = ':'.join(fq_name)
        parent_fqn = ':'.join(fq_name[:-1])

        my_imid = 'contrail:physical-router:' + my_fqn
        if parent_fqn:
            if parent_type is None:
                return (None, None)
            parent_imid = 'contrail:' + parent_type + ':' + parent_fqn
        else: # parent is config-root
            parent_imid = 'contrail:config-root:root'

        # Normalize/escape special chars
        my_imid = escape(my_imid)
        parent_imid = escape(parent_imid)

        return (my_imid, parent_imid)
    #end physical_router_alloc_ifmap_id

    def bgp_router_alloc_ifmap_id(self, parent_type, fq_name):
        my_fqn = ':'.join(fq_name)
        parent_fqn = ':'.join(fq_name[:-1])

        my_imid = 'contrail:bgp-router:' + my_fqn
        if parent_fqn:
            if parent_type is None:
                return (None, None)
            parent_imid = 'contrail:' + parent_type + ':' + parent_fqn
        else: # parent is config-root
            parent_imid = 'contrail:config-root:root'

        # Normalize/escape special chars
        my_imid = escape(my_imid)
        parent_imid = escape(parent_imid)

        return (my_imid, parent_imid)
    #end bgp_router_alloc_ifmap_id

    def virtual_router_alloc_ifmap_id(self, parent_type, fq_name):
        my_fqn = ':'.join(fq_name)
        parent_fqn = ':'.join(fq_name[:-1])

        my_imid = 'contrail:virtual-router:' + my_fqn
        if parent_fqn:
            if parent_type is None:
                return (None, None)
            parent_imid = 'contrail:' + parent_type + ':' + parent_fqn
        else: # parent is config-root
            parent_imid = 'contrail:config-root:root'

        # Normalize/escape special chars
        my_imid = escape(my_imid)
        parent_imid = escape(parent_imid)

        return (my_imid, parent_imid)
    #end virtual_router_alloc_ifmap_id

    def config_root_alloc_ifmap_id(self, parent_type, fq_name):
        my_fqn = ':'.join(fq_name)
        parent_fqn = ':'.join(fq_name[:-1])

        my_imid = 'contrail:config-root:' + my_fqn
        if parent_fqn:
            if parent_type is None:
                return (None, None)
            parent_imid = 'contrail:' + parent_type + ':' + parent_fqn
        else: # parent is config-root
            parent_imid = 'contrail:config-root:root'

        # Normalize/escape special chars
        my_imid = escape(my_imid)
        parent_imid = escape(parent_imid)

        return (my_imid, parent_imid)
    #end config_root_alloc_ifmap_id

    def subnet_alloc_ifmap_id(self, parent_type, fq_name):
        my_fqn = ':'.join(fq_name)
        parent_fqn = ':'.join(fq_name[:-1])

        my_imid = 'contrail:subnet:' + my_fqn
        if parent_fqn:
            if parent_type is None:
                return (None, None)
            parent_imid = 'contrail:' + parent_type + ':' + parent_fqn
        else: # parent is config-root
            parent_imid = 'contrail:config-root:root'

        # Normalize/escape special chars
        my_imid = escape(my_imid)
        parent_imid = escape(parent_imid)

        return (my_imid, parent_imid)
    #end subnet_alloc_ifmap_id

    def global_system_config_alloc_ifmap_id(self, parent_type, fq_name):
        my_fqn = ':'.join(fq_name)
        parent_fqn = ':'.join(fq_name[:-1])

        my_imid = 'contrail:global-system-config:' + my_fqn
        if parent_fqn:
            if parent_type is None:
                return (None, None)
            parent_imid = 'contrail:' + parent_type + ':' + parent_fqn
        else: # parent is config-root
            parent_imid = 'contrail:config-root:root'

        # Normalize/escape special chars
        my_imid = escape(my_imid)
        parent_imid = escape(parent_imid)

        return (my_imid, parent_imid)
    #end global_system_config_alloc_ifmap_id

    def service_appliance_alloc_ifmap_id(self, parent_type, fq_name):
        my_fqn = ':'.join(fq_name)
        parent_fqn = ':'.join(fq_name[:-1])

        my_imid = 'contrail:service-appliance:' + my_fqn
        if parent_fqn:
            if parent_type is None:
                return (None, None)
            parent_imid = 'contrail:' + parent_type + ':' + parent_fqn
        else: # parent is config-root
            parent_imid = 'contrail:config-root:root'

        # Normalize/escape special chars
        my_imid = escape(my_imid)
        parent_imid = escape(parent_imid)

        return (my_imid, parent_imid)
    #end service_appliance_alloc_ifmap_id

    def service_instance_alloc_ifmap_id(self, parent_type, fq_name):
        my_fqn = ':'.join(fq_name)
        parent_fqn = ':'.join(fq_name[:-1])

        my_imid = 'contrail:service-instance:' + my_fqn
        if parent_fqn:
            if parent_type is None:
                return (None, None)
            parent_imid = 'contrail:' + parent_type + ':' + parent_fqn
        else: # parent is config-root
            parent_imid = 'contrail:config-root:root'

        # Normalize/escape special chars
        my_imid = escape(my_imid)
        parent_imid = escape(parent_imid)

        return (my_imid, parent_imid)
    #end service_instance_alloc_ifmap_id

    def namespace_alloc_ifmap_id(self, parent_type, fq_name):
        my_fqn = ':'.join(fq_name)
        parent_fqn = ':'.join(fq_name[:-1])

        my_imid = 'contrail:namespace:' + my_fqn
        if parent_fqn:
            if parent_type is None:
                return (None, None)
            parent_imid = 'contrail:' + parent_type + ':' + parent_fqn
        else: # parent is config-root
            parent_imid = 'contrail:config-root:root'

        # Normalize/escape special chars
        my_imid = escape(my_imid)
        parent_imid = escape(parent_imid)

        return (my_imid, parent_imid)
    #end namespace_alloc_ifmap_id

    def logical_interface_alloc_ifmap_id(self, parent_type, fq_name):
        my_fqn = ':'.join(fq_name)
        parent_fqn = ':'.join(fq_name[:-1])

        my_imid = 'contrail:logical-interface:' + my_fqn
        if parent_fqn:
            if parent_type is None:
                return (None, None)
            parent_imid = 'contrail:' + parent_type + ':' + parent_fqn
        else: # parent is config-root
            parent_imid = 'contrail:config-root:root'

        # Normalize/escape special chars
        my_imid = escape(my_imid)
        parent_imid = escape(parent_imid)

        return (my_imid, parent_imid)
    #end logical_interface_alloc_ifmap_id

    def route_table_alloc_ifmap_id(self, parent_type, fq_name):
        my_fqn = ':'.join(fq_name)
        parent_fqn = ':'.join(fq_name[:-1])

        my_imid = 'contrail:route-table:' + my_fqn
        if parent_fqn:
            if parent_type is None:
                return (None, None)
            parent_imid = 'contrail:' + parent_type + ':' + parent_fqn
        else: # parent is config-root
            parent_imid = 'contrail:config-root:root'

        # Normalize/escape special chars
        my_imid = escape(my_imid)
        parent_imid = escape(parent_imid)

        return (my_imid, parent_imid)
    #end route_table_alloc_ifmap_id

    def physical_interface_alloc_ifmap_id(self, parent_type, fq_name):
        my_fqn = ':'.join(fq_name)
        parent_fqn = ':'.join(fq_name[:-1])

        my_imid = 'contrail:physical-interface:' + my_fqn
        if parent_fqn:
            if parent_type is None:
                return (None, None)
            parent_imid = 'contrail:' + parent_type + ':' + parent_fqn
        else: # parent is config-root
            parent_imid = 'contrail:config-root:root'

        # Normalize/escape special chars
        my_imid = escape(my_imid)
        parent_imid = escape(parent_imid)

        return (my_imid, parent_imid)
    #end physical_interface_alloc_ifmap_id

    def access_control_list_alloc_ifmap_id(self, parent_type, fq_name):
        my_fqn = ':'.join(fq_name)
        parent_fqn = ':'.join(fq_name[:-1])

        my_imid = 'contrail:access-control-list:' + my_fqn
        if parent_fqn:
            if parent_type is None:
                return (None, None)
            parent_imid = 'contrail:' + parent_type + ':' + parent_fqn
        else: # parent is config-root
            parent_imid = 'contrail:config-root:root'

        # Normalize/escape special chars
        my_imid = escape(my_imid)
        parent_imid = escape(parent_imid)

        return (my_imid, parent_imid)
    #end access_control_list_alloc_ifmap_id

    def analytics_node_alloc_ifmap_id(self, parent_type, fq_name):
        my_fqn = ':'.join(fq_name)
        parent_fqn = ':'.join(fq_name[:-1])

        my_imid = 'contrail:analytics-node:' + my_fqn
        if parent_fqn:
            if parent_type is None:
                return (None, None)
            parent_imid = 'contrail:' + parent_type + ':' + parent_fqn
        else: # parent is config-root
            parent_imid = 'contrail:config-root:root'

        # Normalize/escape special chars
        my_imid = escape(my_imid)
        parent_imid = escape(parent_imid)

        return (my_imid, parent_imid)
    #end analytics_node_alloc_ifmap_id

    def virtual_DNS_alloc_ifmap_id(self, parent_type, fq_name):
        my_fqn = ':'.join(fq_name)
        parent_fqn = ':'.join(fq_name[:-1])

        my_imid = 'contrail:virtual-DNS:' + my_fqn
        if parent_fqn:
            if parent_type is None:
                return (None, None)
            parent_imid = 'contrail:' + parent_type + ':' + parent_fqn
        else: # parent is config-root
            parent_imid = 'contrail:config-root:root'

        # Normalize/escape special chars
        my_imid = escape(my_imid)
        parent_imid = escape(parent_imid)

        return (my_imid, parent_imid)
    #end virtual_DNS_alloc_ifmap_id

    def customer_attachment_alloc_ifmap_id(self, parent_type, fq_name):
        my_fqn = ':'.join(fq_name)
        parent_fqn = ':'.join(fq_name[:-1])

        my_imid = 'contrail:customer-attachment:' + my_fqn
        if parent_fqn:
            if parent_type is None:
                return (None, None)
            parent_imid = 'contrail:' + parent_type + ':' + parent_fqn
        else: # parent is config-root
            parent_imid = 'contrail:config-root:root'

        # Normalize/escape special chars
        my_imid = escape(my_imid)
        parent_imid = escape(parent_imid)

        return (my_imid, parent_imid)
    #end customer_attachment_alloc_ifmap_id

    def service_appliance_set_alloc_ifmap_id(self, parent_type, fq_name):
        my_fqn = ':'.join(fq_name)
        parent_fqn = ':'.join(fq_name[:-1])

        my_imid = 'contrail:service-appliance-set:' + my_fqn
        if parent_fqn:
            if parent_type is None:
                return (None, None)
            parent_imid = 'contrail:' + parent_type + ':' + parent_fqn
        else: # parent is config-root
            parent_imid = 'contrail:config-root:root'

        # Normalize/escape special chars
        my_imid = escape(my_imid)
        parent_imid = escape(parent_imid)

        return (my_imid, parent_imid)
    #end service_appliance_set_alloc_ifmap_id

    def config_node_alloc_ifmap_id(self, parent_type, fq_name):
        my_fqn = ':'.join(fq_name)
        parent_fqn = ':'.join(fq_name[:-1])

        my_imid = 'contrail:config-node:' + my_fqn
        if parent_fqn:
            if parent_type is None:
                return (None, None)
            parent_imid = 'contrail:' + parent_type + ':' + parent_fqn
        else: # parent is config-root
            parent_imid = 'contrail:config-root:root'

        # Normalize/escape special chars
        my_imid = escape(my_imid)
        parent_imid = escape(parent_imid)

        return (my_imid, parent_imid)
    #end config_node_alloc_ifmap_id

    def qos_queue_alloc_ifmap_id(self, parent_type, fq_name):
        my_fqn = ':'.join(fq_name)
        parent_fqn = ':'.join(fq_name[:-1])

        my_imid = 'contrail:qos-queue:' + my_fqn
        if parent_fqn:
            if parent_type is None:
                return (None, None)
            parent_imid = 'contrail:' + parent_type + ':' + parent_fqn
        else: # parent is config-root
            parent_imid = 'contrail:config-root:root'

        # Normalize/escape special chars
        my_imid = escape(my_imid)
        parent_imid = escape(parent_imid)

        return (my_imid, parent_imid)
    #end qos_queue_alloc_ifmap_id

    def virtual_machine_alloc_ifmap_id(self, parent_type, fq_name):
        my_fqn = ':'.join(fq_name)
        parent_fqn = ':'.join(fq_name[:-1])

        my_imid = 'contrail:virtual-machine:' + my_fqn
        if parent_fqn:
            if parent_type is None:
                return (None, None)
            parent_imid = 'contrail:' + parent_type + ':' + parent_fqn
        else: # parent is config-root
            parent_imid = 'contrail:config-root:root'

        # Normalize/escape special chars
        my_imid = escape(my_imid)
        parent_imid = escape(parent_imid)

        return (my_imid, parent_imid)
    #end virtual_machine_alloc_ifmap_id

    def interface_route_table_alloc_ifmap_id(self, parent_type, fq_name):
        my_fqn = ':'.join(fq_name)
        parent_fqn = ':'.join(fq_name[:-1])

        my_imid = 'contrail:interface-route-table:' + my_fqn
        if parent_fqn:
            if parent_type is None:
                return (None, None)
            parent_imid = 'contrail:' + parent_type + ':' + parent_fqn
        else: # parent is config-root
            parent_imid = 'contrail:config-root:root'

        # Normalize/escape special chars
        my_imid = escape(my_imid)
        parent_imid = escape(parent_imid)

        return (my_imid, parent_imid)
    #end interface_route_table_alloc_ifmap_id

    def service_template_alloc_ifmap_id(self, parent_type, fq_name):
        my_fqn = ':'.join(fq_name)
        parent_fqn = ':'.join(fq_name[:-1])

        my_imid = 'contrail:service-template:' + my_fqn
        if parent_fqn:
            if parent_type is None:
                return (None, None)
            parent_imid = 'contrail:' + parent_type + ':' + parent_fqn
        else: # parent is config-root
            parent_imid = 'contrail:config-root:root'

        # Normalize/escape special chars
        my_imid = escape(my_imid)
        parent_imid = escape(parent_imid)

        return (my_imid, parent_imid)
    #end service_template_alloc_ifmap_id

    def virtual_ip_alloc_ifmap_id(self, parent_type, fq_name):
        my_fqn = ':'.join(fq_name)
        parent_fqn = ':'.join(fq_name[:-1])

        my_imid = 'contrail:virtual-ip:' + my_fqn
        if parent_fqn:
            if parent_type is None:
                return (None, None)
            parent_imid = 'contrail:' + parent_type + ':' + parent_fqn
        else: # parent is config-root
            parent_imid = 'contrail:config-root:root'

        # Normalize/escape special chars
        my_imid = escape(my_imid)
        parent_imid = escape(parent_imid)

        return (my_imid, parent_imid)
    #end virtual_ip_alloc_ifmap_id

    def loadbalancer_member_alloc_ifmap_id(self, parent_type, fq_name):
        my_fqn = ':'.join(fq_name)
        parent_fqn = ':'.join(fq_name[:-1])

        my_imid = 'contrail:loadbalancer-member:' + my_fqn
        if parent_fqn:
            if parent_type is None:
                return (None, None)
            parent_imid = 'contrail:' + parent_type + ':' + parent_fqn
        else: # parent is config-root
            parent_imid = 'contrail:config-root:root'

        # Normalize/escape special chars
        my_imid = escape(my_imid)
        parent_imid = escape(parent_imid)

        return (my_imid, parent_imid)
    #end loadbalancer_member_alloc_ifmap_id

    def security_group_alloc_ifmap_id(self, parent_type, fq_name):
        my_fqn = ':'.join(fq_name)
        parent_fqn = ':'.join(fq_name[:-1])

        my_imid = 'contrail:security-group:' + my_fqn
        if parent_fqn:
            if parent_type is None:
                return (None, None)
            parent_imid = 'contrail:' + parent_type + ':' + parent_fqn
        else: # parent is config-root
            parent_imid = 'contrail:config-root:root'

        # Normalize/escape special chars
        my_imid = escape(my_imid)
        parent_imid = escape(parent_imid)

        return (my_imid, parent_imid)
    #end security_group_alloc_ifmap_id

    def provider_attachment_alloc_ifmap_id(self, parent_type, fq_name):
        my_fqn = ':'.join(fq_name)
        parent_fqn = ':'.join(fq_name[:-1])

        my_imid = 'contrail:provider-attachment:' + my_fqn
        if parent_fqn:
            if parent_type is None:
                return (None, None)
            parent_imid = 'contrail:' + parent_type + ':' + parent_fqn
        else: # parent is config-root
            parent_imid = 'contrail:config-root:root'

        # Normalize/escape special chars
        my_imid = escape(my_imid)
        parent_imid = escape(parent_imid)

        return (my_imid, parent_imid)
    #end provider_attachment_alloc_ifmap_id

    def virtual_machine_interface_alloc_ifmap_id(self, parent_type, fq_name):
        my_fqn = ':'.join(fq_name)
        parent_fqn = ':'.join(fq_name[:-1])

        my_imid = 'contrail:virtual-machine-interface:' + my_fqn
        if parent_fqn:
            if parent_type is None:
                return (None, None)
            parent_imid = 'contrail:' + parent_type + ':' + parent_fqn
        else: # parent is config-root
            parent_imid = 'contrail:config-root:root'

        # Normalize/escape special chars
        my_imid = escape(my_imid)
        parent_imid = escape(parent_imid)

        return (my_imid, parent_imid)
    #end virtual_machine_interface_alloc_ifmap_id

    def loadbalancer_healthmonitor_alloc_ifmap_id(self, parent_type, fq_name):
        my_fqn = ':'.join(fq_name)
        parent_fqn = ':'.join(fq_name[:-1])

        my_imid = 'contrail:loadbalancer-healthmonitor:' + my_fqn
        if parent_fqn:
            if parent_type is None:
                return (None, None)
            parent_imid = 'contrail:' + parent_type + ':' + parent_fqn
        else: # parent is config-root
            parent_imid = 'contrail:config-root:root'

        # Normalize/escape special chars
        my_imid = escape(my_imid)
        parent_imid = escape(parent_imid)

        return (my_imid, parent_imid)
    #end loadbalancer_healthmonitor_alloc_ifmap_id

    def virtual_network_alloc_ifmap_id(self, parent_type, fq_name):
        my_fqn = ':'.join(fq_name)
        parent_fqn = ':'.join(fq_name[:-1])

        my_imid = 'contrail:virtual-network:' + my_fqn
        if parent_fqn:
            if parent_type is None:
                return (None, None)
            parent_imid = 'contrail:' + parent_type + ':' + parent_fqn
        else: # parent is config-root
            parent_imid = 'contrail:config-root:root'

        # Normalize/escape special chars
        my_imid = escape(my_imid)
        parent_imid = escape(parent_imid)

        return (my_imid, parent_imid)
    #end virtual_network_alloc_ifmap_id

    def project_alloc_ifmap_id(self, parent_type, fq_name):
        my_fqn = ':'.join(fq_name)
        parent_fqn = ':'.join(fq_name[:-1])

        my_imid = 'contrail:project:' + my_fqn
        if parent_fqn:
            if parent_type is None:
                return (None, None)
            parent_imid = 'contrail:' + parent_type + ':' + parent_fqn
        else: # parent is config-root
            parent_imid = 'contrail:config-root:root'

        # Normalize/escape special chars
        my_imid = escape(my_imid)
        parent_imid = escape(parent_imid)

        return (my_imid, parent_imid)
    #end project_alloc_ifmap_id

    def qos_forwarding_class_alloc_ifmap_id(self, parent_type, fq_name):
        my_fqn = ':'.join(fq_name)
        parent_fqn = ':'.join(fq_name[:-1])

        my_imid = 'contrail:qos-forwarding-class:' + my_fqn
        if parent_fqn:
            if parent_type is None:
                return (None, None)
            parent_imid = 'contrail:' + parent_type + ':' + parent_fqn
        else: # parent is config-root
            parent_imid = 'contrail:config-root:root'

        # Normalize/escape special chars
        my_imid = escape(my_imid)
        parent_imid = escape(parent_imid)

        return (my_imid, parent_imid)
    #end qos_forwarding_class_alloc_ifmap_id

    def database_node_alloc_ifmap_id(self, parent_type, fq_name):
        my_fqn = ':'.join(fq_name)
        parent_fqn = ':'.join(fq_name[:-1])

        my_imid = 'contrail:database-node:' + my_fqn
        if parent_fqn:
            if parent_type is None:
                return (None, None)
            parent_imid = 'contrail:' + parent_type + ':' + parent_fqn
        else: # parent is config-root
            parent_imid = 'contrail:config-root:root'

        # Normalize/escape special chars
        my_imid = escape(my_imid)
        parent_imid = escape(parent_imid)

        return (my_imid, parent_imid)
    #end database_node_alloc_ifmap_id

    def routing_instance_alloc_ifmap_id(self, parent_type, fq_name):
        my_fqn = ':'.join(fq_name)
        parent_fqn = ':'.join(fq_name[:-1])

        my_imid = 'contrail:routing-instance:' + my_fqn
        if parent_fqn:
            if parent_type is None:
                return (None, None)
            parent_imid = 'contrail:' + parent_type + ':' + parent_fqn
        else: # parent is config-root
            parent_imid = 'contrail:config-root:root'

        # Normalize/escape special chars
        my_imid = escape(my_imid)
        parent_imid = escape(parent_imid)

        return (my_imid, parent_imid)
    #end routing_instance_alloc_ifmap_id

    def network_ipam_alloc_ifmap_id(self, parent_type, fq_name):
        my_fqn = ':'.join(fq_name)
        parent_fqn = ':'.join(fq_name[:-1])

        my_imid = 'contrail:network-ipam:' + my_fqn
        if parent_fqn:
            if parent_type is None:
                return (None, None)
            parent_imid = 'contrail:' + parent_type + ':' + parent_fqn
        else: # parent is config-root
            parent_imid = 'contrail:config-root:root'

        # Normalize/escape special chars
        my_imid = escape(my_imid)
        parent_imid = escape(parent_imid)

        return (my_imid, parent_imid)
    #end network_ipam_alloc_ifmap_id

    def logical_router_alloc_ifmap_id(self, parent_type, fq_name):
        my_fqn = ':'.join(fq_name)
        parent_fqn = ':'.join(fq_name[:-1])

        my_imid = 'contrail:logical-router:' + my_fqn
        if parent_fqn:
            if parent_type is None:
                return (None, None)
            parent_imid = 'contrail:' + parent_type + ':' + parent_fqn
        else: # parent is config-root
            parent_imid = 'contrail:config-root:root'

        # Normalize/escape special chars
        my_imid = escape(my_imid)
        parent_imid = escape(parent_imid)

        return (my_imid, parent_imid)
    #end logical_router_alloc_ifmap_id


link_name_to_xsd_type = {
    "project-namespace":"SubnetType",
    "connection":"ConnectionType",
    "bgp-peering":"BgpPeeringAttributes",
    "virtual-machine-interface-routing-instance":"PolicyBasedForwardingRuleType",
    "virtual-network-network-policy":"VirtualNetworkPolicyType",
    "instance-target":"InstanceTargetType",
    "virtual-network-network-ipam":"VnSubnetsType"
}

