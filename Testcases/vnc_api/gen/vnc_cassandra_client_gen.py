
# AUTO-GENERATED file from IFMapApiGenerator. Do Not Edit!

import re
import gevent
import json
import pycassa
import datetime
from operator import itemgetter

from pysandesh.gen_py.sandesh.ttypes import SandeshLevel
import cfgm_common.exceptions
from cfgm_common import utils
from resource_xsd import *
from resource_common import *
from resource_server import *

class VncCassandraClientGen(object):
    def __init__(self):
        self._re_match_parent = re.compile('parent:')
        self._re_match_prop = re.compile('prop:')
        self._re_match_ref = re.compile('ref:')
        self._re_match_backref = re.compile('backref:')
        self._re_match_children = re.compile('children:')
    #end __init__

    def _cassandra_domain_alloc(self, fq_name):
        return (True, '')
    #end _cassandra_domain_alloc

    def _cassandra_domain_create(self, obj_ids, obj_dict):
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        bch = self._obj_uuid_cf.batch()

        obj_cols = {}
        obj_cols['fq_name'] = json.dumps(obj_dict['fq_name'])
        obj_cols['type'] = json.dumps('domain')
        if 'parent_type' in obj_dict:
            # non config-root child
            parent_type = obj_dict['parent_type']
            parent_method_type = parent_type.replace('-', '_')
            parent_fq_name = obj_dict['fq_name'][:-1]
            obj_cols['parent_type'] = json.dumps(parent_type)
            parent_uuid = self.fq_name_to_uuid(parent_method_type, parent_fq_name)
            self._create_child(bch, parent_method_type, parent_uuid, 'domain', obj_ids['uuid'])

        # Properties
        field = obj_dict.get('domain_limits', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'domain_limits', field)

        field = obj_dict.get('api_access_list', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'api_access_list', field)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            field['created'] = datetime.datetime.utcnow().isoformat()
            field['last_modified'] = field['created']
            self._create_prop(bch, obj_ids['uuid'], 'id_perms', field)

        field = obj_dict.get('display_name', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'display_name', field)


        # References

        bch.insert(obj_ids['uuid'], obj_cols)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(obj_dict['fq_name'])
        fq_name_cols = {utils.encode_string(fq_name_str) + ':' + obj_ids['uuid']: json.dumps(None)}
        self._obj_fq_name_cf.insert('domain', fq_name_cols)

        return (True, '')
    #end _cassandra_domain_create

    def _cassandra_domain_read(self, obj_uuids, field_names = None):
        # if field_names = None, all fields will be read/returned

        obj_uuid_cf = self._obj_uuid_cf

        # optimize for common case of reading non-backref, non-children fields
        # ignoring columns starting from 'b' and 'c' - significant performance
        # impact in scaled setting. e.g. read of project
        if (field_names is None or
            (set(field_names) & (Domain.backref_fields | Domain.children_fields))):
            # atleast one backref/children field is needed
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_count = 10000000,
                                       include_timestamp = True)
        else: # ignore reading backref + children columns
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_start = 'd',
                                       column_count = 10000000,
                                       include_timestamp = True)

        if (len(obj_uuids) == 1) and not obj_rows:
            raise cfgm_common.exceptions.NoIdError(obj_uuids[0])

        results = []
        for row_key in obj_rows:
            # give chance for zk heartbeat/ping
            gevent.sleep(0)
            obj_uuid = row_key
            obj_cols = obj_rows[obj_uuid]
            result = {}
            result['uuid'] = obj_uuid
            result['fq_name'] = json.loads(obj_cols['fq_name'][0])
            for col_name in obj_cols.keys():
                if self._re_match_parent.match(col_name):
                    # non config-root child
                    (_, _, parent_uuid) = col_name.split(':')
                    parent_type = json.loads(obj_cols['parent_type'][0])
                    result['parent_type'] = parent_type
                    try:
                        result['parent_uuid'] = parent_uuid
                        result['parent_href'] = self._generate_url(parent_type, parent_uuid)
                    except cfgm_common.exceptions.NoIdError:
                        err_msg = 'Unknown uuid for parent ' + result['fq_name'][-2]
                        return (False, err_msg)

                # TODO use compiled RE
                if self._re_match_prop.match(col_name):
                    (_, prop_name) = col_name.split(':')
                    result[prop_name] = json.loads(obj_cols[col_name][0])

                # TODO use compiled RE
                if self._re_match_children.match(col_name):
                    (_, child_type, child_uuid) = col_name.split(':')
                    if field_names and '%ss' %(child_type) not in field_names:
                        continue

                    child_tstamp = obj_cols[col_name][1]
                    try:
                        self._read_child(result, obj_uuid, child_type, child_uuid, child_tstamp)
                    except cfgm_common.exceptions.NoIdError:
                        continue

                # TODO use compiled RE
                if self._re_match_ref.match(col_name):
                    (_, ref_type, ref_uuid) = col_name.split(':')
                    self._read_ref(result, obj_uuid, ref_type, ref_uuid, obj_cols[col_name][0])

                if self._re_match_backref.match(col_name):
                    (_, back_ref_type, back_ref_uuid) = col_name.split(':')
                    if field_names and '%s_back_refs' %(back_ref_type) not in field_names:
                        continue

                    try:
                        self._read_back_ref(result, obj_uuid, back_ref_type, back_ref_uuid,
                                            obj_cols[col_name][0])
                    except cfgm_common.exceptions.NoIdError:
                        continue

            # for all column names

            if 'projects' in result:
                # sort children; TODO do this based on schema
                sorted_children = sorted(result['projects'], key = itemgetter('tstamp'))
                # re-write result's children without timestamp
                result['projects'] = sorted_children
                [child.pop('tstamp') for child in result['projects']]

            if 'namespaces' in result:
                # sort children; TODO do this based on schema
                sorted_children = sorted(result['namespaces'], key = itemgetter('tstamp'))
                # re-write result's children without timestamp
                result['namespaces'] = sorted_children
                [child.pop('tstamp') for child in result['namespaces']]

            if 'service_templates' in result:
                # sort children; TODO do this based on schema
                sorted_children = sorted(result['service_templates'], key = itemgetter('tstamp'))
                # re-write result's children without timestamp
                result['service_templates'] = sorted_children
                [child.pop('tstamp') for child in result['service_templates']]

            if 'virtual_DNSs' in result:
                # sort children; TODO do this based on schema
                sorted_children = sorted(result['virtual_DNSs'], key = itemgetter('tstamp'))
                # re-write result's children without timestamp
                result['virtual_DNSs'] = sorted_children
                [child.pop('tstamp') for child in result['virtual_DNSs']]

            results.append(result)
        # end for all rows

        return (True, results)
    #end _cassandra_domain_read

    def _cassandra_domain_count_children(self, obj_uuid, child_type):
        # if child_type = None, return
        if child_type is None:
            return (False, '')

        obj_uuid_cf = self._obj_uuid_cf
        if child_type not in Domain.children_fields:
            return (False, '%s is not a valid children of Domain' %(child_type))

        col_start = 'children:'+child_type[:-1]+':'
        col_finish = 'children:'+child_type[:-1]+';'
        num_children = obj_uuid_cf.get_count(obj_uuid,
                                   column_start = col_start,
                                   column_finish = col_finish,
                                   max_count = 10000000)
        return (True, num_children)
    #end _cassandra_domain_count_children

    def _cassandra_domain_update(self, obj_uuid, new_obj_dict):
        # Grab ref-uuids and properties in new version
        new_ref_infos = {}

        new_props = {}
        if 'domain_limits' in new_obj_dict:
            new_props['domain_limits'] = new_obj_dict['domain_limits']
        if 'api_access_list' in new_obj_dict:
            new_props['api_access_list'] = new_obj_dict['api_access_list']
        if 'id_perms' in new_obj_dict:
            new_props['id_perms'] = new_obj_dict['id_perms']
        if 'display_name' in new_obj_dict:
            new_props['display_name'] = new_obj_dict['display_name']
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        obj_uuid_cf = self._obj_uuid_cf
        obj_cols_iter = obj_uuid_cf.xget(obj_uuid)
        # TODO optimize this (converts tuple to dict)
        obj_cols = {}
        for col_info in obj_cols_iter:
            obj_cols[col_info[0]] = col_info[1]

        bch = obj_uuid_cf.batch()
        for col_name in obj_cols.keys():
            # TODO use compiled RE
            if re.match('prop:', col_name):
                (_, prop_name) = col_name.split(':')
                if prop_name == 'id_perms':
                    # id-perms always has to be updated for last-mod timestamp
                    # get it from request dict(or from db if not in request dict)
                    new_id_perms = new_obj_dict.get(prop_name, json.loads(obj_cols[col_name]))
                    self.update_last_modified(bch, obj_uuid, new_id_perms)
                elif prop_name in new_obj_dict:
                    self._update_prop(bch, obj_uuid, prop_name, new_props)

            # TODO use compiled RE
            if re.match('ref:', col_name):
                (_, ref_type, ref_uuid) = col_name.split(':')
                self._update_ref(bch, 'domain', obj_uuid, ref_type, ref_uuid, new_ref_infos)
        # for all column names

        # create new refs
        for ref_type in new_ref_infos.keys():
            for ref_uuid in new_ref_infos[ref_type].keys():
                ref_data = new_ref_infos[ref_type][ref_uuid]
                self._create_ref(bch, 'domain', obj_uuid, ref_type, ref_uuid, ref_data)

        # create new props
        for prop_name in new_props.keys():
            self._create_prop(bch, obj_uuid, prop_name, new_props[prop_name])

        bch.send()

        return (True, '')
    #end _cassandra_domain_update

    def _cassandra_domain_list(self, parent_uuids=None, back_ref_uuids=None,
                           obj_uuids=None, count=False, filters=None):
        children_fq_names_uuids = []
        if filters:
            fnames = filters.get('field_names', [])
            fvalues = filters.get('field_values', [])
            filter_fields = [(fnames[i], fvalues[i]) for i in range(len(fnames))]
        else:
            filter_fields = []

        def filter_rows(coll_infos, filter_cols, filter_params):
            filt_infos = {}
            coll_rows = obj_uuid_cf.multiget(coll_infos.keys(),
                                   columns=filter_cols,
                                   column_count=10000000)
            for row in coll_rows:
                # give chance for zk heartbeat/ping
                gevent.sleep(0)
                full_match = True
                for fname, fval in filter_params:
                    if coll_rows[row]['prop:%s' %(fname)] != fval:
                        full_match = False
                        break
                if full_match:
                    filt_infos[row] = coll_infos[row]
            return filt_infos
        # end filter_rows

        def get_fq_name_uuid_list(obj_uuids):
            ret_list = []
            for obj_uuid in obj_uuids:
                try:
                    obj_fq_name = self.uuid_to_fq_name(obj_uuid)
                    ret_list.append((obj_fq_name, obj_uuid))
                except cfgm_common.exceptions.NoIdError:
                    pass
            return ret_list
        # end get_fq_name_uuid_list

        if parent_uuids:
            # go from parent to child
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'children:domain:'
            col_fin = 'children:domain;'
            try:
                obj_rows = obj_uuid_cf.multiget(parent_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_parent_anchor(sort=False):
                # flatten to [('children:<type>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_child_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    child_uuid = col_name.split(':')[2]
                    if obj_uuids and child_uuid not in obj_uuids:
                        continue
                    all_child_infos[child_uuid] = {'uuid': child_uuid, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_child_infos = filter_rows(all_child_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_child_infos = all_child_infos

                if not sort:
                    ret_child_infos = filt_child_infos.values()
                else:
                    ret_child_infos = sorted(filt_child_infos.values(), key=itemgetter('tstamp'))

                return get_fq_name_uuid_list(r['uuid'] for r in ret_child_infos)
            # end filter_rows_parent_anchor

            if count:
                return (True, len(filter_rows_parent_anchor()))

            children_fq_names_uuids = filter_rows_parent_anchor(sort=True)

        if back_ref_uuids:
            # go from anchor to backrefs
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'backref:domain:'
            col_fin = 'backref:domain;'
            try:
                obj_rows = obj_uuid_cf.multiget(back_ref_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_backref_anchor():
                # flatten to [('<fqnstr>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_backref_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    col_name_arr = col_name.split(':')
                    fq_name = col_name_arr[:-1]
                    obj_uuid = col_name_arr[-1]
                    if obj_uuids and obj_uuid not in obj_uuids:
                        continue
                    all_backref_infos[obj_uuid] = \
                        {'uuid': obj_uuid, 'fq_name': fq_name, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_backref_infos = filter_rows(all_backref_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_backref_infos = all_backref_infos

                return [(br_info['fq_name'], br_info['uuid']) for br_info in filt_backref_infos.values()]
            # end filter_rows_backref_anchor

            if count:
                return (True, len(filter_rows_backref_anchor()))

            children_fq_names_uuids = filter_rows_backref_anchor()

        if not parent_uuids and not back_ref_uuids:
            obj_uuid_cf = self._obj_uuid_cf
            if obj_uuids:
                # exact objects specified
                def filter_rows_object_list():
                    all_obj_infos = {}
                    for obj_uuid in obj_uuids:
                        all_obj_infos[obj_uuid] = None

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return get_fq_name_uuid_list(filt_obj_infos.keys())
                # end filter_rows_object_list

                if count:
                    return (True, len(filter_rows_object_list()))
                children_fq_names_uuids = filter_rows_object_list()

            else: # grab all resources of this type
                obj_fq_name_cf = self._obj_fq_name_cf
                try:
                    cols = obj_fq_name_cf.get('domain', column_count = 10000000)
                except pycassa.NotFoundException:
                    if count:
                        return (True, 0)
                    else:
                        return (True, children_fq_names_uuids)

                def filter_rows_no_anchor():
                    all_obj_infos = {}
                    for col_name, col_val in cols.items():
                        # give chance for zk heartbeat/ping
                        gevent.sleep(0)
                        col_name_arr = utils.decode_string(col_name).split(':')
                        obj_uuid = col_name_arr[-1]
                        all_obj_infos[obj_uuid] = (col_name_arr[:-1], obj_uuid)

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return filt_obj_infos.values()
                # end filter_rows_no_anchor

                if count:
                    return (True, len(filter_rows_no_anchor()))

                children_fq_names_uuids = filter_rows_no_anchor()

        return (True, children_fq_names_uuids)
    #end _cassandra_domain_list

    def _cassandra_domain_delete(self, obj_uuid):
        obj_uuid_cf = self._obj_uuid_cf
        fq_name = json.loads(obj_uuid_cf.get(obj_uuid, columns = ['fq_name'])['fq_name'])
        bch = obj_uuid_cf.batch()

        # unlink from parent
        col_start = 'parent:'
        col_fin = 'parent;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, parent_type, parent_uuid) = col_name.split(':')
            self._delete_child(bch, parent_type, parent_uuid, 'domain', obj_uuid)

        # remove refs
        col_start = 'ref:'
        col_fin = 'ref;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, ref_type, ref_uuid) = col_name.split(':')
            self._delete_ref(bch, 'domain', obj_uuid, ref_type, ref_uuid)

        bch.remove(obj_uuid)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(fq_name)
        fq_name_col = utils.encode_string(fq_name_str) + ':' + obj_uuid
        self._obj_fq_name_cf.remove('domain', columns = [fq_name_col])


        return (True, '')
    #end _cassandra_domain_delete

    def _cassandra_global_vrouter_config_alloc(self, fq_name):
        return (True, '')
    #end _cassandra_global_vrouter_config_alloc

    def _cassandra_global_vrouter_config_create(self, obj_ids, obj_dict):
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        bch = self._obj_uuid_cf.batch()

        obj_cols = {}
        obj_cols['fq_name'] = json.dumps(obj_dict['fq_name'])
        obj_cols['type'] = json.dumps('global_vrouter_config')
        if 'parent_type' in obj_dict:
            # non config-root child
            parent_type = obj_dict['parent_type']
            parent_method_type = parent_type.replace('-', '_')
            parent_fq_name = obj_dict['fq_name'][:-1]
            obj_cols['parent_type'] = json.dumps(parent_type)
            parent_uuid = self.fq_name_to_uuid(parent_method_type, parent_fq_name)
            self._create_child(bch, parent_method_type, parent_uuid, 'global_vrouter_config', obj_ids['uuid'])

        # Properties
        field = obj_dict.get('linklocal_services', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'linklocal_services', field)

        field = obj_dict.get('encapsulation_priorities', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'encapsulation_priorities', field)

        field = obj_dict.get('vxlan_network_identifier_mode', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'vxlan_network_identifier_mode', field)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            field['created'] = datetime.datetime.utcnow().isoformat()
            field['last_modified'] = field['created']
            self._create_prop(bch, obj_ids['uuid'], 'id_perms', field)

        field = obj_dict.get('display_name', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'display_name', field)


        # References

        bch.insert(obj_ids['uuid'], obj_cols)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(obj_dict['fq_name'])
        fq_name_cols = {utils.encode_string(fq_name_str) + ':' + obj_ids['uuid']: json.dumps(None)}
        self._obj_fq_name_cf.insert('global_vrouter_config', fq_name_cols)

        return (True, '')
    #end _cassandra_global_vrouter_config_create

    def _cassandra_global_vrouter_config_read(self, obj_uuids, field_names = None):
        # if field_names = None, all fields will be read/returned

        obj_uuid_cf = self._obj_uuid_cf

        # optimize for common case of reading non-backref, non-children fields
        # ignoring columns starting from 'b' and 'c' - significant performance
        # impact in scaled setting. e.g. read of project
        if (field_names is None or
            (set(field_names) & (GlobalVrouterConfig.backref_fields | GlobalVrouterConfig.children_fields))):
            # atleast one backref/children field is needed
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_count = 10000000,
                                       include_timestamp = True)
        else: # ignore reading backref + children columns
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_start = 'd',
                                       column_count = 10000000,
                                       include_timestamp = True)

        if (len(obj_uuids) == 1) and not obj_rows:
            raise cfgm_common.exceptions.NoIdError(obj_uuids[0])

        results = []
        for row_key in obj_rows:
            # give chance for zk heartbeat/ping
            gevent.sleep(0)
            obj_uuid = row_key
            obj_cols = obj_rows[obj_uuid]
            result = {}
            result['uuid'] = obj_uuid
            result['fq_name'] = json.loads(obj_cols['fq_name'][0])
            for col_name in obj_cols.keys():
                if self._re_match_parent.match(col_name):
                    # non config-root child
                    (_, _, parent_uuid) = col_name.split(':')
                    parent_type = json.loads(obj_cols['parent_type'][0])
                    result['parent_type'] = parent_type
                    try:
                        result['parent_uuid'] = parent_uuid
                        result['parent_href'] = self._generate_url(parent_type, parent_uuid)
                    except cfgm_common.exceptions.NoIdError:
                        err_msg = 'Unknown uuid for parent ' + result['fq_name'][-2]
                        return (False, err_msg)

                # TODO use compiled RE
                if self._re_match_prop.match(col_name):
                    (_, prop_name) = col_name.split(':')
                    result[prop_name] = json.loads(obj_cols[col_name][0])

                # TODO use compiled RE
                if self._re_match_children.match(col_name):
                    (_, child_type, child_uuid) = col_name.split(':')
                    if field_names and '%ss' %(child_type) not in field_names:
                        continue

                    child_tstamp = obj_cols[col_name][1]
                    try:
                        self._read_child(result, obj_uuid, child_type, child_uuid, child_tstamp)
                    except cfgm_common.exceptions.NoIdError:
                        continue

                # TODO use compiled RE
                if self._re_match_ref.match(col_name):
                    (_, ref_type, ref_uuid) = col_name.split(':')
                    self._read_ref(result, obj_uuid, ref_type, ref_uuid, obj_cols[col_name][0])

                if self._re_match_backref.match(col_name):
                    (_, back_ref_type, back_ref_uuid) = col_name.split(':')
                    if field_names and '%s_back_refs' %(back_ref_type) not in field_names:
                        continue

                    try:
                        self._read_back_ref(result, obj_uuid, back_ref_type, back_ref_uuid,
                                            obj_cols[col_name][0])
                    except cfgm_common.exceptions.NoIdError:
                        continue

            # for all column names


            results.append(result)
        # end for all rows

        return (True, results)
    #end _cassandra_global_vrouter_config_read

    def _cassandra_global_vrouter_config_count_children(self, obj_uuid, child_type):
        # if child_type = None, return
        if child_type is None:
            return (False, '')

        obj_uuid_cf = self._obj_uuid_cf
        if child_type not in GlobalVrouterConfig.children_fields:
            return (False, '%s is not a valid children of GlobalVrouterConfig' %(child_type))

        col_start = 'children:'+child_type[:-1]+':'
        col_finish = 'children:'+child_type[:-1]+';'
        num_children = obj_uuid_cf.get_count(obj_uuid,
                                   column_start = col_start,
                                   column_finish = col_finish,
                                   max_count = 10000000)
        return (True, num_children)
    #end _cassandra_global_vrouter_config_count_children

    def _cassandra_global_vrouter_config_update(self, obj_uuid, new_obj_dict):
        # Grab ref-uuids and properties in new version
        new_ref_infos = {}

        new_props = {}
        if 'linklocal_services' in new_obj_dict:
            new_props['linklocal_services'] = new_obj_dict['linklocal_services']
        if 'encapsulation_priorities' in new_obj_dict:
            new_props['encapsulation_priorities'] = new_obj_dict['encapsulation_priorities']
        if 'vxlan_network_identifier_mode' in new_obj_dict:
            new_props['vxlan_network_identifier_mode'] = new_obj_dict['vxlan_network_identifier_mode']
        if 'id_perms' in new_obj_dict:
            new_props['id_perms'] = new_obj_dict['id_perms']
        if 'display_name' in new_obj_dict:
            new_props['display_name'] = new_obj_dict['display_name']
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        obj_uuid_cf = self._obj_uuid_cf
        obj_cols_iter = obj_uuid_cf.xget(obj_uuid)
        # TODO optimize this (converts tuple to dict)
        obj_cols = {}
        for col_info in obj_cols_iter:
            obj_cols[col_info[0]] = col_info[1]

        bch = obj_uuid_cf.batch()
        for col_name in obj_cols.keys():
            # TODO use compiled RE
            if re.match('prop:', col_name):
                (_, prop_name) = col_name.split(':')
                if prop_name == 'id_perms':
                    # id-perms always has to be updated for last-mod timestamp
                    # get it from request dict(or from db if not in request dict)
                    new_id_perms = new_obj_dict.get(prop_name, json.loads(obj_cols[col_name]))
                    self.update_last_modified(bch, obj_uuid, new_id_perms)
                elif prop_name in new_obj_dict:
                    self._update_prop(bch, obj_uuid, prop_name, new_props)

            # TODO use compiled RE
            if re.match('ref:', col_name):
                (_, ref_type, ref_uuid) = col_name.split(':')
                self._update_ref(bch, 'global_vrouter_config', obj_uuid, ref_type, ref_uuid, new_ref_infos)
        # for all column names

        # create new refs
        for ref_type in new_ref_infos.keys():
            for ref_uuid in new_ref_infos[ref_type].keys():
                ref_data = new_ref_infos[ref_type][ref_uuid]
                self._create_ref(bch, 'global_vrouter_config', obj_uuid, ref_type, ref_uuid, ref_data)

        # create new props
        for prop_name in new_props.keys():
            self._create_prop(bch, obj_uuid, prop_name, new_props[prop_name])

        bch.send()

        return (True, '')
    #end _cassandra_global_vrouter_config_update

    def _cassandra_global_vrouter_config_list(self, parent_uuids=None, back_ref_uuids=None,
                           obj_uuids=None, count=False, filters=None):
        children_fq_names_uuids = []
        if filters:
            fnames = filters.get('field_names', [])
            fvalues = filters.get('field_values', [])
            filter_fields = [(fnames[i], fvalues[i]) for i in range(len(fnames))]
        else:
            filter_fields = []

        def filter_rows(coll_infos, filter_cols, filter_params):
            filt_infos = {}
            coll_rows = obj_uuid_cf.multiget(coll_infos.keys(),
                                   columns=filter_cols,
                                   column_count=10000000)
            for row in coll_rows:
                # give chance for zk heartbeat/ping
                gevent.sleep(0)
                full_match = True
                for fname, fval in filter_params:
                    if coll_rows[row]['prop:%s' %(fname)] != fval:
                        full_match = False
                        break
                if full_match:
                    filt_infos[row] = coll_infos[row]
            return filt_infos
        # end filter_rows

        def get_fq_name_uuid_list(obj_uuids):
            ret_list = []
            for obj_uuid in obj_uuids:
                try:
                    obj_fq_name = self.uuid_to_fq_name(obj_uuid)
                    ret_list.append((obj_fq_name, obj_uuid))
                except cfgm_common.exceptions.NoIdError:
                    pass
            return ret_list
        # end get_fq_name_uuid_list

        if parent_uuids:
            # go from parent to child
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'children:global_vrouter_config:'
            col_fin = 'children:global_vrouter_config;'
            try:
                obj_rows = obj_uuid_cf.multiget(parent_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_parent_anchor(sort=False):
                # flatten to [('children:<type>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_child_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    child_uuid = col_name.split(':')[2]
                    if obj_uuids and child_uuid not in obj_uuids:
                        continue
                    all_child_infos[child_uuid] = {'uuid': child_uuid, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_child_infos = filter_rows(all_child_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_child_infos = all_child_infos

                if not sort:
                    ret_child_infos = filt_child_infos.values()
                else:
                    ret_child_infos = sorted(filt_child_infos.values(), key=itemgetter('tstamp'))

                return get_fq_name_uuid_list(r['uuid'] for r in ret_child_infos)
            # end filter_rows_parent_anchor

            if count:
                return (True, len(filter_rows_parent_anchor()))

            children_fq_names_uuids = filter_rows_parent_anchor(sort=True)

        if back_ref_uuids:
            # go from anchor to backrefs
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'backref:global_vrouter_config:'
            col_fin = 'backref:global_vrouter_config;'
            try:
                obj_rows = obj_uuid_cf.multiget(back_ref_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_backref_anchor():
                # flatten to [('<fqnstr>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_backref_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    col_name_arr = col_name.split(':')
                    fq_name = col_name_arr[:-1]
                    obj_uuid = col_name_arr[-1]
                    if obj_uuids and obj_uuid not in obj_uuids:
                        continue
                    all_backref_infos[obj_uuid] = \
                        {'uuid': obj_uuid, 'fq_name': fq_name, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_backref_infos = filter_rows(all_backref_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_backref_infos = all_backref_infos

                return [(br_info['fq_name'], br_info['uuid']) for br_info in filt_backref_infos.values()]
            # end filter_rows_backref_anchor

            if count:
                return (True, len(filter_rows_backref_anchor()))

            children_fq_names_uuids = filter_rows_backref_anchor()

        if not parent_uuids and not back_ref_uuids:
            obj_uuid_cf = self._obj_uuid_cf
            if obj_uuids:
                # exact objects specified
                def filter_rows_object_list():
                    all_obj_infos = {}
                    for obj_uuid in obj_uuids:
                        all_obj_infos[obj_uuid] = None

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return get_fq_name_uuid_list(filt_obj_infos.keys())
                # end filter_rows_object_list

                if count:
                    return (True, len(filter_rows_object_list()))
                children_fq_names_uuids = filter_rows_object_list()

            else: # grab all resources of this type
                obj_fq_name_cf = self._obj_fq_name_cf
                try:
                    cols = obj_fq_name_cf.get('global_vrouter_config', column_count = 10000000)
                except pycassa.NotFoundException:
                    if count:
                        return (True, 0)
                    else:
                        return (True, children_fq_names_uuids)

                def filter_rows_no_anchor():
                    all_obj_infos = {}
                    for col_name, col_val in cols.items():
                        # give chance for zk heartbeat/ping
                        gevent.sleep(0)
                        col_name_arr = utils.decode_string(col_name).split(':')
                        obj_uuid = col_name_arr[-1]
                        all_obj_infos[obj_uuid] = (col_name_arr[:-1], obj_uuid)

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return filt_obj_infos.values()
                # end filter_rows_no_anchor

                if count:
                    return (True, len(filter_rows_no_anchor()))

                children_fq_names_uuids = filter_rows_no_anchor()

        return (True, children_fq_names_uuids)
    #end _cassandra_global_vrouter_config_list

    def _cassandra_global_vrouter_config_delete(self, obj_uuid):
        obj_uuid_cf = self._obj_uuid_cf
        fq_name = json.loads(obj_uuid_cf.get(obj_uuid, columns = ['fq_name'])['fq_name'])
        bch = obj_uuid_cf.batch()

        # unlink from parent
        col_start = 'parent:'
        col_fin = 'parent;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, parent_type, parent_uuid) = col_name.split(':')
            self._delete_child(bch, parent_type, parent_uuid, 'global_vrouter_config', obj_uuid)

        # remove refs
        col_start = 'ref:'
        col_fin = 'ref;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, ref_type, ref_uuid) = col_name.split(':')
            self._delete_ref(bch, 'global_vrouter_config', obj_uuid, ref_type, ref_uuid)

        bch.remove(obj_uuid)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(fq_name)
        fq_name_col = utils.encode_string(fq_name_str) + ':' + obj_uuid
        self._obj_fq_name_cf.remove('global_vrouter_config', columns = [fq_name_col])


        return (True, '')
    #end _cassandra_global_vrouter_config_delete

    def _cassandra_instance_ip_alloc(self, fq_name):
        return (True, '')
    #end _cassandra_instance_ip_alloc

    def _cassandra_instance_ip_create(self, obj_ids, obj_dict):
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        bch = self._obj_uuid_cf.batch()

        obj_cols = {}
        obj_cols['fq_name'] = json.dumps(obj_dict['fq_name'])
        obj_cols['type'] = json.dumps('instance_ip')

        # Properties
        field = obj_dict.get('instance_ip_address', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'instance_ip_address', field)

        field = obj_dict.get('instance_ip_family', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'instance_ip_family', field)

        field = obj_dict.get('instance_ip_mode', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'instance_ip_mode', field)

        field = obj_dict.get('subnet_uuid', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'subnet_uuid', field)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            field['created'] = datetime.datetime.utcnow().isoformat()
            field['last_modified'] = field['created']
            self._create_prop(bch, obj_ids['uuid'], 'id_perms', field)

        field = obj_dict.get('display_name', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'display_name', field)


        # References
        refs = obj_dict.get('virtual_network_refs', [])
        for ref in refs:
            ref_uuid = self.fq_name_to_uuid('virtual_network', ref['to'])
            ref_attr = ref.get('attr', None)
            ref_data = {'attr': ref_attr, 'is_weakref': False}
            self._create_ref(bch, 'instance_ip', obj_ids['uuid'], 'virtual_network', ref_uuid, ref_data)
        refs = obj_dict.get('virtual_machine_interface_refs', [])
        for ref in refs:
            ref_uuid = self.fq_name_to_uuid('virtual_machine_interface', ref['to'])
            ref_attr = ref.get('attr', None)
            ref_data = {'attr': ref_attr, 'is_weakref': False}
            self._create_ref(bch, 'instance_ip', obj_ids['uuid'], 'virtual_machine_interface', ref_uuid, ref_data)

        bch.insert(obj_ids['uuid'], obj_cols)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(obj_dict['fq_name'])
        fq_name_cols = {utils.encode_string(fq_name_str) + ':' + obj_ids['uuid']: json.dumps(None)}
        self._obj_fq_name_cf.insert('instance_ip', fq_name_cols)

        return (True, '')
    #end _cassandra_instance_ip_create

    def _cassandra_instance_ip_read(self, obj_uuids, field_names = None):
        # if field_names = None, all fields will be read/returned

        obj_uuid_cf = self._obj_uuid_cf

        # optimize for common case of reading non-backref, non-children fields
        # ignoring columns starting from 'b' and 'c' - significant performance
        # impact in scaled setting. e.g. read of project
        if (field_names is None or
            (set(field_names) & (InstanceIp.backref_fields | InstanceIp.children_fields))):
            # atleast one backref/children field is needed
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_count = 10000000,
                                       include_timestamp = True)
        else: # ignore reading backref + children columns
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_start = 'd',
                                       column_count = 10000000,
                                       include_timestamp = True)

        if (len(obj_uuids) == 1) and not obj_rows:
            raise cfgm_common.exceptions.NoIdError(obj_uuids[0])

        results = []
        for row_key in obj_rows:
            # give chance for zk heartbeat/ping
            gevent.sleep(0)
            obj_uuid = row_key
            obj_cols = obj_rows[obj_uuid]
            result = {}
            result['uuid'] = obj_uuid
            result['fq_name'] = json.loads(obj_cols['fq_name'][0])
            for col_name in obj_cols.keys():
                if self._re_match_parent.match(col_name):
                    # non config-root child
                    (_, _, parent_uuid) = col_name.split(':')
                    parent_type = json.loads(obj_cols['parent_type'][0])
                    result['parent_type'] = parent_type
                    try:
                        result['parent_uuid'] = parent_uuid
                        result['parent_href'] = self._generate_url(parent_type, parent_uuid)
                    except cfgm_common.exceptions.NoIdError:
                        err_msg = 'Unknown uuid for parent ' + result['fq_name'][-2]
                        return (False, err_msg)

                # TODO use compiled RE
                if self._re_match_prop.match(col_name):
                    (_, prop_name) = col_name.split(':')
                    result[prop_name] = json.loads(obj_cols[col_name][0])

                # TODO use compiled RE
                if self._re_match_children.match(col_name):
                    (_, child_type, child_uuid) = col_name.split(':')
                    if field_names and '%ss' %(child_type) not in field_names:
                        continue

                    child_tstamp = obj_cols[col_name][1]
                    try:
                        self._read_child(result, obj_uuid, child_type, child_uuid, child_tstamp)
                    except cfgm_common.exceptions.NoIdError:
                        continue

                # TODO use compiled RE
                if self._re_match_ref.match(col_name):
                    (_, ref_type, ref_uuid) = col_name.split(':')
                    self._read_ref(result, obj_uuid, ref_type, ref_uuid, obj_cols[col_name][0])

                if self._re_match_backref.match(col_name):
                    (_, back_ref_type, back_ref_uuid) = col_name.split(':')
                    if field_names and '%s_back_refs' %(back_ref_type) not in field_names:
                        continue

                    try:
                        self._read_back_ref(result, obj_uuid, back_ref_type, back_ref_uuid,
                                            obj_cols[col_name][0])
                    except cfgm_common.exceptions.NoIdError:
                        continue

            # for all column names


            results.append(result)
        # end for all rows

        return (True, results)
    #end _cassandra_instance_ip_read

    def _cassandra_instance_ip_count_children(self, obj_uuid, child_type):
        # if child_type = None, return
        if child_type is None:
            return (False, '')

        obj_uuid_cf = self._obj_uuid_cf
        if child_type not in InstanceIp.children_fields:
            return (False, '%s is not a valid children of InstanceIp' %(child_type))

        col_start = 'children:'+child_type[:-1]+':'
        col_finish = 'children:'+child_type[:-1]+';'
        num_children = obj_uuid_cf.get_count(obj_uuid,
                                   column_start = col_start,
                                   column_finish = col_finish,
                                   max_count = 10000000)
        return (True, num_children)
    #end _cassandra_instance_ip_count_children

    def _cassandra_instance_ip_update(self, obj_uuid, new_obj_dict):
        # Grab ref-uuids and properties in new version
        new_ref_infos = {}

        if 'virtual_network_refs' in new_obj_dict:
            new_ref_infos['virtual_network'] = {}
            new_refs = new_obj_dict['virtual_network_refs']
            if new_refs:
                for new_ref in new_refs:
                    new_ref_uuid = self.fq_name_to_uuid('virtual_network', new_ref['to'])
                    new_ref_attr = new_ref.get('attr', None)
                    new_ref_data = {'attr': new_ref_attr, 'is_weakref': False}
                    new_ref_infos['virtual_network'][new_ref_uuid] = new_ref_data

        if 'virtual_machine_interface_refs' in new_obj_dict:
            new_ref_infos['virtual_machine_interface'] = {}
            new_refs = new_obj_dict['virtual_machine_interface_refs']
            if new_refs:
                for new_ref in new_refs:
                    new_ref_uuid = self.fq_name_to_uuid('virtual_machine_interface', new_ref['to'])
                    new_ref_attr = new_ref.get('attr', None)
                    new_ref_data = {'attr': new_ref_attr, 'is_weakref': False}
                    new_ref_infos['virtual_machine_interface'][new_ref_uuid] = new_ref_data

        new_props = {}
        if 'instance_ip_address' in new_obj_dict:
            new_props['instance_ip_address'] = new_obj_dict['instance_ip_address']
        if 'instance_ip_family' in new_obj_dict:
            new_props['instance_ip_family'] = new_obj_dict['instance_ip_family']
        if 'instance_ip_mode' in new_obj_dict:
            new_props['instance_ip_mode'] = new_obj_dict['instance_ip_mode']
        if 'subnet_uuid' in new_obj_dict:
            new_props['subnet_uuid'] = new_obj_dict['subnet_uuid']
        if 'id_perms' in new_obj_dict:
            new_props['id_perms'] = new_obj_dict['id_perms']
        if 'display_name' in new_obj_dict:
            new_props['display_name'] = new_obj_dict['display_name']
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        obj_uuid_cf = self._obj_uuid_cf
        obj_cols_iter = obj_uuid_cf.xget(obj_uuid)
        # TODO optimize this (converts tuple to dict)
        obj_cols = {}
        for col_info in obj_cols_iter:
            obj_cols[col_info[0]] = col_info[1]

        bch = obj_uuid_cf.batch()
        for col_name in obj_cols.keys():
            # TODO use compiled RE
            if re.match('prop:', col_name):
                (_, prop_name) = col_name.split(':')
                if prop_name == 'id_perms':
                    # id-perms always has to be updated for last-mod timestamp
                    # get it from request dict(or from db if not in request dict)
                    new_id_perms = new_obj_dict.get(prop_name, json.loads(obj_cols[col_name]))
                    self.update_last_modified(bch, obj_uuid, new_id_perms)
                elif prop_name in new_obj_dict:
                    self._update_prop(bch, obj_uuid, prop_name, new_props)

            # TODO use compiled RE
            if re.match('ref:', col_name):
                (_, ref_type, ref_uuid) = col_name.split(':')
                self._update_ref(bch, 'instance_ip', obj_uuid, ref_type, ref_uuid, new_ref_infos)
        # for all column names

        # create new refs
        for ref_type in new_ref_infos.keys():
            for ref_uuid in new_ref_infos[ref_type].keys():
                ref_data = new_ref_infos[ref_type][ref_uuid]
                self._create_ref(bch, 'instance_ip', obj_uuid, ref_type, ref_uuid, ref_data)

        # create new props
        for prop_name in new_props.keys():
            self._create_prop(bch, obj_uuid, prop_name, new_props[prop_name])

        bch.send()

        return (True, '')
    #end _cassandra_instance_ip_update

    def _cassandra_instance_ip_list(self, parent_uuids=None, back_ref_uuids=None,
                           obj_uuids=None, count=False, filters=None):
        children_fq_names_uuids = []
        if filters:
            fnames = filters.get('field_names', [])
            fvalues = filters.get('field_values', [])
            filter_fields = [(fnames[i], fvalues[i]) for i in range(len(fnames))]
        else:
            filter_fields = []

        def filter_rows(coll_infos, filter_cols, filter_params):
            filt_infos = {}
            coll_rows = obj_uuid_cf.multiget(coll_infos.keys(),
                                   columns=filter_cols,
                                   column_count=10000000)
            for row in coll_rows:
                # give chance for zk heartbeat/ping
                gevent.sleep(0)
                full_match = True
                for fname, fval in filter_params:
                    if coll_rows[row]['prop:%s' %(fname)] != fval:
                        full_match = False
                        break
                if full_match:
                    filt_infos[row] = coll_infos[row]
            return filt_infos
        # end filter_rows

        def get_fq_name_uuid_list(obj_uuids):
            ret_list = []
            for obj_uuid in obj_uuids:
                try:
                    obj_fq_name = self.uuid_to_fq_name(obj_uuid)
                    ret_list.append((obj_fq_name, obj_uuid))
                except cfgm_common.exceptions.NoIdError:
                    pass
            return ret_list
        # end get_fq_name_uuid_list

        if parent_uuids:
            # go from parent to child
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'children:instance_ip:'
            col_fin = 'children:instance_ip;'
            try:
                obj_rows = obj_uuid_cf.multiget(parent_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_parent_anchor(sort=False):
                # flatten to [('children:<type>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_child_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    child_uuid = col_name.split(':')[2]
                    if obj_uuids and child_uuid not in obj_uuids:
                        continue
                    all_child_infos[child_uuid] = {'uuid': child_uuid, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_child_infos = filter_rows(all_child_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_child_infos = all_child_infos

                if not sort:
                    ret_child_infos = filt_child_infos.values()
                else:
                    ret_child_infos = sorted(filt_child_infos.values(), key=itemgetter('tstamp'))

                return get_fq_name_uuid_list(r['uuid'] for r in ret_child_infos)
            # end filter_rows_parent_anchor

            if count:
                return (True, len(filter_rows_parent_anchor()))

            children_fq_names_uuids = filter_rows_parent_anchor(sort=True)

        if back_ref_uuids:
            # go from anchor to backrefs
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'backref:instance_ip:'
            col_fin = 'backref:instance_ip;'
            try:
                obj_rows = obj_uuid_cf.multiget(back_ref_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_backref_anchor():
                # flatten to [('<fqnstr>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_backref_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    col_name_arr = col_name.split(':')
                    fq_name = col_name_arr[:-1]
                    obj_uuid = col_name_arr[-1]
                    if obj_uuids and obj_uuid not in obj_uuids:
                        continue
                    all_backref_infos[obj_uuid] = \
                        {'uuid': obj_uuid, 'fq_name': fq_name, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_backref_infos = filter_rows(all_backref_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_backref_infos = all_backref_infos

                return [(br_info['fq_name'], br_info['uuid']) for br_info in filt_backref_infos.values()]
            # end filter_rows_backref_anchor

            if count:
                return (True, len(filter_rows_backref_anchor()))

            children_fq_names_uuids = filter_rows_backref_anchor()

        if not parent_uuids and not back_ref_uuids:
            obj_uuid_cf = self._obj_uuid_cf
            if obj_uuids:
                # exact objects specified
                def filter_rows_object_list():
                    all_obj_infos = {}
                    for obj_uuid in obj_uuids:
                        all_obj_infos[obj_uuid] = None

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return get_fq_name_uuid_list(filt_obj_infos.keys())
                # end filter_rows_object_list

                if count:
                    return (True, len(filter_rows_object_list()))
                children_fq_names_uuids = filter_rows_object_list()

            else: # grab all resources of this type
                obj_fq_name_cf = self._obj_fq_name_cf
                try:
                    cols = obj_fq_name_cf.get('instance_ip', column_count = 10000000)
                except pycassa.NotFoundException:
                    if count:
                        return (True, 0)
                    else:
                        return (True, children_fq_names_uuids)

                def filter_rows_no_anchor():
                    all_obj_infos = {}
                    for col_name, col_val in cols.items():
                        # give chance for zk heartbeat/ping
                        gevent.sleep(0)
                        col_name_arr = utils.decode_string(col_name).split(':')
                        obj_uuid = col_name_arr[-1]
                        all_obj_infos[obj_uuid] = (col_name_arr[:-1], obj_uuid)

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return filt_obj_infos.values()
                # end filter_rows_no_anchor

                if count:
                    return (True, len(filter_rows_no_anchor()))

                children_fq_names_uuids = filter_rows_no_anchor()

        return (True, children_fq_names_uuids)
    #end _cassandra_instance_ip_list

    def _cassandra_instance_ip_delete(self, obj_uuid):
        obj_uuid_cf = self._obj_uuid_cf
        fq_name = json.loads(obj_uuid_cf.get(obj_uuid, columns = ['fq_name'])['fq_name'])
        bch = obj_uuid_cf.batch()

        # unlink from parent
        col_start = 'parent:'
        col_fin = 'parent;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, parent_type, parent_uuid) = col_name.split(':')
            self._delete_child(bch, parent_type, parent_uuid, 'instance_ip', obj_uuid)

        # remove refs
        col_start = 'ref:'
        col_fin = 'ref;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, ref_type, ref_uuid) = col_name.split(':')
            self._delete_ref(bch, 'instance_ip', obj_uuid, ref_type, ref_uuid)

        bch.remove(obj_uuid)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(fq_name)
        fq_name_col = utils.encode_string(fq_name_str) + ':' + obj_uuid
        self._obj_fq_name_cf.remove('instance_ip', columns = [fq_name_col])


        return (True, '')
    #end _cassandra_instance_ip_delete

    def _cassandra_network_policy_alloc(self, fq_name):
        return (True, '')
    #end _cassandra_network_policy_alloc

    def _cassandra_network_policy_create(self, obj_ids, obj_dict):
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        bch = self._obj_uuid_cf.batch()

        obj_cols = {}
        obj_cols['fq_name'] = json.dumps(obj_dict['fq_name'])
        obj_cols['type'] = json.dumps('network_policy')
        if 'parent_type' in obj_dict:
            # non config-root child
            parent_type = obj_dict['parent_type']
            parent_method_type = parent_type.replace('-', '_')
            parent_fq_name = obj_dict['fq_name'][:-1]
            obj_cols['parent_type'] = json.dumps(parent_type)
            parent_uuid = self.fq_name_to_uuid(parent_method_type, parent_fq_name)
            self._create_child(bch, parent_method_type, parent_uuid, 'network_policy', obj_ids['uuid'])

        # Properties
        field = obj_dict.get('network_policy_entries', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'network_policy_entries', field)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            field['created'] = datetime.datetime.utcnow().isoformat()
            field['last_modified'] = field['created']
            self._create_prop(bch, obj_ids['uuid'], 'id_perms', field)

        field = obj_dict.get('display_name', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'display_name', field)


        # References

        bch.insert(obj_ids['uuid'], obj_cols)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(obj_dict['fq_name'])
        fq_name_cols = {utils.encode_string(fq_name_str) + ':' + obj_ids['uuid']: json.dumps(None)}
        self._obj_fq_name_cf.insert('network_policy', fq_name_cols)

        return (True, '')
    #end _cassandra_network_policy_create

    def _cassandra_network_policy_read(self, obj_uuids, field_names = None):
        # if field_names = None, all fields will be read/returned

        obj_uuid_cf = self._obj_uuid_cf

        # optimize for common case of reading non-backref, non-children fields
        # ignoring columns starting from 'b' and 'c' - significant performance
        # impact in scaled setting. e.g. read of project
        if (field_names is None or
            (set(field_names) & (NetworkPolicy.backref_fields | NetworkPolicy.children_fields))):
            # atleast one backref/children field is needed
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_count = 10000000,
                                       include_timestamp = True)
        else: # ignore reading backref + children columns
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_start = 'd',
                                       column_count = 10000000,
                                       include_timestamp = True)

        if (len(obj_uuids) == 1) and not obj_rows:
            raise cfgm_common.exceptions.NoIdError(obj_uuids[0])

        results = []
        for row_key in obj_rows:
            # give chance for zk heartbeat/ping
            gevent.sleep(0)
            obj_uuid = row_key
            obj_cols = obj_rows[obj_uuid]
            result = {}
            result['uuid'] = obj_uuid
            result['fq_name'] = json.loads(obj_cols['fq_name'][0])
            for col_name in obj_cols.keys():
                if self._re_match_parent.match(col_name):
                    # non config-root child
                    (_, _, parent_uuid) = col_name.split(':')
                    parent_type = json.loads(obj_cols['parent_type'][0])
                    result['parent_type'] = parent_type
                    try:
                        result['parent_uuid'] = parent_uuid
                        result['parent_href'] = self._generate_url(parent_type, parent_uuid)
                    except cfgm_common.exceptions.NoIdError:
                        err_msg = 'Unknown uuid for parent ' + result['fq_name'][-2]
                        return (False, err_msg)

                # TODO use compiled RE
                if self._re_match_prop.match(col_name):
                    (_, prop_name) = col_name.split(':')
                    result[prop_name] = json.loads(obj_cols[col_name][0])

                # TODO use compiled RE
                if self._re_match_children.match(col_name):
                    (_, child_type, child_uuid) = col_name.split(':')
                    if field_names and '%ss' %(child_type) not in field_names:
                        continue

                    child_tstamp = obj_cols[col_name][1]
                    try:
                        self._read_child(result, obj_uuid, child_type, child_uuid, child_tstamp)
                    except cfgm_common.exceptions.NoIdError:
                        continue

                # TODO use compiled RE
                if self._re_match_ref.match(col_name):
                    (_, ref_type, ref_uuid) = col_name.split(':')
                    self._read_ref(result, obj_uuid, ref_type, ref_uuid, obj_cols[col_name][0])

                if self._re_match_backref.match(col_name):
                    (_, back_ref_type, back_ref_uuid) = col_name.split(':')
                    if field_names and '%s_back_refs' %(back_ref_type) not in field_names:
                        continue

                    try:
                        self._read_back_ref(result, obj_uuid, back_ref_type, back_ref_uuid,
                                            obj_cols[col_name][0])
                    except cfgm_common.exceptions.NoIdError:
                        continue

            # for all column names


            results.append(result)
        # end for all rows

        return (True, results)
    #end _cassandra_network_policy_read

    def _cassandra_network_policy_count_children(self, obj_uuid, child_type):
        # if child_type = None, return
        if child_type is None:
            return (False, '')

        obj_uuid_cf = self._obj_uuid_cf
        if child_type not in NetworkPolicy.children_fields:
            return (False, '%s is not a valid children of NetworkPolicy' %(child_type))

        col_start = 'children:'+child_type[:-1]+':'
        col_finish = 'children:'+child_type[:-1]+';'
        num_children = obj_uuid_cf.get_count(obj_uuid,
                                   column_start = col_start,
                                   column_finish = col_finish,
                                   max_count = 10000000)
        return (True, num_children)
    #end _cassandra_network_policy_count_children

    def _cassandra_network_policy_update(self, obj_uuid, new_obj_dict):
        # Grab ref-uuids and properties in new version
        new_ref_infos = {}

        new_props = {}
        if 'network_policy_entries' in new_obj_dict:
            new_props['network_policy_entries'] = new_obj_dict['network_policy_entries']
        if 'id_perms' in new_obj_dict:
            new_props['id_perms'] = new_obj_dict['id_perms']
        if 'display_name' in new_obj_dict:
            new_props['display_name'] = new_obj_dict['display_name']
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        obj_uuid_cf = self._obj_uuid_cf
        obj_cols_iter = obj_uuid_cf.xget(obj_uuid)
        # TODO optimize this (converts tuple to dict)
        obj_cols = {}
        for col_info in obj_cols_iter:
            obj_cols[col_info[0]] = col_info[1]

        bch = obj_uuid_cf.batch()
        for col_name in obj_cols.keys():
            # TODO use compiled RE
            if re.match('prop:', col_name):
                (_, prop_name) = col_name.split(':')
                if prop_name == 'id_perms':
                    # id-perms always has to be updated for last-mod timestamp
                    # get it from request dict(or from db if not in request dict)
                    new_id_perms = new_obj_dict.get(prop_name, json.loads(obj_cols[col_name]))
                    self.update_last_modified(bch, obj_uuid, new_id_perms)
                elif prop_name in new_obj_dict:
                    self._update_prop(bch, obj_uuid, prop_name, new_props)

            # TODO use compiled RE
            if re.match('ref:', col_name):
                (_, ref_type, ref_uuid) = col_name.split(':')
                self._update_ref(bch, 'network_policy', obj_uuid, ref_type, ref_uuid, new_ref_infos)
        # for all column names

        # create new refs
        for ref_type in new_ref_infos.keys():
            for ref_uuid in new_ref_infos[ref_type].keys():
                ref_data = new_ref_infos[ref_type][ref_uuid]
                self._create_ref(bch, 'network_policy', obj_uuid, ref_type, ref_uuid, ref_data)

        # create new props
        for prop_name in new_props.keys():
            self._create_prop(bch, obj_uuid, prop_name, new_props[prop_name])

        bch.send()

        return (True, '')
    #end _cassandra_network_policy_update

    def _cassandra_network_policy_list(self, parent_uuids=None, back_ref_uuids=None,
                           obj_uuids=None, count=False, filters=None):
        children_fq_names_uuids = []
        if filters:
            fnames = filters.get('field_names', [])
            fvalues = filters.get('field_values', [])
            filter_fields = [(fnames[i], fvalues[i]) for i in range(len(fnames))]
        else:
            filter_fields = []

        def filter_rows(coll_infos, filter_cols, filter_params):
            filt_infos = {}
            coll_rows = obj_uuid_cf.multiget(coll_infos.keys(),
                                   columns=filter_cols,
                                   column_count=10000000)
            for row in coll_rows:
                # give chance for zk heartbeat/ping
                gevent.sleep(0)
                full_match = True
                for fname, fval in filter_params:
                    if coll_rows[row]['prop:%s' %(fname)] != fval:
                        full_match = False
                        break
                if full_match:
                    filt_infos[row] = coll_infos[row]
            return filt_infos
        # end filter_rows

        def get_fq_name_uuid_list(obj_uuids):
            ret_list = []
            for obj_uuid in obj_uuids:
                try:
                    obj_fq_name = self.uuid_to_fq_name(obj_uuid)
                    ret_list.append((obj_fq_name, obj_uuid))
                except cfgm_common.exceptions.NoIdError:
                    pass
            return ret_list
        # end get_fq_name_uuid_list

        if parent_uuids:
            # go from parent to child
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'children:network_policy:'
            col_fin = 'children:network_policy;'
            try:
                obj_rows = obj_uuid_cf.multiget(parent_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_parent_anchor(sort=False):
                # flatten to [('children:<type>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_child_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    child_uuid = col_name.split(':')[2]
                    if obj_uuids and child_uuid not in obj_uuids:
                        continue
                    all_child_infos[child_uuid] = {'uuid': child_uuid, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_child_infos = filter_rows(all_child_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_child_infos = all_child_infos

                if not sort:
                    ret_child_infos = filt_child_infos.values()
                else:
                    ret_child_infos = sorted(filt_child_infos.values(), key=itemgetter('tstamp'))

                return get_fq_name_uuid_list(r['uuid'] for r in ret_child_infos)
            # end filter_rows_parent_anchor

            if count:
                return (True, len(filter_rows_parent_anchor()))

            children_fq_names_uuids = filter_rows_parent_anchor(sort=True)

        if back_ref_uuids:
            # go from anchor to backrefs
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'backref:network_policy:'
            col_fin = 'backref:network_policy;'
            try:
                obj_rows = obj_uuid_cf.multiget(back_ref_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_backref_anchor():
                # flatten to [('<fqnstr>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_backref_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    col_name_arr = col_name.split(':')
                    fq_name = col_name_arr[:-1]
                    obj_uuid = col_name_arr[-1]
                    if obj_uuids and obj_uuid not in obj_uuids:
                        continue
                    all_backref_infos[obj_uuid] = \
                        {'uuid': obj_uuid, 'fq_name': fq_name, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_backref_infos = filter_rows(all_backref_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_backref_infos = all_backref_infos

                return [(br_info['fq_name'], br_info['uuid']) for br_info in filt_backref_infos.values()]
            # end filter_rows_backref_anchor

            if count:
                return (True, len(filter_rows_backref_anchor()))

            children_fq_names_uuids = filter_rows_backref_anchor()

        if not parent_uuids and not back_ref_uuids:
            obj_uuid_cf = self._obj_uuid_cf
            if obj_uuids:
                # exact objects specified
                def filter_rows_object_list():
                    all_obj_infos = {}
                    for obj_uuid in obj_uuids:
                        all_obj_infos[obj_uuid] = None

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return get_fq_name_uuid_list(filt_obj_infos.keys())
                # end filter_rows_object_list

                if count:
                    return (True, len(filter_rows_object_list()))
                children_fq_names_uuids = filter_rows_object_list()

            else: # grab all resources of this type
                obj_fq_name_cf = self._obj_fq_name_cf
                try:
                    cols = obj_fq_name_cf.get('network_policy', column_count = 10000000)
                except pycassa.NotFoundException:
                    if count:
                        return (True, 0)
                    else:
                        return (True, children_fq_names_uuids)

                def filter_rows_no_anchor():
                    all_obj_infos = {}
                    for col_name, col_val in cols.items():
                        # give chance for zk heartbeat/ping
                        gevent.sleep(0)
                        col_name_arr = utils.decode_string(col_name).split(':')
                        obj_uuid = col_name_arr[-1]
                        all_obj_infos[obj_uuid] = (col_name_arr[:-1], obj_uuid)

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return filt_obj_infos.values()
                # end filter_rows_no_anchor

                if count:
                    return (True, len(filter_rows_no_anchor()))

                children_fq_names_uuids = filter_rows_no_anchor()

        return (True, children_fq_names_uuids)
    #end _cassandra_network_policy_list

    def _cassandra_network_policy_delete(self, obj_uuid):
        obj_uuid_cf = self._obj_uuid_cf
        fq_name = json.loads(obj_uuid_cf.get(obj_uuid, columns = ['fq_name'])['fq_name'])
        bch = obj_uuid_cf.batch()

        # unlink from parent
        col_start = 'parent:'
        col_fin = 'parent;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, parent_type, parent_uuid) = col_name.split(':')
            self._delete_child(bch, parent_type, parent_uuid, 'network_policy', obj_uuid)

        # remove refs
        col_start = 'ref:'
        col_fin = 'ref;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, ref_type, ref_uuid) = col_name.split(':')
            self._delete_ref(bch, 'network_policy', obj_uuid, ref_type, ref_uuid)

        bch.remove(obj_uuid)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(fq_name)
        fq_name_col = utils.encode_string(fq_name_str) + ':' + obj_uuid
        self._obj_fq_name_cf.remove('network_policy', columns = [fq_name_col])


        return (True, '')
    #end _cassandra_network_policy_delete

    def _cassandra_loadbalancer_pool_alloc(self, fq_name):
        return (True, '')
    #end _cassandra_loadbalancer_pool_alloc

    def _cassandra_loadbalancer_pool_create(self, obj_ids, obj_dict):
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        bch = self._obj_uuid_cf.batch()

        obj_cols = {}
        obj_cols['fq_name'] = json.dumps(obj_dict['fq_name'])
        obj_cols['type'] = json.dumps('loadbalancer_pool')
        if 'parent_type' in obj_dict:
            # non config-root child
            parent_type = obj_dict['parent_type']
            parent_method_type = parent_type.replace('-', '_')
            parent_fq_name = obj_dict['fq_name'][:-1]
            obj_cols['parent_type'] = json.dumps(parent_type)
            parent_uuid = self.fq_name_to_uuid(parent_method_type, parent_fq_name)
            self._create_child(bch, parent_method_type, parent_uuid, 'loadbalancer_pool', obj_ids['uuid'])

        # Properties
        field = obj_dict.get('loadbalancer_pool_properties', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'loadbalancer_pool_properties', field)

        field = obj_dict.get('loadbalancer_pool_provider', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'loadbalancer_pool_provider', field)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            field['created'] = datetime.datetime.utcnow().isoformat()
            field['last_modified'] = field['created']
            self._create_prop(bch, obj_ids['uuid'], 'id_perms', field)

        field = obj_dict.get('display_name', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'display_name', field)


        # References
        refs = obj_dict.get('service_instance_refs', [])
        for ref in refs:
            ref_uuid = self.fq_name_to_uuid('service_instance', ref['to'])
            ref_attr = ref.get('attr', None)
            ref_data = {'attr': ref_attr, 'is_weakref': False}
            self._create_ref(bch, 'loadbalancer_pool', obj_ids['uuid'], 'service_instance', ref_uuid, ref_data)
        refs = obj_dict.get('virtual_machine_interface_refs', [])
        for ref in refs:
            ref_uuid = self.fq_name_to_uuid('virtual_machine_interface', ref['to'])
            ref_attr = ref.get('attr', None)
            ref_data = {'attr': ref_attr, 'is_weakref': False}
            self._create_ref(bch, 'loadbalancer_pool', obj_ids['uuid'], 'virtual_machine_interface', ref_uuid, ref_data)
        refs = obj_dict.get('service_appliance_set_refs', [])
        for ref in refs:
            ref_uuid = self.fq_name_to_uuid('service_appliance_set', ref['to'])
            ref_attr = ref.get('attr', None)
            ref_data = {'attr': ref_attr, 'is_weakref': False}
            self._create_ref(bch, 'loadbalancer_pool', obj_ids['uuid'], 'service_appliance_set', ref_uuid, ref_data)
        refs = obj_dict.get('loadbalancer_healthmonitor_refs', [])
        for ref in refs:
            ref_uuid = self.fq_name_to_uuid('loadbalancer_healthmonitor', ref['to'])
            ref_attr = ref.get('attr', None)
            ref_data = {'attr': ref_attr, 'is_weakref': False}
            self._create_ref(bch, 'loadbalancer_pool', obj_ids['uuid'], 'loadbalancer_healthmonitor', ref_uuid, ref_data)

        bch.insert(obj_ids['uuid'], obj_cols)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(obj_dict['fq_name'])
        fq_name_cols = {utils.encode_string(fq_name_str) + ':' + obj_ids['uuid']: json.dumps(None)}
        self._obj_fq_name_cf.insert('loadbalancer_pool', fq_name_cols)

        return (True, '')
    #end _cassandra_loadbalancer_pool_create

    def _cassandra_loadbalancer_pool_read(self, obj_uuids, field_names = None):
        # if field_names = None, all fields will be read/returned

        obj_uuid_cf = self._obj_uuid_cf

        # optimize for common case of reading non-backref, non-children fields
        # ignoring columns starting from 'b' and 'c' - significant performance
        # impact in scaled setting. e.g. read of project
        if (field_names is None or
            (set(field_names) & (LoadbalancerPool.backref_fields | LoadbalancerPool.children_fields))):
            # atleast one backref/children field is needed
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_count = 10000000,
                                       include_timestamp = True)
        else: # ignore reading backref + children columns
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_start = 'd',
                                       column_count = 10000000,
                                       include_timestamp = True)

        if (len(obj_uuids) == 1) and not obj_rows:
            raise cfgm_common.exceptions.NoIdError(obj_uuids[0])

        results = []
        for row_key in obj_rows:
            # give chance for zk heartbeat/ping
            gevent.sleep(0)
            obj_uuid = row_key
            obj_cols = obj_rows[obj_uuid]
            result = {}
            result['uuid'] = obj_uuid
            result['fq_name'] = json.loads(obj_cols['fq_name'][0])
            for col_name in obj_cols.keys():
                if self._re_match_parent.match(col_name):
                    # non config-root child
                    (_, _, parent_uuid) = col_name.split(':')
                    parent_type = json.loads(obj_cols['parent_type'][0])
                    result['parent_type'] = parent_type
                    try:
                        result['parent_uuid'] = parent_uuid
                        result['parent_href'] = self._generate_url(parent_type, parent_uuid)
                    except cfgm_common.exceptions.NoIdError:
                        err_msg = 'Unknown uuid for parent ' + result['fq_name'][-2]
                        return (False, err_msg)

                # TODO use compiled RE
                if self._re_match_prop.match(col_name):
                    (_, prop_name) = col_name.split(':')
                    result[prop_name] = json.loads(obj_cols[col_name][0])

                # TODO use compiled RE
                if self._re_match_children.match(col_name):
                    (_, child_type, child_uuid) = col_name.split(':')
                    if field_names and '%ss' %(child_type) not in field_names:
                        continue

                    child_tstamp = obj_cols[col_name][1]
                    try:
                        self._read_child(result, obj_uuid, child_type, child_uuid, child_tstamp)
                    except cfgm_common.exceptions.NoIdError:
                        continue

                # TODO use compiled RE
                if self._re_match_ref.match(col_name):
                    (_, ref_type, ref_uuid) = col_name.split(':')
                    self._read_ref(result, obj_uuid, ref_type, ref_uuid, obj_cols[col_name][0])

                if self._re_match_backref.match(col_name):
                    (_, back_ref_type, back_ref_uuid) = col_name.split(':')
                    if field_names and '%s_back_refs' %(back_ref_type) not in field_names:
                        continue

                    try:
                        self._read_back_ref(result, obj_uuid, back_ref_type, back_ref_uuid,
                                            obj_cols[col_name][0])
                    except cfgm_common.exceptions.NoIdError:
                        continue

            # for all column names

            if 'loadbalancer_members' in result:
                # sort children; TODO do this based on schema
                sorted_children = sorted(result['loadbalancer_members'], key = itemgetter('tstamp'))
                # re-write result's children without timestamp
                result['loadbalancer_members'] = sorted_children
                [child.pop('tstamp') for child in result['loadbalancer_members']]

            results.append(result)
        # end for all rows

        return (True, results)
    #end _cassandra_loadbalancer_pool_read

    def _cassandra_loadbalancer_pool_count_children(self, obj_uuid, child_type):
        # if child_type = None, return
        if child_type is None:
            return (False, '')

        obj_uuid_cf = self._obj_uuid_cf
        if child_type not in LoadbalancerPool.children_fields:
            return (False, '%s is not a valid children of LoadbalancerPool' %(child_type))

        col_start = 'children:'+child_type[:-1]+':'
        col_finish = 'children:'+child_type[:-1]+';'
        num_children = obj_uuid_cf.get_count(obj_uuid,
                                   column_start = col_start,
                                   column_finish = col_finish,
                                   max_count = 10000000)
        return (True, num_children)
    #end _cassandra_loadbalancer_pool_count_children

    def _cassandra_loadbalancer_pool_update(self, obj_uuid, new_obj_dict):
        # Grab ref-uuids and properties in new version
        new_ref_infos = {}

        if 'service_instance_refs' in new_obj_dict:
            new_ref_infos['service_instance'] = {}
            new_refs = new_obj_dict['service_instance_refs']
            if new_refs:
                for new_ref in new_refs:
                    new_ref_uuid = self.fq_name_to_uuid('service_instance', new_ref['to'])
                    new_ref_attr = new_ref.get('attr', None)
                    new_ref_data = {'attr': new_ref_attr, 'is_weakref': False}
                    new_ref_infos['service_instance'][new_ref_uuid] = new_ref_data

        if 'virtual_machine_interface_refs' in new_obj_dict:
            new_ref_infos['virtual_machine_interface'] = {}
            new_refs = new_obj_dict['virtual_machine_interface_refs']
            if new_refs:
                for new_ref in new_refs:
                    new_ref_uuid = self.fq_name_to_uuid('virtual_machine_interface', new_ref['to'])
                    new_ref_attr = new_ref.get('attr', None)
                    new_ref_data = {'attr': new_ref_attr, 'is_weakref': False}
                    new_ref_infos['virtual_machine_interface'][new_ref_uuid] = new_ref_data

        if 'service_appliance_set_refs' in new_obj_dict:
            new_ref_infos['service_appliance_set'] = {}
            new_refs = new_obj_dict['service_appliance_set_refs']
            if new_refs:
                for new_ref in new_refs:
                    new_ref_uuid = self.fq_name_to_uuid('service_appliance_set', new_ref['to'])
                    new_ref_attr = new_ref.get('attr', None)
                    new_ref_data = {'attr': new_ref_attr, 'is_weakref': False}
                    new_ref_infos['service_appliance_set'][new_ref_uuid] = new_ref_data

        if 'loadbalancer_healthmonitor_refs' in new_obj_dict:
            new_ref_infos['loadbalancer_healthmonitor'] = {}
            new_refs = new_obj_dict['loadbalancer_healthmonitor_refs']
            if new_refs:
                for new_ref in new_refs:
                    new_ref_uuid = self.fq_name_to_uuid('loadbalancer_healthmonitor', new_ref['to'])
                    new_ref_attr = new_ref.get('attr', None)
                    new_ref_data = {'attr': new_ref_attr, 'is_weakref': False}
                    new_ref_infos['loadbalancer_healthmonitor'][new_ref_uuid] = new_ref_data

        new_props = {}
        if 'loadbalancer_pool_properties' in new_obj_dict:
            new_props['loadbalancer_pool_properties'] = new_obj_dict['loadbalancer_pool_properties']
        if 'loadbalancer_pool_provider' in new_obj_dict:
            new_props['loadbalancer_pool_provider'] = new_obj_dict['loadbalancer_pool_provider']
        if 'id_perms' in new_obj_dict:
            new_props['id_perms'] = new_obj_dict['id_perms']
        if 'display_name' in new_obj_dict:
            new_props['display_name'] = new_obj_dict['display_name']
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        obj_uuid_cf = self._obj_uuid_cf
        obj_cols_iter = obj_uuid_cf.xget(obj_uuid)
        # TODO optimize this (converts tuple to dict)
        obj_cols = {}
        for col_info in obj_cols_iter:
            obj_cols[col_info[0]] = col_info[1]

        bch = obj_uuid_cf.batch()
        for col_name in obj_cols.keys():
            # TODO use compiled RE
            if re.match('prop:', col_name):
                (_, prop_name) = col_name.split(':')
                if prop_name == 'id_perms':
                    # id-perms always has to be updated for last-mod timestamp
                    # get it from request dict(or from db if not in request dict)
                    new_id_perms = new_obj_dict.get(prop_name, json.loads(obj_cols[col_name]))
                    self.update_last_modified(bch, obj_uuid, new_id_perms)
                elif prop_name in new_obj_dict:
                    self._update_prop(bch, obj_uuid, prop_name, new_props)

            # TODO use compiled RE
            if re.match('ref:', col_name):
                (_, ref_type, ref_uuid) = col_name.split(':')
                self._update_ref(bch, 'loadbalancer_pool', obj_uuid, ref_type, ref_uuid, new_ref_infos)
        # for all column names

        # create new refs
        for ref_type in new_ref_infos.keys():
            for ref_uuid in new_ref_infos[ref_type].keys():
                ref_data = new_ref_infos[ref_type][ref_uuid]
                self._create_ref(bch, 'loadbalancer_pool', obj_uuid, ref_type, ref_uuid, ref_data)

        # create new props
        for prop_name in new_props.keys():
            self._create_prop(bch, obj_uuid, prop_name, new_props[prop_name])

        bch.send()

        return (True, '')
    #end _cassandra_loadbalancer_pool_update

    def _cassandra_loadbalancer_pool_list(self, parent_uuids=None, back_ref_uuids=None,
                           obj_uuids=None, count=False, filters=None):
        children_fq_names_uuids = []
        if filters:
            fnames = filters.get('field_names', [])
            fvalues = filters.get('field_values', [])
            filter_fields = [(fnames[i], fvalues[i]) for i in range(len(fnames))]
        else:
            filter_fields = []

        def filter_rows(coll_infos, filter_cols, filter_params):
            filt_infos = {}
            coll_rows = obj_uuid_cf.multiget(coll_infos.keys(),
                                   columns=filter_cols,
                                   column_count=10000000)
            for row in coll_rows:
                # give chance for zk heartbeat/ping
                gevent.sleep(0)
                full_match = True
                for fname, fval in filter_params:
                    if coll_rows[row]['prop:%s' %(fname)] != fval:
                        full_match = False
                        break
                if full_match:
                    filt_infos[row] = coll_infos[row]
            return filt_infos
        # end filter_rows

        def get_fq_name_uuid_list(obj_uuids):
            ret_list = []
            for obj_uuid in obj_uuids:
                try:
                    obj_fq_name = self.uuid_to_fq_name(obj_uuid)
                    ret_list.append((obj_fq_name, obj_uuid))
                except cfgm_common.exceptions.NoIdError:
                    pass
            return ret_list
        # end get_fq_name_uuid_list

        if parent_uuids:
            # go from parent to child
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'children:loadbalancer_pool:'
            col_fin = 'children:loadbalancer_pool;'
            try:
                obj_rows = obj_uuid_cf.multiget(parent_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_parent_anchor(sort=False):
                # flatten to [('children:<type>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_child_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    child_uuid = col_name.split(':')[2]
                    if obj_uuids and child_uuid not in obj_uuids:
                        continue
                    all_child_infos[child_uuid] = {'uuid': child_uuid, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_child_infos = filter_rows(all_child_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_child_infos = all_child_infos

                if not sort:
                    ret_child_infos = filt_child_infos.values()
                else:
                    ret_child_infos = sorted(filt_child_infos.values(), key=itemgetter('tstamp'))

                return get_fq_name_uuid_list(r['uuid'] for r in ret_child_infos)
            # end filter_rows_parent_anchor

            if count:
                return (True, len(filter_rows_parent_anchor()))

            children_fq_names_uuids = filter_rows_parent_anchor(sort=True)

        if back_ref_uuids:
            # go from anchor to backrefs
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'backref:loadbalancer_pool:'
            col_fin = 'backref:loadbalancer_pool;'
            try:
                obj_rows = obj_uuid_cf.multiget(back_ref_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_backref_anchor():
                # flatten to [('<fqnstr>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_backref_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    col_name_arr = col_name.split(':')
                    fq_name = col_name_arr[:-1]
                    obj_uuid = col_name_arr[-1]
                    if obj_uuids and obj_uuid not in obj_uuids:
                        continue
                    all_backref_infos[obj_uuid] = \
                        {'uuid': obj_uuid, 'fq_name': fq_name, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_backref_infos = filter_rows(all_backref_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_backref_infos = all_backref_infos

                return [(br_info['fq_name'], br_info['uuid']) for br_info in filt_backref_infos.values()]
            # end filter_rows_backref_anchor

            if count:
                return (True, len(filter_rows_backref_anchor()))

            children_fq_names_uuids = filter_rows_backref_anchor()

        if not parent_uuids and not back_ref_uuids:
            obj_uuid_cf = self._obj_uuid_cf
            if obj_uuids:
                # exact objects specified
                def filter_rows_object_list():
                    all_obj_infos = {}
                    for obj_uuid in obj_uuids:
                        all_obj_infos[obj_uuid] = None

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return get_fq_name_uuid_list(filt_obj_infos.keys())
                # end filter_rows_object_list

                if count:
                    return (True, len(filter_rows_object_list()))
                children_fq_names_uuids = filter_rows_object_list()

            else: # grab all resources of this type
                obj_fq_name_cf = self._obj_fq_name_cf
                try:
                    cols = obj_fq_name_cf.get('loadbalancer_pool', column_count = 10000000)
                except pycassa.NotFoundException:
                    if count:
                        return (True, 0)
                    else:
                        return (True, children_fq_names_uuids)

                def filter_rows_no_anchor():
                    all_obj_infos = {}
                    for col_name, col_val in cols.items():
                        # give chance for zk heartbeat/ping
                        gevent.sleep(0)
                        col_name_arr = utils.decode_string(col_name).split(':')
                        obj_uuid = col_name_arr[-1]
                        all_obj_infos[obj_uuid] = (col_name_arr[:-1], obj_uuid)

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return filt_obj_infos.values()
                # end filter_rows_no_anchor

                if count:
                    return (True, len(filter_rows_no_anchor()))

                children_fq_names_uuids = filter_rows_no_anchor()

        return (True, children_fq_names_uuids)
    #end _cassandra_loadbalancer_pool_list

    def _cassandra_loadbalancer_pool_delete(self, obj_uuid):
        obj_uuid_cf = self._obj_uuid_cf
        fq_name = json.loads(obj_uuid_cf.get(obj_uuid, columns = ['fq_name'])['fq_name'])
        bch = obj_uuid_cf.batch()

        # unlink from parent
        col_start = 'parent:'
        col_fin = 'parent;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, parent_type, parent_uuid) = col_name.split(':')
            self._delete_child(bch, parent_type, parent_uuid, 'loadbalancer_pool', obj_uuid)

        # remove refs
        col_start = 'ref:'
        col_fin = 'ref;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, ref_type, ref_uuid) = col_name.split(':')
            self._delete_ref(bch, 'loadbalancer_pool', obj_uuid, ref_type, ref_uuid)

        bch.remove(obj_uuid)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(fq_name)
        fq_name_col = utils.encode_string(fq_name_str) + ':' + obj_uuid
        self._obj_fq_name_cf.remove('loadbalancer_pool', columns = [fq_name_col])


        return (True, '')
    #end _cassandra_loadbalancer_pool_delete

    def _cassandra_virtual_DNS_record_alloc(self, fq_name):
        return (True, '')
    #end _cassandra_virtual_DNS_record_alloc

    def _cassandra_virtual_DNS_record_create(self, obj_ids, obj_dict):
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        bch = self._obj_uuid_cf.batch()

        obj_cols = {}
        obj_cols['fq_name'] = json.dumps(obj_dict['fq_name'])
        obj_cols['type'] = json.dumps('virtual_DNS_record')
        if 'parent_type' in obj_dict:
            # non config-root child
            parent_type = obj_dict['parent_type']
            parent_method_type = parent_type.replace('-', '_')
            parent_fq_name = obj_dict['fq_name'][:-1]
            obj_cols['parent_type'] = json.dumps(parent_type)
            parent_uuid = self.fq_name_to_uuid(parent_method_type, parent_fq_name)
            self._create_child(bch, parent_method_type, parent_uuid, 'virtual_DNS_record', obj_ids['uuid'])

        # Properties
        field = obj_dict.get('virtual_DNS_record_data', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'virtual_DNS_record_data', field)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            field['created'] = datetime.datetime.utcnow().isoformat()
            field['last_modified'] = field['created']
            self._create_prop(bch, obj_ids['uuid'], 'id_perms', field)

        field = obj_dict.get('display_name', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'display_name', field)


        # References

        bch.insert(obj_ids['uuid'], obj_cols)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(obj_dict['fq_name'])
        fq_name_cols = {utils.encode_string(fq_name_str) + ':' + obj_ids['uuid']: json.dumps(None)}
        self._obj_fq_name_cf.insert('virtual_DNS_record', fq_name_cols)

        return (True, '')
    #end _cassandra_virtual_DNS_record_create

    def _cassandra_virtual_DNS_record_read(self, obj_uuids, field_names = None):
        # if field_names = None, all fields will be read/returned

        obj_uuid_cf = self._obj_uuid_cf

        # optimize for common case of reading non-backref, non-children fields
        # ignoring columns starting from 'b' and 'c' - significant performance
        # impact in scaled setting. e.g. read of project
        if (field_names is None or
            (set(field_names) & (VirtualDnsRecord.backref_fields | VirtualDnsRecord.children_fields))):
            # atleast one backref/children field is needed
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_count = 10000000,
                                       include_timestamp = True)
        else: # ignore reading backref + children columns
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_start = 'd',
                                       column_count = 10000000,
                                       include_timestamp = True)

        if (len(obj_uuids) == 1) and not obj_rows:
            raise cfgm_common.exceptions.NoIdError(obj_uuids[0])

        results = []
        for row_key in obj_rows:
            # give chance for zk heartbeat/ping
            gevent.sleep(0)
            obj_uuid = row_key
            obj_cols = obj_rows[obj_uuid]
            result = {}
            result['uuid'] = obj_uuid
            result['fq_name'] = json.loads(obj_cols['fq_name'][0])
            for col_name in obj_cols.keys():
                if self._re_match_parent.match(col_name):
                    # non config-root child
                    (_, _, parent_uuid) = col_name.split(':')
                    parent_type = json.loads(obj_cols['parent_type'][0])
                    result['parent_type'] = parent_type
                    try:
                        result['parent_uuid'] = parent_uuid
                        result['parent_href'] = self._generate_url(parent_type, parent_uuid)
                    except cfgm_common.exceptions.NoIdError:
                        err_msg = 'Unknown uuid for parent ' + result['fq_name'][-2]
                        return (False, err_msg)

                # TODO use compiled RE
                if self._re_match_prop.match(col_name):
                    (_, prop_name) = col_name.split(':')
                    result[prop_name] = json.loads(obj_cols[col_name][0])

                # TODO use compiled RE
                if self._re_match_children.match(col_name):
                    (_, child_type, child_uuid) = col_name.split(':')
                    if field_names and '%ss' %(child_type) not in field_names:
                        continue

                    child_tstamp = obj_cols[col_name][1]
                    try:
                        self._read_child(result, obj_uuid, child_type, child_uuid, child_tstamp)
                    except cfgm_common.exceptions.NoIdError:
                        continue

                # TODO use compiled RE
                if self._re_match_ref.match(col_name):
                    (_, ref_type, ref_uuid) = col_name.split(':')
                    self._read_ref(result, obj_uuid, ref_type, ref_uuid, obj_cols[col_name][0])

                if self._re_match_backref.match(col_name):
                    (_, back_ref_type, back_ref_uuid) = col_name.split(':')
                    if field_names and '%s_back_refs' %(back_ref_type) not in field_names:
                        continue

                    try:
                        self._read_back_ref(result, obj_uuid, back_ref_type, back_ref_uuid,
                                            obj_cols[col_name][0])
                    except cfgm_common.exceptions.NoIdError:
                        continue

            # for all column names


            results.append(result)
        # end for all rows

        return (True, results)
    #end _cassandra_virtual_DNS_record_read

    def _cassandra_virtual_DNS_record_count_children(self, obj_uuid, child_type):
        # if child_type = None, return
        if child_type is None:
            return (False, '')

        obj_uuid_cf = self._obj_uuid_cf
        if child_type not in VirtualDnsRecord.children_fields:
            return (False, '%s is not a valid children of VirtualDnsRecord' %(child_type))

        col_start = 'children:'+child_type[:-1]+':'
        col_finish = 'children:'+child_type[:-1]+';'
        num_children = obj_uuid_cf.get_count(obj_uuid,
                                   column_start = col_start,
                                   column_finish = col_finish,
                                   max_count = 10000000)
        return (True, num_children)
    #end _cassandra_virtual_DNS_record_count_children

    def _cassandra_virtual_DNS_record_update(self, obj_uuid, new_obj_dict):
        # Grab ref-uuids and properties in new version
        new_ref_infos = {}

        new_props = {}
        if 'virtual_DNS_record_data' in new_obj_dict:
            new_props['virtual_DNS_record_data'] = new_obj_dict['virtual_DNS_record_data']
        if 'id_perms' in new_obj_dict:
            new_props['id_perms'] = new_obj_dict['id_perms']
        if 'display_name' in new_obj_dict:
            new_props['display_name'] = new_obj_dict['display_name']
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        obj_uuid_cf = self._obj_uuid_cf
        obj_cols_iter = obj_uuid_cf.xget(obj_uuid)
        # TODO optimize this (converts tuple to dict)
        obj_cols = {}
        for col_info in obj_cols_iter:
            obj_cols[col_info[0]] = col_info[1]

        bch = obj_uuid_cf.batch()
        for col_name in obj_cols.keys():
            # TODO use compiled RE
            if re.match('prop:', col_name):
                (_, prop_name) = col_name.split(':')
                if prop_name == 'id_perms':
                    # id-perms always has to be updated for last-mod timestamp
                    # get it from request dict(or from db if not in request dict)
                    new_id_perms = new_obj_dict.get(prop_name, json.loads(obj_cols[col_name]))
                    self.update_last_modified(bch, obj_uuid, new_id_perms)
                elif prop_name in new_obj_dict:
                    self._update_prop(bch, obj_uuid, prop_name, new_props)

            # TODO use compiled RE
            if re.match('ref:', col_name):
                (_, ref_type, ref_uuid) = col_name.split(':')
                self._update_ref(bch, 'virtual_DNS_record', obj_uuid, ref_type, ref_uuid, new_ref_infos)
        # for all column names

        # create new refs
        for ref_type in new_ref_infos.keys():
            for ref_uuid in new_ref_infos[ref_type].keys():
                ref_data = new_ref_infos[ref_type][ref_uuid]
                self._create_ref(bch, 'virtual_DNS_record', obj_uuid, ref_type, ref_uuid, ref_data)

        # create new props
        for prop_name in new_props.keys():
            self._create_prop(bch, obj_uuid, prop_name, new_props[prop_name])

        bch.send()

        return (True, '')
    #end _cassandra_virtual_DNS_record_update

    def _cassandra_virtual_DNS_record_list(self, parent_uuids=None, back_ref_uuids=None,
                           obj_uuids=None, count=False, filters=None):
        children_fq_names_uuids = []
        if filters:
            fnames = filters.get('field_names', [])
            fvalues = filters.get('field_values', [])
            filter_fields = [(fnames[i], fvalues[i]) for i in range(len(fnames))]
        else:
            filter_fields = []

        def filter_rows(coll_infos, filter_cols, filter_params):
            filt_infos = {}
            coll_rows = obj_uuid_cf.multiget(coll_infos.keys(),
                                   columns=filter_cols,
                                   column_count=10000000)
            for row in coll_rows:
                # give chance for zk heartbeat/ping
                gevent.sleep(0)
                full_match = True
                for fname, fval in filter_params:
                    if coll_rows[row]['prop:%s' %(fname)] != fval:
                        full_match = False
                        break
                if full_match:
                    filt_infos[row] = coll_infos[row]
            return filt_infos
        # end filter_rows

        def get_fq_name_uuid_list(obj_uuids):
            ret_list = []
            for obj_uuid in obj_uuids:
                try:
                    obj_fq_name = self.uuid_to_fq_name(obj_uuid)
                    ret_list.append((obj_fq_name, obj_uuid))
                except cfgm_common.exceptions.NoIdError:
                    pass
            return ret_list
        # end get_fq_name_uuid_list

        if parent_uuids:
            # go from parent to child
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'children:virtual_DNS_record:'
            col_fin = 'children:virtual_DNS_record;'
            try:
                obj_rows = obj_uuid_cf.multiget(parent_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_parent_anchor(sort=False):
                # flatten to [('children:<type>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_child_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    child_uuid = col_name.split(':')[2]
                    if obj_uuids and child_uuid not in obj_uuids:
                        continue
                    all_child_infos[child_uuid] = {'uuid': child_uuid, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_child_infos = filter_rows(all_child_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_child_infos = all_child_infos

                if not sort:
                    ret_child_infos = filt_child_infos.values()
                else:
                    ret_child_infos = sorted(filt_child_infos.values(), key=itemgetter('tstamp'))

                return get_fq_name_uuid_list(r['uuid'] for r in ret_child_infos)
            # end filter_rows_parent_anchor

            if count:
                return (True, len(filter_rows_parent_anchor()))

            children_fq_names_uuids = filter_rows_parent_anchor(sort=True)

        if back_ref_uuids:
            # go from anchor to backrefs
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'backref:virtual_DNS_record:'
            col_fin = 'backref:virtual_DNS_record;'
            try:
                obj_rows = obj_uuid_cf.multiget(back_ref_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_backref_anchor():
                # flatten to [('<fqnstr>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_backref_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    col_name_arr = col_name.split(':')
                    fq_name = col_name_arr[:-1]
                    obj_uuid = col_name_arr[-1]
                    if obj_uuids and obj_uuid not in obj_uuids:
                        continue
                    all_backref_infos[obj_uuid] = \
                        {'uuid': obj_uuid, 'fq_name': fq_name, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_backref_infos = filter_rows(all_backref_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_backref_infos = all_backref_infos

                return [(br_info['fq_name'], br_info['uuid']) for br_info in filt_backref_infos.values()]
            # end filter_rows_backref_anchor

            if count:
                return (True, len(filter_rows_backref_anchor()))

            children_fq_names_uuids = filter_rows_backref_anchor()

        if not parent_uuids and not back_ref_uuids:
            obj_uuid_cf = self._obj_uuid_cf
            if obj_uuids:
                # exact objects specified
                def filter_rows_object_list():
                    all_obj_infos = {}
                    for obj_uuid in obj_uuids:
                        all_obj_infos[obj_uuid] = None

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return get_fq_name_uuid_list(filt_obj_infos.keys())
                # end filter_rows_object_list

                if count:
                    return (True, len(filter_rows_object_list()))
                children_fq_names_uuids = filter_rows_object_list()

            else: # grab all resources of this type
                obj_fq_name_cf = self._obj_fq_name_cf
                try:
                    cols = obj_fq_name_cf.get('virtual_DNS_record', column_count = 10000000)
                except pycassa.NotFoundException:
                    if count:
                        return (True, 0)
                    else:
                        return (True, children_fq_names_uuids)

                def filter_rows_no_anchor():
                    all_obj_infos = {}
                    for col_name, col_val in cols.items():
                        # give chance for zk heartbeat/ping
                        gevent.sleep(0)
                        col_name_arr = utils.decode_string(col_name).split(':')
                        obj_uuid = col_name_arr[-1]
                        all_obj_infos[obj_uuid] = (col_name_arr[:-1], obj_uuid)

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return filt_obj_infos.values()
                # end filter_rows_no_anchor

                if count:
                    return (True, len(filter_rows_no_anchor()))

                children_fq_names_uuids = filter_rows_no_anchor()

        return (True, children_fq_names_uuids)
    #end _cassandra_virtual_DNS_record_list

    def _cassandra_virtual_DNS_record_delete(self, obj_uuid):
        obj_uuid_cf = self._obj_uuid_cf
        fq_name = json.loads(obj_uuid_cf.get(obj_uuid, columns = ['fq_name'])['fq_name'])
        bch = obj_uuid_cf.batch()

        # unlink from parent
        col_start = 'parent:'
        col_fin = 'parent;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, parent_type, parent_uuid) = col_name.split(':')
            self._delete_child(bch, parent_type, parent_uuid, 'virtual_DNS_record', obj_uuid)

        # remove refs
        col_start = 'ref:'
        col_fin = 'ref;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, ref_type, ref_uuid) = col_name.split(':')
            self._delete_ref(bch, 'virtual_DNS_record', obj_uuid, ref_type, ref_uuid)

        bch.remove(obj_uuid)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(fq_name)
        fq_name_col = utils.encode_string(fq_name_str) + ':' + obj_uuid
        self._obj_fq_name_cf.remove('virtual_DNS_record', columns = [fq_name_col])


        return (True, '')
    #end _cassandra_virtual_DNS_record_delete

    def _cassandra_route_target_alloc(self, fq_name):
        return (True, '')
    #end _cassandra_route_target_alloc

    def _cassandra_route_target_create(self, obj_ids, obj_dict):
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        bch = self._obj_uuid_cf.batch()

        obj_cols = {}
        obj_cols['fq_name'] = json.dumps(obj_dict['fq_name'])
        obj_cols['type'] = json.dumps('route_target')

        # Properties
        field = obj_dict.get('id_perms', None)
        if field is not None:
            field['created'] = datetime.datetime.utcnow().isoformat()
            field['last_modified'] = field['created']
            self._create_prop(bch, obj_ids['uuid'], 'id_perms', field)

        field = obj_dict.get('display_name', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'display_name', field)


        # References

        bch.insert(obj_ids['uuid'], obj_cols)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(obj_dict['fq_name'])
        fq_name_cols = {utils.encode_string(fq_name_str) + ':' + obj_ids['uuid']: json.dumps(None)}
        self._obj_fq_name_cf.insert('route_target', fq_name_cols)

        return (True, '')
    #end _cassandra_route_target_create

    def _cassandra_route_target_read(self, obj_uuids, field_names = None):
        # if field_names = None, all fields will be read/returned

        obj_uuid_cf = self._obj_uuid_cf

        # optimize for common case of reading non-backref, non-children fields
        # ignoring columns starting from 'b' and 'c' - significant performance
        # impact in scaled setting. e.g. read of project
        if (field_names is None or
            (set(field_names) & (RouteTarget.backref_fields | RouteTarget.children_fields))):
            # atleast one backref/children field is needed
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_count = 10000000,
                                       include_timestamp = True)
        else: # ignore reading backref + children columns
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_start = 'd',
                                       column_count = 10000000,
                                       include_timestamp = True)

        if (len(obj_uuids) == 1) and not obj_rows:
            raise cfgm_common.exceptions.NoIdError(obj_uuids[0])

        results = []
        for row_key in obj_rows:
            # give chance for zk heartbeat/ping
            gevent.sleep(0)
            obj_uuid = row_key
            obj_cols = obj_rows[obj_uuid]
            result = {}
            result['uuid'] = obj_uuid
            result['fq_name'] = json.loads(obj_cols['fq_name'][0])
            for col_name in obj_cols.keys():
                if self._re_match_parent.match(col_name):
                    # non config-root child
                    (_, _, parent_uuid) = col_name.split(':')
                    parent_type = json.loads(obj_cols['parent_type'][0])
                    result['parent_type'] = parent_type
                    try:
                        result['parent_uuid'] = parent_uuid
                        result['parent_href'] = self._generate_url(parent_type, parent_uuid)
                    except cfgm_common.exceptions.NoIdError:
                        err_msg = 'Unknown uuid for parent ' + result['fq_name'][-2]
                        return (False, err_msg)

                # TODO use compiled RE
                if self._re_match_prop.match(col_name):
                    (_, prop_name) = col_name.split(':')
                    result[prop_name] = json.loads(obj_cols[col_name][0])

                # TODO use compiled RE
                if self._re_match_children.match(col_name):
                    (_, child_type, child_uuid) = col_name.split(':')
                    if field_names and '%ss' %(child_type) not in field_names:
                        continue

                    child_tstamp = obj_cols[col_name][1]
                    try:
                        self._read_child(result, obj_uuid, child_type, child_uuid, child_tstamp)
                    except cfgm_common.exceptions.NoIdError:
                        continue

                # TODO use compiled RE
                if self._re_match_ref.match(col_name):
                    (_, ref_type, ref_uuid) = col_name.split(':')
                    self._read_ref(result, obj_uuid, ref_type, ref_uuid, obj_cols[col_name][0])

                if self._re_match_backref.match(col_name):
                    (_, back_ref_type, back_ref_uuid) = col_name.split(':')
                    if field_names and '%s_back_refs' %(back_ref_type) not in field_names:
                        continue

                    try:
                        self._read_back_ref(result, obj_uuid, back_ref_type, back_ref_uuid,
                                            obj_cols[col_name][0])
                    except cfgm_common.exceptions.NoIdError:
                        continue

            # for all column names


            results.append(result)
        # end for all rows

        return (True, results)
    #end _cassandra_route_target_read

    def _cassandra_route_target_count_children(self, obj_uuid, child_type):
        # if child_type = None, return
        if child_type is None:
            return (False, '')

        obj_uuid_cf = self._obj_uuid_cf
        if child_type not in RouteTarget.children_fields:
            return (False, '%s is not a valid children of RouteTarget' %(child_type))

        col_start = 'children:'+child_type[:-1]+':'
        col_finish = 'children:'+child_type[:-1]+';'
        num_children = obj_uuid_cf.get_count(obj_uuid,
                                   column_start = col_start,
                                   column_finish = col_finish,
                                   max_count = 10000000)
        return (True, num_children)
    #end _cassandra_route_target_count_children

    def _cassandra_route_target_update(self, obj_uuid, new_obj_dict):
        # Grab ref-uuids and properties in new version
        new_ref_infos = {}

        new_props = {}
        if 'id_perms' in new_obj_dict:
            new_props['id_perms'] = new_obj_dict['id_perms']
        if 'display_name' in new_obj_dict:
            new_props['display_name'] = new_obj_dict['display_name']
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        obj_uuid_cf = self._obj_uuid_cf
        obj_cols_iter = obj_uuid_cf.xget(obj_uuid)
        # TODO optimize this (converts tuple to dict)
        obj_cols = {}
        for col_info in obj_cols_iter:
            obj_cols[col_info[0]] = col_info[1]

        bch = obj_uuid_cf.batch()
        for col_name in obj_cols.keys():
            # TODO use compiled RE
            if re.match('prop:', col_name):
                (_, prop_name) = col_name.split(':')
                if prop_name == 'id_perms':
                    # id-perms always has to be updated for last-mod timestamp
                    # get it from request dict(or from db if not in request dict)
                    new_id_perms = new_obj_dict.get(prop_name, json.loads(obj_cols[col_name]))
                    self.update_last_modified(bch, obj_uuid, new_id_perms)
                elif prop_name in new_obj_dict:
                    self._update_prop(bch, obj_uuid, prop_name, new_props)

            # TODO use compiled RE
            if re.match('ref:', col_name):
                (_, ref_type, ref_uuid) = col_name.split(':')
                self._update_ref(bch, 'route_target', obj_uuid, ref_type, ref_uuid, new_ref_infos)
        # for all column names

        # create new refs
        for ref_type in new_ref_infos.keys():
            for ref_uuid in new_ref_infos[ref_type].keys():
                ref_data = new_ref_infos[ref_type][ref_uuid]
                self._create_ref(bch, 'route_target', obj_uuid, ref_type, ref_uuid, ref_data)

        # create new props
        for prop_name in new_props.keys():
            self._create_prop(bch, obj_uuid, prop_name, new_props[prop_name])

        bch.send()

        return (True, '')
    #end _cassandra_route_target_update

    def _cassandra_route_target_list(self, parent_uuids=None, back_ref_uuids=None,
                           obj_uuids=None, count=False, filters=None):
        children_fq_names_uuids = []
        if filters:
            fnames = filters.get('field_names', [])
            fvalues = filters.get('field_values', [])
            filter_fields = [(fnames[i], fvalues[i]) for i in range(len(fnames))]
        else:
            filter_fields = []

        def filter_rows(coll_infos, filter_cols, filter_params):
            filt_infos = {}
            coll_rows = obj_uuid_cf.multiget(coll_infos.keys(),
                                   columns=filter_cols,
                                   column_count=10000000)
            for row in coll_rows:
                # give chance for zk heartbeat/ping
                gevent.sleep(0)
                full_match = True
                for fname, fval in filter_params:
                    if coll_rows[row]['prop:%s' %(fname)] != fval:
                        full_match = False
                        break
                if full_match:
                    filt_infos[row] = coll_infos[row]
            return filt_infos
        # end filter_rows

        def get_fq_name_uuid_list(obj_uuids):
            ret_list = []
            for obj_uuid in obj_uuids:
                try:
                    obj_fq_name = self.uuid_to_fq_name(obj_uuid)
                    ret_list.append((obj_fq_name, obj_uuid))
                except cfgm_common.exceptions.NoIdError:
                    pass
            return ret_list
        # end get_fq_name_uuid_list

        if parent_uuids:
            # go from parent to child
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'children:route_target:'
            col_fin = 'children:route_target;'
            try:
                obj_rows = obj_uuid_cf.multiget(parent_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_parent_anchor(sort=False):
                # flatten to [('children:<type>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_child_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    child_uuid = col_name.split(':')[2]
                    if obj_uuids and child_uuid not in obj_uuids:
                        continue
                    all_child_infos[child_uuid] = {'uuid': child_uuid, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_child_infos = filter_rows(all_child_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_child_infos = all_child_infos

                if not sort:
                    ret_child_infos = filt_child_infos.values()
                else:
                    ret_child_infos = sorted(filt_child_infos.values(), key=itemgetter('tstamp'))

                return get_fq_name_uuid_list(r['uuid'] for r in ret_child_infos)
            # end filter_rows_parent_anchor

            if count:
                return (True, len(filter_rows_parent_anchor()))

            children_fq_names_uuids = filter_rows_parent_anchor(sort=True)

        if back_ref_uuids:
            # go from anchor to backrefs
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'backref:route_target:'
            col_fin = 'backref:route_target;'
            try:
                obj_rows = obj_uuid_cf.multiget(back_ref_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_backref_anchor():
                # flatten to [('<fqnstr>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_backref_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    col_name_arr = col_name.split(':')
                    fq_name = col_name_arr[:-1]
                    obj_uuid = col_name_arr[-1]
                    if obj_uuids and obj_uuid not in obj_uuids:
                        continue
                    all_backref_infos[obj_uuid] = \
                        {'uuid': obj_uuid, 'fq_name': fq_name, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_backref_infos = filter_rows(all_backref_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_backref_infos = all_backref_infos

                return [(br_info['fq_name'], br_info['uuid']) for br_info in filt_backref_infos.values()]
            # end filter_rows_backref_anchor

            if count:
                return (True, len(filter_rows_backref_anchor()))

            children_fq_names_uuids = filter_rows_backref_anchor()

        if not parent_uuids and not back_ref_uuids:
            obj_uuid_cf = self._obj_uuid_cf
            if obj_uuids:
                # exact objects specified
                def filter_rows_object_list():
                    all_obj_infos = {}
                    for obj_uuid in obj_uuids:
                        all_obj_infos[obj_uuid] = None

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return get_fq_name_uuid_list(filt_obj_infos.keys())
                # end filter_rows_object_list

                if count:
                    return (True, len(filter_rows_object_list()))
                children_fq_names_uuids = filter_rows_object_list()

            else: # grab all resources of this type
                obj_fq_name_cf = self._obj_fq_name_cf
                try:
                    cols = obj_fq_name_cf.get('route_target', column_count = 10000000)
                except pycassa.NotFoundException:
                    if count:
                        return (True, 0)
                    else:
                        return (True, children_fq_names_uuids)

                def filter_rows_no_anchor():
                    all_obj_infos = {}
                    for col_name, col_val in cols.items():
                        # give chance for zk heartbeat/ping
                        gevent.sleep(0)
                        col_name_arr = utils.decode_string(col_name).split(':')
                        obj_uuid = col_name_arr[-1]
                        all_obj_infos[obj_uuid] = (col_name_arr[:-1], obj_uuid)

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return filt_obj_infos.values()
                # end filter_rows_no_anchor

                if count:
                    return (True, len(filter_rows_no_anchor()))

                children_fq_names_uuids = filter_rows_no_anchor()

        return (True, children_fq_names_uuids)
    #end _cassandra_route_target_list

    def _cassandra_route_target_delete(self, obj_uuid):
        obj_uuid_cf = self._obj_uuid_cf
        fq_name = json.loads(obj_uuid_cf.get(obj_uuid, columns = ['fq_name'])['fq_name'])
        bch = obj_uuid_cf.batch()

        # unlink from parent
        col_start = 'parent:'
        col_fin = 'parent;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, parent_type, parent_uuid) = col_name.split(':')
            self._delete_child(bch, parent_type, parent_uuid, 'route_target', obj_uuid)

        # remove refs
        col_start = 'ref:'
        col_fin = 'ref;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, ref_type, ref_uuid) = col_name.split(':')
            self._delete_ref(bch, 'route_target', obj_uuid, ref_type, ref_uuid)

        bch.remove(obj_uuid)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(fq_name)
        fq_name_col = utils.encode_string(fq_name_str) + ':' + obj_uuid
        self._obj_fq_name_cf.remove('route_target', columns = [fq_name_col])


        return (True, '')
    #end _cassandra_route_target_delete

    def _cassandra_floating_ip_alloc(self, fq_name):
        return (True, '')
    #end _cassandra_floating_ip_alloc

    def _cassandra_floating_ip_create(self, obj_ids, obj_dict):
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        bch = self._obj_uuid_cf.batch()

        obj_cols = {}
        obj_cols['fq_name'] = json.dumps(obj_dict['fq_name'])
        obj_cols['type'] = json.dumps('floating_ip')
        if 'parent_type' in obj_dict:
            # non config-root child
            parent_type = obj_dict['parent_type']
            parent_method_type = parent_type.replace('-', '_')
            parent_fq_name = obj_dict['fq_name'][:-1]
            obj_cols['parent_type'] = json.dumps(parent_type)
            parent_uuid = self.fq_name_to_uuid(parent_method_type, parent_fq_name)
            self._create_child(bch, parent_method_type, parent_uuid, 'floating_ip', obj_ids['uuid'])

        # Properties
        field = obj_dict.get('floating_ip_address', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'floating_ip_address', field)

        field = obj_dict.get('floating_ip_is_virtual_ip', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'floating_ip_is_virtual_ip', field)

        field = obj_dict.get('floating_ip_fixed_ip_address', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'floating_ip_fixed_ip_address', field)

        field = obj_dict.get('floating_ip_address_family', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'floating_ip_address_family', field)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            field['created'] = datetime.datetime.utcnow().isoformat()
            field['last_modified'] = field['created']
            self._create_prop(bch, obj_ids['uuid'], 'id_perms', field)

        field = obj_dict.get('display_name', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'display_name', field)


        # References
        refs = obj_dict.get('project_refs', [])
        for ref in refs:
            ref_uuid = self.fq_name_to_uuid('project', ref['to'])
            ref_attr = ref.get('attr', None)
            ref_data = {'attr': ref_attr, 'is_weakref': False}
            self._create_ref(bch, 'floating_ip', obj_ids['uuid'], 'project', ref_uuid, ref_data)
        refs = obj_dict.get('virtual_machine_interface_refs', [])
        for ref in refs:
            ref_uuid = self.fq_name_to_uuid('virtual_machine_interface', ref['to'])
            ref_attr = ref.get('attr', None)
            ref_data = {'attr': ref_attr, 'is_weakref': False}
            self._create_ref(bch, 'floating_ip', obj_ids['uuid'], 'virtual_machine_interface', ref_uuid, ref_data)

        bch.insert(obj_ids['uuid'], obj_cols)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(obj_dict['fq_name'])
        fq_name_cols = {utils.encode_string(fq_name_str) + ':' + obj_ids['uuid']: json.dumps(None)}
        self._obj_fq_name_cf.insert('floating_ip', fq_name_cols)

        return (True, '')
    #end _cassandra_floating_ip_create

    def _cassandra_floating_ip_read(self, obj_uuids, field_names = None):
        # if field_names = None, all fields will be read/returned

        obj_uuid_cf = self._obj_uuid_cf

        # optimize for common case of reading non-backref, non-children fields
        # ignoring columns starting from 'b' and 'c' - significant performance
        # impact in scaled setting. e.g. read of project
        if (field_names is None or
            (set(field_names) & (FloatingIp.backref_fields | FloatingIp.children_fields))):
            # atleast one backref/children field is needed
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_count = 10000000,
                                       include_timestamp = True)
        else: # ignore reading backref + children columns
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_start = 'd',
                                       column_count = 10000000,
                                       include_timestamp = True)

        if (len(obj_uuids) == 1) and not obj_rows:
            raise cfgm_common.exceptions.NoIdError(obj_uuids[0])

        results = []
        for row_key in obj_rows:
            # give chance for zk heartbeat/ping
            gevent.sleep(0)
            obj_uuid = row_key
            obj_cols = obj_rows[obj_uuid]
            result = {}
            result['uuid'] = obj_uuid
            result['fq_name'] = json.loads(obj_cols['fq_name'][0])
            for col_name in obj_cols.keys():
                if self._re_match_parent.match(col_name):
                    # non config-root child
                    (_, _, parent_uuid) = col_name.split(':')
                    parent_type = json.loads(obj_cols['parent_type'][0])
                    result['parent_type'] = parent_type
                    try:
                        result['parent_uuid'] = parent_uuid
                        result['parent_href'] = self._generate_url(parent_type, parent_uuid)
                    except cfgm_common.exceptions.NoIdError:
                        err_msg = 'Unknown uuid for parent ' + result['fq_name'][-2]
                        return (False, err_msg)

                # TODO use compiled RE
                if self._re_match_prop.match(col_name):
                    (_, prop_name) = col_name.split(':')
                    result[prop_name] = json.loads(obj_cols[col_name][0])

                # TODO use compiled RE
                if self._re_match_children.match(col_name):
                    (_, child_type, child_uuid) = col_name.split(':')
                    if field_names and '%ss' %(child_type) not in field_names:
                        continue

                    child_tstamp = obj_cols[col_name][1]
                    try:
                        self._read_child(result, obj_uuid, child_type, child_uuid, child_tstamp)
                    except cfgm_common.exceptions.NoIdError:
                        continue

                # TODO use compiled RE
                if self._re_match_ref.match(col_name):
                    (_, ref_type, ref_uuid) = col_name.split(':')
                    self._read_ref(result, obj_uuid, ref_type, ref_uuid, obj_cols[col_name][0])

                if self._re_match_backref.match(col_name):
                    (_, back_ref_type, back_ref_uuid) = col_name.split(':')
                    if field_names and '%s_back_refs' %(back_ref_type) not in field_names:
                        continue

                    try:
                        self._read_back_ref(result, obj_uuid, back_ref_type, back_ref_uuid,
                                            obj_cols[col_name][0])
                    except cfgm_common.exceptions.NoIdError:
                        continue

            # for all column names


            results.append(result)
        # end for all rows

        return (True, results)
    #end _cassandra_floating_ip_read

    def _cassandra_floating_ip_count_children(self, obj_uuid, child_type):
        # if child_type = None, return
        if child_type is None:
            return (False, '')

        obj_uuid_cf = self._obj_uuid_cf
        if child_type not in FloatingIp.children_fields:
            return (False, '%s is not a valid children of FloatingIp' %(child_type))

        col_start = 'children:'+child_type[:-1]+':'
        col_finish = 'children:'+child_type[:-1]+';'
        num_children = obj_uuid_cf.get_count(obj_uuid,
                                   column_start = col_start,
                                   column_finish = col_finish,
                                   max_count = 10000000)
        return (True, num_children)
    #end _cassandra_floating_ip_count_children

    def _cassandra_floating_ip_update(self, obj_uuid, new_obj_dict):
        # Grab ref-uuids and properties in new version
        new_ref_infos = {}

        if 'project_refs' in new_obj_dict:
            new_ref_infos['project'] = {}
            new_refs = new_obj_dict['project_refs']
            if new_refs:
                for new_ref in new_refs:
                    new_ref_uuid = self.fq_name_to_uuid('project', new_ref['to'])
                    new_ref_attr = new_ref.get('attr', None)
                    new_ref_data = {'attr': new_ref_attr, 'is_weakref': False}
                    new_ref_infos['project'][new_ref_uuid] = new_ref_data

        if 'virtual_machine_interface_refs' in new_obj_dict:
            new_ref_infos['virtual_machine_interface'] = {}
            new_refs = new_obj_dict['virtual_machine_interface_refs']
            if new_refs:
                for new_ref in new_refs:
                    new_ref_uuid = self.fq_name_to_uuid('virtual_machine_interface', new_ref['to'])
                    new_ref_attr = new_ref.get('attr', None)
                    new_ref_data = {'attr': new_ref_attr, 'is_weakref': False}
                    new_ref_infos['virtual_machine_interface'][new_ref_uuid] = new_ref_data

        new_props = {}
        if 'floating_ip_address' in new_obj_dict:
            new_props['floating_ip_address'] = new_obj_dict['floating_ip_address']
        if 'floating_ip_is_virtual_ip' in new_obj_dict:
            new_props['floating_ip_is_virtual_ip'] = new_obj_dict['floating_ip_is_virtual_ip']
        if 'floating_ip_fixed_ip_address' in new_obj_dict:
            new_props['floating_ip_fixed_ip_address'] = new_obj_dict['floating_ip_fixed_ip_address']
        if 'floating_ip_address_family' in new_obj_dict:
            new_props['floating_ip_address_family'] = new_obj_dict['floating_ip_address_family']
        if 'id_perms' in new_obj_dict:
            new_props['id_perms'] = new_obj_dict['id_perms']
        if 'display_name' in new_obj_dict:
            new_props['display_name'] = new_obj_dict['display_name']
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        obj_uuid_cf = self._obj_uuid_cf
        obj_cols_iter = obj_uuid_cf.xget(obj_uuid)
        # TODO optimize this (converts tuple to dict)
        obj_cols = {}
        for col_info in obj_cols_iter:
            obj_cols[col_info[0]] = col_info[1]

        bch = obj_uuid_cf.batch()
        for col_name in obj_cols.keys():
            # TODO use compiled RE
            if re.match('prop:', col_name):
                (_, prop_name) = col_name.split(':')
                if prop_name == 'id_perms':
                    # id-perms always has to be updated for last-mod timestamp
                    # get it from request dict(or from db if not in request dict)
                    new_id_perms = new_obj_dict.get(prop_name, json.loads(obj_cols[col_name]))
                    self.update_last_modified(bch, obj_uuid, new_id_perms)
                elif prop_name in new_obj_dict:
                    self._update_prop(bch, obj_uuid, prop_name, new_props)

            # TODO use compiled RE
            if re.match('ref:', col_name):
                (_, ref_type, ref_uuid) = col_name.split(':')
                self._update_ref(bch, 'floating_ip', obj_uuid, ref_type, ref_uuid, new_ref_infos)
        # for all column names

        # create new refs
        for ref_type in new_ref_infos.keys():
            for ref_uuid in new_ref_infos[ref_type].keys():
                ref_data = new_ref_infos[ref_type][ref_uuid]
                self._create_ref(bch, 'floating_ip', obj_uuid, ref_type, ref_uuid, ref_data)

        # create new props
        for prop_name in new_props.keys():
            self._create_prop(bch, obj_uuid, prop_name, new_props[prop_name])

        bch.send()

        return (True, '')
    #end _cassandra_floating_ip_update

    def _cassandra_floating_ip_list(self, parent_uuids=None, back_ref_uuids=None,
                           obj_uuids=None, count=False, filters=None):
        children_fq_names_uuids = []
        if filters:
            fnames = filters.get('field_names', [])
            fvalues = filters.get('field_values', [])
            filter_fields = [(fnames[i], fvalues[i]) for i in range(len(fnames))]
        else:
            filter_fields = []

        def filter_rows(coll_infos, filter_cols, filter_params):
            filt_infos = {}
            coll_rows = obj_uuid_cf.multiget(coll_infos.keys(),
                                   columns=filter_cols,
                                   column_count=10000000)
            for row in coll_rows:
                # give chance for zk heartbeat/ping
                gevent.sleep(0)
                full_match = True
                for fname, fval in filter_params:
                    if coll_rows[row]['prop:%s' %(fname)] != fval:
                        full_match = False
                        break
                if full_match:
                    filt_infos[row] = coll_infos[row]
            return filt_infos
        # end filter_rows

        def get_fq_name_uuid_list(obj_uuids):
            ret_list = []
            for obj_uuid in obj_uuids:
                try:
                    obj_fq_name = self.uuid_to_fq_name(obj_uuid)
                    ret_list.append((obj_fq_name, obj_uuid))
                except cfgm_common.exceptions.NoIdError:
                    pass
            return ret_list
        # end get_fq_name_uuid_list

        if parent_uuids:
            # go from parent to child
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'children:floating_ip:'
            col_fin = 'children:floating_ip;'
            try:
                obj_rows = obj_uuid_cf.multiget(parent_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_parent_anchor(sort=False):
                # flatten to [('children:<type>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_child_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    child_uuid = col_name.split(':')[2]
                    if obj_uuids and child_uuid not in obj_uuids:
                        continue
                    all_child_infos[child_uuid] = {'uuid': child_uuid, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_child_infos = filter_rows(all_child_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_child_infos = all_child_infos

                if not sort:
                    ret_child_infos = filt_child_infos.values()
                else:
                    ret_child_infos = sorted(filt_child_infos.values(), key=itemgetter('tstamp'))

                return get_fq_name_uuid_list(r['uuid'] for r in ret_child_infos)
            # end filter_rows_parent_anchor

            if count:
                return (True, len(filter_rows_parent_anchor()))

            children_fq_names_uuids = filter_rows_parent_anchor(sort=True)

        if back_ref_uuids:
            # go from anchor to backrefs
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'backref:floating_ip:'
            col_fin = 'backref:floating_ip;'
            try:
                obj_rows = obj_uuid_cf.multiget(back_ref_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_backref_anchor():
                # flatten to [('<fqnstr>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_backref_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    col_name_arr = col_name.split(':')
                    fq_name = col_name_arr[:-1]
                    obj_uuid = col_name_arr[-1]
                    if obj_uuids and obj_uuid not in obj_uuids:
                        continue
                    all_backref_infos[obj_uuid] = \
                        {'uuid': obj_uuid, 'fq_name': fq_name, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_backref_infos = filter_rows(all_backref_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_backref_infos = all_backref_infos

                return [(br_info['fq_name'], br_info['uuid']) for br_info in filt_backref_infos.values()]
            # end filter_rows_backref_anchor

            if count:
                return (True, len(filter_rows_backref_anchor()))

            children_fq_names_uuids = filter_rows_backref_anchor()

        if not parent_uuids and not back_ref_uuids:
            obj_uuid_cf = self._obj_uuid_cf
            if obj_uuids:
                # exact objects specified
                def filter_rows_object_list():
                    all_obj_infos = {}
                    for obj_uuid in obj_uuids:
                        all_obj_infos[obj_uuid] = None

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return get_fq_name_uuid_list(filt_obj_infos.keys())
                # end filter_rows_object_list

                if count:
                    return (True, len(filter_rows_object_list()))
                children_fq_names_uuids = filter_rows_object_list()

            else: # grab all resources of this type
                obj_fq_name_cf = self._obj_fq_name_cf
                try:
                    cols = obj_fq_name_cf.get('floating_ip', column_count = 10000000)
                except pycassa.NotFoundException:
                    if count:
                        return (True, 0)
                    else:
                        return (True, children_fq_names_uuids)

                def filter_rows_no_anchor():
                    all_obj_infos = {}
                    for col_name, col_val in cols.items():
                        # give chance for zk heartbeat/ping
                        gevent.sleep(0)
                        col_name_arr = utils.decode_string(col_name).split(':')
                        obj_uuid = col_name_arr[-1]
                        all_obj_infos[obj_uuid] = (col_name_arr[:-1], obj_uuid)

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return filt_obj_infos.values()
                # end filter_rows_no_anchor

                if count:
                    return (True, len(filter_rows_no_anchor()))

                children_fq_names_uuids = filter_rows_no_anchor()

        return (True, children_fq_names_uuids)
    #end _cassandra_floating_ip_list

    def _cassandra_floating_ip_delete(self, obj_uuid):
        obj_uuid_cf = self._obj_uuid_cf
        fq_name = json.loads(obj_uuid_cf.get(obj_uuid, columns = ['fq_name'])['fq_name'])
        bch = obj_uuid_cf.batch()

        # unlink from parent
        col_start = 'parent:'
        col_fin = 'parent;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, parent_type, parent_uuid) = col_name.split(':')
            self._delete_child(bch, parent_type, parent_uuid, 'floating_ip', obj_uuid)

        # remove refs
        col_start = 'ref:'
        col_fin = 'ref;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, ref_type, ref_uuid) = col_name.split(':')
            self._delete_ref(bch, 'floating_ip', obj_uuid, ref_type, ref_uuid)

        bch.remove(obj_uuid)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(fq_name)
        fq_name_col = utils.encode_string(fq_name_str) + ':' + obj_uuid
        self._obj_fq_name_cf.remove('floating_ip', columns = [fq_name_col])


        return (True, '')
    #end _cassandra_floating_ip_delete

    def _cassandra_floating_ip_pool_alloc(self, fq_name):
        return (True, '')
    #end _cassandra_floating_ip_pool_alloc

    def _cassandra_floating_ip_pool_create(self, obj_ids, obj_dict):
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        bch = self._obj_uuid_cf.batch()

        obj_cols = {}
        obj_cols['fq_name'] = json.dumps(obj_dict['fq_name'])
        obj_cols['type'] = json.dumps('floating_ip_pool')
        if 'parent_type' in obj_dict:
            # non config-root child
            parent_type = obj_dict['parent_type']
            parent_method_type = parent_type.replace('-', '_')
            parent_fq_name = obj_dict['fq_name'][:-1]
            obj_cols['parent_type'] = json.dumps(parent_type)
            parent_uuid = self.fq_name_to_uuid(parent_method_type, parent_fq_name)
            self._create_child(bch, parent_method_type, parent_uuid, 'floating_ip_pool', obj_ids['uuid'])

        # Properties
        field = obj_dict.get('floating_ip_pool_prefixes', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'floating_ip_pool_prefixes', field)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            field['created'] = datetime.datetime.utcnow().isoformat()
            field['last_modified'] = field['created']
            self._create_prop(bch, obj_ids['uuid'], 'id_perms', field)

        field = obj_dict.get('display_name', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'display_name', field)


        # References

        bch.insert(obj_ids['uuid'], obj_cols)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(obj_dict['fq_name'])
        fq_name_cols = {utils.encode_string(fq_name_str) + ':' + obj_ids['uuid']: json.dumps(None)}
        self._obj_fq_name_cf.insert('floating_ip_pool', fq_name_cols)

        return (True, '')
    #end _cassandra_floating_ip_pool_create

    def _cassandra_floating_ip_pool_read(self, obj_uuids, field_names = None):
        # if field_names = None, all fields will be read/returned

        obj_uuid_cf = self._obj_uuid_cf

        # optimize for common case of reading non-backref, non-children fields
        # ignoring columns starting from 'b' and 'c' - significant performance
        # impact in scaled setting. e.g. read of project
        if (field_names is None or
            (set(field_names) & (FloatingIpPool.backref_fields | FloatingIpPool.children_fields))):
            # atleast one backref/children field is needed
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_count = 10000000,
                                       include_timestamp = True)
        else: # ignore reading backref + children columns
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_start = 'd',
                                       column_count = 10000000,
                                       include_timestamp = True)

        if (len(obj_uuids) == 1) and not obj_rows:
            raise cfgm_common.exceptions.NoIdError(obj_uuids[0])

        results = []
        for row_key in obj_rows:
            # give chance for zk heartbeat/ping
            gevent.sleep(0)
            obj_uuid = row_key
            obj_cols = obj_rows[obj_uuid]
            result = {}
            result['uuid'] = obj_uuid
            result['fq_name'] = json.loads(obj_cols['fq_name'][0])
            for col_name in obj_cols.keys():
                if self._re_match_parent.match(col_name):
                    # non config-root child
                    (_, _, parent_uuid) = col_name.split(':')
                    parent_type = json.loads(obj_cols['parent_type'][0])
                    result['parent_type'] = parent_type
                    try:
                        result['parent_uuid'] = parent_uuid
                        result['parent_href'] = self._generate_url(parent_type, parent_uuid)
                    except cfgm_common.exceptions.NoIdError:
                        err_msg = 'Unknown uuid for parent ' + result['fq_name'][-2]
                        return (False, err_msg)

                # TODO use compiled RE
                if self._re_match_prop.match(col_name):
                    (_, prop_name) = col_name.split(':')
                    result[prop_name] = json.loads(obj_cols[col_name][0])

                # TODO use compiled RE
                if self._re_match_children.match(col_name):
                    (_, child_type, child_uuid) = col_name.split(':')
                    if field_names and '%ss' %(child_type) not in field_names:
                        continue

                    child_tstamp = obj_cols[col_name][1]
                    try:
                        self._read_child(result, obj_uuid, child_type, child_uuid, child_tstamp)
                    except cfgm_common.exceptions.NoIdError:
                        continue

                # TODO use compiled RE
                if self._re_match_ref.match(col_name):
                    (_, ref_type, ref_uuid) = col_name.split(':')
                    self._read_ref(result, obj_uuid, ref_type, ref_uuid, obj_cols[col_name][0])

                if self._re_match_backref.match(col_name):
                    (_, back_ref_type, back_ref_uuid) = col_name.split(':')
                    if field_names and '%s_back_refs' %(back_ref_type) not in field_names:
                        continue

                    try:
                        self._read_back_ref(result, obj_uuid, back_ref_type, back_ref_uuid,
                                            obj_cols[col_name][0])
                    except cfgm_common.exceptions.NoIdError:
                        continue

            # for all column names

            if 'floating_ips' in result:
                # sort children; TODO do this based on schema
                sorted_children = sorted(result['floating_ips'], key = itemgetter('tstamp'))
                # re-write result's children without timestamp
                result['floating_ips'] = sorted_children
                [child.pop('tstamp') for child in result['floating_ips']]

            results.append(result)
        # end for all rows

        return (True, results)
    #end _cassandra_floating_ip_pool_read

    def _cassandra_floating_ip_pool_count_children(self, obj_uuid, child_type):
        # if child_type = None, return
        if child_type is None:
            return (False, '')

        obj_uuid_cf = self._obj_uuid_cf
        if child_type not in FloatingIpPool.children_fields:
            return (False, '%s is not a valid children of FloatingIpPool' %(child_type))

        col_start = 'children:'+child_type[:-1]+':'
        col_finish = 'children:'+child_type[:-1]+';'
        num_children = obj_uuid_cf.get_count(obj_uuid,
                                   column_start = col_start,
                                   column_finish = col_finish,
                                   max_count = 10000000)
        return (True, num_children)
    #end _cassandra_floating_ip_pool_count_children

    def _cassandra_floating_ip_pool_update(self, obj_uuid, new_obj_dict):
        # Grab ref-uuids and properties in new version
        new_ref_infos = {}

        new_props = {}
        if 'floating_ip_pool_prefixes' in new_obj_dict:
            new_props['floating_ip_pool_prefixes'] = new_obj_dict['floating_ip_pool_prefixes']
        if 'id_perms' in new_obj_dict:
            new_props['id_perms'] = new_obj_dict['id_perms']
        if 'display_name' in new_obj_dict:
            new_props['display_name'] = new_obj_dict['display_name']
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        obj_uuid_cf = self._obj_uuid_cf
        obj_cols_iter = obj_uuid_cf.xget(obj_uuid)
        # TODO optimize this (converts tuple to dict)
        obj_cols = {}
        for col_info in obj_cols_iter:
            obj_cols[col_info[0]] = col_info[1]

        bch = obj_uuid_cf.batch()
        for col_name in obj_cols.keys():
            # TODO use compiled RE
            if re.match('prop:', col_name):
                (_, prop_name) = col_name.split(':')
                if prop_name == 'id_perms':
                    # id-perms always has to be updated for last-mod timestamp
                    # get it from request dict(or from db if not in request dict)
                    new_id_perms = new_obj_dict.get(prop_name, json.loads(obj_cols[col_name]))
                    self.update_last_modified(bch, obj_uuid, new_id_perms)
                elif prop_name in new_obj_dict:
                    self._update_prop(bch, obj_uuid, prop_name, new_props)

            # TODO use compiled RE
            if re.match('ref:', col_name):
                (_, ref_type, ref_uuid) = col_name.split(':')
                self._update_ref(bch, 'floating_ip_pool', obj_uuid, ref_type, ref_uuid, new_ref_infos)
        # for all column names

        # create new refs
        for ref_type in new_ref_infos.keys():
            for ref_uuid in new_ref_infos[ref_type].keys():
                ref_data = new_ref_infos[ref_type][ref_uuid]
                self._create_ref(bch, 'floating_ip_pool', obj_uuid, ref_type, ref_uuid, ref_data)

        # create new props
        for prop_name in new_props.keys():
            self._create_prop(bch, obj_uuid, prop_name, new_props[prop_name])

        bch.send()

        return (True, '')
    #end _cassandra_floating_ip_pool_update

    def _cassandra_floating_ip_pool_list(self, parent_uuids=None, back_ref_uuids=None,
                           obj_uuids=None, count=False, filters=None):
        children_fq_names_uuids = []
        if filters:
            fnames = filters.get('field_names', [])
            fvalues = filters.get('field_values', [])
            filter_fields = [(fnames[i], fvalues[i]) for i in range(len(fnames))]
        else:
            filter_fields = []

        def filter_rows(coll_infos, filter_cols, filter_params):
            filt_infos = {}
            coll_rows = obj_uuid_cf.multiget(coll_infos.keys(),
                                   columns=filter_cols,
                                   column_count=10000000)
            for row in coll_rows:
                # give chance for zk heartbeat/ping
                gevent.sleep(0)
                full_match = True
                for fname, fval in filter_params:
                    if coll_rows[row]['prop:%s' %(fname)] != fval:
                        full_match = False
                        break
                if full_match:
                    filt_infos[row] = coll_infos[row]
            return filt_infos
        # end filter_rows

        def get_fq_name_uuid_list(obj_uuids):
            ret_list = []
            for obj_uuid in obj_uuids:
                try:
                    obj_fq_name = self.uuid_to_fq_name(obj_uuid)
                    ret_list.append((obj_fq_name, obj_uuid))
                except cfgm_common.exceptions.NoIdError:
                    pass
            return ret_list
        # end get_fq_name_uuid_list

        if parent_uuids:
            # go from parent to child
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'children:floating_ip_pool:'
            col_fin = 'children:floating_ip_pool;'
            try:
                obj_rows = obj_uuid_cf.multiget(parent_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_parent_anchor(sort=False):
                # flatten to [('children:<type>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_child_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    child_uuid = col_name.split(':')[2]
                    if obj_uuids and child_uuid not in obj_uuids:
                        continue
                    all_child_infos[child_uuid] = {'uuid': child_uuid, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_child_infos = filter_rows(all_child_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_child_infos = all_child_infos

                if not sort:
                    ret_child_infos = filt_child_infos.values()
                else:
                    ret_child_infos = sorted(filt_child_infos.values(), key=itemgetter('tstamp'))

                return get_fq_name_uuid_list(r['uuid'] for r in ret_child_infos)
            # end filter_rows_parent_anchor

            if count:
                return (True, len(filter_rows_parent_anchor()))

            children_fq_names_uuids = filter_rows_parent_anchor(sort=True)

        if back_ref_uuids:
            # go from anchor to backrefs
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'backref:floating_ip_pool:'
            col_fin = 'backref:floating_ip_pool;'
            try:
                obj_rows = obj_uuid_cf.multiget(back_ref_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_backref_anchor():
                # flatten to [('<fqnstr>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_backref_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    col_name_arr = col_name.split(':')
                    fq_name = col_name_arr[:-1]
                    obj_uuid = col_name_arr[-1]
                    if obj_uuids and obj_uuid not in obj_uuids:
                        continue
                    all_backref_infos[obj_uuid] = \
                        {'uuid': obj_uuid, 'fq_name': fq_name, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_backref_infos = filter_rows(all_backref_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_backref_infos = all_backref_infos

                return [(br_info['fq_name'], br_info['uuid']) for br_info in filt_backref_infos.values()]
            # end filter_rows_backref_anchor

            if count:
                return (True, len(filter_rows_backref_anchor()))

            children_fq_names_uuids = filter_rows_backref_anchor()

        if not parent_uuids and not back_ref_uuids:
            obj_uuid_cf = self._obj_uuid_cf
            if obj_uuids:
                # exact objects specified
                def filter_rows_object_list():
                    all_obj_infos = {}
                    for obj_uuid in obj_uuids:
                        all_obj_infos[obj_uuid] = None

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return get_fq_name_uuid_list(filt_obj_infos.keys())
                # end filter_rows_object_list

                if count:
                    return (True, len(filter_rows_object_list()))
                children_fq_names_uuids = filter_rows_object_list()

            else: # grab all resources of this type
                obj_fq_name_cf = self._obj_fq_name_cf
                try:
                    cols = obj_fq_name_cf.get('floating_ip_pool', column_count = 10000000)
                except pycassa.NotFoundException:
                    if count:
                        return (True, 0)
                    else:
                        return (True, children_fq_names_uuids)

                def filter_rows_no_anchor():
                    all_obj_infos = {}
                    for col_name, col_val in cols.items():
                        # give chance for zk heartbeat/ping
                        gevent.sleep(0)
                        col_name_arr = utils.decode_string(col_name).split(':')
                        obj_uuid = col_name_arr[-1]
                        all_obj_infos[obj_uuid] = (col_name_arr[:-1], obj_uuid)

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return filt_obj_infos.values()
                # end filter_rows_no_anchor

                if count:
                    return (True, len(filter_rows_no_anchor()))

                children_fq_names_uuids = filter_rows_no_anchor()

        return (True, children_fq_names_uuids)
    #end _cassandra_floating_ip_pool_list

    def _cassandra_floating_ip_pool_delete(self, obj_uuid):
        obj_uuid_cf = self._obj_uuid_cf
        fq_name = json.loads(obj_uuid_cf.get(obj_uuid, columns = ['fq_name'])['fq_name'])
        bch = obj_uuid_cf.batch()

        # unlink from parent
        col_start = 'parent:'
        col_fin = 'parent;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, parent_type, parent_uuid) = col_name.split(':')
            self._delete_child(bch, parent_type, parent_uuid, 'floating_ip_pool', obj_uuid)

        # remove refs
        col_start = 'ref:'
        col_fin = 'ref;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, ref_type, ref_uuid) = col_name.split(':')
            self._delete_ref(bch, 'floating_ip_pool', obj_uuid, ref_type, ref_uuid)

        bch.remove(obj_uuid)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(fq_name)
        fq_name_col = utils.encode_string(fq_name_str) + ':' + obj_uuid
        self._obj_fq_name_cf.remove('floating_ip_pool', columns = [fq_name_col])


        return (True, '')
    #end _cassandra_floating_ip_pool_delete

    def _cassandra_physical_router_alloc(self, fq_name):
        return (True, '')
    #end _cassandra_physical_router_alloc

    def _cassandra_physical_router_create(self, obj_ids, obj_dict):
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        bch = self._obj_uuid_cf.batch()

        obj_cols = {}
        obj_cols['fq_name'] = json.dumps(obj_dict['fq_name'])
        obj_cols['type'] = json.dumps('physical_router')
        if 'parent_type' in obj_dict:
            # non config-root child
            parent_type = obj_dict['parent_type']
            parent_method_type = parent_type.replace('-', '_')
            parent_fq_name = obj_dict['fq_name'][:-1]
            obj_cols['parent_type'] = json.dumps(parent_type)
            parent_uuid = self.fq_name_to_uuid(parent_method_type, parent_fq_name)
            self._create_child(bch, parent_method_type, parent_uuid, 'physical_router', obj_ids['uuid'])

        # Properties
        field = obj_dict.get('physical_router_management_ip', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'physical_router_management_ip', field)

        field = obj_dict.get('physical_router_dataplane_ip', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'physical_router_dataplane_ip', field)

        field = obj_dict.get('physical_router_vendor_name', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'physical_router_vendor_name', field)

        field = obj_dict.get('physical_router_product_name', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'physical_router_product_name', field)

        field = obj_dict.get('physical_router_vnc_managed', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'physical_router_vnc_managed', field)

        field = obj_dict.get('physical_router_user_credentials', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'physical_router_user_credentials', field)

        field = obj_dict.get('physical_router_snmp_credentials', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'physical_router_snmp_credentials', field)

        field = obj_dict.get('physical_router_junos_service_ports', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'physical_router_junos_service_ports', field)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            field['created'] = datetime.datetime.utcnow().isoformat()
            field['last_modified'] = field['created']
            self._create_prop(bch, obj_ids['uuid'], 'id_perms', field)

        field = obj_dict.get('display_name', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'display_name', field)


        # References
        refs = obj_dict.get('virtual_router_refs', [])
        for ref in refs:
            ref_uuid = self.fq_name_to_uuid('virtual_router', ref['to'])
            ref_attr = ref.get('attr', None)
            ref_data = {'attr': ref_attr, 'is_weakref': False}
            self._create_ref(bch, 'physical_router', obj_ids['uuid'], 'virtual_router', ref_uuid, ref_data)
        refs = obj_dict.get('bgp_router_refs', [])
        for ref in refs:
            ref_uuid = self.fq_name_to_uuid('bgp_router', ref['to'])
            ref_attr = ref.get('attr', None)
            ref_data = {'attr': ref_attr, 'is_weakref': False}
            self._create_ref(bch, 'physical_router', obj_ids['uuid'], 'bgp_router', ref_uuid, ref_data)
        refs = obj_dict.get('virtual_network_refs', [])
        for ref in refs:
            ref_uuid = self.fq_name_to_uuid('virtual_network', ref['to'])
            ref_attr = ref.get('attr', None)
            ref_data = {'attr': ref_attr, 'is_weakref': False}
            self._create_ref(bch, 'physical_router', obj_ids['uuid'], 'virtual_network', ref_uuid, ref_data)

        bch.insert(obj_ids['uuid'], obj_cols)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(obj_dict['fq_name'])
        fq_name_cols = {utils.encode_string(fq_name_str) + ':' + obj_ids['uuid']: json.dumps(None)}
        self._obj_fq_name_cf.insert('physical_router', fq_name_cols)

        return (True, '')
    #end _cassandra_physical_router_create

    def _cassandra_physical_router_read(self, obj_uuids, field_names = None):
        # if field_names = None, all fields will be read/returned

        obj_uuid_cf = self._obj_uuid_cf

        # optimize for common case of reading non-backref, non-children fields
        # ignoring columns starting from 'b' and 'c' - significant performance
        # impact in scaled setting. e.g. read of project
        if (field_names is None or
            (set(field_names) & (PhysicalRouter.backref_fields | PhysicalRouter.children_fields))):
            # atleast one backref/children field is needed
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_count = 10000000,
                                       include_timestamp = True)
        else: # ignore reading backref + children columns
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_start = 'd',
                                       column_count = 10000000,
                                       include_timestamp = True)

        if (len(obj_uuids) == 1) and not obj_rows:
            raise cfgm_common.exceptions.NoIdError(obj_uuids[0])

        results = []
        for row_key in obj_rows:
            # give chance for zk heartbeat/ping
            gevent.sleep(0)
            obj_uuid = row_key
            obj_cols = obj_rows[obj_uuid]
            result = {}
            result['uuid'] = obj_uuid
            result['fq_name'] = json.loads(obj_cols['fq_name'][0])
            for col_name in obj_cols.keys():
                if self._re_match_parent.match(col_name):
                    # non config-root child
                    (_, _, parent_uuid) = col_name.split(':')
                    parent_type = json.loads(obj_cols['parent_type'][0])
                    result['parent_type'] = parent_type
                    try:
                        result['parent_uuid'] = parent_uuid
                        result['parent_href'] = self._generate_url(parent_type, parent_uuid)
                    except cfgm_common.exceptions.NoIdError:
                        err_msg = 'Unknown uuid for parent ' + result['fq_name'][-2]
                        return (False, err_msg)

                # TODO use compiled RE
                if self._re_match_prop.match(col_name):
                    (_, prop_name) = col_name.split(':')
                    result[prop_name] = json.loads(obj_cols[col_name][0])

                # TODO use compiled RE
                if self._re_match_children.match(col_name):
                    (_, child_type, child_uuid) = col_name.split(':')
                    if field_names and '%ss' %(child_type) not in field_names:
                        continue

                    child_tstamp = obj_cols[col_name][1]
                    try:
                        self._read_child(result, obj_uuid, child_type, child_uuid, child_tstamp)
                    except cfgm_common.exceptions.NoIdError:
                        continue

                # TODO use compiled RE
                if self._re_match_ref.match(col_name):
                    (_, ref_type, ref_uuid) = col_name.split(':')
                    self._read_ref(result, obj_uuid, ref_type, ref_uuid, obj_cols[col_name][0])

                if self._re_match_backref.match(col_name):
                    (_, back_ref_type, back_ref_uuid) = col_name.split(':')
                    if field_names and '%s_back_refs' %(back_ref_type) not in field_names:
                        continue

                    try:
                        self._read_back_ref(result, obj_uuid, back_ref_type, back_ref_uuid,
                                            obj_cols[col_name][0])
                    except cfgm_common.exceptions.NoIdError:
                        continue

            # for all column names

            if 'physical_interfaces' in result:
                # sort children; TODO do this based on schema
                sorted_children = sorted(result['physical_interfaces'], key = itemgetter('tstamp'))
                # re-write result's children without timestamp
                result['physical_interfaces'] = sorted_children
                [child.pop('tstamp') for child in result['physical_interfaces']]

            if 'logical_interfaces' in result:
                # sort children; TODO do this based on schema
                sorted_children = sorted(result['logical_interfaces'], key = itemgetter('tstamp'))
                # re-write result's children without timestamp
                result['logical_interfaces'] = sorted_children
                [child.pop('tstamp') for child in result['logical_interfaces']]

            results.append(result)
        # end for all rows

        return (True, results)
    #end _cassandra_physical_router_read

    def _cassandra_physical_router_count_children(self, obj_uuid, child_type):
        # if child_type = None, return
        if child_type is None:
            return (False, '')

        obj_uuid_cf = self._obj_uuid_cf
        if child_type not in PhysicalRouter.children_fields:
            return (False, '%s is not a valid children of PhysicalRouter' %(child_type))

        col_start = 'children:'+child_type[:-1]+':'
        col_finish = 'children:'+child_type[:-1]+';'
        num_children = obj_uuid_cf.get_count(obj_uuid,
                                   column_start = col_start,
                                   column_finish = col_finish,
                                   max_count = 10000000)
        return (True, num_children)
    #end _cassandra_physical_router_count_children

    def _cassandra_physical_router_update(self, obj_uuid, new_obj_dict):
        # Grab ref-uuids and properties in new version
        new_ref_infos = {}

        if 'virtual_router_refs' in new_obj_dict:
            new_ref_infos['virtual_router'] = {}
            new_refs = new_obj_dict['virtual_router_refs']
            if new_refs:
                for new_ref in new_refs:
                    new_ref_uuid = self.fq_name_to_uuid('virtual_router', new_ref['to'])
                    new_ref_attr = new_ref.get('attr', None)
                    new_ref_data = {'attr': new_ref_attr, 'is_weakref': False}
                    new_ref_infos['virtual_router'][new_ref_uuid] = new_ref_data

        if 'bgp_router_refs' in new_obj_dict:
            new_ref_infos['bgp_router'] = {}
            new_refs = new_obj_dict['bgp_router_refs']
            if new_refs:
                for new_ref in new_refs:
                    new_ref_uuid = self.fq_name_to_uuid('bgp_router', new_ref['to'])
                    new_ref_attr = new_ref.get('attr', None)
                    new_ref_data = {'attr': new_ref_attr, 'is_weakref': False}
                    new_ref_infos['bgp_router'][new_ref_uuid] = new_ref_data

        if 'virtual_network_refs' in new_obj_dict:
            new_ref_infos['virtual_network'] = {}
            new_refs = new_obj_dict['virtual_network_refs']
            if new_refs:
                for new_ref in new_refs:
                    new_ref_uuid = self.fq_name_to_uuid('virtual_network', new_ref['to'])
                    new_ref_attr = new_ref.get('attr', None)
                    new_ref_data = {'attr': new_ref_attr, 'is_weakref': False}
                    new_ref_infos['virtual_network'][new_ref_uuid] = new_ref_data

        new_props = {}
        if 'physical_router_management_ip' in new_obj_dict:
            new_props['physical_router_management_ip'] = new_obj_dict['physical_router_management_ip']
        if 'physical_router_dataplane_ip' in new_obj_dict:
            new_props['physical_router_dataplane_ip'] = new_obj_dict['physical_router_dataplane_ip']
        if 'physical_router_vendor_name' in new_obj_dict:
            new_props['physical_router_vendor_name'] = new_obj_dict['physical_router_vendor_name']
        if 'physical_router_product_name' in new_obj_dict:
            new_props['physical_router_product_name'] = new_obj_dict['physical_router_product_name']
        if 'physical_router_vnc_managed' in new_obj_dict:
            new_props['physical_router_vnc_managed'] = new_obj_dict['physical_router_vnc_managed']
        if 'physical_router_user_credentials' in new_obj_dict:
            new_props['physical_router_user_credentials'] = new_obj_dict['physical_router_user_credentials']
        if 'physical_router_snmp_credentials' in new_obj_dict:
            new_props['physical_router_snmp_credentials'] = new_obj_dict['physical_router_snmp_credentials']
        if 'physical_router_junos_service_ports' in new_obj_dict:
            new_props['physical_router_junos_service_ports'] = new_obj_dict['physical_router_junos_service_ports']
        if 'id_perms' in new_obj_dict:
            new_props['id_perms'] = new_obj_dict['id_perms']
        if 'display_name' in new_obj_dict:
            new_props['display_name'] = new_obj_dict['display_name']
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        obj_uuid_cf = self._obj_uuid_cf
        obj_cols_iter = obj_uuid_cf.xget(obj_uuid)
        # TODO optimize this (converts tuple to dict)
        obj_cols = {}
        for col_info in obj_cols_iter:
            obj_cols[col_info[0]] = col_info[1]

        bch = obj_uuid_cf.batch()
        for col_name in obj_cols.keys():
            # TODO use compiled RE
            if re.match('prop:', col_name):
                (_, prop_name) = col_name.split(':')
                if prop_name == 'id_perms':
                    # id-perms always has to be updated for last-mod timestamp
                    # get it from request dict(or from db if not in request dict)
                    new_id_perms = new_obj_dict.get(prop_name, json.loads(obj_cols[col_name]))
                    self.update_last_modified(bch, obj_uuid, new_id_perms)
                elif prop_name in new_obj_dict:
                    self._update_prop(bch, obj_uuid, prop_name, new_props)

            # TODO use compiled RE
            if re.match('ref:', col_name):
                (_, ref_type, ref_uuid) = col_name.split(':')
                self._update_ref(bch, 'physical_router', obj_uuid, ref_type, ref_uuid, new_ref_infos)
        # for all column names

        # create new refs
        for ref_type in new_ref_infos.keys():
            for ref_uuid in new_ref_infos[ref_type].keys():
                ref_data = new_ref_infos[ref_type][ref_uuid]
                self._create_ref(bch, 'physical_router', obj_uuid, ref_type, ref_uuid, ref_data)

        # create new props
        for prop_name in new_props.keys():
            self._create_prop(bch, obj_uuid, prop_name, new_props[prop_name])

        bch.send()

        return (True, '')
    #end _cassandra_physical_router_update

    def _cassandra_physical_router_list(self, parent_uuids=None, back_ref_uuids=None,
                           obj_uuids=None, count=False, filters=None):
        children_fq_names_uuids = []
        if filters:
            fnames = filters.get('field_names', [])
            fvalues = filters.get('field_values', [])
            filter_fields = [(fnames[i], fvalues[i]) for i in range(len(fnames))]
        else:
            filter_fields = []

        def filter_rows(coll_infos, filter_cols, filter_params):
            filt_infos = {}
            coll_rows = obj_uuid_cf.multiget(coll_infos.keys(),
                                   columns=filter_cols,
                                   column_count=10000000)
            for row in coll_rows:
                # give chance for zk heartbeat/ping
                gevent.sleep(0)
                full_match = True
                for fname, fval in filter_params:
                    if coll_rows[row]['prop:%s' %(fname)] != fval:
                        full_match = False
                        break
                if full_match:
                    filt_infos[row] = coll_infos[row]
            return filt_infos
        # end filter_rows

        def get_fq_name_uuid_list(obj_uuids):
            ret_list = []
            for obj_uuid in obj_uuids:
                try:
                    obj_fq_name = self.uuid_to_fq_name(obj_uuid)
                    ret_list.append((obj_fq_name, obj_uuid))
                except cfgm_common.exceptions.NoIdError:
                    pass
            return ret_list
        # end get_fq_name_uuid_list

        if parent_uuids:
            # go from parent to child
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'children:physical_router:'
            col_fin = 'children:physical_router;'
            try:
                obj_rows = obj_uuid_cf.multiget(parent_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_parent_anchor(sort=False):
                # flatten to [('children:<type>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_child_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    child_uuid = col_name.split(':')[2]
                    if obj_uuids and child_uuid not in obj_uuids:
                        continue
                    all_child_infos[child_uuid] = {'uuid': child_uuid, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_child_infos = filter_rows(all_child_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_child_infos = all_child_infos

                if not sort:
                    ret_child_infos = filt_child_infos.values()
                else:
                    ret_child_infos = sorted(filt_child_infos.values(), key=itemgetter('tstamp'))

                return get_fq_name_uuid_list(r['uuid'] for r in ret_child_infos)
            # end filter_rows_parent_anchor

            if count:
                return (True, len(filter_rows_parent_anchor()))

            children_fq_names_uuids = filter_rows_parent_anchor(sort=True)

        if back_ref_uuids:
            # go from anchor to backrefs
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'backref:physical_router:'
            col_fin = 'backref:physical_router;'
            try:
                obj_rows = obj_uuid_cf.multiget(back_ref_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_backref_anchor():
                # flatten to [('<fqnstr>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_backref_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    col_name_arr = col_name.split(':')
                    fq_name = col_name_arr[:-1]
                    obj_uuid = col_name_arr[-1]
                    if obj_uuids and obj_uuid not in obj_uuids:
                        continue
                    all_backref_infos[obj_uuid] = \
                        {'uuid': obj_uuid, 'fq_name': fq_name, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_backref_infos = filter_rows(all_backref_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_backref_infos = all_backref_infos

                return [(br_info['fq_name'], br_info['uuid']) for br_info in filt_backref_infos.values()]
            # end filter_rows_backref_anchor

            if count:
                return (True, len(filter_rows_backref_anchor()))

            children_fq_names_uuids = filter_rows_backref_anchor()

        if not parent_uuids and not back_ref_uuids:
            obj_uuid_cf = self._obj_uuid_cf
            if obj_uuids:
                # exact objects specified
                def filter_rows_object_list():
                    all_obj_infos = {}
                    for obj_uuid in obj_uuids:
                        all_obj_infos[obj_uuid] = None

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return get_fq_name_uuid_list(filt_obj_infos.keys())
                # end filter_rows_object_list

                if count:
                    return (True, len(filter_rows_object_list()))
                children_fq_names_uuids = filter_rows_object_list()

            else: # grab all resources of this type
                obj_fq_name_cf = self._obj_fq_name_cf
                try:
                    cols = obj_fq_name_cf.get('physical_router', column_count = 10000000)
                except pycassa.NotFoundException:
                    if count:
                        return (True, 0)
                    else:
                        return (True, children_fq_names_uuids)

                def filter_rows_no_anchor():
                    all_obj_infos = {}
                    for col_name, col_val in cols.items():
                        # give chance for zk heartbeat/ping
                        gevent.sleep(0)
                        col_name_arr = utils.decode_string(col_name).split(':')
                        obj_uuid = col_name_arr[-1]
                        all_obj_infos[obj_uuid] = (col_name_arr[:-1], obj_uuid)

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return filt_obj_infos.values()
                # end filter_rows_no_anchor

                if count:
                    return (True, len(filter_rows_no_anchor()))

                children_fq_names_uuids = filter_rows_no_anchor()

        return (True, children_fq_names_uuids)
    #end _cassandra_physical_router_list

    def _cassandra_physical_router_delete(self, obj_uuid):
        obj_uuid_cf = self._obj_uuid_cf
        fq_name = json.loads(obj_uuid_cf.get(obj_uuid, columns = ['fq_name'])['fq_name'])
        bch = obj_uuid_cf.batch()

        # unlink from parent
        col_start = 'parent:'
        col_fin = 'parent;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, parent_type, parent_uuid) = col_name.split(':')
            self._delete_child(bch, parent_type, parent_uuid, 'physical_router', obj_uuid)

        # remove refs
        col_start = 'ref:'
        col_fin = 'ref;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, ref_type, ref_uuid) = col_name.split(':')
            self._delete_ref(bch, 'physical_router', obj_uuid, ref_type, ref_uuid)

        bch.remove(obj_uuid)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(fq_name)
        fq_name_col = utils.encode_string(fq_name_str) + ':' + obj_uuid
        self._obj_fq_name_cf.remove('physical_router', columns = [fq_name_col])


        return (True, '')
    #end _cassandra_physical_router_delete

    def _cassandra_bgp_router_alloc(self, fq_name):
        return (True, '')
    #end _cassandra_bgp_router_alloc

    def _cassandra_bgp_router_create(self, obj_ids, obj_dict):
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        bch = self._obj_uuid_cf.batch()

        obj_cols = {}
        obj_cols['fq_name'] = json.dumps(obj_dict['fq_name'])
        obj_cols['type'] = json.dumps('bgp_router')
        if 'parent_type' in obj_dict:
            # non config-root child
            parent_type = obj_dict['parent_type']
            parent_method_type = parent_type.replace('-', '_')
            parent_fq_name = obj_dict['fq_name'][:-1]
            obj_cols['parent_type'] = json.dumps(parent_type)
            parent_uuid = self.fq_name_to_uuid(parent_method_type, parent_fq_name)
            self._create_child(bch, parent_method_type, parent_uuid, 'bgp_router', obj_ids['uuid'])

        # Properties
        field = obj_dict.get('bgp_router_parameters', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'bgp_router_parameters', field)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            field['created'] = datetime.datetime.utcnow().isoformat()
            field['last_modified'] = field['created']
            self._create_prop(bch, obj_ids['uuid'], 'id_perms', field)

        field = obj_dict.get('display_name', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'display_name', field)


        # References
        refs = obj_dict.get('bgp_router_refs', [])
        for ref in refs:
            ref_uuid = self.fq_name_to_uuid('bgp_router', ref['to'])
            ref_attr = ref.get('attr', None)
            ref_data = {'attr': ref_attr, 'is_weakref': False}
            self._create_ref(bch, 'bgp_router', obj_ids['uuid'], 'bgp_router', ref_uuid, ref_data)

        bch.insert(obj_ids['uuid'], obj_cols)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(obj_dict['fq_name'])
        fq_name_cols = {utils.encode_string(fq_name_str) + ':' + obj_ids['uuid']: json.dumps(None)}
        self._obj_fq_name_cf.insert('bgp_router', fq_name_cols)

        return (True, '')
    #end _cassandra_bgp_router_create

    def _cassandra_bgp_router_read(self, obj_uuids, field_names = None):
        # if field_names = None, all fields will be read/returned

        obj_uuid_cf = self._obj_uuid_cf

        # optimize for common case of reading non-backref, non-children fields
        # ignoring columns starting from 'b' and 'c' - significant performance
        # impact in scaled setting. e.g. read of project
        if (field_names is None or
            (set(field_names) & (BgpRouter.backref_fields | BgpRouter.children_fields))):
            # atleast one backref/children field is needed
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_count = 10000000,
                                       include_timestamp = True)
        else: # ignore reading backref + children columns
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_start = 'd',
                                       column_count = 10000000,
                                       include_timestamp = True)

        if (len(obj_uuids) == 1) and not obj_rows:
            raise cfgm_common.exceptions.NoIdError(obj_uuids[0])

        results = []
        for row_key in obj_rows:
            # give chance for zk heartbeat/ping
            gevent.sleep(0)
            obj_uuid = row_key
            obj_cols = obj_rows[obj_uuid]
            result = {}
            result['uuid'] = obj_uuid
            result['fq_name'] = json.loads(obj_cols['fq_name'][0])
            for col_name in obj_cols.keys():
                if self._re_match_parent.match(col_name):
                    # non config-root child
                    (_, _, parent_uuid) = col_name.split(':')
                    parent_type = json.loads(obj_cols['parent_type'][0])
                    result['parent_type'] = parent_type
                    try:
                        result['parent_uuid'] = parent_uuid
                        result['parent_href'] = self._generate_url(parent_type, parent_uuid)
                    except cfgm_common.exceptions.NoIdError:
                        err_msg = 'Unknown uuid for parent ' + result['fq_name'][-2]
                        return (False, err_msg)

                # TODO use compiled RE
                if self._re_match_prop.match(col_name):
                    (_, prop_name) = col_name.split(':')
                    result[prop_name] = json.loads(obj_cols[col_name][0])

                # TODO use compiled RE
                if self._re_match_children.match(col_name):
                    (_, child_type, child_uuid) = col_name.split(':')
                    if field_names and '%ss' %(child_type) not in field_names:
                        continue

                    child_tstamp = obj_cols[col_name][1]
                    try:
                        self._read_child(result, obj_uuid, child_type, child_uuid, child_tstamp)
                    except cfgm_common.exceptions.NoIdError:
                        continue

                # TODO use compiled RE
                if self._re_match_ref.match(col_name):
                    (_, ref_type, ref_uuid) = col_name.split(':')
                    self._read_ref(result, obj_uuid, ref_type, ref_uuid, obj_cols[col_name][0])

                if self._re_match_backref.match(col_name):
                    (_, back_ref_type, back_ref_uuid) = col_name.split(':')
                    if field_names and '%s_back_refs' %(back_ref_type) not in field_names:
                        continue

                    try:
                        self._read_back_ref(result, obj_uuid, back_ref_type, back_ref_uuid,
                                            obj_cols[col_name][0])
                    except cfgm_common.exceptions.NoIdError:
                        continue

            # for all column names


            results.append(result)
        # end for all rows

        return (True, results)
    #end _cassandra_bgp_router_read

    def _cassandra_bgp_router_count_children(self, obj_uuid, child_type):
        # if child_type = None, return
        if child_type is None:
            return (False, '')

        obj_uuid_cf = self._obj_uuid_cf
        if child_type not in BgpRouter.children_fields:
            return (False, '%s is not a valid children of BgpRouter' %(child_type))

        col_start = 'children:'+child_type[:-1]+':'
        col_finish = 'children:'+child_type[:-1]+';'
        num_children = obj_uuid_cf.get_count(obj_uuid,
                                   column_start = col_start,
                                   column_finish = col_finish,
                                   max_count = 10000000)
        return (True, num_children)
    #end _cassandra_bgp_router_count_children

    def _cassandra_bgp_router_update(self, obj_uuid, new_obj_dict):
        # Grab ref-uuids and properties in new version
        new_ref_infos = {}

        if 'bgp_router_refs' in new_obj_dict:
            new_ref_infos['bgp_router'] = {}
            new_refs = new_obj_dict['bgp_router_refs']
            if new_refs:
                for new_ref in new_refs:
                    new_ref_uuid = self.fq_name_to_uuid('bgp_router', new_ref['to'])
                    new_ref_attr = new_ref.get('attr', None)
                    new_ref_data = {'attr': new_ref_attr, 'is_weakref': False}
                    new_ref_infos['bgp_router'][new_ref_uuid] = new_ref_data

        new_props = {}
        if 'bgp_router_parameters' in new_obj_dict:
            new_props['bgp_router_parameters'] = new_obj_dict['bgp_router_parameters']
        if 'id_perms' in new_obj_dict:
            new_props['id_perms'] = new_obj_dict['id_perms']
        if 'display_name' in new_obj_dict:
            new_props['display_name'] = new_obj_dict['display_name']
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        obj_uuid_cf = self._obj_uuid_cf
        obj_cols_iter = obj_uuid_cf.xget(obj_uuid)
        # TODO optimize this (converts tuple to dict)
        obj_cols = {}
        for col_info in obj_cols_iter:
            obj_cols[col_info[0]] = col_info[1]

        bch = obj_uuid_cf.batch()
        for col_name in obj_cols.keys():
            # TODO use compiled RE
            if re.match('prop:', col_name):
                (_, prop_name) = col_name.split(':')
                if prop_name == 'id_perms':
                    # id-perms always has to be updated for last-mod timestamp
                    # get it from request dict(or from db if not in request dict)
                    new_id_perms = new_obj_dict.get(prop_name, json.loads(obj_cols[col_name]))
                    self.update_last_modified(bch, obj_uuid, new_id_perms)
                elif prop_name in new_obj_dict:
                    self._update_prop(bch, obj_uuid, prop_name, new_props)

            # TODO use compiled RE
            if re.match('ref:', col_name):
                (_, ref_type, ref_uuid) = col_name.split(':')
                self._update_ref(bch, 'bgp_router', obj_uuid, ref_type, ref_uuid, new_ref_infos)
        # for all column names

        # create new refs
        for ref_type in new_ref_infos.keys():
            for ref_uuid in new_ref_infos[ref_type].keys():
                ref_data = new_ref_infos[ref_type][ref_uuid]
                self._create_ref(bch, 'bgp_router', obj_uuid, ref_type, ref_uuid, ref_data)

        # create new props
        for prop_name in new_props.keys():
            self._create_prop(bch, obj_uuid, prop_name, new_props[prop_name])

        bch.send()

        return (True, '')
    #end _cassandra_bgp_router_update

    def _cassandra_bgp_router_list(self, parent_uuids=None, back_ref_uuids=None,
                           obj_uuids=None, count=False, filters=None):
        children_fq_names_uuids = []
        if filters:
            fnames = filters.get('field_names', [])
            fvalues = filters.get('field_values', [])
            filter_fields = [(fnames[i], fvalues[i]) for i in range(len(fnames))]
        else:
            filter_fields = []

        def filter_rows(coll_infos, filter_cols, filter_params):
            filt_infos = {}
            coll_rows = obj_uuid_cf.multiget(coll_infos.keys(),
                                   columns=filter_cols,
                                   column_count=10000000)
            for row in coll_rows:
                # give chance for zk heartbeat/ping
                gevent.sleep(0)
                full_match = True
                for fname, fval in filter_params:
                    if coll_rows[row]['prop:%s' %(fname)] != fval:
                        full_match = False
                        break
                if full_match:
                    filt_infos[row] = coll_infos[row]
            return filt_infos
        # end filter_rows

        def get_fq_name_uuid_list(obj_uuids):
            ret_list = []
            for obj_uuid in obj_uuids:
                try:
                    obj_fq_name = self.uuid_to_fq_name(obj_uuid)
                    ret_list.append((obj_fq_name, obj_uuid))
                except cfgm_common.exceptions.NoIdError:
                    pass
            return ret_list
        # end get_fq_name_uuid_list

        if parent_uuids:
            # go from parent to child
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'children:bgp_router:'
            col_fin = 'children:bgp_router;'
            try:
                obj_rows = obj_uuid_cf.multiget(parent_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_parent_anchor(sort=False):
                # flatten to [('children:<type>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_child_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    child_uuid = col_name.split(':')[2]
                    if obj_uuids and child_uuid not in obj_uuids:
                        continue
                    all_child_infos[child_uuid] = {'uuid': child_uuid, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_child_infos = filter_rows(all_child_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_child_infos = all_child_infos

                if not sort:
                    ret_child_infos = filt_child_infos.values()
                else:
                    ret_child_infos = sorted(filt_child_infos.values(), key=itemgetter('tstamp'))

                return get_fq_name_uuid_list(r['uuid'] for r in ret_child_infos)
            # end filter_rows_parent_anchor

            if count:
                return (True, len(filter_rows_parent_anchor()))

            children_fq_names_uuids = filter_rows_parent_anchor(sort=True)

        if back_ref_uuids:
            # go from anchor to backrefs
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'backref:bgp_router:'
            col_fin = 'backref:bgp_router;'
            try:
                obj_rows = obj_uuid_cf.multiget(back_ref_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_backref_anchor():
                # flatten to [('<fqnstr>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_backref_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    col_name_arr = col_name.split(':')
                    fq_name = col_name_arr[:-1]
                    obj_uuid = col_name_arr[-1]
                    if obj_uuids and obj_uuid not in obj_uuids:
                        continue
                    all_backref_infos[obj_uuid] = \
                        {'uuid': obj_uuid, 'fq_name': fq_name, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_backref_infos = filter_rows(all_backref_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_backref_infos = all_backref_infos

                return [(br_info['fq_name'], br_info['uuid']) for br_info in filt_backref_infos.values()]
            # end filter_rows_backref_anchor

            if count:
                return (True, len(filter_rows_backref_anchor()))

            children_fq_names_uuids = filter_rows_backref_anchor()

        if not parent_uuids and not back_ref_uuids:
            obj_uuid_cf = self._obj_uuid_cf
            if obj_uuids:
                # exact objects specified
                def filter_rows_object_list():
                    all_obj_infos = {}
                    for obj_uuid in obj_uuids:
                        all_obj_infos[obj_uuid] = None

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return get_fq_name_uuid_list(filt_obj_infos.keys())
                # end filter_rows_object_list

                if count:
                    return (True, len(filter_rows_object_list()))
                children_fq_names_uuids = filter_rows_object_list()

            else: # grab all resources of this type
                obj_fq_name_cf = self._obj_fq_name_cf
                try:
                    cols = obj_fq_name_cf.get('bgp_router', column_count = 10000000)
                except pycassa.NotFoundException:
                    if count:
                        return (True, 0)
                    else:
                        return (True, children_fq_names_uuids)

                def filter_rows_no_anchor():
                    all_obj_infos = {}
                    for col_name, col_val in cols.items():
                        # give chance for zk heartbeat/ping
                        gevent.sleep(0)
                        col_name_arr = utils.decode_string(col_name).split(':')
                        obj_uuid = col_name_arr[-1]
                        all_obj_infos[obj_uuid] = (col_name_arr[:-1], obj_uuid)

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return filt_obj_infos.values()
                # end filter_rows_no_anchor

                if count:
                    return (True, len(filter_rows_no_anchor()))

                children_fq_names_uuids = filter_rows_no_anchor()

        return (True, children_fq_names_uuids)
    #end _cassandra_bgp_router_list

    def _cassandra_bgp_router_delete(self, obj_uuid):
        obj_uuid_cf = self._obj_uuid_cf
        fq_name = json.loads(obj_uuid_cf.get(obj_uuid, columns = ['fq_name'])['fq_name'])
        bch = obj_uuid_cf.batch()

        # unlink from parent
        col_start = 'parent:'
        col_fin = 'parent;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, parent_type, parent_uuid) = col_name.split(':')
            self._delete_child(bch, parent_type, parent_uuid, 'bgp_router', obj_uuid)

        # remove refs
        col_start = 'ref:'
        col_fin = 'ref;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, ref_type, ref_uuid) = col_name.split(':')
            self._delete_ref(bch, 'bgp_router', obj_uuid, ref_type, ref_uuid)

        bch.remove(obj_uuid)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(fq_name)
        fq_name_col = utils.encode_string(fq_name_str) + ':' + obj_uuid
        self._obj_fq_name_cf.remove('bgp_router', columns = [fq_name_col])


        return (True, '')
    #end _cassandra_bgp_router_delete

    def _cassandra_virtual_router_alloc(self, fq_name):
        return (True, '')
    #end _cassandra_virtual_router_alloc

    def _cassandra_virtual_router_create(self, obj_ids, obj_dict):
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        bch = self._obj_uuid_cf.batch()

        obj_cols = {}
        obj_cols['fq_name'] = json.dumps(obj_dict['fq_name'])
        obj_cols['type'] = json.dumps('virtual_router')
        if 'parent_type' in obj_dict:
            # non config-root child
            parent_type = obj_dict['parent_type']
            parent_method_type = parent_type.replace('-', '_')
            parent_fq_name = obj_dict['fq_name'][:-1]
            obj_cols['parent_type'] = json.dumps(parent_type)
            parent_uuid = self.fq_name_to_uuid(parent_method_type, parent_fq_name)
            self._create_child(bch, parent_method_type, parent_uuid, 'virtual_router', obj_ids['uuid'])

        # Properties
        field = obj_dict.get('virtual_router_type', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'virtual_router_type', field)

        field = obj_dict.get('virtual_router_ip_address', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'virtual_router_ip_address', field)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            field['created'] = datetime.datetime.utcnow().isoformat()
            field['last_modified'] = field['created']
            self._create_prop(bch, obj_ids['uuid'], 'id_perms', field)

        field = obj_dict.get('display_name', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'display_name', field)


        # References
        refs = obj_dict.get('bgp_router_refs', [])
        for ref in refs:
            ref_uuid = self.fq_name_to_uuid('bgp_router', ref['to'])
            ref_attr = ref.get('attr', None)
            ref_data = {'attr': ref_attr, 'is_weakref': False}
            self._create_ref(bch, 'virtual_router', obj_ids['uuid'], 'bgp_router', ref_uuid, ref_data)
        refs = obj_dict.get('virtual_machine_refs', [])
        for ref in refs:
            ref_uuid = self.fq_name_to_uuid('virtual_machine', ref['to'])
            ref_attr = ref.get('attr', None)
            ref_data = {'attr': ref_attr, 'is_weakref': False}
            self._create_ref(bch, 'virtual_router', obj_ids['uuid'], 'virtual_machine', ref_uuid, ref_data)

        bch.insert(obj_ids['uuid'], obj_cols)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(obj_dict['fq_name'])
        fq_name_cols = {utils.encode_string(fq_name_str) + ':' + obj_ids['uuid']: json.dumps(None)}
        self._obj_fq_name_cf.insert('virtual_router', fq_name_cols)

        return (True, '')
    #end _cassandra_virtual_router_create

    def _cassandra_virtual_router_read(self, obj_uuids, field_names = None):
        # if field_names = None, all fields will be read/returned

        obj_uuid_cf = self._obj_uuid_cf

        # optimize for common case of reading non-backref, non-children fields
        # ignoring columns starting from 'b' and 'c' - significant performance
        # impact in scaled setting. e.g. read of project
        if (field_names is None or
            (set(field_names) & (VirtualRouter.backref_fields | VirtualRouter.children_fields))):
            # atleast one backref/children field is needed
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_count = 10000000,
                                       include_timestamp = True)
        else: # ignore reading backref + children columns
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_start = 'd',
                                       column_count = 10000000,
                                       include_timestamp = True)

        if (len(obj_uuids) == 1) and not obj_rows:
            raise cfgm_common.exceptions.NoIdError(obj_uuids[0])

        results = []
        for row_key in obj_rows:
            # give chance for zk heartbeat/ping
            gevent.sleep(0)
            obj_uuid = row_key
            obj_cols = obj_rows[obj_uuid]
            result = {}
            result['uuid'] = obj_uuid
            result['fq_name'] = json.loads(obj_cols['fq_name'][0])
            for col_name in obj_cols.keys():
                if self._re_match_parent.match(col_name):
                    # non config-root child
                    (_, _, parent_uuid) = col_name.split(':')
                    parent_type = json.loads(obj_cols['parent_type'][0])
                    result['parent_type'] = parent_type
                    try:
                        result['parent_uuid'] = parent_uuid
                        result['parent_href'] = self._generate_url(parent_type, parent_uuid)
                    except cfgm_common.exceptions.NoIdError:
                        err_msg = 'Unknown uuid for parent ' + result['fq_name'][-2]
                        return (False, err_msg)

                # TODO use compiled RE
                if self._re_match_prop.match(col_name):
                    (_, prop_name) = col_name.split(':')
                    result[prop_name] = json.loads(obj_cols[col_name][0])

                # TODO use compiled RE
                if self._re_match_children.match(col_name):
                    (_, child_type, child_uuid) = col_name.split(':')
                    if field_names and '%ss' %(child_type) not in field_names:
                        continue

                    child_tstamp = obj_cols[col_name][1]
                    try:
                        self._read_child(result, obj_uuid, child_type, child_uuid, child_tstamp)
                    except cfgm_common.exceptions.NoIdError:
                        continue

                # TODO use compiled RE
                if self._re_match_ref.match(col_name):
                    (_, ref_type, ref_uuid) = col_name.split(':')
                    self._read_ref(result, obj_uuid, ref_type, ref_uuid, obj_cols[col_name][0])

                if self._re_match_backref.match(col_name):
                    (_, back_ref_type, back_ref_uuid) = col_name.split(':')
                    if field_names and '%s_back_refs' %(back_ref_type) not in field_names:
                        continue

                    try:
                        self._read_back_ref(result, obj_uuid, back_ref_type, back_ref_uuid,
                                            obj_cols[col_name][0])
                    except cfgm_common.exceptions.NoIdError:
                        continue

            # for all column names


            results.append(result)
        # end for all rows

        return (True, results)
    #end _cassandra_virtual_router_read

    def _cassandra_virtual_router_count_children(self, obj_uuid, child_type):
        # if child_type = None, return
        if child_type is None:
            return (False, '')

        obj_uuid_cf = self._obj_uuid_cf
        if child_type not in VirtualRouter.children_fields:
            return (False, '%s is not a valid children of VirtualRouter' %(child_type))

        col_start = 'children:'+child_type[:-1]+':'
        col_finish = 'children:'+child_type[:-1]+';'
        num_children = obj_uuid_cf.get_count(obj_uuid,
                                   column_start = col_start,
                                   column_finish = col_finish,
                                   max_count = 10000000)
        return (True, num_children)
    #end _cassandra_virtual_router_count_children

    def _cassandra_virtual_router_update(self, obj_uuid, new_obj_dict):
        # Grab ref-uuids and properties in new version
        new_ref_infos = {}

        if 'bgp_router_refs' in new_obj_dict:
            new_ref_infos['bgp_router'] = {}
            new_refs = new_obj_dict['bgp_router_refs']
            if new_refs:
                for new_ref in new_refs:
                    new_ref_uuid = self.fq_name_to_uuid('bgp_router', new_ref['to'])
                    new_ref_attr = new_ref.get('attr', None)
                    new_ref_data = {'attr': new_ref_attr, 'is_weakref': False}
                    new_ref_infos['bgp_router'][new_ref_uuid] = new_ref_data

        if 'virtual_machine_refs' in new_obj_dict:
            new_ref_infos['virtual_machine'] = {}
            new_refs = new_obj_dict['virtual_machine_refs']
            if new_refs:
                for new_ref in new_refs:
                    new_ref_uuid = self.fq_name_to_uuid('virtual_machine', new_ref['to'])
                    new_ref_attr = new_ref.get('attr', None)
                    new_ref_data = {'attr': new_ref_attr, 'is_weakref': False}
                    new_ref_infos['virtual_machine'][new_ref_uuid] = new_ref_data

        new_props = {}
        if 'virtual_router_type' in new_obj_dict:
            new_props['virtual_router_type'] = new_obj_dict['virtual_router_type']
        if 'virtual_router_ip_address' in new_obj_dict:
            new_props['virtual_router_ip_address'] = new_obj_dict['virtual_router_ip_address']
        if 'id_perms' in new_obj_dict:
            new_props['id_perms'] = new_obj_dict['id_perms']
        if 'display_name' in new_obj_dict:
            new_props['display_name'] = new_obj_dict['display_name']
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        obj_uuid_cf = self._obj_uuid_cf
        obj_cols_iter = obj_uuid_cf.xget(obj_uuid)
        # TODO optimize this (converts tuple to dict)
        obj_cols = {}
        for col_info in obj_cols_iter:
            obj_cols[col_info[0]] = col_info[1]

        bch = obj_uuid_cf.batch()
        for col_name in obj_cols.keys():
            # TODO use compiled RE
            if re.match('prop:', col_name):
                (_, prop_name) = col_name.split(':')
                if prop_name == 'id_perms':
                    # id-perms always has to be updated for last-mod timestamp
                    # get it from request dict(or from db if not in request dict)
                    new_id_perms = new_obj_dict.get(prop_name, json.loads(obj_cols[col_name]))
                    self.update_last_modified(bch, obj_uuid, new_id_perms)
                elif prop_name in new_obj_dict:
                    self._update_prop(bch, obj_uuid, prop_name, new_props)

            # TODO use compiled RE
            if re.match('ref:', col_name):
                (_, ref_type, ref_uuid) = col_name.split(':')
                self._update_ref(bch, 'virtual_router', obj_uuid, ref_type, ref_uuid, new_ref_infos)
        # for all column names

        # create new refs
        for ref_type in new_ref_infos.keys():
            for ref_uuid in new_ref_infos[ref_type].keys():
                ref_data = new_ref_infos[ref_type][ref_uuid]
                self._create_ref(bch, 'virtual_router', obj_uuid, ref_type, ref_uuid, ref_data)

        # create new props
        for prop_name in new_props.keys():
            self._create_prop(bch, obj_uuid, prop_name, new_props[prop_name])

        bch.send()

        return (True, '')
    #end _cassandra_virtual_router_update

    def _cassandra_virtual_router_list(self, parent_uuids=None, back_ref_uuids=None,
                           obj_uuids=None, count=False, filters=None):
        children_fq_names_uuids = []
        if filters:
            fnames = filters.get('field_names', [])
            fvalues = filters.get('field_values', [])
            filter_fields = [(fnames[i], fvalues[i]) for i in range(len(fnames))]
        else:
            filter_fields = []

        def filter_rows(coll_infos, filter_cols, filter_params):
            filt_infos = {}
            coll_rows = obj_uuid_cf.multiget(coll_infos.keys(),
                                   columns=filter_cols,
                                   column_count=10000000)
            for row in coll_rows:
                # give chance for zk heartbeat/ping
                gevent.sleep(0)
                full_match = True
                for fname, fval in filter_params:
                    if coll_rows[row]['prop:%s' %(fname)] != fval:
                        full_match = False
                        break
                if full_match:
                    filt_infos[row] = coll_infos[row]
            return filt_infos
        # end filter_rows

        def get_fq_name_uuid_list(obj_uuids):
            ret_list = []
            for obj_uuid in obj_uuids:
                try:
                    obj_fq_name = self.uuid_to_fq_name(obj_uuid)
                    ret_list.append((obj_fq_name, obj_uuid))
                except cfgm_common.exceptions.NoIdError:
                    pass
            return ret_list
        # end get_fq_name_uuid_list

        if parent_uuids:
            # go from parent to child
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'children:virtual_router:'
            col_fin = 'children:virtual_router;'
            try:
                obj_rows = obj_uuid_cf.multiget(parent_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_parent_anchor(sort=False):
                # flatten to [('children:<type>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_child_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    child_uuid = col_name.split(':')[2]
                    if obj_uuids and child_uuid not in obj_uuids:
                        continue
                    all_child_infos[child_uuid] = {'uuid': child_uuid, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_child_infos = filter_rows(all_child_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_child_infos = all_child_infos

                if not sort:
                    ret_child_infos = filt_child_infos.values()
                else:
                    ret_child_infos = sorted(filt_child_infos.values(), key=itemgetter('tstamp'))

                return get_fq_name_uuid_list(r['uuid'] for r in ret_child_infos)
            # end filter_rows_parent_anchor

            if count:
                return (True, len(filter_rows_parent_anchor()))

            children_fq_names_uuids = filter_rows_parent_anchor(sort=True)

        if back_ref_uuids:
            # go from anchor to backrefs
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'backref:virtual_router:'
            col_fin = 'backref:virtual_router;'
            try:
                obj_rows = obj_uuid_cf.multiget(back_ref_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_backref_anchor():
                # flatten to [('<fqnstr>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_backref_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    col_name_arr = col_name.split(':')
                    fq_name = col_name_arr[:-1]
                    obj_uuid = col_name_arr[-1]
                    if obj_uuids and obj_uuid not in obj_uuids:
                        continue
                    all_backref_infos[obj_uuid] = \
                        {'uuid': obj_uuid, 'fq_name': fq_name, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_backref_infos = filter_rows(all_backref_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_backref_infos = all_backref_infos

                return [(br_info['fq_name'], br_info['uuid']) for br_info in filt_backref_infos.values()]
            # end filter_rows_backref_anchor

            if count:
                return (True, len(filter_rows_backref_anchor()))

            children_fq_names_uuids = filter_rows_backref_anchor()

        if not parent_uuids and not back_ref_uuids:
            obj_uuid_cf = self._obj_uuid_cf
            if obj_uuids:
                # exact objects specified
                def filter_rows_object_list():
                    all_obj_infos = {}
                    for obj_uuid in obj_uuids:
                        all_obj_infos[obj_uuid] = None

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return get_fq_name_uuid_list(filt_obj_infos.keys())
                # end filter_rows_object_list

                if count:
                    return (True, len(filter_rows_object_list()))
                children_fq_names_uuids = filter_rows_object_list()

            else: # grab all resources of this type
                obj_fq_name_cf = self._obj_fq_name_cf
                try:
                    cols = obj_fq_name_cf.get('virtual_router', column_count = 10000000)
                except pycassa.NotFoundException:
                    if count:
                        return (True, 0)
                    else:
                        return (True, children_fq_names_uuids)

                def filter_rows_no_anchor():
                    all_obj_infos = {}
                    for col_name, col_val in cols.items():
                        # give chance for zk heartbeat/ping
                        gevent.sleep(0)
                        col_name_arr = utils.decode_string(col_name).split(':')
                        obj_uuid = col_name_arr[-1]
                        all_obj_infos[obj_uuid] = (col_name_arr[:-1], obj_uuid)

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return filt_obj_infos.values()
                # end filter_rows_no_anchor

                if count:
                    return (True, len(filter_rows_no_anchor()))

                children_fq_names_uuids = filter_rows_no_anchor()

        return (True, children_fq_names_uuids)
    #end _cassandra_virtual_router_list

    def _cassandra_virtual_router_delete(self, obj_uuid):
        obj_uuid_cf = self._obj_uuid_cf
        fq_name = json.loads(obj_uuid_cf.get(obj_uuid, columns = ['fq_name'])['fq_name'])
        bch = obj_uuid_cf.batch()

        # unlink from parent
        col_start = 'parent:'
        col_fin = 'parent;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, parent_type, parent_uuid) = col_name.split(':')
            self._delete_child(bch, parent_type, parent_uuid, 'virtual_router', obj_uuid)

        # remove refs
        col_start = 'ref:'
        col_fin = 'ref;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, ref_type, ref_uuid) = col_name.split(':')
            self._delete_ref(bch, 'virtual_router', obj_uuid, ref_type, ref_uuid)

        bch.remove(obj_uuid)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(fq_name)
        fq_name_col = utils.encode_string(fq_name_str) + ':' + obj_uuid
        self._obj_fq_name_cf.remove('virtual_router', columns = [fq_name_col])


        return (True, '')
    #end _cassandra_virtual_router_delete

    def _cassandra_config_root_alloc(self, fq_name):
        return (True, '')
    #end _cassandra_config_root_alloc

    def _cassandra_config_root_create(self, obj_ids, obj_dict):
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        bch = self._obj_uuid_cf.batch()

        obj_cols = {}
        obj_cols['fq_name'] = json.dumps(obj_dict['fq_name'])
        obj_cols['type'] = json.dumps('config_root')

        # Properties
        field = obj_dict.get('id_perms', None)
        if field is not None:
            field['created'] = datetime.datetime.utcnow().isoformat()
            field['last_modified'] = field['created']
            self._create_prop(bch, obj_ids['uuid'], 'id_perms', field)

        field = obj_dict.get('display_name', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'display_name', field)


        # References

        bch.insert(obj_ids['uuid'], obj_cols)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(obj_dict['fq_name'])
        fq_name_cols = {utils.encode_string(fq_name_str) + ':' + obj_ids['uuid']: json.dumps(None)}
        self._obj_fq_name_cf.insert('config_root', fq_name_cols)

        return (True, '')
    #end _cassandra_config_root_create

    def _cassandra_config_root_read(self, obj_uuids, field_names = None):
        # if field_names = None, all fields will be read/returned

        obj_uuid_cf = self._obj_uuid_cf

        # optimize for common case of reading non-backref, non-children fields
        # ignoring columns starting from 'b' and 'c' - significant performance
        # impact in scaled setting. e.g. read of project
        if (field_names is None or
            (set(field_names) & (ConfigRoot.backref_fields | ConfigRoot.children_fields))):
            # atleast one backref/children field is needed
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_count = 10000000,
                                       include_timestamp = True)
        else: # ignore reading backref + children columns
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_start = 'd',
                                       column_count = 10000000,
                                       include_timestamp = True)

        if (len(obj_uuids) == 1) and not obj_rows:
            raise cfgm_common.exceptions.NoIdError(obj_uuids[0])

        results = []
        for row_key in obj_rows:
            # give chance for zk heartbeat/ping
            gevent.sleep(0)
            obj_uuid = row_key
            obj_cols = obj_rows[obj_uuid]
            result = {}
            result['uuid'] = obj_uuid
            result['fq_name'] = json.loads(obj_cols['fq_name'][0])
            for col_name in obj_cols.keys():
                if self._re_match_parent.match(col_name):
                    # non config-root child
                    (_, _, parent_uuid) = col_name.split(':')
                    parent_type = json.loads(obj_cols['parent_type'][0])
                    result['parent_type'] = parent_type
                    try:
                        result['parent_uuid'] = parent_uuid
                        result['parent_href'] = self._generate_url(parent_type, parent_uuid)
                    except cfgm_common.exceptions.NoIdError:
                        err_msg = 'Unknown uuid for parent ' + result['fq_name'][-2]
                        return (False, err_msg)

                # TODO use compiled RE
                if self._re_match_prop.match(col_name):
                    (_, prop_name) = col_name.split(':')
                    result[prop_name] = json.loads(obj_cols[col_name][0])

                # TODO use compiled RE
                if self._re_match_children.match(col_name):
                    (_, child_type, child_uuid) = col_name.split(':')
                    if field_names and '%ss' %(child_type) not in field_names:
                        continue

                    child_tstamp = obj_cols[col_name][1]
                    try:
                        self._read_child(result, obj_uuid, child_type, child_uuid, child_tstamp)
                    except cfgm_common.exceptions.NoIdError:
                        continue

                # TODO use compiled RE
                if self._re_match_ref.match(col_name):
                    (_, ref_type, ref_uuid) = col_name.split(':')
                    self._read_ref(result, obj_uuid, ref_type, ref_uuid, obj_cols[col_name][0])

                if self._re_match_backref.match(col_name):
                    (_, back_ref_type, back_ref_uuid) = col_name.split(':')
                    if field_names and '%s_back_refs' %(back_ref_type) not in field_names:
                        continue

                    try:
                        self._read_back_ref(result, obj_uuid, back_ref_type, back_ref_uuid,
                                            obj_cols[col_name][0])
                    except cfgm_common.exceptions.NoIdError:
                        continue

            # for all column names

            if 'global_system_configs' in result:
                # sort children; TODO do this based on schema
                sorted_children = sorted(result['global_system_configs'], key = itemgetter('tstamp'))
                # re-write result's children without timestamp
                result['global_system_configs'] = sorted_children
                [child.pop('tstamp') for child in result['global_system_configs']]

            if 'domains' in result:
                # sort children; TODO do this based on schema
                sorted_children = sorted(result['domains'], key = itemgetter('tstamp'))
                # re-write result's children without timestamp
                result['domains'] = sorted_children
                [child.pop('tstamp') for child in result['domains']]

            results.append(result)
        # end for all rows

        return (True, results)
    #end _cassandra_config_root_read

    def _cassandra_config_root_count_children(self, obj_uuid, child_type):
        # if child_type = None, return
        if child_type is None:
            return (False, '')

        obj_uuid_cf = self._obj_uuid_cf
        if child_type not in ConfigRoot.children_fields:
            return (False, '%s is not a valid children of ConfigRoot' %(child_type))

        col_start = 'children:'+child_type[:-1]+':'
        col_finish = 'children:'+child_type[:-1]+';'
        num_children = obj_uuid_cf.get_count(obj_uuid,
                                   column_start = col_start,
                                   column_finish = col_finish,
                                   max_count = 10000000)
        return (True, num_children)
    #end _cassandra_config_root_count_children

    def _cassandra_config_root_update(self, obj_uuid, new_obj_dict):
        # Grab ref-uuids and properties in new version
        new_ref_infos = {}

        new_props = {}
        if 'id_perms' in new_obj_dict:
            new_props['id_perms'] = new_obj_dict['id_perms']
        if 'display_name' in new_obj_dict:
            new_props['display_name'] = new_obj_dict['display_name']
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        obj_uuid_cf = self._obj_uuid_cf
        obj_cols_iter = obj_uuid_cf.xget(obj_uuid)
        # TODO optimize this (converts tuple to dict)
        obj_cols = {}
        for col_info in obj_cols_iter:
            obj_cols[col_info[0]] = col_info[1]

        bch = obj_uuid_cf.batch()
        for col_name in obj_cols.keys():
            # TODO use compiled RE
            if re.match('prop:', col_name):
                (_, prop_name) = col_name.split(':')
                if prop_name == 'id_perms':
                    # id-perms always has to be updated for last-mod timestamp
                    # get it from request dict(or from db if not in request dict)
                    new_id_perms = new_obj_dict.get(prop_name, json.loads(obj_cols[col_name]))
                    self.update_last_modified(bch, obj_uuid, new_id_perms)
                elif prop_name in new_obj_dict:
                    self._update_prop(bch, obj_uuid, prop_name, new_props)

            # TODO use compiled RE
            if re.match('ref:', col_name):
                (_, ref_type, ref_uuid) = col_name.split(':')
                self._update_ref(bch, 'config_root', obj_uuid, ref_type, ref_uuid, new_ref_infos)
        # for all column names

        # create new refs
        for ref_type in new_ref_infos.keys():
            for ref_uuid in new_ref_infos[ref_type].keys():
                ref_data = new_ref_infos[ref_type][ref_uuid]
                self._create_ref(bch, 'config_root', obj_uuid, ref_type, ref_uuid, ref_data)

        # create new props
        for prop_name in new_props.keys():
            self._create_prop(bch, obj_uuid, prop_name, new_props[prop_name])

        bch.send()

        return (True, '')
    #end _cassandra_config_root_update

    def _cassandra_config_root_list(self, parent_uuids=None, back_ref_uuids=None,
                           obj_uuids=None, count=False, filters=None):
        children_fq_names_uuids = []
        if filters:
            fnames = filters.get('field_names', [])
            fvalues = filters.get('field_values', [])
            filter_fields = [(fnames[i], fvalues[i]) for i in range(len(fnames))]
        else:
            filter_fields = []

        def filter_rows(coll_infos, filter_cols, filter_params):
            filt_infos = {}
            coll_rows = obj_uuid_cf.multiget(coll_infos.keys(),
                                   columns=filter_cols,
                                   column_count=10000000)
            for row in coll_rows:
                # give chance for zk heartbeat/ping
                gevent.sleep(0)
                full_match = True
                for fname, fval in filter_params:
                    if coll_rows[row]['prop:%s' %(fname)] != fval:
                        full_match = False
                        break
                if full_match:
                    filt_infos[row] = coll_infos[row]
            return filt_infos
        # end filter_rows

        def get_fq_name_uuid_list(obj_uuids):
            ret_list = []
            for obj_uuid in obj_uuids:
                try:
                    obj_fq_name = self.uuid_to_fq_name(obj_uuid)
                    ret_list.append((obj_fq_name, obj_uuid))
                except cfgm_common.exceptions.NoIdError:
                    pass
            return ret_list
        # end get_fq_name_uuid_list

        if parent_uuids:
            # go from parent to child
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'children:config_root:'
            col_fin = 'children:config_root;'
            try:
                obj_rows = obj_uuid_cf.multiget(parent_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_parent_anchor(sort=False):
                # flatten to [('children:<type>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_child_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    child_uuid = col_name.split(':')[2]
                    if obj_uuids and child_uuid not in obj_uuids:
                        continue
                    all_child_infos[child_uuid] = {'uuid': child_uuid, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_child_infos = filter_rows(all_child_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_child_infos = all_child_infos

                if not sort:
                    ret_child_infos = filt_child_infos.values()
                else:
                    ret_child_infos = sorted(filt_child_infos.values(), key=itemgetter('tstamp'))

                return get_fq_name_uuid_list(r['uuid'] for r in ret_child_infos)
            # end filter_rows_parent_anchor

            if count:
                return (True, len(filter_rows_parent_anchor()))

            children_fq_names_uuids = filter_rows_parent_anchor(sort=True)

        if back_ref_uuids:
            # go from anchor to backrefs
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'backref:config_root:'
            col_fin = 'backref:config_root;'
            try:
                obj_rows = obj_uuid_cf.multiget(back_ref_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_backref_anchor():
                # flatten to [('<fqnstr>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_backref_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    col_name_arr = col_name.split(':')
                    fq_name = col_name_arr[:-1]
                    obj_uuid = col_name_arr[-1]
                    if obj_uuids and obj_uuid not in obj_uuids:
                        continue
                    all_backref_infos[obj_uuid] = \
                        {'uuid': obj_uuid, 'fq_name': fq_name, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_backref_infos = filter_rows(all_backref_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_backref_infos = all_backref_infos

                return [(br_info['fq_name'], br_info['uuid']) for br_info in filt_backref_infos.values()]
            # end filter_rows_backref_anchor

            if count:
                return (True, len(filter_rows_backref_anchor()))

            children_fq_names_uuids = filter_rows_backref_anchor()

        if not parent_uuids and not back_ref_uuids:
            obj_uuid_cf = self._obj_uuid_cf
            if obj_uuids:
                # exact objects specified
                def filter_rows_object_list():
                    all_obj_infos = {}
                    for obj_uuid in obj_uuids:
                        all_obj_infos[obj_uuid] = None

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return get_fq_name_uuid_list(filt_obj_infos.keys())
                # end filter_rows_object_list

                if count:
                    return (True, len(filter_rows_object_list()))
                children_fq_names_uuids = filter_rows_object_list()

            else: # grab all resources of this type
                obj_fq_name_cf = self._obj_fq_name_cf
                try:
                    cols = obj_fq_name_cf.get('config_root', column_count = 10000000)
                except pycassa.NotFoundException:
                    if count:
                        return (True, 0)
                    else:
                        return (True, children_fq_names_uuids)

                def filter_rows_no_anchor():
                    all_obj_infos = {}
                    for col_name, col_val in cols.items():
                        # give chance for zk heartbeat/ping
                        gevent.sleep(0)
                        col_name_arr = utils.decode_string(col_name).split(':')
                        obj_uuid = col_name_arr[-1]
                        all_obj_infos[obj_uuid] = (col_name_arr[:-1], obj_uuid)

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return filt_obj_infos.values()
                # end filter_rows_no_anchor

                if count:
                    return (True, len(filter_rows_no_anchor()))

                children_fq_names_uuids = filter_rows_no_anchor()

        return (True, children_fq_names_uuids)
    #end _cassandra_config_root_list

    def _cassandra_config_root_delete(self, obj_uuid):
        obj_uuid_cf = self._obj_uuid_cf
        fq_name = json.loads(obj_uuid_cf.get(obj_uuid, columns = ['fq_name'])['fq_name'])
        bch = obj_uuid_cf.batch()

        # unlink from parent
        col_start = 'parent:'
        col_fin = 'parent;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, parent_type, parent_uuid) = col_name.split(':')
            self._delete_child(bch, parent_type, parent_uuid, 'config_root', obj_uuid)

        # remove refs
        col_start = 'ref:'
        col_fin = 'ref;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, ref_type, ref_uuid) = col_name.split(':')
            self._delete_ref(bch, 'config_root', obj_uuid, ref_type, ref_uuid)

        bch.remove(obj_uuid)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(fq_name)
        fq_name_col = utils.encode_string(fq_name_str) + ':' + obj_uuid
        self._obj_fq_name_cf.remove('config_root', columns = [fq_name_col])


        return (True, '')
    #end _cassandra_config_root_delete

    def _cassandra_subnet_alloc(self, fq_name):
        return (True, '')
    #end _cassandra_subnet_alloc

    def _cassandra_subnet_create(self, obj_ids, obj_dict):
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        bch = self._obj_uuid_cf.batch()

        obj_cols = {}
        obj_cols['fq_name'] = json.dumps(obj_dict['fq_name'])
        obj_cols['type'] = json.dumps('subnet')

        # Properties
        field = obj_dict.get('subnet_ip_prefix', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'subnet_ip_prefix', field)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            field['created'] = datetime.datetime.utcnow().isoformat()
            field['last_modified'] = field['created']
            self._create_prop(bch, obj_ids['uuid'], 'id_perms', field)

        field = obj_dict.get('display_name', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'display_name', field)


        # References
        refs = obj_dict.get('virtual_machine_interface_refs', [])
        for ref in refs:
            ref_uuid = self.fq_name_to_uuid('virtual_machine_interface', ref['to'])
            ref_attr = ref.get('attr', None)
            ref_data = {'attr': ref_attr, 'is_weakref': False}
            self._create_ref(bch, 'subnet', obj_ids['uuid'], 'virtual_machine_interface', ref_uuid, ref_data)

        bch.insert(obj_ids['uuid'], obj_cols)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(obj_dict['fq_name'])
        fq_name_cols = {utils.encode_string(fq_name_str) + ':' + obj_ids['uuid']: json.dumps(None)}
        self._obj_fq_name_cf.insert('subnet', fq_name_cols)

        return (True, '')
    #end _cassandra_subnet_create

    def _cassandra_subnet_read(self, obj_uuids, field_names = None):
        # if field_names = None, all fields will be read/returned

        obj_uuid_cf = self._obj_uuid_cf

        # optimize for common case of reading non-backref, non-children fields
        # ignoring columns starting from 'b' and 'c' - significant performance
        # impact in scaled setting. e.g. read of project
        if (field_names is None or
            (set(field_names) & (Subnet.backref_fields | Subnet.children_fields))):
            # atleast one backref/children field is needed
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_count = 10000000,
                                       include_timestamp = True)
        else: # ignore reading backref + children columns
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_start = 'd',
                                       column_count = 10000000,
                                       include_timestamp = True)

        if (len(obj_uuids) == 1) and not obj_rows:
            raise cfgm_common.exceptions.NoIdError(obj_uuids[0])

        results = []
        for row_key in obj_rows:
            # give chance for zk heartbeat/ping
            gevent.sleep(0)
            obj_uuid = row_key
            obj_cols = obj_rows[obj_uuid]
            result = {}
            result['uuid'] = obj_uuid
            result['fq_name'] = json.loads(obj_cols['fq_name'][0])
            for col_name in obj_cols.keys():
                if self._re_match_parent.match(col_name):
                    # non config-root child
                    (_, _, parent_uuid) = col_name.split(':')
                    parent_type = json.loads(obj_cols['parent_type'][0])
                    result['parent_type'] = parent_type
                    try:
                        result['parent_uuid'] = parent_uuid
                        result['parent_href'] = self._generate_url(parent_type, parent_uuid)
                    except cfgm_common.exceptions.NoIdError:
                        err_msg = 'Unknown uuid for parent ' + result['fq_name'][-2]
                        return (False, err_msg)

                # TODO use compiled RE
                if self._re_match_prop.match(col_name):
                    (_, prop_name) = col_name.split(':')
                    result[prop_name] = json.loads(obj_cols[col_name][0])

                # TODO use compiled RE
                if self._re_match_children.match(col_name):
                    (_, child_type, child_uuid) = col_name.split(':')
                    if field_names and '%ss' %(child_type) not in field_names:
                        continue

                    child_tstamp = obj_cols[col_name][1]
                    try:
                        self._read_child(result, obj_uuid, child_type, child_uuid, child_tstamp)
                    except cfgm_common.exceptions.NoIdError:
                        continue

                # TODO use compiled RE
                if self._re_match_ref.match(col_name):
                    (_, ref_type, ref_uuid) = col_name.split(':')
                    self._read_ref(result, obj_uuid, ref_type, ref_uuid, obj_cols[col_name][0])

                if self._re_match_backref.match(col_name):
                    (_, back_ref_type, back_ref_uuid) = col_name.split(':')
                    if field_names and '%s_back_refs' %(back_ref_type) not in field_names:
                        continue

                    try:
                        self._read_back_ref(result, obj_uuid, back_ref_type, back_ref_uuid,
                                            obj_cols[col_name][0])
                    except cfgm_common.exceptions.NoIdError:
                        continue

            # for all column names


            results.append(result)
        # end for all rows

        return (True, results)
    #end _cassandra_subnet_read

    def _cassandra_subnet_count_children(self, obj_uuid, child_type):
        # if child_type = None, return
        if child_type is None:
            return (False, '')

        obj_uuid_cf = self._obj_uuid_cf
        if child_type not in Subnet.children_fields:
            return (False, '%s is not a valid children of Subnet' %(child_type))

        col_start = 'children:'+child_type[:-1]+':'
        col_finish = 'children:'+child_type[:-1]+';'
        num_children = obj_uuid_cf.get_count(obj_uuid,
                                   column_start = col_start,
                                   column_finish = col_finish,
                                   max_count = 10000000)
        return (True, num_children)
    #end _cassandra_subnet_count_children

    def _cassandra_subnet_update(self, obj_uuid, new_obj_dict):
        # Grab ref-uuids and properties in new version
        new_ref_infos = {}

        if 'virtual_machine_interface_refs' in new_obj_dict:
            new_ref_infos['virtual_machine_interface'] = {}
            new_refs = new_obj_dict['virtual_machine_interface_refs']
            if new_refs:
                for new_ref in new_refs:
                    new_ref_uuid = self.fq_name_to_uuid('virtual_machine_interface', new_ref['to'])
                    new_ref_attr = new_ref.get('attr', None)
                    new_ref_data = {'attr': new_ref_attr, 'is_weakref': False}
                    new_ref_infos['virtual_machine_interface'][new_ref_uuid] = new_ref_data

        new_props = {}
        if 'subnet_ip_prefix' in new_obj_dict:
            new_props['subnet_ip_prefix'] = new_obj_dict['subnet_ip_prefix']
        if 'id_perms' in new_obj_dict:
            new_props['id_perms'] = new_obj_dict['id_perms']
        if 'display_name' in new_obj_dict:
            new_props['display_name'] = new_obj_dict['display_name']
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        obj_uuid_cf = self._obj_uuid_cf
        obj_cols_iter = obj_uuid_cf.xget(obj_uuid)
        # TODO optimize this (converts tuple to dict)
        obj_cols = {}
        for col_info in obj_cols_iter:
            obj_cols[col_info[0]] = col_info[1]

        bch = obj_uuid_cf.batch()
        for col_name in obj_cols.keys():
            # TODO use compiled RE
            if re.match('prop:', col_name):
                (_, prop_name) = col_name.split(':')
                if prop_name == 'id_perms':
                    # id-perms always has to be updated for last-mod timestamp
                    # get it from request dict(or from db if not in request dict)
                    new_id_perms = new_obj_dict.get(prop_name, json.loads(obj_cols[col_name]))
                    self.update_last_modified(bch, obj_uuid, new_id_perms)
                elif prop_name in new_obj_dict:
                    self._update_prop(bch, obj_uuid, prop_name, new_props)

            # TODO use compiled RE
            if re.match('ref:', col_name):
                (_, ref_type, ref_uuid) = col_name.split(':')
                self._update_ref(bch, 'subnet', obj_uuid, ref_type, ref_uuid, new_ref_infos)
        # for all column names

        # create new refs
        for ref_type in new_ref_infos.keys():
            for ref_uuid in new_ref_infos[ref_type].keys():
                ref_data = new_ref_infos[ref_type][ref_uuid]
                self._create_ref(bch, 'subnet', obj_uuid, ref_type, ref_uuid, ref_data)

        # create new props
        for prop_name in new_props.keys():
            self._create_prop(bch, obj_uuid, prop_name, new_props[prop_name])

        bch.send()

        return (True, '')
    #end _cassandra_subnet_update

    def _cassandra_subnet_list(self, parent_uuids=None, back_ref_uuids=None,
                           obj_uuids=None, count=False, filters=None):
        children_fq_names_uuids = []
        if filters:
            fnames = filters.get('field_names', [])
            fvalues = filters.get('field_values', [])
            filter_fields = [(fnames[i], fvalues[i]) for i in range(len(fnames))]
        else:
            filter_fields = []

        def filter_rows(coll_infos, filter_cols, filter_params):
            filt_infos = {}
            coll_rows = obj_uuid_cf.multiget(coll_infos.keys(),
                                   columns=filter_cols,
                                   column_count=10000000)
            for row in coll_rows:
                # give chance for zk heartbeat/ping
                gevent.sleep(0)
                full_match = True
                for fname, fval in filter_params:
                    if coll_rows[row]['prop:%s' %(fname)] != fval:
                        full_match = False
                        break
                if full_match:
                    filt_infos[row] = coll_infos[row]
            return filt_infos
        # end filter_rows

        def get_fq_name_uuid_list(obj_uuids):
            ret_list = []
            for obj_uuid in obj_uuids:
                try:
                    obj_fq_name = self.uuid_to_fq_name(obj_uuid)
                    ret_list.append((obj_fq_name, obj_uuid))
                except cfgm_common.exceptions.NoIdError:
                    pass
            return ret_list
        # end get_fq_name_uuid_list

        if parent_uuids:
            # go from parent to child
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'children:subnet:'
            col_fin = 'children:subnet;'
            try:
                obj_rows = obj_uuid_cf.multiget(parent_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_parent_anchor(sort=False):
                # flatten to [('children:<type>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_child_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    child_uuid = col_name.split(':')[2]
                    if obj_uuids and child_uuid not in obj_uuids:
                        continue
                    all_child_infos[child_uuid] = {'uuid': child_uuid, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_child_infos = filter_rows(all_child_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_child_infos = all_child_infos

                if not sort:
                    ret_child_infos = filt_child_infos.values()
                else:
                    ret_child_infos = sorted(filt_child_infos.values(), key=itemgetter('tstamp'))

                return get_fq_name_uuid_list(r['uuid'] for r in ret_child_infos)
            # end filter_rows_parent_anchor

            if count:
                return (True, len(filter_rows_parent_anchor()))

            children_fq_names_uuids = filter_rows_parent_anchor(sort=True)

        if back_ref_uuids:
            # go from anchor to backrefs
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'backref:subnet:'
            col_fin = 'backref:subnet;'
            try:
                obj_rows = obj_uuid_cf.multiget(back_ref_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_backref_anchor():
                # flatten to [('<fqnstr>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_backref_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    col_name_arr = col_name.split(':')
                    fq_name = col_name_arr[:-1]
                    obj_uuid = col_name_arr[-1]
                    if obj_uuids and obj_uuid not in obj_uuids:
                        continue
                    all_backref_infos[obj_uuid] = \
                        {'uuid': obj_uuid, 'fq_name': fq_name, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_backref_infos = filter_rows(all_backref_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_backref_infos = all_backref_infos

                return [(br_info['fq_name'], br_info['uuid']) for br_info in filt_backref_infos.values()]
            # end filter_rows_backref_anchor

            if count:
                return (True, len(filter_rows_backref_anchor()))

            children_fq_names_uuids = filter_rows_backref_anchor()

        if not parent_uuids and not back_ref_uuids:
            obj_uuid_cf = self._obj_uuid_cf
            if obj_uuids:
                # exact objects specified
                def filter_rows_object_list():
                    all_obj_infos = {}
                    for obj_uuid in obj_uuids:
                        all_obj_infos[obj_uuid] = None

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return get_fq_name_uuid_list(filt_obj_infos.keys())
                # end filter_rows_object_list

                if count:
                    return (True, len(filter_rows_object_list()))
                children_fq_names_uuids = filter_rows_object_list()

            else: # grab all resources of this type
                obj_fq_name_cf = self._obj_fq_name_cf
                try:
                    cols = obj_fq_name_cf.get('subnet', column_count = 10000000)
                except pycassa.NotFoundException:
                    if count:
                        return (True, 0)
                    else:
                        return (True, children_fq_names_uuids)

                def filter_rows_no_anchor():
                    all_obj_infos = {}
                    for col_name, col_val in cols.items():
                        # give chance for zk heartbeat/ping
                        gevent.sleep(0)
                        col_name_arr = utils.decode_string(col_name).split(':')
                        obj_uuid = col_name_arr[-1]
                        all_obj_infos[obj_uuid] = (col_name_arr[:-1], obj_uuid)

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return filt_obj_infos.values()
                # end filter_rows_no_anchor

                if count:
                    return (True, len(filter_rows_no_anchor()))

                children_fq_names_uuids = filter_rows_no_anchor()

        return (True, children_fq_names_uuids)
    #end _cassandra_subnet_list

    def _cassandra_subnet_delete(self, obj_uuid):
        obj_uuid_cf = self._obj_uuid_cf
        fq_name = json.loads(obj_uuid_cf.get(obj_uuid, columns = ['fq_name'])['fq_name'])
        bch = obj_uuid_cf.batch()

        # unlink from parent
        col_start = 'parent:'
        col_fin = 'parent;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, parent_type, parent_uuid) = col_name.split(':')
            self._delete_child(bch, parent_type, parent_uuid, 'subnet', obj_uuid)

        # remove refs
        col_start = 'ref:'
        col_fin = 'ref;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, ref_type, ref_uuid) = col_name.split(':')
            self._delete_ref(bch, 'subnet', obj_uuid, ref_type, ref_uuid)

        bch.remove(obj_uuid)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(fq_name)
        fq_name_col = utils.encode_string(fq_name_str) + ':' + obj_uuid
        self._obj_fq_name_cf.remove('subnet', columns = [fq_name_col])


        return (True, '')
    #end _cassandra_subnet_delete

    def _cassandra_global_system_config_alloc(self, fq_name):
        return (True, '')
    #end _cassandra_global_system_config_alloc

    def _cassandra_global_system_config_create(self, obj_ids, obj_dict):
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        bch = self._obj_uuid_cf.batch()

        obj_cols = {}
        obj_cols['fq_name'] = json.dumps(obj_dict['fq_name'])
        obj_cols['type'] = json.dumps('global_system_config')
        if 'parent_type' in obj_dict:
            # non config-root child
            parent_type = obj_dict['parent_type']
            parent_method_type = parent_type.replace('-', '_')
            parent_fq_name = obj_dict['fq_name'][:-1]
            obj_cols['parent_type'] = json.dumps(parent_type)
            parent_uuid = self.fq_name_to_uuid(parent_method_type, parent_fq_name)
            self._create_child(bch, parent_method_type, parent_uuid, 'global_system_config', obj_ids['uuid'])

        # Properties
        field = obj_dict.get('autonomous_system', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'autonomous_system', field)

        field = obj_dict.get('config_version', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'config_version', field)

        field = obj_dict.get('plugin_tuning', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'plugin_tuning', field)

        field = obj_dict.get('ibgp_auto_mesh', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'ibgp_auto_mesh', field)

        field = obj_dict.get('ip_fabric_subnets', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'ip_fabric_subnets', field)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            field['created'] = datetime.datetime.utcnow().isoformat()
            field['last_modified'] = field['created']
            self._create_prop(bch, obj_ids['uuid'], 'id_perms', field)

        field = obj_dict.get('display_name', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'display_name', field)


        # References
        refs = obj_dict.get('bgp_router_refs', [])
        for ref in refs:
            ref_uuid = self.fq_name_to_uuid('bgp_router', ref['to'])
            ref_attr = ref.get('attr', None)
            ref_data = {'attr': ref_attr, 'is_weakref': False}
            self._create_ref(bch, 'global_system_config', obj_ids['uuid'], 'bgp_router', ref_uuid, ref_data)

        bch.insert(obj_ids['uuid'], obj_cols)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(obj_dict['fq_name'])
        fq_name_cols = {utils.encode_string(fq_name_str) + ':' + obj_ids['uuid']: json.dumps(None)}
        self._obj_fq_name_cf.insert('global_system_config', fq_name_cols)

        return (True, '')
    #end _cassandra_global_system_config_create

    def _cassandra_global_system_config_read(self, obj_uuids, field_names = None):
        # if field_names = None, all fields will be read/returned

        obj_uuid_cf = self._obj_uuid_cf

        # optimize for common case of reading non-backref, non-children fields
        # ignoring columns starting from 'b' and 'c' - significant performance
        # impact in scaled setting. e.g. read of project
        if (field_names is None or
            (set(field_names) & (GlobalSystemConfig.backref_fields | GlobalSystemConfig.children_fields))):
            # atleast one backref/children field is needed
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_count = 10000000,
                                       include_timestamp = True)
        else: # ignore reading backref + children columns
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_start = 'd',
                                       column_count = 10000000,
                                       include_timestamp = True)

        if (len(obj_uuids) == 1) and not obj_rows:
            raise cfgm_common.exceptions.NoIdError(obj_uuids[0])

        results = []
        for row_key in obj_rows:
            # give chance for zk heartbeat/ping
            gevent.sleep(0)
            obj_uuid = row_key
            obj_cols = obj_rows[obj_uuid]
            result = {}
            result['uuid'] = obj_uuid
            result['fq_name'] = json.loads(obj_cols['fq_name'][0])
            for col_name in obj_cols.keys():
                if self._re_match_parent.match(col_name):
                    # non config-root child
                    (_, _, parent_uuid) = col_name.split(':')
                    parent_type = json.loads(obj_cols['parent_type'][0])
                    result['parent_type'] = parent_type
                    try:
                        result['parent_uuid'] = parent_uuid
                        result['parent_href'] = self._generate_url(parent_type, parent_uuid)
                    except cfgm_common.exceptions.NoIdError:
                        err_msg = 'Unknown uuid for parent ' + result['fq_name'][-2]
                        return (False, err_msg)

                # TODO use compiled RE
                if self._re_match_prop.match(col_name):
                    (_, prop_name) = col_name.split(':')
                    result[prop_name] = json.loads(obj_cols[col_name][0])

                # TODO use compiled RE
                if self._re_match_children.match(col_name):
                    (_, child_type, child_uuid) = col_name.split(':')
                    if field_names and '%ss' %(child_type) not in field_names:
                        continue

                    child_tstamp = obj_cols[col_name][1]
                    try:
                        self._read_child(result, obj_uuid, child_type, child_uuid, child_tstamp)
                    except cfgm_common.exceptions.NoIdError:
                        continue

                # TODO use compiled RE
                if self._re_match_ref.match(col_name):
                    (_, ref_type, ref_uuid) = col_name.split(':')
                    self._read_ref(result, obj_uuid, ref_type, ref_uuid, obj_cols[col_name][0])

                if self._re_match_backref.match(col_name):
                    (_, back_ref_type, back_ref_uuid) = col_name.split(':')
                    if field_names and '%s_back_refs' %(back_ref_type) not in field_names:
                        continue

                    try:
                        self._read_back_ref(result, obj_uuid, back_ref_type, back_ref_uuid,
                                            obj_cols[col_name][0])
                    except cfgm_common.exceptions.NoIdError:
                        continue

            # for all column names

            if 'global_vrouter_configs' in result:
                # sort children; TODO do this based on schema
                sorted_children = sorted(result['global_vrouter_configs'], key = itemgetter('tstamp'))
                # re-write result's children without timestamp
                result['global_vrouter_configs'] = sorted_children
                [child.pop('tstamp') for child in result['global_vrouter_configs']]

            if 'physical_routers' in result:
                # sort children; TODO do this based on schema
                sorted_children = sorted(result['physical_routers'], key = itemgetter('tstamp'))
                # re-write result's children without timestamp
                result['physical_routers'] = sorted_children
                [child.pop('tstamp') for child in result['physical_routers']]

            if 'virtual_routers' in result:
                # sort children; TODO do this based on schema
                sorted_children = sorted(result['virtual_routers'], key = itemgetter('tstamp'))
                # re-write result's children without timestamp
                result['virtual_routers'] = sorted_children
                [child.pop('tstamp') for child in result['virtual_routers']]

            if 'config_nodes' in result:
                # sort children; TODO do this based on schema
                sorted_children = sorted(result['config_nodes'], key = itemgetter('tstamp'))
                # re-write result's children without timestamp
                result['config_nodes'] = sorted_children
                [child.pop('tstamp') for child in result['config_nodes']]

            if 'analytics_nodes' in result:
                # sort children; TODO do this based on schema
                sorted_children = sorted(result['analytics_nodes'], key = itemgetter('tstamp'))
                # re-write result's children without timestamp
                result['analytics_nodes'] = sorted_children
                [child.pop('tstamp') for child in result['analytics_nodes']]

            if 'database_nodes' in result:
                # sort children; TODO do this based on schema
                sorted_children = sorted(result['database_nodes'], key = itemgetter('tstamp'))
                # re-write result's children without timestamp
                result['database_nodes'] = sorted_children
                [child.pop('tstamp') for child in result['database_nodes']]

            if 'service_appliance_sets' in result:
                # sort children; TODO do this based on schema
                sorted_children = sorted(result['service_appliance_sets'], key = itemgetter('tstamp'))
                # re-write result's children without timestamp
                result['service_appliance_sets'] = sorted_children
                [child.pop('tstamp') for child in result['service_appliance_sets']]

            results.append(result)
        # end for all rows

        return (True, results)
    #end _cassandra_global_system_config_read

    def _cassandra_global_system_config_count_children(self, obj_uuid, child_type):
        # if child_type = None, return
        if child_type is None:
            return (False, '')

        obj_uuid_cf = self._obj_uuid_cf
        if child_type not in GlobalSystemConfig.children_fields:
            return (False, '%s is not a valid children of GlobalSystemConfig' %(child_type))

        col_start = 'children:'+child_type[:-1]+':'
        col_finish = 'children:'+child_type[:-1]+';'
        num_children = obj_uuid_cf.get_count(obj_uuid,
                                   column_start = col_start,
                                   column_finish = col_finish,
                                   max_count = 10000000)
        return (True, num_children)
    #end _cassandra_global_system_config_count_children

    def _cassandra_global_system_config_update(self, obj_uuid, new_obj_dict):
        # Grab ref-uuids and properties in new version
        new_ref_infos = {}

        if 'bgp_router_refs' in new_obj_dict:
            new_ref_infos['bgp_router'] = {}
            new_refs = new_obj_dict['bgp_router_refs']
            if new_refs:
                for new_ref in new_refs:
                    new_ref_uuid = self.fq_name_to_uuid('bgp_router', new_ref['to'])
                    new_ref_attr = new_ref.get('attr', None)
                    new_ref_data = {'attr': new_ref_attr, 'is_weakref': False}
                    new_ref_infos['bgp_router'][new_ref_uuid] = new_ref_data

        new_props = {}
        if 'autonomous_system' in new_obj_dict:
            new_props['autonomous_system'] = new_obj_dict['autonomous_system']
        if 'config_version' in new_obj_dict:
            new_props['config_version'] = new_obj_dict['config_version']
        if 'plugin_tuning' in new_obj_dict:
            new_props['plugin_tuning'] = new_obj_dict['plugin_tuning']
        if 'ibgp_auto_mesh' in new_obj_dict:
            new_props['ibgp_auto_mesh'] = new_obj_dict['ibgp_auto_mesh']
        if 'ip_fabric_subnets' in new_obj_dict:
            new_props['ip_fabric_subnets'] = new_obj_dict['ip_fabric_subnets']
        if 'id_perms' in new_obj_dict:
            new_props['id_perms'] = new_obj_dict['id_perms']
        if 'display_name' in new_obj_dict:
            new_props['display_name'] = new_obj_dict['display_name']
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        obj_uuid_cf = self._obj_uuid_cf
        obj_cols_iter = obj_uuid_cf.xget(obj_uuid)
        # TODO optimize this (converts tuple to dict)
        obj_cols = {}
        for col_info in obj_cols_iter:
            obj_cols[col_info[0]] = col_info[1]

        bch = obj_uuid_cf.batch()
        for col_name in obj_cols.keys():
            # TODO use compiled RE
            if re.match('prop:', col_name):
                (_, prop_name) = col_name.split(':')
                if prop_name == 'id_perms':
                    # id-perms always has to be updated for last-mod timestamp
                    # get it from request dict(or from db if not in request dict)
                    new_id_perms = new_obj_dict.get(prop_name, json.loads(obj_cols[col_name]))
                    self.update_last_modified(bch, obj_uuid, new_id_perms)
                elif prop_name in new_obj_dict:
                    self._update_prop(bch, obj_uuid, prop_name, new_props)

            # TODO use compiled RE
            if re.match('ref:', col_name):
                (_, ref_type, ref_uuid) = col_name.split(':')
                self._update_ref(bch, 'global_system_config', obj_uuid, ref_type, ref_uuid, new_ref_infos)
        # for all column names

        # create new refs
        for ref_type in new_ref_infos.keys():
            for ref_uuid in new_ref_infos[ref_type].keys():
                ref_data = new_ref_infos[ref_type][ref_uuid]
                self._create_ref(bch, 'global_system_config', obj_uuid, ref_type, ref_uuid, ref_data)

        # create new props
        for prop_name in new_props.keys():
            self._create_prop(bch, obj_uuid, prop_name, new_props[prop_name])

        bch.send()

        return (True, '')
    #end _cassandra_global_system_config_update

    def _cassandra_global_system_config_list(self, parent_uuids=None, back_ref_uuids=None,
                           obj_uuids=None, count=False, filters=None):
        children_fq_names_uuids = []
        if filters:
            fnames = filters.get('field_names', [])
            fvalues = filters.get('field_values', [])
            filter_fields = [(fnames[i], fvalues[i]) for i in range(len(fnames))]
        else:
            filter_fields = []

        def filter_rows(coll_infos, filter_cols, filter_params):
            filt_infos = {}
            coll_rows = obj_uuid_cf.multiget(coll_infos.keys(),
                                   columns=filter_cols,
                                   column_count=10000000)
            for row in coll_rows:
                # give chance for zk heartbeat/ping
                gevent.sleep(0)
                full_match = True
                for fname, fval in filter_params:
                    if coll_rows[row]['prop:%s' %(fname)] != fval:
                        full_match = False
                        break
                if full_match:
                    filt_infos[row] = coll_infos[row]
            return filt_infos
        # end filter_rows

        def get_fq_name_uuid_list(obj_uuids):
            ret_list = []
            for obj_uuid in obj_uuids:
                try:
                    obj_fq_name = self.uuid_to_fq_name(obj_uuid)
                    ret_list.append((obj_fq_name, obj_uuid))
                except cfgm_common.exceptions.NoIdError:
                    pass
            return ret_list
        # end get_fq_name_uuid_list

        if parent_uuids:
            # go from parent to child
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'children:global_system_config:'
            col_fin = 'children:global_system_config;'
            try:
                obj_rows = obj_uuid_cf.multiget(parent_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_parent_anchor(sort=False):
                # flatten to [('children:<type>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_child_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    child_uuid = col_name.split(':')[2]
                    if obj_uuids and child_uuid not in obj_uuids:
                        continue
                    all_child_infos[child_uuid] = {'uuid': child_uuid, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_child_infos = filter_rows(all_child_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_child_infos = all_child_infos

                if not sort:
                    ret_child_infos = filt_child_infos.values()
                else:
                    ret_child_infos = sorted(filt_child_infos.values(), key=itemgetter('tstamp'))

                return get_fq_name_uuid_list(r['uuid'] for r in ret_child_infos)
            # end filter_rows_parent_anchor

            if count:
                return (True, len(filter_rows_parent_anchor()))

            children_fq_names_uuids = filter_rows_parent_anchor(sort=True)

        if back_ref_uuids:
            # go from anchor to backrefs
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'backref:global_system_config:'
            col_fin = 'backref:global_system_config;'
            try:
                obj_rows = obj_uuid_cf.multiget(back_ref_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_backref_anchor():
                # flatten to [('<fqnstr>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_backref_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    col_name_arr = col_name.split(':')
                    fq_name = col_name_arr[:-1]
                    obj_uuid = col_name_arr[-1]
                    if obj_uuids and obj_uuid not in obj_uuids:
                        continue
                    all_backref_infos[obj_uuid] = \
                        {'uuid': obj_uuid, 'fq_name': fq_name, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_backref_infos = filter_rows(all_backref_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_backref_infos = all_backref_infos

                return [(br_info['fq_name'], br_info['uuid']) for br_info in filt_backref_infos.values()]
            # end filter_rows_backref_anchor

            if count:
                return (True, len(filter_rows_backref_anchor()))

            children_fq_names_uuids = filter_rows_backref_anchor()

        if not parent_uuids and not back_ref_uuids:
            obj_uuid_cf = self._obj_uuid_cf
            if obj_uuids:
                # exact objects specified
                def filter_rows_object_list():
                    all_obj_infos = {}
                    for obj_uuid in obj_uuids:
                        all_obj_infos[obj_uuid] = None

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return get_fq_name_uuid_list(filt_obj_infos.keys())
                # end filter_rows_object_list

                if count:
                    return (True, len(filter_rows_object_list()))
                children_fq_names_uuids = filter_rows_object_list()

            else: # grab all resources of this type
                obj_fq_name_cf = self._obj_fq_name_cf
                try:
                    cols = obj_fq_name_cf.get('global_system_config', column_count = 10000000)
                except pycassa.NotFoundException:
                    if count:
                        return (True, 0)
                    else:
                        return (True, children_fq_names_uuids)

                def filter_rows_no_anchor():
                    all_obj_infos = {}
                    for col_name, col_val in cols.items():
                        # give chance for zk heartbeat/ping
                        gevent.sleep(0)
                        col_name_arr = utils.decode_string(col_name).split(':')
                        obj_uuid = col_name_arr[-1]
                        all_obj_infos[obj_uuid] = (col_name_arr[:-1], obj_uuid)

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return filt_obj_infos.values()
                # end filter_rows_no_anchor

                if count:
                    return (True, len(filter_rows_no_anchor()))

                children_fq_names_uuids = filter_rows_no_anchor()

        return (True, children_fq_names_uuids)
    #end _cassandra_global_system_config_list

    def _cassandra_global_system_config_delete(self, obj_uuid):
        obj_uuid_cf = self._obj_uuid_cf
        fq_name = json.loads(obj_uuid_cf.get(obj_uuid, columns = ['fq_name'])['fq_name'])
        bch = obj_uuid_cf.batch()

        # unlink from parent
        col_start = 'parent:'
        col_fin = 'parent;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, parent_type, parent_uuid) = col_name.split(':')
            self._delete_child(bch, parent_type, parent_uuid, 'global_system_config', obj_uuid)

        # remove refs
        col_start = 'ref:'
        col_fin = 'ref;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, ref_type, ref_uuid) = col_name.split(':')
            self._delete_ref(bch, 'global_system_config', obj_uuid, ref_type, ref_uuid)

        bch.remove(obj_uuid)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(fq_name)
        fq_name_col = utils.encode_string(fq_name_str) + ':' + obj_uuid
        self._obj_fq_name_cf.remove('global_system_config', columns = [fq_name_col])


        return (True, '')
    #end _cassandra_global_system_config_delete

    def _cassandra_service_appliance_alloc(self, fq_name):
        return (True, '')
    #end _cassandra_service_appliance_alloc

    def _cassandra_service_appliance_create(self, obj_ids, obj_dict):
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        bch = self._obj_uuid_cf.batch()

        obj_cols = {}
        obj_cols['fq_name'] = json.dumps(obj_dict['fq_name'])
        obj_cols['type'] = json.dumps('service_appliance')
        if 'parent_type' in obj_dict:
            # non config-root child
            parent_type = obj_dict['parent_type']
            parent_method_type = parent_type.replace('-', '_')
            parent_fq_name = obj_dict['fq_name'][:-1]
            obj_cols['parent_type'] = json.dumps(parent_type)
            parent_uuid = self.fq_name_to_uuid(parent_method_type, parent_fq_name)
            self._create_child(bch, parent_method_type, parent_uuid, 'service_appliance', obj_ids['uuid'])

        # Properties
        field = obj_dict.get('service_appliance_user_credentials', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'service_appliance_user_credentials', field)

        field = obj_dict.get('service_appliance_ip_address', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'service_appliance_ip_address', field)

        field = obj_dict.get('service_appliance_properties', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'service_appliance_properties', field)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            field['created'] = datetime.datetime.utcnow().isoformat()
            field['last_modified'] = field['created']
            self._create_prop(bch, obj_ids['uuid'], 'id_perms', field)

        field = obj_dict.get('display_name', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'display_name', field)


        # References

        bch.insert(obj_ids['uuid'], obj_cols)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(obj_dict['fq_name'])
        fq_name_cols = {utils.encode_string(fq_name_str) + ':' + obj_ids['uuid']: json.dumps(None)}
        self._obj_fq_name_cf.insert('service_appliance', fq_name_cols)

        return (True, '')
    #end _cassandra_service_appliance_create

    def _cassandra_service_appliance_read(self, obj_uuids, field_names = None):
        # if field_names = None, all fields will be read/returned

        obj_uuid_cf = self._obj_uuid_cf

        # optimize for common case of reading non-backref, non-children fields
        # ignoring columns starting from 'b' and 'c' - significant performance
        # impact in scaled setting. e.g. read of project
        if (field_names is None or
            (set(field_names) & (ServiceAppliance.backref_fields | ServiceAppliance.children_fields))):
            # atleast one backref/children field is needed
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_count = 10000000,
                                       include_timestamp = True)
        else: # ignore reading backref + children columns
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_start = 'd',
                                       column_count = 10000000,
                                       include_timestamp = True)

        if (len(obj_uuids) == 1) and not obj_rows:
            raise cfgm_common.exceptions.NoIdError(obj_uuids[0])

        results = []
        for row_key in obj_rows:
            # give chance for zk heartbeat/ping
            gevent.sleep(0)
            obj_uuid = row_key
            obj_cols = obj_rows[obj_uuid]
            result = {}
            result['uuid'] = obj_uuid
            result['fq_name'] = json.loads(obj_cols['fq_name'][0])
            for col_name in obj_cols.keys():
                if self._re_match_parent.match(col_name):
                    # non config-root child
                    (_, _, parent_uuid) = col_name.split(':')
                    parent_type = json.loads(obj_cols['parent_type'][0])
                    result['parent_type'] = parent_type
                    try:
                        result['parent_uuid'] = parent_uuid
                        result['parent_href'] = self._generate_url(parent_type, parent_uuid)
                    except cfgm_common.exceptions.NoIdError:
                        err_msg = 'Unknown uuid for parent ' + result['fq_name'][-2]
                        return (False, err_msg)

                # TODO use compiled RE
                if self._re_match_prop.match(col_name):
                    (_, prop_name) = col_name.split(':')
                    result[prop_name] = json.loads(obj_cols[col_name][0])

                # TODO use compiled RE
                if self._re_match_children.match(col_name):
                    (_, child_type, child_uuid) = col_name.split(':')
                    if field_names and '%ss' %(child_type) not in field_names:
                        continue

                    child_tstamp = obj_cols[col_name][1]
                    try:
                        self._read_child(result, obj_uuid, child_type, child_uuid, child_tstamp)
                    except cfgm_common.exceptions.NoIdError:
                        continue

                # TODO use compiled RE
                if self._re_match_ref.match(col_name):
                    (_, ref_type, ref_uuid) = col_name.split(':')
                    self._read_ref(result, obj_uuid, ref_type, ref_uuid, obj_cols[col_name][0])

                if self._re_match_backref.match(col_name):
                    (_, back_ref_type, back_ref_uuid) = col_name.split(':')
                    if field_names and '%s_back_refs' %(back_ref_type) not in field_names:
                        continue

                    try:
                        self._read_back_ref(result, obj_uuid, back_ref_type, back_ref_uuid,
                                            obj_cols[col_name][0])
                    except cfgm_common.exceptions.NoIdError:
                        continue

            # for all column names


            results.append(result)
        # end for all rows

        return (True, results)
    #end _cassandra_service_appliance_read

    def _cassandra_service_appliance_count_children(self, obj_uuid, child_type):
        # if child_type = None, return
        if child_type is None:
            return (False, '')

        obj_uuid_cf = self._obj_uuid_cf
        if child_type not in ServiceAppliance.children_fields:
            return (False, '%s is not a valid children of ServiceAppliance' %(child_type))

        col_start = 'children:'+child_type[:-1]+':'
        col_finish = 'children:'+child_type[:-1]+';'
        num_children = obj_uuid_cf.get_count(obj_uuid,
                                   column_start = col_start,
                                   column_finish = col_finish,
                                   max_count = 10000000)
        return (True, num_children)
    #end _cassandra_service_appliance_count_children

    def _cassandra_service_appliance_update(self, obj_uuid, new_obj_dict):
        # Grab ref-uuids and properties in new version
        new_ref_infos = {}

        new_props = {}
        if 'service_appliance_user_credentials' in new_obj_dict:
            new_props['service_appliance_user_credentials'] = new_obj_dict['service_appliance_user_credentials']
        if 'service_appliance_ip_address' in new_obj_dict:
            new_props['service_appliance_ip_address'] = new_obj_dict['service_appliance_ip_address']
        if 'service_appliance_properties' in new_obj_dict:
            new_props['service_appliance_properties'] = new_obj_dict['service_appliance_properties']
        if 'id_perms' in new_obj_dict:
            new_props['id_perms'] = new_obj_dict['id_perms']
        if 'display_name' in new_obj_dict:
            new_props['display_name'] = new_obj_dict['display_name']
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        obj_uuid_cf = self._obj_uuid_cf
        obj_cols_iter = obj_uuid_cf.xget(obj_uuid)
        # TODO optimize this (converts tuple to dict)
        obj_cols = {}
        for col_info in obj_cols_iter:
            obj_cols[col_info[0]] = col_info[1]

        bch = obj_uuid_cf.batch()
        for col_name in obj_cols.keys():
            # TODO use compiled RE
            if re.match('prop:', col_name):
                (_, prop_name) = col_name.split(':')
                if prop_name == 'id_perms':
                    # id-perms always has to be updated for last-mod timestamp
                    # get it from request dict(or from db if not in request dict)
                    new_id_perms = new_obj_dict.get(prop_name, json.loads(obj_cols[col_name]))
                    self.update_last_modified(bch, obj_uuid, new_id_perms)
                elif prop_name in new_obj_dict:
                    self._update_prop(bch, obj_uuid, prop_name, new_props)

            # TODO use compiled RE
            if re.match('ref:', col_name):
                (_, ref_type, ref_uuid) = col_name.split(':')
                self._update_ref(bch, 'service_appliance', obj_uuid, ref_type, ref_uuid, new_ref_infos)
        # for all column names

        # create new refs
        for ref_type in new_ref_infos.keys():
            for ref_uuid in new_ref_infos[ref_type].keys():
                ref_data = new_ref_infos[ref_type][ref_uuid]
                self._create_ref(bch, 'service_appliance', obj_uuid, ref_type, ref_uuid, ref_data)

        # create new props
        for prop_name in new_props.keys():
            self._create_prop(bch, obj_uuid, prop_name, new_props[prop_name])

        bch.send()

        return (True, '')
    #end _cassandra_service_appliance_update

    def _cassandra_service_appliance_list(self, parent_uuids=None, back_ref_uuids=None,
                           obj_uuids=None, count=False, filters=None):
        children_fq_names_uuids = []
        if filters:
            fnames = filters.get('field_names', [])
            fvalues = filters.get('field_values', [])
            filter_fields = [(fnames[i], fvalues[i]) for i in range(len(fnames))]
        else:
            filter_fields = []

        def filter_rows(coll_infos, filter_cols, filter_params):
            filt_infos = {}
            coll_rows = obj_uuid_cf.multiget(coll_infos.keys(),
                                   columns=filter_cols,
                                   column_count=10000000)
            for row in coll_rows:
                # give chance for zk heartbeat/ping
                gevent.sleep(0)
                full_match = True
                for fname, fval in filter_params:
                    if coll_rows[row]['prop:%s' %(fname)] != fval:
                        full_match = False
                        break
                if full_match:
                    filt_infos[row] = coll_infos[row]
            return filt_infos
        # end filter_rows

        def get_fq_name_uuid_list(obj_uuids):
            ret_list = []
            for obj_uuid in obj_uuids:
                try:
                    obj_fq_name = self.uuid_to_fq_name(obj_uuid)
                    ret_list.append((obj_fq_name, obj_uuid))
                except cfgm_common.exceptions.NoIdError:
                    pass
            return ret_list
        # end get_fq_name_uuid_list

        if parent_uuids:
            # go from parent to child
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'children:service_appliance:'
            col_fin = 'children:service_appliance;'
            try:
                obj_rows = obj_uuid_cf.multiget(parent_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_parent_anchor(sort=False):
                # flatten to [('children:<type>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_child_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    child_uuid = col_name.split(':')[2]
                    if obj_uuids and child_uuid not in obj_uuids:
                        continue
                    all_child_infos[child_uuid] = {'uuid': child_uuid, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_child_infos = filter_rows(all_child_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_child_infos = all_child_infos

                if not sort:
                    ret_child_infos = filt_child_infos.values()
                else:
                    ret_child_infos = sorted(filt_child_infos.values(), key=itemgetter('tstamp'))

                return get_fq_name_uuid_list(r['uuid'] for r in ret_child_infos)
            # end filter_rows_parent_anchor

            if count:
                return (True, len(filter_rows_parent_anchor()))

            children_fq_names_uuids = filter_rows_parent_anchor(sort=True)

        if back_ref_uuids:
            # go from anchor to backrefs
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'backref:service_appliance:'
            col_fin = 'backref:service_appliance;'
            try:
                obj_rows = obj_uuid_cf.multiget(back_ref_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_backref_anchor():
                # flatten to [('<fqnstr>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_backref_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    col_name_arr = col_name.split(':')
                    fq_name = col_name_arr[:-1]
                    obj_uuid = col_name_arr[-1]
                    if obj_uuids and obj_uuid not in obj_uuids:
                        continue
                    all_backref_infos[obj_uuid] = \
                        {'uuid': obj_uuid, 'fq_name': fq_name, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_backref_infos = filter_rows(all_backref_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_backref_infos = all_backref_infos

                return [(br_info['fq_name'], br_info['uuid']) for br_info in filt_backref_infos.values()]
            # end filter_rows_backref_anchor

            if count:
                return (True, len(filter_rows_backref_anchor()))

            children_fq_names_uuids = filter_rows_backref_anchor()

        if not parent_uuids and not back_ref_uuids:
            obj_uuid_cf = self._obj_uuid_cf
            if obj_uuids:
                # exact objects specified
                def filter_rows_object_list():
                    all_obj_infos = {}
                    for obj_uuid in obj_uuids:
                        all_obj_infos[obj_uuid] = None

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return get_fq_name_uuid_list(filt_obj_infos.keys())
                # end filter_rows_object_list

                if count:
                    return (True, len(filter_rows_object_list()))
                children_fq_names_uuids = filter_rows_object_list()

            else: # grab all resources of this type
                obj_fq_name_cf = self._obj_fq_name_cf
                try:
                    cols = obj_fq_name_cf.get('service_appliance', column_count = 10000000)
                except pycassa.NotFoundException:
                    if count:
                        return (True, 0)
                    else:
                        return (True, children_fq_names_uuids)

                def filter_rows_no_anchor():
                    all_obj_infos = {}
                    for col_name, col_val in cols.items():
                        # give chance for zk heartbeat/ping
                        gevent.sleep(0)
                        col_name_arr = utils.decode_string(col_name).split(':')
                        obj_uuid = col_name_arr[-1]
                        all_obj_infos[obj_uuid] = (col_name_arr[:-1], obj_uuid)

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return filt_obj_infos.values()
                # end filter_rows_no_anchor

                if count:
                    return (True, len(filter_rows_no_anchor()))

                children_fq_names_uuids = filter_rows_no_anchor()

        return (True, children_fq_names_uuids)
    #end _cassandra_service_appliance_list

    def _cassandra_service_appliance_delete(self, obj_uuid):
        obj_uuid_cf = self._obj_uuid_cf
        fq_name = json.loads(obj_uuid_cf.get(obj_uuid, columns = ['fq_name'])['fq_name'])
        bch = obj_uuid_cf.batch()

        # unlink from parent
        col_start = 'parent:'
        col_fin = 'parent;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, parent_type, parent_uuid) = col_name.split(':')
            self._delete_child(bch, parent_type, parent_uuid, 'service_appliance', obj_uuid)

        # remove refs
        col_start = 'ref:'
        col_fin = 'ref;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, ref_type, ref_uuid) = col_name.split(':')
            self._delete_ref(bch, 'service_appliance', obj_uuid, ref_type, ref_uuid)

        bch.remove(obj_uuid)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(fq_name)
        fq_name_col = utils.encode_string(fq_name_str) + ':' + obj_uuid
        self._obj_fq_name_cf.remove('service_appliance', columns = [fq_name_col])


        return (True, '')
    #end _cassandra_service_appliance_delete

    def _cassandra_service_instance_alloc(self, fq_name):
        return (True, '')
    #end _cassandra_service_instance_alloc

    def _cassandra_service_instance_create(self, obj_ids, obj_dict):
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        bch = self._obj_uuid_cf.batch()

        obj_cols = {}
        obj_cols['fq_name'] = json.dumps(obj_dict['fq_name'])
        obj_cols['type'] = json.dumps('service_instance')
        if 'parent_type' in obj_dict:
            # non config-root child
            parent_type = obj_dict['parent_type']
            parent_method_type = parent_type.replace('-', '_')
            parent_fq_name = obj_dict['fq_name'][:-1]
            obj_cols['parent_type'] = json.dumps(parent_type)
            parent_uuid = self.fq_name_to_uuid(parent_method_type, parent_fq_name)
            self._create_child(bch, parent_method_type, parent_uuid, 'service_instance', obj_ids['uuid'])

        # Properties
        field = obj_dict.get('service_instance_properties', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'service_instance_properties', field)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            field['created'] = datetime.datetime.utcnow().isoformat()
            field['last_modified'] = field['created']
            self._create_prop(bch, obj_ids['uuid'], 'id_perms', field)

        field = obj_dict.get('display_name', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'display_name', field)


        # References
        refs = obj_dict.get('service_template_refs', [])
        for ref in refs:
            ref_uuid = self.fq_name_to_uuid('service_template', ref['to'])
            ref_attr = ref.get('attr', None)
            ref_data = {'attr': ref_attr, 'is_weakref': False}
            self._create_ref(bch, 'service_instance', obj_ids['uuid'], 'service_template', ref_uuid, ref_data)

        bch.insert(obj_ids['uuid'], obj_cols)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(obj_dict['fq_name'])
        fq_name_cols = {utils.encode_string(fq_name_str) + ':' + obj_ids['uuid']: json.dumps(None)}
        self._obj_fq_name_cf.insert('service_instance', fq_name_cols)

        return (True, '')
    #end _cassandra_service_instance_create

    def _cassandra_service_instance_read(self, obj_uuids, field_names = None):
        # if field_names = None, all fields will be read/returned

        obj_uuid_cf = self._obj_uuid_cf

        # optimize for common case of reading non-backref, non-children fields
        # ignoring columns starting from 'b' and 'c' - significant performance
        # impact in scaled setting. e.g. read of project
        if (field_names is None or
            (set(field_names) & (ServiceInstance.backref_fields | ServiceInstance.children_fields))):
            # atleast one backref/children field is needed
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_count = 10000000,
                                       include_timestamp = True)
        else: # ignore reading backref + children columns
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_start = 'd',
                                       column_count = 10000000,
                                       include_timestamp = True)

        if (len(obj_uuids) == 1) and not obj_rows:
            raise cfgm_common.exceptions.NoIdError(obj_uuids[0])

        results = []
        for row_key in obj_rows:
            # give chance for zk heartbeat/ping
            gevent.sleep(0)
            obj_uuid = row_key
            obj_cols = obj_rows[obj_uuid]
            result = {}
            result['uuid'] = obj_uuid
            result['fq_name'] = json.loads(obj_cols['fq_name'][0])
            for col_name in obj_cols.keys():
                if self._re_match_parent.match(col_name):
                    # non config-root child
                    (_, _, parent_uuid) = col_name.split(':')
                    parent_type = json.loads(obj_cols['parent_type'][0])
                    result['parent_type'] = parent_type
                    try:
                        result['parent_uuid'] = parent_uuid
                        result['parent_href'] = self._generate_url(parent_type, parent_uuid)
                    except cfgm_common.exceptions.NoIdError:
                        err_msg = 'Unknown uuid for parent ' + result['fq_name'][-2]
                        return (False, err_msg)

                # TODO use compiled RE
                if self._re_match_prop.match(col_name):
                    (_, prop_name) = col_name.split(':')
                    result[prop_name] = json.loads(obj_cols[col_name][0])

                # TODO use compiled RE
                if self._re_match_children.match(col_name):
                    (_, child_type, child_uuid) = col_name.split(':')
                    if field_names and '%ss' %(child_type) not in field_names:
                        continue

                    child_tstamp = obj_cols[col_name][1]
                    try:
                        self._read_child(result, obj_uuid, child_type, child_uuid, child_tstamp)
                    except cfgm_common.exceptions.NoIdError:
                        continue

                # TODO use compiled RE
                if self._re_match_ref.match(col_name):
                    (_, ref_type, ref_uuid) = col_name.split(':')
                    self._read_ref(result, obj_uuid, ref_type, ref_uuid, obj_cols[col_name][0])

                if self._re_match_backref.match(col_name):
                    (_, back_ref_type, back_ref_uuid) = col_name.split(':')
                    if field_names and '%s_back_refs' %(back_ref_type) not in field_names:
                        continue

                    try:
                        self._read_back_ref(result, obj_uuid, back_ref_type, back_ref_uuid,
                                            obj_cols[col_name][0])
                    except cfgm_common.exceptions.NoIdError:
                        continue

            # for all column names


            results.append(result)
        # end for all rows

        return (True, results)
    #end _cassandra_service_instance_read

    def _cassandra_service_instance_count_children(self, obj_uuid, child_type):
        # if child_type = None, return
        if child_type is None:
            return (False, '')

        obj_uuid_cf = self._obj_uuid_cf
        if child_type not in ServiceInstance.children_fields:
            return (False, '%s is not a valid children of ServiceInstance' %(child_type))

        col_start = 'children:'+child_type[:-1]+':'
        col_finish = 'children:'+child_type[:-1]+';'
        num_children = obj_uuid_cf.get_count(obj_uuid,
                                   column_start = col_start,
                                   column_finish = col_finish,
                                   max_count = 10000000)
        return (True, num_children)
    #end _cassandra_service_instance_count_children

    def _cassandra_service_instance_update(self, obj_uuid, new_obj_dict):
        # Grab ref-uuids and properties in new version
        new_ref_infos = {}

        if 'service_template_refs' in new_obj_dict:
            new_ref_infos['service_template'] = {}
            new_refs = new_obj_dict['service_template_refs']
            if new_refs:
                for new_ref in new_refs:
                    new_ref_uuid = self.fq_name_to_uuid('service_template', new_ref['to'])
                    new_ref_attr = new_ref.get('attr', None)
                    new_ref_data = {'attr': new_ref_attr, 'is_weakref': False}
                    new_ref_infos['service_template'][new_ref_uuid] = new_ref_data

        new_props = {}
        if 'service_instance_properties' in new_obj_dict:
            new_props['service_instance_properties'] = new_obj_dict['service_instance_properties']
        if 'id_perms' in new_obj_dict:
            new_props['id_perms'] = new_obj_dict['id_perms']
        if 'display_name' in new_obj_dict:
            new_props['display_name'] = new_obj_dict['display_name']
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        obj_uuid_cf = self._obj_uuid_cf
        obj_cols_iter = obj_uuid_cf.xget(obj_uuid)
        # TODO optimize this (converts tuple to dict)
        obj_cols = {}
        for col_info in obj_cols_iter:
            obj_cols[col_info[0]] = col_info[1]

        bch = obj_uuid_cf.batch()
        for col_name in obj_cols.keys():
            # TODO use compiled RE
            if re.match('prop:', col_name):
                (_, prop_name) = col_name.split(':')
                if prop_name == 'id_perms':
                    # id-perms always has to be updated for last-mod timestamp
                    # get it from request dict(or from db if not in request dict)
                    new_id_perms = new_obj_dict.get(prop_name, json.loads(obj_cols[col_name]))
                    self.update_last_modified(bch, obj_uuid, new_id_perms)
                elif prop_name in new_obj_dict:
                    self._update_prop(bch, obj_uuid, prop_name, new_props)

            # TODO use compiled RE
            if re.match('ref:', col_name):
                (_, ref_type, ref_uuid) = col_name.split(':')
                self._update_ref(bch, 'service_instance', obj_uuid, ref_type, ref_uuid, new_ref_infos)
        # for all column names

        # create new refs
        for ref_type in new_ref_infos.keys():
            for ref_uuid in new_ref_infos[ref_type].keys():
                ref_data = new_ref_infos[ref_type][ref_uuid]
                self._create_ref(bch, 'service_instance', obj_uuid, ref_type, ref_uuid, ref_data)

        # create new props
        for prop_name in new_props.keys():
            self._create_prop(bch, obj_uuid, prop_name, new_props[prop_name])

        bch.send()

        return (True, '')
    #end _cassandra_service_instance_update

    def _cassandra_service_instance_list(self, parent_uuids=None, back_ref_uuids=None,
                           obj_uuids=None, count=False, filters=None):
        children_fq_names_uuids = []
        if filters:
            fnames = filters.get('field_names', [])
            fvalues = filters.get('field_values', [])
            filter_fields = [(fnames[i], fvalues[i]) for i in range(len(fnames))]
        else:
            filter_fields = []

        def filter_rows(coll_infos, filter_cols, filter_params):
            filt_infos = {}
            coll_rows = obj_uuid_cf.multiget(coll_infos.keys(),
                                   columns=filter_cols,
                                   column_count=10000000)
            for row in coll_rows:
                # give chance for zk heartbeat/ping
                gevent.sleep(0)
                full_match = True
                for fname, fval in filter_params:
                    if coll_rows[row]['prop:%s' %(fname)] != fval:
                        full_match = False
                        break
                if full_match:
                    filt_infos[row] = coll_infos[row]
            return filt_infos
        # end filter_rows

        def get_fq_name_uuid_list(obj_uuids):
            ret_list = []
            for obj_uuid in obj_uuids:
                try:
                    obj_fq_name = self.uuid_to_fq_name(obj_uuid)
                    ret_list.append((obj_fq_name, obj_uuid))
                except cfgm_common.exceptions.NoIdError:
                    pass
            return ret_list
        # end get_fq_name_uuid_list

        if parent_uuids:
            # go from parent to child
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'children:service_instance:'
            col_fin = 'children:service_instance;'
            try:
                obj_rows = obj_uuid_cf.multiget(parent_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_parent_anchor(sort=False):
                # flatten to [('children:<type>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_child_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    child_uuid = col_name.split(':')[2]
                    if obj_uuids and child_uuid not in obj_uuids:
                        continue
                    all_child_infos[child_uuid] = {'uuid': child_uuid, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_child_infos = filter_rows(all_child_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_child_infos = all_child_infos

                if not sort:
                    ret_child_infos = filt_child_infos.values()
                else:
                    ret_child_infos = sorted(filt_child_infos.values(), key=itemgetter('tstamp'))

                return get_fq_name_uuid_list(r['uuid'] for r in ret_child_infos)
            # end filter_rows_parent_anchor

            if count:
                return (True, len(filter_rows_parent_anchor()))

            children_fq_names_uuids = filter_rows_parent_anchor(sort=True)

        if back_ref_uuids:
            # go from anchor to backrefs
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'backref:service_instance:'
            col_fin = 'backref:service_instance;'
            try:
                obj_rows = obj_uuid_cf.multiget(back_ref_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_backref_anchor():
                # flatten to [('<fqnstr>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_backref_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    col_name_arr = col_name.split(':')
                    fq_name = col_name_arr[:-1]
                    obj_uuid = col_name_arr[-1]
                    if obj_uuids and obj_uuid not in obj_uuids:
                        continue
                    all_backref_infos[obj_uuid] = \
                        {'uuid': obj_uuid, 'fq_name': fq_name, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_backref_infos = filter_rows(all_backref_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_backref_infos = all_backref_infos

                return [(br_info['fq_name'], br_info['uuid']) for br_info in filt_backref_infos.values()]
            # end filter_rows_backref_anchor

            if count:
                return (True, len(filter_rows_backref_anchor()))

            children_fq_names_uuids = filter_rows_backref_anchor()

        if not parent_uuids and not back_ref_uuids:
            obj_uuid_cf = self._obj_uuid_cf
            if obj_uuids:
                # exact objects specified
                def filter_rows_object_list():
                    all_obj_infos = {}
                    for obj_uuid in obj_uuids:
                        all_obj_infos[obj_uuid] = None

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return get_fq_name_uuid_list(filt_obj_infos.keys())
                # end filter_rows_object_list

                if count:
                    return (True, len(filter_rows_object_list()))
                children_fq_names_uuids = filter_rows_object_list()

            else: # grab all resources of this type
                obj_fq_name_cf = self._obj_fq_name_cf
                try:
                    cols = obj_fq_name_cf.get('service_instance', column_count = 10000000)
                except pycassa.NotFoundException:
                    if count:
                        return (True, 0)
                    else:
                        return (True, children_fq_names_uuids)

                def filter_rows_no_anchor():
                    all_obj_infos = {}
                    for col_name, col_val in cols.items():
                        # give chance for zk heartbeat/ping
                        gevent.sleep(0)
                        col_name_arr = utils.decode_string(col_name).split(':')
                        obj_uuid = col_name_arr[-1]
                        all_obj_infos[obj_uuid] = (col_name_arr[:-1], obj_uuid)

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return filt_obj_infos.values()
                # end filter_rows_no_anchor

                if count:
                    return (True, len(filter_rows_no_anchor()))

                children_fq_names_uuids = filter_rows_no_anchor()

        return (True, children_fq_names_uuids)
    #end _cassandra_service_instance_list

    def _cassandra_service_instance_delete(self, obj_uuid):
        obj_uuid_cf = self._obj_uuid_cf
        fq_name = json.loads(obj_uuid_cf.get(obj_uuid, columns = ['fq_name'])['fq_name'])
        bch = obj_uuid_cf.batch()

        # unlink from parent
        col_start = 'parent:'
        col_fin = 'parent;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, parent_type, parent_uuid) = col_name.split(':')
            self._delete_child(bch, parent_type, parent_uuid, 'service_instance', obj_uuid)

        # remove refs
        col_start = 'ref:'
        col_fin = 'ref;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, ref_type, ref_uuid) = col_name.split(':')
            self._delete_ref(bch, 'service_instance', obj_uuid, ref_type, ref_uuid)

        bch.remove(obj_uuid)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(fq_name)
        fq_name_col = utils.encode_string(fq_name_str) + ':' + obj_uuid
        self._obj_fq_name_cf.remove('service_instance', columns = [fq_name_col])


        return (True, '')
    #end _cassandra_service_instance_delete

    def _cassandra_namespace_alloc(self, fq_name):
        return (True, '')
    #end _cassandra_namespace_alloc

    def _cassandra_namespace_create(self, obj_ids, obj_dict):
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        bch = self._obj_uuid_cf.batch()

        obj_cols = {}
        obj_cols['fq_name'] = json.dumps(obj_dict['fq_name'])
        obj_cols['type'] = json.dumps('namespace')
        if 'parent_type' in obj_dict:
            # non config-root child
            parent_type = obj_dict['parent_type']
            parent_method_type = parent_type.replace('-', '_')
            parent_fq_name = obj_dict['fq_name'][:-1]
            obj_cols['parent_type'] = json.dumps(parent_type)
            parent_uuid = self.fq_name_to_uuid(parent_method_type, parent_fq_name)
            self._create_child(bch, parent_method_type, parent_uuid, 'namespace', obj_ids['uuid'])

        # Properties
        field = obj_dict.get('namespace_cidr', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'namespace_cidr', field)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            field['created'] = datetime.datetime.utcnow().isoformat()
            field['last_modified'] = field['created']
            self._create_prop(bch, obj_ids['uuid'], 'id_perms', field)

        field = obj_dict.get('display_name', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'display_name', field)


        # References

        bch.insert(obj_ids['uuid'], obj_cols)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(obj_dict['fq_name'])
        fq_name_cols = {utils.encode_string(fq_name_str) + ':' + obj_ids['uuid']: json.dumps(None)}
        self._obj_fq_name_cf.insert('namespace', fq_name_cols)

        return (True, '')
    #end _cassandra_namespace_create

    def _cassandra_namespace_read(self, obj_uuids, field_names = None):
        # if field_names = None, all fields will be read/returned

        obj_uuid_cf = self._obj_uuid_cf

        # optimize for common case of reading non-backref, non-children fields
        # ignoring columns starting from 'b' and 'c' - significant performance
        # impact in scaled setting. e.g. read of project
        if (field_names is None or
            (set(field_names) & (Namespace.backref_fields | Namespace.children_fields))):
            # atleast one backref/children field is needed
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_count = 10000000,
                                       include_timestamp = True)
        else: # ignore reading backref + children columns
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_start = 'd',
                                       column_count = 10000000,
                                       include_timestamp = True)

        if (len(obj_uuids) == 1) and not obj_rows:
            raise cfgm_common.exceptions.NoIdError(obj_uuids[0])

        results = []
        for row_key in obj_rows:
            # give chance for zk heartbeat/ping
            gevent.sleep(0)
            obj_uuid = row_key
            obj_cols = obj_rows[obj_uuid]
            result = {}
            result['uuid'] = obj_uuid
            result['fq_name'] = json.loads(obj_cols['fq_name'][0])
            for col_name in obj_cols.keys():
                if self._re_match_parent.match(col_name):
                    # non config-root child
                    (_, _, parent_uuid) = col_name.split(':')
                    parent_type = json.loads(obj_cols['parent_type'][0])
                    result['parent_type'] = parent_type
                    try:
                        result['parent_uuid'] = parent_uuid
                        result['parent_href'] = self._generate_url(parent_type, parent_uuid)
                    except cfgm_common.exceptions.NoIdError:
                        err_msg = 'Unknown uuid for parent ' + result['fq_name'][-2]
                        return (False, err_msg)

                # TODO use compiled RE
                if self._re_match_prop.match(col_name):
                    (_, prop_name) = col_name.split(':')
                    result[prop_name] = json.loads(obj_cols[col_name][0])

                # TODO use compiled RE
                if self._re_match_children.match(col_name):
                    (_, child_type, child_uuid) = col_name.split(':')
                    if field_names and '%ss' %(child_type) not in field_names:
                        continue

                    child_tstamp = obj_cols[col_name][1]
                    try:
                        self._read_child(result, obj_uuid, child_type, child_uuid, child_tstamp)
                    except cfgm_common.exceptions.NoIdError:
                        continue

                # TODO use compiled RE
                if self._re_match_ref.match(col_name):
                    (_, ref_type, ref_uuid) = col_name.split(':')
                    self._read_ref(result, obj_uuid, ref_type, ref_uuid, obj_cols[col_name][0])

                if self._re_match_backref.match(col_name):
                    (_, back_ref_type, back_ref_uuid) = col_name.split(':')
                    if field_names and '%s_back_refs' %(back_ref_type) not in field_names:
                        continue

                    try:
                        self._read_back_ref(result, obj_uuid, back_ref_type, back_ref_uuid,
                                            obj_cols[col_name][0])
                    except cfgm_common.exceptions.NoIdError:
                        continue

            # for all column names


            results.append(result)
        # end for all rows

        return (True, results)
    #end _cassandra_namespace_read

    def _cassandra_namespace_count_children(self, obj_uuid, child_type):
        # if child_type = None, return
        if child_type is None:
            return (False, '')

        obj_uuid_cf = self._obj_uuid_cf
        if child_type not in Namespace.children_fields:
            return (False, '%s is not a valid children of Namespace' %(child_type))

        col_start = 'children:'+child_type[:-1]+':'
        col_finish = 'children:'+child_type[:-1]+';'
        num_children = obj_uuid_cf.get_count(obj_uuid,
                                   column_start = col_start,
                                   column_finish = col_finish,
                                   max_count = 10000000)
        return (True, num_children)
    #end _cassandra_namespace_count_children

    def _cassandra_namespace_update(self, obj_uuid, new_obj_dict):
        # Grab ref-uuids and properties in new version
        new_ref_infos = {}

        new_props = {}
        if 'namespace_cidr' in new_obj_dict:
            new_props['namespace_cidr'] = new_obj_dict['namespace_cidr']
        if 'id_perms' in new_obj_dict:
            new_props['id_perms'] = new_obj_dict['id_perms']
        if 'display_name' in new_obj_dict:
            new_props['display_name'] = new_obj_dict['display_name']
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        obj_uuid_cf = self._obj_uuid_cf
        obj_cols_iter = obj_uuid_cf.xget(obj_uuid)
        # TODO optimize this (converts tuple to dict)
        obj_cols = {}
        for col_info in obj_cols_iter:
            obj_cols[col_info[0]] = col_info[1]

        bch = obj_uuid_cf.batch()
        for col_name in obj_cols.keys():
            # TODO use compiled RE
            if re.match('prop:', col_name):
                (_, prop_name) = col_name.split(':')
                if prop_name == 'id_perms':
                    # id-perms always has to be updated for last-mod timestamp
                    # get it from request dict(or from db if not in request dict)
                    new_id_perms = new_obj_dict.get(prop_name, json.loads(obj_cols[col_name]))
                    self.update_last_modified(bch, obj_uuid, new_id_perms)
                elif prop_name in new_obj_dict:
                    self._update_prop(bch, obj_uuid, prop_name, new_props)

            # TODO use compiled RE
            if re.match('ref:', col_name):
                (_, ref_type, ref_uuid) = col_name.split(':')
                self._update_ref(bch, 'namespace', obj_uuid, ref_type, ref_uuid, new_ref_infos)
        # for all column names

        # create new refs
        for ref_type in new_ref_infos.keys():
            for ref_uuid in new_ref_infos[ref_type].keys():
                ref_data = new_ref_infos[ref_type][ref_uuid]
                self._create_ref(bch, 'namespace', obj_uuid, ref_type, ref_uuid, ref_data)

        # create new props
        for prop_name in new_props.keys():
            self._create_prop(bch, obj_uuid, prop_name, new_props[prop_name])

        bch.send()

        return (True, '')
    #end _cassandra_namespace_update

    def _cassandra_namespace_list(self, parent_uuids=None, back_ref_uuids=None,
                           obj_uuids=None, count=False, filters=None):
        children_fq_names_uuids = []
        if filters:
            fnames = filters.get('field_names', [])
            fvalues = filters.get('field_values', [])
            filter_fields = [(fnames[i], fvalues[i]) for i in range(len(fnames))]
        else:
            filter_fields = []

        def filter_rows(coll_infos, filter_cols, filter_params):
            filt_infos = {}
            coll_rows = obj_uuid_cf.multiget(coll_infos.keys(),
                                   columns=filter_cols,
                                   column_count=10000000)
            for row in coll_rows:
                # give chance for zk heartbeat/ping
                gevent.sleep(0)
                full_match = True
                for fname, fval in filter_params:
                    if coll_rows[row]['prop:%s' %(fname)] != fval:
                        full_match = False
                        break
                if full_match:
                    filt_infos[row] = coll_infos[row]
            return filt_infos
        # end filter_rows

        def get_fq_name_uuid_list(obj_uuids):
            ret_list = []
            for obj_uuid in obj_uuids:
                try:
                    obj_fq_name = self.uuid_to_fq_name(obj_uuid)
                    ret_list.append((obj_fq_name, obj_uuid))
                except cfgm_common.exceptions.NoIdError:
                    pass
            return ret_list
        # end get_fq_name_uuid_list

        if parent_uuids:
            # go from parent to child
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'children:namespace:'
            col_fin = 'children:namespace;'
            try:
                obj_rows = obj_uuid_cf.multiget(parent_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_parent_anchor(sort=False):
                # flatten to [('children:<type>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_child_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    child_uuid = col_name.split(':')[2]
                    if obj_uuids and child_uuid not in obj_uuids:
                        continue
                    all_child_infos[child_uuid] = {'uuid': child_uuid, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_child_infos = filter_rows(all_child_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_child_infos = all_child_infos

                if not sort:
                    ret_child_infos = filt_child_infos.values()
                else:
                    ret_child_infos = sorted(filt_child_infos.values(), key=itemgetter('tstamp'))

                return get_fq_name_uuid_list(r['uuid'] for r in ret_child_infos)
            # end filter_rows_parent_anchor

            if count:
                return (True, len(filter_rows_parent_anchor()))

            children_fq_names_uuids = filter_rows_parent_anchor(sort=True)

        if back_ref_uuids:
            # go from anchor to backrefs
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'backref:namespace:'
            col_fin = 'backref:namespace;'
            try:
                obj_rows = obj_uuid_cf.multiget(back_ref_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_backref_anchor():
                # flatten to [('<fqnstr>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_backref_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    col_name_arr = col_name.split(':')
                    fq_name = col_name_arr[:-1]
                    obj_uuid = col_name_arr[-1]
                    if obj_uuids and obj_uuid not in obj_uuids:
                        continue
                    all_backref_infos[obj_uuid] = \
                        {'uuid': obj_uuid, 'fq_name': fq_name, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_backref_infos = filter_rows(all_backref_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_backref_infos = all_backref_infos

                return [(br_info['fq_name'], br_info['uuid']) for br_info in filt_backref_infos.values()]
            # end filter_rows_backref_anchor

            if count:
                return (True, len(filter_rows_backref_anchor()))

            children_fq_names_uuids = filter_rows_backref_anchor()

        if not parent_uuids and not back_ref_uuids:
            obj_uuid_cf = self._obj_uuid_cf
            if obj_uuids:
                # exact objects specified
                def filter_rows_object_list():
                    all_obj_infos = {}
                    for obj_uuid in obj_uuids:
                        all_obj_infos[obj_uuid] = None

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return get_fq_name_uuid_list(filt_obj_infos.keys())
                # end filter_rows_object_list

                if count:
                    return (True, len(filter_rows_object_list()))
                children_fq_names_uuids = filter_rows_object_list()

            else: # grab all resources of this type
                obj_fq_name_cf = self._obj_fq_name_cf
                try:
                    cols = obj_fq_name_cf.get('namespace', column_count = 10000000)
                except pycassa.NotFoundException:
                    if count:
                        return (True, 0)
                    else:
                        return (True, children_fq_names_uuids)

                def filter_rows_no_anchor():
                    all_obj_infos = {}
                    for col_name, col_val in cols.items():
                        # give chance for zk heartbeat/ping
                        gevent.sleep(0)
                        col_name_arr = utils.decode_string(col_name).split(':')
                        obj_uuid = col_name_arr[-1]
                        all_obj_infos[obj_uuid] = (col_name_arr[:-1], obj_uuid)

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return filt_obj_infos.values()
                # end filter_rows_no_anchor

                if count:
                    return (True, len(filter_rows_no_anchor()))

                children_fq_names_uuids = filter_rows_no_anchor()

        return (True, children_fq_names_uuids)
    #end _cassandra_namespace_list

    def _cassandra_namespace_delete(self, obj_uuid):
        obj_uuid_cf = self._obj_uuid_cf
        fq_name = json.loads(obj_uuid_cf.get(obj_uuid, columns = ['fq_name'])['fq_name'])
        bch = obj_uuid_cf.batch()

        # unlink from parent
        col_start = 'parent:'
        col_fin = 'parent;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, parent_type, parent_uuid) = col_name.split(':')
            self._delete_child(bch, parent_type, parent_uuid, 'namespace', obj_uuid)

        # remove refs
        col_start = 'ref:'
        col_fin = 'ref;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, ref_type, ref_uuid) = col_name.split(':')
            self._delete_ref(bch, 'namespace', obj_uuid, ref_type, ref_uuid)

        bch.remove(obj_uuid)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(fq_name)
        fq_name_col = utils.encode_string(fq_name_str) + ':' + obj_uuid
        self._obj_fq_name_cf.remove('namespace', columns = [fq_name_col])


        return (True, '')
    #end _cassandra_namespace_delete

    def _cassandra_logical_interface_alloc(self, fq_name):
        return (True, '')
    #end _cassandra_logical_interface_alloc

    def _cassandra_logical_interface_create(self, obj_ids, obj_dict):
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        bch = self._obj_uuid_cf.batch()

        obj_cols = {}
        obj_cols['fq_name'] = json.dumps(obj_dict['fq_name'])
        obj_cols['type'] = json.dumps('logical_interface')
        if 'parent_type' in obj_dict:
            # non config-root child
            parent_type = obj_dict['parent_type']
            parent_method_type = parent_type.replace('-', '_')
            parent_fq_name = obj_dict['fq_name'][:-1]
            obj_cols['parent_type'] = json.dumps(parent_type)
            parent_uuid = self.fq_name_to_uuid(parent_method_type, parent_fq_name)
            self._create_child(bch, parent_method_type, parent_uuid, 'logical_interface', obj_ids['uuid'])

        # Properties
        field = obj_dict.get('logical_interface_vlan_tag', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'logical_interface_vlan_tag', field)

        field = obj_dict.get('logical_interface_type', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'logical_interface_type', field)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            field['created'] = datetime.datetime.utcnow().isoformat()
            field['last_modified'] = field['created']
            self._create_prop(bch, obj_ids['uuid'], 'id_perms', field)

        field = obj_dict.get('display_name', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'display_name', field)


        # References
        refs = obj_dict.get('virtual_machine_interface_refs', [])
        for ref in refs:
            ref_uuid = self.fq_name_to_uuid('virtual_machine_interface', ref['to'])
            ref_attr = ref.get('attr', None)
            ref_data = {'attr': ref_attr, 'is_weakref': False}
            self._create_ref(bch, 'logical_interface', obj_ids['uuid'], 'virtual_machine_interface', ref_uuid, ref_data)

        bch.insert(obj_ids['uuid'], obj_cols)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(obj_dict['fq_name'])
        fq_name_cols = {utils.encode_string(fq_name_str) + ':' + obj_ids['uuid']: json.dumps(None)}
        self._obj_fq_name_cf.insert('logical_interface', fq_name_cols)

        return (True, '')
    #end _cassandra_logical_interface_create

    def _cassandra_logical_interface_read(self, obj_uuids, field_names = None):
        # if field_names = None, all fields will be read/returned

        obj_uuid_cf = self._obj_uuid_cf

        # optimize for common case of reading non-backref, non-children fields
        # ignoring columns starting from 'b' and 'c' - significant performance
        # impact in scaled setting. e.g. read of project
        if (field_names is None or
            (set(field_names) & (LogicalInterface.backref_fields | LogicalInterface.children_fields))):
            # atleast one backref/children field is needed
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_count = 10000000,
                                       include_timestamp = True)
        else: # ignore reading backref + children columns
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_start = 'd',
                                       column_count = 10000000,
                                       include_timestamp = True)

        if (len(obj_uuids) == 1) and not obj_rows:
            raise cfgm_common.exceptions.NoIdError(obj_uuids[0])

        results = []
        for row_key in obj_rows:
            # give chance for zk heartbeat/ping
            gevent.sleep(0)
            obj_uuid = row_key
            obj_cols = obj_rows[obj_uuid]
            result = {}
            result['uuid'] = obj_uuid
            result['fq_name'] = json.loads(obj_cols['fq_name'][0])
            for col_name in obj_cols.keys():
                if self._re_match_parent.match(col_name):
                    # non config-root child
                    (_, _, parent_uuid) = col_name.split(':')
                    parent_type = json.loads(obj_cols['parent_type'][0])
                    result['parent_type'] = parent_type
                    try:
                        result['parent_uuid'] = parent_uuid
                        result['parent_href'] = self._generate_url(parent_type, parent_uuid)
                    except cfgm_common.exceptions.NoIdError:
                        err_msg = 'Unknown uuid for parent ' + result['fq_name'][-2]
                        return (False, err_msg)

                # TODO use compiled RE
                if self._re_match_prop.match(col_name):
                    (_, prop_name) = col_name.split(':')
                    result[prop_name] = json.loads(obj_cols[col_name][0])

                # TODO use compiled RE
                if self._re_match_children.match(col_name):
                    (_, child_type, child_uuid) = col_name.split(':')
                    if field_names and '%ss' %(child_type) not in field_names:
                        continue

                    child_tstamp = obj_cols[col_name][1]
                    try:
                        self._read_child(result, obj_uuid, child_type, child_uuid, child_tstamp)
                    except cfgm_common.exceptions.NoIdError:
                        continue

                # TODO use compiled RE
                if self._re_match_ref.match(col_name):
                    (_, ref_type, ref_uuid) = col_name.split(':')
                    self._read_ref(result, obj_uuid, ref_type, ref_uuid, obj_cols[col_name][0])

                if self._re_match_backref.match(col_name):
                    (_, back_ref_type, back_ref_uuid) = col_name.split(':')
                    if field_names and '%s_back_refs' %(back_ref_type) not in field_names:
                        continue

                    try:
                        self._read_back_ref(result, obj_uuid, back_ref_type, back_ref_uuid,
                                            obj_cols[col_name][0])
                    except cfgm_common.exceptions.NoIdError:
                        continue

            # for all column names


            results.append(result)
        # end for all rows

        return (True, results)
    #end _cassandra_logical_interface_read

    def _cassandra_logical_interface_count_children(self, obj_uuid, child_type):
        # if child_type = None, return
        if child_type is None:
            return (False, '')

        obj_uuid_cf = self._obj_uuid_cf
        if child_type not in LogicalInterface.children_fields:
            return (False, '%s is not a valid children of LogicalInterface' %(child_type))

        col_start = 'children:'+child_type[:-1]+':'
        col_finish = 'children:'+child_type[:-1]+';'
        num_children = obj_uuid_cf.get_count(obj_uuid,
                                   column_start = col_start,
                                   column_finish = col_finish,
                                   max_count = 10000000)
        return (True, num_children)
    #end _cassandra_logical_interface_count_children

    def _cassandra_logical_interface_update(self, obj_uuid, new_obj_dict):
        # Grab ref-uuids and properties in new version
        new_ref_infos = {}

        if 'virtual_machine_interface_refs' in new_obj_dict:
            new_ref_infos['virtual_machine_interface'] = {}
            new_refs = new_obj_dict['virtual_machine_interface_refs']
            if new_refs:
                for new_ref in new_refs:
                    new_ref_uuid = self.fq_name_to_uuid('virtual_machine_interface', new_ref['to'])
                    new_ref_attr = new_ref.get('attr', None)
                    new_ref_data = {'attr': new_ref_attr, 'is_weakref': False}
                    new_ref_infos['virtual_machine_interface'][new_ref_uuid] = new_ref_data

        new_props = {}
        if 'logical_interface_vlan_tag' in new_obj_dict:
            new_props['logical_interface_vlan_tag'] = new_obj_dict['logical_interface_vlan_tag']
        if 'logical_interface_type' in new_obj_dict:
            new_props['logical_interface_type'] = new_obj_dict['logical_interface_type']
        if 'id_perms' in new_obj_dict:
            new_props['id_perms'] = new_obj_dict['id_perms']
        if 'display_name' in new_obj_dict:
            new_props['display_name'] = new_obj_dict['display_name']
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        obj_uuid_cf = self._obj_uuid_cf
        obj_cols_iter = obj_uuid_cf.xget(obj_uuid)
        # TODO optimize this (converts tuple to dict)
        obj_cols = {}
        for col_info in obj_cols_iter:
            obj_cols[col_info[0]] = col_info[1]

        bch = obj_uuid_cf.batch()
        for col_name in obj_cols.keys():
            # TODO use compiled RE
            if re.match('prop:', col_name):
                (_, prop_name) = col_name.split(':')
                if prop_name == 'id_perms':
                    # id-perms always has to be updated for last-mod timestamp
                    # get it from request dict(or from db if not in request dict)
                    new_id_perms = new_obj_dict.get(prop_name, json.loads(obj_cols[col_name]))
                    self.update_last_modified(bch, obj_uuid, new_id_perms)
                elif prop_name in new_obj_dict:
                    self._update_prop(bch, obj_uuid, prop_name, new_props)

            # TODO use compiled RE
            if re.match('ref:', col_name):
                (_, ref_type, ref_uuid) = col_name.split(':')
                self._update_ref(bch, 'logical_interface', obj_uuid, ref_type, ref_uuid, new_ref_infos)
        # for all column names

        # create new refs
        for ref_type in new_ref_infos.keys():
            for ref_uuid in new_ref_infos[ref_type].keys():
                ref_data = new_ref_infos[ref_type][ref_uuid]
                self._create_ref(bch, 'logical_interface', obj_uuid, ref_type, ref_uuid, ref_data)

        # create new props
        for prop_name in new_props.keys():
            self._create_prop(bch, obj_uuid, prop_name, new_props[prop_name])

        bch.send()

        return (True, '')
    #end _cassandra_logical_interface_update

    def _cassandra_logical_interface_list(self, parent_uuids=None, back_ref_uuids=None,
                           obj_uuids=None, count=False, filters=None):
        children_fq_names_uuids = []
        if filters:
            fnames = filters.get('field_names', [])
            fvalues = filters.get('field_values', [])
            filter_fields = [(fnames[i], fvalues[i]) for i in range(len(fnames))]
        else:
            filter_fields = []

        def filter_rows(coll_infos, filter_cols, filter_params):
            filt_infos = {}
            coll_rows = obj_uuid_cf.multiget(coll_infos.keys(),
                                   columns=filter_cols,
                                   column_count=10000000)
            for row in coll_rows:
                # give chance for zk heartbeat/ping
                gevent.sleep(0)
                full_match = True
                for fname, fval in filter_params:
                    if coll_rows[row]['prop:%s' %(fname)] != fval:
                        full_match = False
                        break
                if full_match:
                    filt_infos[row] = coll_infos[row]
            return filt_infos
        # end filter_rows

        def get_fq_name_uuid_list(obj_uuids):
            ret_list = []
            for obj_uuid in obj_uuids:
                try:
                    obj_fq_name = self.uuid_to_fq_name(obj_uuid)
                    ret_list.append((obj_fq_name, obj_uuid))
                except cfgm_common.exceptions.NoIdError:
                    pass
            return ret_list
        # end get_fq_name_uuid_list

        if parent_uuids:
            # go from parent to child
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'children:logical_interface:'
            col_fin = 'children:logical_interface;'
            try:
                obj_rows = obj_uuid_cf.multiget(parent_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_parent_anchor(sort=False):
                # flatten to [('children:<type>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_child_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    child_uuid = col_name.split(':')[2]
                    if obj_uuids and child_uuid not in obj_uuids:
                        continue
                    all_child_infos[child_uuid] = {'uuid': child_uuid, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_child_infos = filter_rows(all_child_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_child_infos = all_child_infos

                if not sort:
                    ret_child_infos = filt_child_infos.values()
                else:
                    ret_child_infos = sorted(filt_child_infos.values(), key=itemgetter('tstamp'))

                return get_fq_name_uuid_list(r['uuid'] for r in ret_child_infos)
            # end filter_rows_parent_anchor

            if count:
                return (True, len(filter_rows_parent_anchor()))

            children_fq_names_uuids = filter_rows_parent_anchor(sort=True)

        if back_ref_uuids:
            # go from anchor to backrefs
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'backref:logical_interface:'
            col_fin = 'backref:logical_interface;'
            try:
                obj_rows = obj_uuid_cf.multiget(back_ref_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_backref_anchor():
                # flatten to [('<fqnstr>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_backref_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    col_name_arr = col_name.split(':')
                    fq_name = col_name_arr[:-1]
                    obj_uuid = col_name_arr[-1]
                    if obj_uuids and obj_uuid not in obj_uuids:
                        continue
                    all_backref_infos[obj_uuid] = \
                        {'uuid': obj_uuid, 'fq_name': fq_name, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_backref_infos = filter_rows(all_backref_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_backref_infos = all_backref_infos

                return [(br_info['fq_name'], br_info['uuid']) for br_info in filt_backref_infos.values()]
            # end filter_rows_backref_anchor

            if count:
                return (True, len(filter_rows_backref_anchor()))

            children_fq_names_uuids = filter_rows_backref_anchor()

        if not parent_uuids and not back_ref_uuids:
            obj_uuid_cf = self._obj_uuid_cf
            if obj_uuids:
                # exact objects specified
                def filter_rows_object_list():
                    all_obj_infos = {}
                    for obj_uuid in obj_uuids:
                        all_obj_infos[obj_uuid] = None

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return get_fq_name_uuid_list(filt_obj_infos.keys())
                # end filter_rows_object_list

                if count:
                    return (True, len(filter_rows_object_list()))
                children_fq_names_uuids = filter_rows_object_list()

            else: # grab all resources of this type
                obj_fq_name_cf = self._obj_fq_name_cf
                try:
                    cols = obj_fq_name_cf.get('logical_interface', column_count = 10000000)
                except pycassa.NotFoundException:
                    if count:
                        return (True, 0)
                    else:
                        return (True, children_fq_names_uuids)

                def filter_rows_no_anchor():
                    all_obj_infos = {}
                    for col_name, col_val in cols.items():
                        # give chance for zk heartbeat/ping
                        gevent.sleep(0)
                        col_name_arr = utils.decode_string(col_name).split(':')
                        obj_uuid = col_name_arr[-1]
                        all_obj_infos[obj_uuid] = (col_name_arr[:-1], obj_uuid)

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return filt_obj_infos.values()
                # end filter_rows_no_anchor

                if count:
                    return (True, len(filter_rows_no_anchor()))

                children_fq_names_uuids = filter_rows_no_anchor()

        return (True, children_fq_names_uuids)
    #end _cassandra_logical_interface_list

    def _cassandra_logical_interface_delete(self, obj_uuid):
        obj_uuid_cf = self._obj_uuid_cf
        fq_name = json.loads(obj_uuid_cf.get(obj_uuid, columns = ['fq_name'])['fq_name'])
        bch = obj_uuid_cf.batch()

        # unlink from parent
        col_start = 'parent:'
        col_fin = 'parent;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, parent_type, parent_uuid) = col_name.split(':')
            self._delete_child(bch, parent_type, parent_uuid, 'logical_interface', obj_uuid)

        # remove refs
        col_start = 'ref:'
        col_fin = 'ref;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, ref_type, ref_uuid) = col_name.split(':')
            self._delete_ref(bch, 'logical_interface', obj_uuid, ref_type, ref_uuid)

        bch.remove(obj_uuid)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(fq_name)
        fq_name_col = utils.encode_string(fq_name_str) + ':' + obj_uuid
        self._obj_fq_name_cf.remove('logical_interface', columns = [fq_name_col])


        return (True, '')
    #end _cassandra_logical_interface_delete

    def _cassandra_route_table_alloc(self, fq_name):
        return (True, '')
    #end _cassandra_route_table_alloc

    def _cassandra_route_table_create(self, obj_ids, obj_dict):
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        bch = self._obj_uuid_cf.batch()

        obj_cols = {}
        obj_cols['fq_name'] = json.dumps(obj_dict['fq_name'])
        obj_cols['type'] = json.dumps('route_table')
        if 'parent_type' in obj_dict:
            # non config-root child
            parent_type = obj_dict['parent_type']
            parent_method_type = parent_type.replace('-', '_')
            parent_fq_name = obj_dict['fq_name'][:-1]
            obj_cols['parent_type'] = json.dumps(parent_type)
            parent_uuid = self.fq_name_to_uuid(parent_method_type, parent_fq_name)
            self._create_child(bch, parent_method_type, parent_uuid, 'route_table', obj_ids['uuid'])

        # Properties
        field = obj_dict.get('routes', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'routes', field)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            field['created'] = datetime.datetime.utcnow().isoformat()
            field['last_modified'] = field['created']
            self._create_prop(bch, obj_ids['uuid'], 'id_perms', field)

        field = obj_dict.get('display_name', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'display_name', field)


        # References

        bch.insert(obj_ids['uuid'], obj_cols)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(obj_dict['fq_name'])
        fq_name_cols = {utils.encode_string(fq_name_str) + ':' + obj_ids['uuid']: json.dumps(None)}
        self._obj_fq_name_cf.insert('route_table', fq_name_cols)

        return (True, '')
    #end _cassandra_route_table_create

    def _cassandra_route_table_read(self, obj_uuids, field_names = None):
        # if field_names = None, all fields will be read/returned

        obj_uuid_cf = self._obj_uuid_cf

        # optimize for common case of reading non-backref, non-children fields
        # ignoring columns starting from 'b' and 'c' - significant performance
        # impact in scaled setting. e.g. read of project
        if (field_names is None or
            (set(field_names) & (RouteTable.backref_fields | RouteTable.children_fields))):
            # atleast one backref/children field is needed
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_count = 10000000,
                                       include_timestamp = True)
        else: # ignore reading backref + children columns
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_start = 'd',
                                       column_count = 10000000,
                                       include_timestamp = True)

        if (len(obj_uuids) == 1) and not obj_rows:
            raise cfgm_common.exceptions.NoIdError(obj_uuids[0])

        results = []
        for row_key in obj_rows:
            # give chance for zk heartbeat/ping
            gevent.sleep(0)
            obj_uuid = row_key
            obj_cols = obj_rows[obj_uuid]
            result = {}
            result['uuid'] = obj_uuid
            result['fq_name'] = json.loads(obj_cols['fq_name'][0])
            for col_name in obj_cols.keys():
                if self._re_match_parent.match(col_name):
                    # non config-root child
                    (_, _, parent_uuid) = col_name.split(':')
                    parent_type = json.loads(obj_cols['parent_type'][0])
                    result['parent_type'] = parent_type
                    try:
                        result['parent_uuid'] = parent_uuid
                        result['parent_href'] = self._generate_url(parent_type, parent_uuid)
                    except cfgm_common.exceptions.NoIdError:
                        err_msg = 'Unknown uuid for parent ' + result['fq_name'][-2]
                        return (False, err_msg)

                # TODO use compiled RE
                if self._re_match_prop.match(col_name):
                    (_, prop_name) = col_name.split(':')
                    result[prop_name] = json.loads(obj_cols[col_name][0])

                # TODO use compiled RE
                if self._re_match_children.match(col_name):
                    (_, child_type, child_uuid) = col_name.split(':')
                    if field_names and '%ss' %(child_type) not in field_names:
                        continue

                    child_tstamp = obj_cols[col_name][1]
                    try:
                        self._read_child(result, obj_uuid, child_type, child_uuid, child_tstamp)
                    except cfgm_common.exceptions.NoIdError:
                        continue

                # TODO use compiled RE
                if self._re_match_ref.match(col_name):
                    (_, ref_type, ref_uuid) = col_name.split(':')
                    self._read_ref(result, obj_uuid, ref_type, ref_uuid, obj_cols[col_name][0])

                if self._re_match_backref.match(col_name):
                    (_, back_ref_type, back_ref_uuid) = col_name.split(':')
                    if field_names and '%s_back_refs' %(back_ref_type) not in field_names:
                        continue

                    try:
                        self._read_back_ref(result, obj_uuid, back_ref_type, back_ref_uuid,
                                            obj_cols[col_name][0])
                    except cfgm_common.exceptions.NoIdError:
                        continue

            # for all column names


            results.append(result)
        # end for all rows

        return (True, results)
    #end _cassandra_route_table_read

    def _cassandra_route_table_count_children(self, obj_uuid, child_type):
        # if child_type = None, return
        if child_type is None:
            return (False, '')

        obj_uuid_cf = self._obj_uuid_cf
        if child_type not in RouteTable.children_fields:
            return (False, '%s is not a valid children of RouteTable' %(child_type))

        col_start = 'children:'+child_type[:-1]+':'
        col_finish = 'children:'+child_type[:-1]+';'
        num_children = obj_uuid_cf.get_count(obj_uuid,
                                   column_start = col_start,
                                   column_finish = col_finish,
                                   max_count = 10000000)
        return (True, num_children)
    #end _cassandra_route_table_count_children

    def _cassandra_route_table_update(self, obj_uuid, new_obj_dict):
        # Grab ref-uuids and properties in new version
        new_ref_infos = {}

        new_props = {}
        if 'routes' in new_obj_dict:
            new_props['routes'] = new_obj_dict['routes']
        if 'id_perms' in new_obj_dict:
            new_props['id_perms'] = new_obj_dict['id_perms']
        if 'display_name' in new_obj_dict:
            new_props['display_name'] = new_obj_dict['display_name']
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        obj_uuid_cf = self._obj_uuid_cf
        obj_cols_iter = obj_uuid_cf.xget(obj_uuid)
        # TODO optimize this (converts tuple to dict)
        obj_cols = {}
        for col_info in obj_cols_iter:
            obj_cols[col_info[0]] = col_info[1]

        bch = obj_uuid_cf.batch()
        for col_name in obj_cols.keys():
            # TODO use compiled RE
            if re.match('prop:', col_name):
                (_, prop_name) = col_name.split(':')
                if prop_name == 'id_perms':
                    # id-perms always has to be updated for last-mod timestamp
                    # get it from request dict(or from db if not in request dict)
                    new_id_perms = new_obj_dict.get(prop_name, json.loads(obj_cols[col_name]))
                    self.update_last_modified(bch, obj_uuid, new_id_perms)
                elif prop_name in new_obj_dict:
                    self._update_prop(bch, obj_uuid, prop_name, new_props)

            # TODO use compiled RE
            if re.match('ref:', col_name):
                (_, ref_type, ref_uuid) = col_name.split(':')
                self._update_ref(bch, 'route_table', obj_uuid, ref_type, ref_uuid, new_ref_infos)
        # for all column names

        # create new refs
        for ref_type in new_ref_infos.keys():
            for ref_uuid in new_ref_infos[ref_type].keys():
                ref_data = new_ref_infos[ref_type][ref_uuid]
                self._create_ref(bch, 'route_table', obj_uuid, ref_type, ref_uuid, ref_data)

        # create new props
        for prop_name in new_props.keys():
            self._create_prop(bch, obj_uuid, prop_name, new_props[prop_name])

        bch.send()

        return (True, '')
    #end _cassandra_route_table_update

    def _cassandra_route_table_list(self, parent_uuids=None, back_ref_uuids=None,
                           obj_uuids=None, count=False, filters=None):
        children_fq_names_uuids = []
        if filters:
            fnames = filters.get('field_names', [])
            fvalues = filters.get('field_values', [])
            filter_fields = [(fnames[i], fvalues[i]) for i in range(len(fnames))]
        else:
            filter_fields = []

        def filter_rows(coll_infos, filter_cols, filter_params):
            filt_infos = {}
            coll_rows = obj_uuid_cf.multiget(coll_infos.keys(),
                                   columns=filter_cols,
                                   column_count=10000000)
            for row in coll_rows:
                # give chance for zk heartbeat/ping
                gevent.sleep(0)
                full_match = True
                for fname, fval in filter_params:
                    if coll_rows[row]['prop:%s' %(fname)] != fval:
                        full_match = False
                        break
                if full_match:
                    filt_infos[row] = coll_infos[row]
            return filt_infos
        # end filter_rows

        def get_fq_name_uuid_list(obj_uuids):
            ret_list = []
            for obj_uuid in obj_uuids:
                try:
                    obj_fq_name = self.uuid_to_fq_name(obj_uuid)
                    ret_list.append((obj_fq_name, obj_uuid))
                except cfgm_common.exceptions.NoIdError:
                    pass
            return ret_list
        # end get_fq_name_uuid_list

        if parent_uuids:
            # go from parent to child
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'children:route_table:'
            col_fin = 'children:route_table;'
            try:
                obj_rows = obj_uuid_cf.multiget(parent_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_parent_anchor(sort=False):
                # flatten to [('children:<type>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_child_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    child_uuid = col_name.split(':')[2]
                    if obj_uuids and child_uuid not in obj_uuids:
                        continue
                    all_child_infos[child_uuid] = {'uuid': child_uuid, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_child_infos = filter_rows(all_child_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_child_infos = all_child_infos

                if not sort:
                    ret_child_infos = filt_child_infos.values()
                else:
                    ret_child_infos = sorted(filt_child_infos.values(), key=itemgetter('tstamp'))

                return get_fq_name_uuid_list(r['uuid'] for r in ret_child_infos)
            # end filter_rows_parent_anchor

            if count:
                return (True, len(filter_rows_parent_anchor()))

            children_fq_names_uuids = filter_rows_parent_anchor(sort=True)

        if back_ref_uuids:
            # go from anchor to backrefs
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'backref:route_table:'
            col_fin = 'backref:route_table;'
            try:
                obj_rows = obj_uuid_cf.multiget(back_ref_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_backref_anchor():
                # flatten to [('<fqnstr>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_backref_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    col_name_arr = col_name.split(':')
                    fq_name = col_name_arr[:-1]
                    obj_uuid = col_name_arr[-1]
                    if obj_uuids and obj_uuid not in obj_uuids:
                        continue
                    all_backref_infos[obj_uuid] = \
                        {'uuid': obj_uuid, 'fq_name': fq_name, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_backref_infos = filter_rows(all_backref_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_backref_infos = all_backref_infos

                return [(br_info['fq_name'], br_info['uuid']) for br_info in filt_backref_infos.values()]
            # end filter_rows_backref_anchor

            if count:
                return (True, len(filter_rows_backref_anchor()))

            children_fq_names_uuids = filter_rows_backref_anchor()

        if not parent_uuids and not back_ref_uuids:
            obj_uuid_cf = self._obj_uuid_cf
            if obj_uuids:
                # exact objects specified
                def filter_rows_object_list():
                    all_obj_infos = {}
                    for obj_uuid in obj_uuids:
                        all_obj_infos[obj_uuid] = None

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return get_fq_name_uuid_list(filt_obj_infos.keys())
                # end filter_rows_object_list

                if count:
                    return (True, len(filter_rows_object_list()))
                children_fq_names_uuids = filter_rows_object_list()

            else: # grab all resources of this type
                obj_fq_name_cf = self._obj_fq_name_cf
                try:
                    cols = obj_fq_name_cf.get('route_table', column_count = 10000000)
                except pycassa.NotFoundException:
                    if count:
                        return (True, 0)
                    else:
                        return (True, children_fq_names_uuids)

                def filter_rows_no_anchor():
                    all_obj_infos = {}
                    for col_name, col_val in cols.items():
                        # give chance for zk heartbeat/ping
                        gevent.sleep(0)
                        col_name_arr = utils.decode_string(col_name).split(':')
                        obj_uuid = col_name_arr[-1]
                        all_obj_infos[obj_uuid] = (col_name_arr[:-1], obj_uuid)

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return filt_obj_infos.values()
                # end filter_rows_no_anchor

                if count:
                    return (True, len(filter_rows_no_anchor()))

                children_fq_names_uuids = filter_rows_no_anchor()

        return (True, children_fq_names_uuids)
    #end _cassandra_route_table_list

    def _cassandra_route_table_delete(self, obj_uuid):
        obj_uuid_cf = self._obj_uuid_cf
        fq_name = json.loads(obj_uuid_cf.get(obj_uuid, columns = ['fq_name'])['fq_name'])
        bch = obj_uuid_cf.batch()

        # unlink from parent
        col_start = 'parent:'
        col_fin = 'parent;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, parent_type, parent_uuid) = col_name.split(':')
            self._delete_child(bch, parent_type, parent_uuid, 'route_table', obj_uuid)

        # remove refs
        col_start = 'ref:'
        col_fin = 'ref;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, ref_type, ref_uuid) = col_name.split(':')
            self._delete_ref(bch, 'route_table', obj_uuid, ref_type, ref_uuid)

        bch.remove(obj_uuid)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(fq_name)
        fq_name_col = utils.encode_string(fq_name_str) + ':' + obj_uuid
        self._obj_fq_name_cf.remove('route_table', columns = [fq_name_col])


        return (True, '')
    #end _cassandra_route_table_delete

    def _cassandra_physical_interface_alloc(self, fq_name):
        return (True, '')
    #end _cassandra_physical_interface_alloc

    def _cassandra_physical_interface_create(self, obj_ids, obj_dict):
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        bch = self._obj_uuid_cf.batch()

        obj_cols = {}
        obj_cols['fq_name'] = json.dumps(obj_dict['fq_name'])
        obj_cols['type'] = json.dumps('physical_interface')
        if 'parent_type' in obj_dict:
            # non config-root child
            parent_type = obj_dict['parent_type']
            parent_method_type = parent_type.replace('-', '_')
            parent_fq_name = obj_dict['fq_name'][:-1]
            obj_cols['parent_type'] = json.dumps(parent_type)
            parent_uuid = self.fq_name_to_uuid(parent_method_type, parent_fq_name)
            self._create_child(bch, parent_method_type, parent_uuid, 'physical_interface', obj_ids['uuid'])

        # Properties
        field = obj_dict.get('id_perms', None)
        if field is not None:
            field['created'] = datetime.datetime.utcnow().isoformat()
            field['last_modified'] = field['created']
            self._create_prop(bch, obj_ids['uuid'], 'id_perms', field)

        field = obj_dict.get('display_name', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'display_name', field)


        # References

        bch.insert(obj_ids['uuid'], obj_cols)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(obj_dict['fq_name'])
        fq_name_cols = {utils.encode_string(fq_name_str) + ':' + obj_ids['uuid']: json.dumps(None)}
        self._obj_fq_name_cf.insert('physical_interface', fq_name_cols)

        return (True, '')
    #end _cassandra_physical_interface_create

    def _cassandra_physical_interface_read(self, obj_uuids, field_names = None):
        # if field_names = None, all fields will be read/returned

        obj_uuid_cf = self._obj_uuid_cf

        # optimize for common case of reading non-backref, non-children fields
        # ignoring columns starting from 'b' and 'c' - significant performance
        # impact in scaled setting. e.g. read of project
        if (field_names is None or
            (set(field_names) & (PhysicalInterface.backref_fields | PhysicalInterface.children_fields))):
            # atleast one backref/children field is needed
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_count = 10000000,
                                       include_timestamp = True)
        else: # ignore reading backref + children columns
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_start = 'd',
                                       column_count = 10000000,
                                       include_timestamp = True)

        if (len(obj_uuids) == 1) and not obj_rows:
            raise cfgm_common.exceptions.NoIdError(obj_uuids[0])

        results = []
        for row_key in obj_rows:
            # give chance for zk heartbeat/ping
            gevent.sleep(0)
            obj_uuid = row_key
            obj_cols = obj_rows[obj_uuid]
            result = {}
            result['uuid'] = obj_uuid
            result['fq_name'] = json.loads(obj_cols['fq_name'][0])
            for col_name in obj_cols.keys():
                if self._re_match_parent.match(col_name):
                    # non config-root child
                    (_, _, parent_uuid) = col_name.split(':')
                    parent_type = json.loads(obj_cols['parent_type'][0])
                    result['parent_type'] = parent_type
                    try:
                        result['parent_uuid'] = parent_uuid
                        result['parent_href'] = self._generate_url(parent_type, parent_uuid)
                    except cfgm_common.exceptions.NoIdError:
                        err_msg = 'Unknown uuid for parent ' + result['fq_name'][-2]
                        return (False, err_msg)

                # TODO use compiled RE
                if self._re_match_prop.match(col_name):
                    (_, prop_name) = col_name.split(':')
                    result[prop_name] = json.loads(obj_cols[col_name][0])

                # TODO use compiled RE
                if self._re_match_children.match(col_name):
                    (_, child_type, child_uuid) = col_name.split(':')
                    if field_names and '%ss' %(child_type) not in field_names:
                        continue

                    child_tstamp = obj_cols[col_name][1]
                    try:
                        self._read_child(result, obj_uuid, child_type, child_uuid, child_tstamp)
                    except cfgm_common.exceptions.NoIdError:
                        continue

                # TODO use compiled RE
                if self._re_match_ref.match(col_name):
                    (_, ref_type, ref_uuid) = col_name.split(':')
                    self._read_ref(result, obj_uuid, ref_type, ref_uuid, obj_cols[col_name][0])

                if self._re_match_backref.match(col_name):
                    (_, back_ref_type, back_ref_uuid) = col_name.split(':')
                    if field_names and '%s_back_refs' %(back_ref_type) not in field_names:
                        continue

                    try:
                        self._read_back_ref(result, obj_uuid, back_ref_type, back_ref_uuid,
                                            obj_cols[col_name][0])
                    except cfgm_common.exceptions.NoIdError:
                        continue

            # for all column names

            if 'logical_interfaces' in result:
                # sort children; TODO do this based on schema
                sorted_children = sorted(result['logical_interfaces'], key = itemgetter('tstamp'))
                # re-write result's children without timestamp
                result['logical_interfaces'] = sorted_children
                [child.pop('tstamp') for child in result['logical_interfaces']]

            results.append(result)
        # end for all rows

        return (True, results)
    #end _cassandra_physical_interface_read

    def _cassandra_physical_interface_count_children(self, obj_uuid, child_type):
        # if child_type = None, return
        if child_type is None:
            return (False, '')

        obj_uuid_cf = self._obj_uuid_cf
        if child_type not in PhysicalInterface.children_fields:
            return (False, '%s is not a valid children of PhysicalInterface' %(child_type))

        col_start = 'children:'+child_type[:-1]+':'
        col_finish = 'children:'+child_type[:-1]+';'
        num_children = obj_uuid_cf.get_count(obj_uuid,
                                   column_start = col_start,
                                   column_finish = col_finish,
                                   max_count = 10000000)
        return (True, num_children)
    #end _cassandra_physical_interface_count_children

    def _cassandra_physical_interface_update(self, obj_uuid, new_obj_dict):
        # Grab ref-uuids and properties in new version
        new_ref_infos = {}

        new_props = {}
        if 'id_perms' in new_obj_dict:
            new_props['id_perms'] = new_obj_dict['id_perms']
        if 'display_name' in new_obj_dict:
            new_props['display_name'] = new_obj_dict['display_name']
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        obj_uuid_cf = self._obj_uuid_cf
        obj_cols_iter = obj_uuid_cf.xget(obj_uuid)
        # TODO optimize this (converts tuple to dict)
        obj_cols = {}
        for col_info in obj_cols_iter:
            obj_cols[col_info[0]] = col_info[1]

        bch = obj_uuid_cf.batch()
        for col_name in obj_cols.keys():
            # TODO use compiled RE
            if re.match('prop:', col_name):
                (_, prop_name) = col_name.split(':')
                if prop_name == 'id_perms':
                    # id-perms always has to be updated for last-mod timestamp
                    # get it from request dict(or from db if not in request dict)
                    new_id_perms = new_obj_dict.get(prop_name, json.loads(obj_cols[col_name]))
                    self.update_last_modified(bch, obj_uuid, new_id_perms)
                elif prop_name in new_obj_dict:
                    self._update_prop(bch, obj_uuid, prop_name, new_props)

            # TODO use compiled RE
            if re.match('ref:', col_name):
                (_, ref_type, ref_uuid) = col_name.split(':')
                self._update_ref(bch, 'physical_interface', obj_uuid, ref_type, ref_uuid, new_ref_infos)
        # for all column names

        # create new refs
        for ref_type in new_ref_infos.keys():
            for ref_uuid in new_ref_infos[ref_type].keys():
                ref_data = new_ref_infos[ref_type][ref_uuid]
                self._create_ref(bch, 'physical_interface', obj_uuid, ref_type, ref_uuid, ref_data)

        # create new props
        for prop_name in new_props.keys():
            self._create_prop(bch, obj_uuid, prop_name, new_props[prop_name])

        bch.send()

        return (True, '')
    #end _cassandra_physical_interface_update

    def _cassandra_physical_interface_list(self, parent_uuids=None, back_ref_uuids=None,
                           obj_uuids=None, count=False, filters=None):
        children_fq_names_uuids = []
        if filters:
            fnames = filters.get('field_names', [])
            fvalues = filters.get('field_values', [])
            filter_fields = [(fnames[i], fvalues[i]) for i in range(len(fnames))]
        else:
            filter_fields = []

        def filter_rows(coll_infos, filter_cols, filter_params):
            filt_infos = {}
            coll_rows = obj_uuid_cf.multiget(coll_infos.keys(),
                                   columns=filter_cols,
                                   column_count=10000000)
            for row in coll_rows:
                # give chance for zk heartbeat/ping
                gevent.sleep(0)
                full_match = True
                for fname, fval in filter_params:
                    if coll_rows[row]['prop:%s' %(fname)] != fval:
                        full_match = False
                        break
                if full_match:
                    filt_infos[row] = coll_infos[row]
            return filt_infos
        # end filter_rows

        def get_fq_name_uuid_list(obj_uuids):
            ret_list = []
            for obj_uuid in obj_uuids:
                try:
                    obj_fq_name = self.uuid_to_fq_name(obj_uuid)
                    ret_list.append((obj_fq_name, obj_uuid))
                except cfgm_common.exceptions.NoIdError:
                    pass
            return ret_list
        # end get_fq_name_uuid_list

        if parent_uuids:
            # go from parent to child
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'children:physical_interface:'
            col_fin = 'children:physical_interface;'
            try:
                obj_rows = obj_uuid_cf.multiget(parent_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_parent_anchor(sort=False):
                # flatten to [('children:<type>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_child_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    child_uuid = col_name.split(':')[2]
                    if obj_uuids and child_uuid not in obj_uuids:
                        continue
                    all_child_infos[child_uuid] = {'uuid': child_uuid, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_child_infos = filter_rows(all_child_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_child_infos = all_child_infos

                if not sort:
                    ret_child_infos = filt_child_infos.values()
                else:
                    ret_child_infos = sorted(filt_child_infos.values(), key=itemgetter('tstamp'))

                return get_fq_name_uuid_list(r['uuid'] for r in ret_child_infos)
            # end filter_rows_parent_anchor

            if count:
                return (True, len(filter_rows_parent_anchor()))

            children_fq_names_uuids = filter_rows_parent_anchor(sort=True)

        if back_ref_uuids:
            # go from anchor to backrefs
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'backref:physical_interface:'
            col_fin = 'backref:physical_interface;'
            try:
                obj_rows = obj_uuid_cf.multiget(back_ref_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_backref_anchor():
                # flatten to [('<fqnstr>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_backref_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    col_name_arr = col_name.split(':')
                    fq_name = col_name_arr[:-1]
                    obj_uuid = col_name_arr[-1]
                    if obj_uuids and obj_uuid not in obj_uuids:
                        continue
                    all_backref_infos[obj_uuid] = \
                        {'uuid': obj_uuid, 'fq_name': fq_name, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_backref_infos = filter_rows(all_backref_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_backref_infos = all_backref_infos

                return [(br_info['fq_name'], br_info['uuid']) for br_info in filt_backref_infos.values()]
            # end filter_rows_backref_anchor

            if count:
                return (True, len(filter_rows_backref_anchor()))

            children_fq_names_uuids = filter_rows_backref_anchor()

        if not parent_uuids and not back_ref_uuids:
            obj_uuid_cf = self._obj_uuid_cf
            if obj_uuids:
                # exact objects specified
                def filter_rows_object_list():
                    all_obj_infos = {}
                    for obj_uuid in obj_uuids:
                        all_obj_infos[obj_uuid] = None

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return get_fq_name_uuid_list(filt_obj_infos.keys())
                # end filter_rows_object_list

                if count:
                    return (True, len(filter_rows_object_list()))
                children_fq_names_uuids = filter_rows_object_list()

            else: # grab all resources of this type
                obj_fq_name_cf = self._obj_fq_name_cf
                try:
                    cols = obj_fq_name_cf.get('physical_interface', column_count = 10000000)
                except pycassa.NotFoundException:
                    if count:
                        return (True, 0)
                    else:
                        return (True, children_fq_names_uuids)

                def filter_rows_no_anchor():
                    all_obj_infos = {}
                    for col_name, col_val in cols.items():
                        # give chance for zk heartbeat/ping
                        gevent.sleep(0)
                        col_name_arr = utils.decode_string(col_name).split(':')
                        obj_uuid = col_name_arr[-1]
                        all_obj_infos[obj_uuid] = (col_name_arr[:-1], obj_uuid)

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return filt_obj_infos.values()
                # end filter_rows_no_anchor

                if count:
                    return (True, len(filter_rows_no_anchor()))

                children_fq_names_uuids = filter_rows_no_anchor()

        return (True, children_fq_names_uuids)
    #end _cassandra_physical_interface_list

    def _cassandra_physical_interface_delete(self, obj_uuid):
        obj_uuid_cf = self._obj_uuid_cf
        fq_name = json.loads(obj_uuid_cf.get(obj_uuid, columns = ['fq_name'])['fq_name'])
        bch = obj_uuid_cf.batch()

        # unlink from parent
        col_start = 'parent:'
        col_fin = 'parent;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, parent_type, parent_uuid) = col_name.split(':')
            self._delete_child(bch, parent_type, parent_uuid, 'physical_interface', obj_uuid)

        # remove refs
        col_start = 'ref:'
        col_fin = 'ref;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, ref_type, ref_uuid) = col_name.split(':')
            self._delete_ref(bch, 'physical_interface', obj_uuid, ref_type, ref_uuid)

        bch.remove(obj_uuid)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(fq_name)
        fq_name_col = utils.encode_string(fq_name_str) + ':' + obj_uuid
        self._obj_fq_name_cf.remove('physical_interface', columns = [fq_name_col])


        return (True, '')
    #end _cassandra_physical_interface_delete

    def _cassandra_access_control_list_alloc(self, fq_name):
        return (True, '')
    #end _cassandra_access_control_list_alloc

    def _cassandra_access_control_list_create(self, obj_ids, obj_dict):
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        bch = self._obj_uuid_cf.batch()

        obj_cols = {}
        obj_cols['fq_name'] = json.dumps(obj_dict['fq_name'])
        obj_cols['type'] = json.dumps('access_control_list')
        if 'parent_type' in obj_dict:
            # non config-root child
            parent_type = obj_dict['parent_type']
            parent_method_type = parent_type.replace('-', '_')
            parent_fq_name = obj_dict['fq_name'][:-1]
            obj_cols['parent_type'] = json.dumps(parent_type)
            parent_uuid = self.fq_name_to_uuid(parent_method_type, parent_fq_name)
            self._create_child(bch, parent_method_type, parent_uuid, 'access_control_list', obj_ids['uuid'])

        # Properties
        field = obj_dict.get('access_control_list_entries', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'access_control_list_entries', field)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            field['created'] = datetime.datetime.utcnow().isoformat()
            field['last_modified'] = field['created']
            self._create_prop(bch, obj_ids['uuid'], 'id_perms', field)

        field = obj_dict.get('display_name', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'display_name', field)


        # References

        bch.insert(obj_ids['uuid'], obj_cols)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(obj_dict['fq_name'])
        fq_name_cols = {utils.encode_string(fq_name_str) + ':' + obj_ids['uuid']: json.dumps(None)}
        self._obj_fq_name_cf.insert('access_control_list', fq_name_cols)

        return (True, '')
    #end _cassandra_access_control_list_create

    def _cassandra_access_control_list_read(self, obj_uuids, field_names = None):
        # if field_names = None, all fields will be read/returned

        obj_uuid_cf = self._obj_uuid_cf

        # optimize for common case of reading non-backref, non-children fields
        # ignoring columns starting from 'b' and 'c' - significant performance
        # impact in scaled setting. e.g. read of project
        if (field_names is None or
            (set(field_names) & (AccessControlList.backref_fields | AccessControlList.children_fields))):
            # atleast one backref/children field is needed
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_count = 10000000,
                                       include_timestamp = True)
        else: # ignore reading backref + children columns
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_start = 'd',
                                       column_count = 10000000,
                                       include_timestamp = True)

        if (len(obj_uuids) == 1) and not obj_rows:
            raise cfgm_common.exceptions.NoIdError(obj_uuids[0])

        results = []
        for row_key in obj_rows:
            # give chance for zk heartbeat/ping
            gevent.sleep(0)
            obj_uuid = row_key
            obj_cols = obj_rows[obj_uuid]
            result = {}
            result['uuid'] = obj_uuid
            result['fq_name'] = json.loads(obj_cols['fq_name'][0])
            for col_name in obj_cols.keys():
                if self._re_match_parent.match(col_name):
                    # non config-root child
                    (_, _, parent_uuid) = col_name.split(':')
                    parent_type = json.loads(obj_cols['parent_type'][0])
                    result['parent_type'] = parent_type
                    try:
                        result['parent_uuid'] = parent_uuid
                        result['parent_href'] = self._generate_url(parent_type, parent_uuid)
                    except cfgm_common.exceptions.NoIdError:
                        err_msg = 'Unknown uuid for parent ' + result['fq_name'][-2]
                        return (False, err_msg)

                # TODO use compiled RE
                if self._re_match_prop.match(col_name):
                    (_, prop_name) = col_name.split(':')
                    result[prop_name] = json.loads(obj_cols[col_name][0])

                # TODO use compiled RE
                if self._re_match_children.match(col_name):
                    (_, child_type, child_uuid) = col_name.split(':')
                    if field_names and '%ss' %(child_type) not in field_names:
                        continue

                    child_tstamp = obj_cols[col_name][1]
                    try:
                        self._read_child(result, obj_uuid, child_type, child_uuid, child_tstamp)
                    except cfgm_common.exceptions.NoIdError:
                        continue

                # TODO use compiled RE
                if self._re_match_ref.match(col_name):
                    (_, ref_type, ref_uuid) = col_name.split(':')
                    self._read_ref(result, obj_uuid, ref_type, ref_uuid, obj_cols[col_name][0])

                if self._re_match_backref.match(col_name):
                    (_, back_ref_type, back_ref_uuid) = col_name.split(':')
                    if field_names and '%s_back_refs' %(back_ref_type) not in field_names:
                        continue

                    try:
                        self._read_back_ref(result, obj_uuid, back_ref_type, back_ref_uuid,
                                            obj_cols[col_name][0])
                    except cfgm_common.exceptions.NoIdError:
                        continue

            # for all column names


            results.append(result)
        # end for all rows

        return (True, results)
    #end _cassandra_access_control_list_read

    def _cassandra_access_control_list_count_children(self, obj_uuid, child_type):
        # if child_type = None, return
        if child_type is None:
            return (False, '')

        obj_uuid_cf = self._obj_uuid_cf
        if child_type not in AccessControlList.children_fields:
            return (False, '%s is not a valid children of AccessControlList' %(child_type))

        col_start = 'children:'+child_type[:-1]+':'
        col_finish = 'children:'+child_type[:-1]+';'
        num_children = obj_uuid_cf.get_count(obj_uuid,
                                   column_start = col_start,
                                   column_finish = col_finish,
                                   max_count = 10000000)
        return (True, num_children)
    #end _cassandra_access_control_list_count_children

    def _cassandra_access_control_list_update(self, obj_uuid, new_obj_dict):
        # Grab ref-uuids and properties in new version
        new_ref_infos = {}

        new_props = {}
        if 'access_control_list_entries' in new_obj_dict:
            new_props['access_control_list_entries'] = new_obj_dict['access_control_list_entries']
        if 'id_perms' in new_obj_dict:
            new_props['id_perms'] = new_obj_dict['id_perms']
        if 'display_name' in new_obj_dict:
            new_props['display_name'] = new_obj_dict['display_name']
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        obj_uuid_cf = self._obj_uuid_cf
        obj_cols_iter = obj_uuid_cf.xget(obj_uuid)
        # TODO optimize this (converts tuple to dict)
        obj_cols = {}
        for col_info in obj_cols_iter:
            obj_cols[col_info[0]] = col_info[1]

        bch = obj_uuid_cf.batch()
        for col_name in obj_cols.keys():
            # TODO use compiled RE
            if re.match('prop:', col_name):
                (_, prop_name) = col_name.split(':')
                if prop_name == 'id_perms':
                    # id-perms always has to be updated for last-mod timestamp
                    # get it from request dict(or from db if not in request dict)
                    new_id_perms = new_obj_dict.get(prop_name, json.loads(obj_cols[col_name]))
                    self.update_last_modified(bch, obj_uuid, new_id_perms)
                elif prop_name in new_obj_dict:
                    self._update_prop(bch, obj_uuid, prop_name, new_props)

            # TODO use compiled RE
            if re.match('ref:', col_name):
                (_, ref_type, ref_uuid) = col_name.split(':')
                self._update_ref(bch, 'access_control_list', obj_uuid, ref_type, ref_uuid, new_ref_infos)
        # for all column names

        # create new refs
        for ref_type in new_ref_infos.keys():
            for ref_uuid in new_ref_infos[ref_type].keys():
                ref_data = new_ref_infos[ref_type][ref_uuid]
                self._create_ref(bch, 'access_control_list', obj_uuid, ref_type, ref_uuid, ref_data)

        # create new props
        for prop_name in new_props.keys():
            self._create_prop(bch, obj_uuid, prop_name, new_props[prop_name])

        bch.send()

        return (True, '')
    #end _cassandra_access_control_list_update

    def _cassandra_access_control_list_list(self, parent_uuids=None, back_ref_uuids=None,
                           obj_uuids=None, count=False, filters=None):
        children_fq_names_uuids = []
        if filters:
            fnames = filters.get('field_names', [])
            fvalues = filters.get('field_values', [])
            filter_fields = [(fnames[i], fvalues[i]) for i in range(len(fnames))]
        else:
            filter_fields = []

        def filter_rows(coll_infos, filter_cols, filter_params):
            filt_infos = {}
            coll_rows = obj_uuid_cf.multiget(coll_infos.keys(),
                                   columns=filter_cols,
                                   column_count=10000000)
            for row in coll_rows:
                # give chance for zk heartbeat/ping
                gevent.sleep(0)
                full_match = True
                for fname, fval in filter_params:
                    if coll_rows[row]['prop:%s' %(fname)] != fval:
                        full_match = False
                        break
                if full_match:
                    filt_infos[row] = coll_infos[row]
            return filt_infos
        # end filter_rows

        def get_fq_name_uuid_list(obj_uuids):
            ret_list = []
            for obj_uuid in obj_uuids:
                try:
                    obj_fq_name = self.uuid_to_fq_name(obj_uuid)
                    ret_list.append((obj_fq_name, obj_uuid))
                except cfgm_common.exceptions.NoIdError:
                    pass
            return ret_list
        # end get_fq_name_uuid_list

        if parent_uuids:
            # go from parent to child
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'children:access_control_list:'
            col_fin = 'children:access_control_list;'
            try:
                obj_rows = obj_uuid_cf.multiget(parent_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_parent_anchor(sort=False):
                # flatten to [('children:<type>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_child_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    child_uuid = col_name.split(':')[2]
                    if obj_uuids and child_uuid not in obj_uuids:
                        continue
                    all_child_infos[child_uuid] = {'uuid': child_uuid, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_child_infos = filter_rows(all_child_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_child_infos = all_child_infos

                if not sort:
                    ret_child_infos = filt_child_infos.values()
                else:
                    ret_child_infos = sorted(filt_child_infos.values(), key=itemgetter('tstamp'))

                return get_fq_name_uuid_list(r['uuid'] for r in ret_child_infos)
            # end filter_rows_parent_anchor

            if count:
                return (True, len(filter_rows_parent_anchor()))

            children_fq_names_uuids = filter_rows_parent_anchor(sort=True)

        if back_ref_uuids:
            # go from anchor to backrefs
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'backref:access_control_list:'
            col_fin = 'backref:access_control_list;'
            try:
                obj_rows = obj_uuid_cf.multiget(back_ref_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_backref_anchor():
                # flatten to [('<fqnstr>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_backref_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    col_name_arr = col_name.split(':')
                    fq_name = col_name_arr[:-1]
                    obj_uuid = col_name_arr[-1]
                    if obj_uuids and obj_uuid not in obj_uuids:
                        continue
                    all_backref_infos[obj_uuid] = \
                        {'uuid': obj_uuid, 'fq_name': fq_name, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_backref_infos = filter_rows(all_backref_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_backref_infos = all_backref_infos

                return [(br_info['fq_name'], br_info['uuid']) for br_info in filt_backref_infos.values()]
            # end filter_rows_backref_anchor

            if count:
                return (True, len(filter_rows_backref_anchor()))

            children_fq_names_uuids = filter_rows_backref_anchor()

        if not parent_uuids and not back_ref_uuids:
            obj_uuid_cf = self._obj_uuid_cf
            if obj_uuids:
                # exact objects specified
                def filter_rows_object_list():
                    all_obj_infos = {}
                    for obj_uuid in obj_uuids:
                        all_obj_infos[obj_uuid] = None

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return get_fq_name_uuid_list(filt_obj_infos.keys())
                # end filter_rows_object_list

                if count:
                    return (True, len(filter_rows_object_list()))
                children_fq_names_uuids = filter_rows_object_list()

            else: # grab all resources of this type
                obj_fq_name_cf = self._obj_fq_name_cf
                try:
                    cols = obj_fq_name_cf.get('access_control_list', column_count = 10000000)
                except pycassa.NotFoundException:
                    if count:
                        return (True, 0)
                    else:
                        return (True, children_fq_names_uuids)

                def filter_rows_no_anchor():
                    all_obj_infos = {}
                    for col_name, col_val in cols.items():
                        # give chance for zk heartbeat/ping
                        gevent.sleep(0)
                        col_name_arr = utils.decode_string(col_name).split(':')
                        obj_uuid = col_name_arr[-1]
                        all_obj_infos[obj_uuid] = (col_name_arr[:-1], obj_uuid)

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return filt_obj_infos.values()
                # end filter_rows_no_anchor

                if count:
                    return (True, len(filter_rows_no_anchor()))

                children_fq_names_uuids = filter_rows_no_anchor()

        return (True, children_fq_names_uuids)
    #end _cassandra_access_control_list_list

    def _cassandra_access_control_list_delete(self, obj_uuid):
        obj_uuid_cf = self._obj_uuid_cf
        fq_name = json.loads(obj_uuid_cf.get(obj_uuid, columns = ['fq_name'])['fq_name'])
        bch = obj_uuid_cf.batch()

        # unlink from parent
        col_start = 'parent:'
        col_fin = 'parent;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, parent_type, parent_uuid) = col_name.split(':')
            self._delete_child(bch, parent_type, parent_uuid, 'access_control_list', obj_uuid)

        # remove refs
        col_start = 'ref:'
        col_fin = 'ref;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, ref_type, ref_uuid) = col_name.split(':')
            self._delete_ref(bch, 'access_control_list', obj_uuid, ref_type, ref_uuid)

        bch.remove(obj_uuid)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(fq_name)
        fq_name_col = utils.encode_string(fq_name_str) + ':' + obj_uuid
        self._obj_fq_name_cf.remove('access_control_list', columns = [fq_name_col])


        return (True, '')
    #end _cassandra_access_control_list_delete

    def _cassandra_analytics_node_alloc(self, fq_name):
        return (True, '')
    #end _cassandra_analytics_node_alloc

    def _cassandra_analytics_node_create(self, obj_ids, obj_dict):
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        bch = self._obj_uuid_cf.batch()

        obj_cols = {}
        obj_cols['fq_name'] = json.dumps(obj_dict['fq_name'])
        obj_cols['type'] = json.dumps('analytics_node')
        if 'parent_type' in obj_dict:
            # non config-root child
            parent_type = obj_dict['parent_type']
            parent_method_type = parent_type.replace('-', '_')
            parent_fq_name = obj_dict['fq_name'][:-1]
            obj_cols['parent_type'] = json.dumps(parent_type)
            parent_uuid = self.fq_name_to_uuid(parent_method_type, parent_fq_name)
            self._create_child(bch, parent_method_type, parent_uuid, 'analytics_node', obj_ids['uuid'])

        # Properties
        field = obj_dict.get('analytics_node_ip_address', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'analytics_node_ip_address', field)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            field['created'] = datetime.datetime.utcnow().isoformat()
            field['last_modified'] = field['created']
            self._create_prop(bch, obj_ids['uuid'], 'id_perms', field)

        field = obj_dict.get('display_name', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'display_name', field)


        # References

        bch.insert(obj_ids['uuid'], obj_cols)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(obj_dict['fq_name'])
        fq_name_cols = {utils.encode_string(fq_name_str) + ':' + obj_ids['uuid']: json.dumps(None)}
        self._obj_fq_name_cf.insert('analytics_node', fq_name_cols)

        return (True, '')
    #end _cassandra_analytics_node_create

    def _cassandra_analytics_node_read(self, obj_uuids, field_names = None):
        # if field_names = None, all fields will be read/returned

        obj_uuid_cf = self._obj_uuid_cf

        # optimize for common case of reading non-backref, non-children fields
        # ignoring columns starting from 'b' and 'c' - significant performance
        # impact in scaled setting. e.g. read of project
        if (field_names is None or
            (set(field_names) & (AnalyticsNode.backref_fields | AnalyticsNode.children_fields))):
            # atleast one backref/children field is needed
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_count = 10000000,
                                       include_timestamp = True)
        else: # ignore reading backref + children columns
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_start = 'd',
                                       column_count = 10000000,
                                       include_timestamp = True)

        if (len(obj_uuids) == 1) and not obj_rows:
            raise cfgm_common.exceptions.NoIdError(obj_uuids[0])

        results = []
        for row_key in obj_rows:
            # give chance for zk heartbeat/ping
            gevent.sleep(0)
            obj_uuid = row_key
            obj_cols = obj_rows[obj_uuid]
            result = {}
            result['uuid'] = obj_uuid
            result['fq_name'] = json.loads(obj_cols['fq_name'][0])
            for col_name in obj_cols.keys():
                if self._re_match_parent.match(col_name):
                    # non config-root child
                    (_, _, parent_uuid) = col_name.split(':')
                    parent_type = json.loads(obj_cols['parent_type'][0])
                    result['parent_type'] = parent_type
                    try:
                        result['parent_uuid'] = parent_uuid
                        result['parent_href'] = self._generate_url(parent_type, parent_uuid)
                    except cfgm_common.exceptions.NoIdError:
                        err_msg = 'Unknown uuid for parent ' + result['fq_name'][-2]
                        return (False, err_msg)

                # TODO use compiled RE
                if self._re_match_prop.match(col_name):
                    (_, prop_name) = col_name.split(':')
                    result[prop_name] = json.loads(obj_cols[col_name][0])

                # TODO use compiled RE
                if self._re_match_children.match(col_name):
                    (_, child_type, child_uuid) = col_name.split(':')
                    if field_names and '%ss' %(child_type) not in field_names:
                        continue

                    child_tstamp = obj_cols[col_name][1]
                    try:
                        self._read_child(result, obj_uuid, child_type, child_uuid, child_tstamp)
                    except cfgm_common.exceptions.NoIdError:
                        continue

                # TODO use compiled RE
                if self._re_match_ref.match(col_name):
                    (_, ref_type, ref_uuid) = col_name.split(':')
                    self._read_ref(result, obj_uuid, ref_type, ref_uuid, obj_cols[col_name][0])

                if self._re_match_backref.match(col_name):
                    (_, back_ref_type, back_ref_uuid) = col_name.split(':')
                    if field_names and '%s_back_refs' %(back_ref_type) not in field_names:
                        continue

                    try:
                        self._read_back_ref(result, obj_uuid, back_ref_type, back_ref_uuid,
                                            obj_cols[col_name][0])
                    except cfgm_common.exceptions.NoIdError:
                        continue

            # for all column names


            results.append(result)
        # end for all rows

        return (True, results)
    #end _cassandra_analytics_node_read

    def _cassandra_analytics_node_count_children(self, obj_uuid, child_type):
        # if child_type = None, return
        if child_type is None:
            return (False, '')

        obj_uuid_cf = self._obj_uuid_cf
        if child_type not in AnalyticsNode.children_fields:
            return (False, '%s is not a valid children of AnalyticsNode' %(child_type))

        col_start = 'children:'+child_type[:-1]+':'
        col_finish = 'children:'+child_type[:-1]+';'
        num_children = obj_uuid_cf.get_count(obj_uuid,
                                   column_start = col_start,
                                   column_finish = col_finish,
                                   max_count = 10000000)
        return (True, num_children)
    #end _cassandra_analytics_node_count_children

    def _cassandra_analytics_node_update(self, obj_uuid, new_obj_dict):
        # Grab ref-uuids and properties in new version
        new_ref_infos = {}

        new_props = {}
        if 'analytics_node_ip_address' in new_obj_dict:
            new_props['analytics_node_ip_address'] = new_obj_dict['analytics_node_ip_address']
        if 'id_perms' in new_obj_dict:
            new_props['id_perms'] = new_obj_dict['id_perms']
        if 'display_name' in new_obj_dict:
            new_props['display_name'] = new_obj_dict['display_name']
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        obj_uuid_cf = self._obj_uuid_cf
        obj_cols_iter = obj_uuid_cf.xget(obj_uuid)
        # TODO optimize this (converts tuple to dict)
        obj_cols = {}
        for col_info in obj_cols_iter:
            obj_cols[col_info[0]] = col_info[1]

        bch = obj_uuid_cf.batch()
        for col_name in obj_cols.keys():
            # TODO use compiled RE
            if re.match('prop:', col_name):
                (_, prop_name) = col_name.split(':')
                if prop_name == 'id_perms':
                    # id-perms always has to be updated for last-mod timestamp
                    # get it from request dict(or from db if not in request dict)
                    new_id_perms = new_obj_dict.get(prop_name, json.loads(obj_cols[col_name]))
                    self.update_last_modified(bch, obj_uuid, new_id_perms)
                elif prop_name in new_obj_dict:
                    self._update_prop(bch, obj_uuid, prop_name, new_props)

            # TODO use compiled RE
            if re.match('ref:', col_name):
                (_, ref_type, ref_uuid) = col_name.split(':')
                self._update_ref(bch, 'analytics_node', obj_uuid, ref_type, ref_uuid, new_ref_infos)
        # for all column names

        # create new refs
        for ref_type in new_ref_infos.keys():
            for ref_uuid in new_ref_infos[ref_type].keys():
                ref_data = new_ref_infos[ref_type][ref_uuid]
                self._create_ref(bch, 'analytics_node', obj_uuid, ref_type, ref_uuid, ref_data)

        # create new props
        for prop_name in new_props.keys():
            self._create_prop(bch, obj_uuid, prop_name, new_props[prop_name])

        bch.send()

        return (True, '')
    #end _cassandra_analytics_node_update

    def _cassandra_analytics_node_list(self, parent_uuids=None, back_ref_uuids=None,
                           obj_uuids=None, count=False, filters=None):
        children_fq_names_uuids = []
        if filters:
            fnames = filters.get('field_names', [])
            fvalues = filters.get('field_values', [])
            filter_fields = [(fnames[i], fvalues[i]) for i in range(len(fnames))]
        else:
            filter_fields = []

        def filter_rows(coll_infos, filter_cols, filter_params):
            filt_infos = {}
            coll_rows = obj_uuid_cf.multiget(coll_infos.keys(),
                                   columns=filter_cols,
                                   column_count=10000000)
            for row in coll_rows:
                # give chance for zk heartbeat/ping
                gevent.sleep(0)
                full_match = True
                for fname, fval in filter_params:
                    if coll_rows[row]['prop:%s' %(fname)] != fval:
                        full_match = False
                        break
                if full_match:
                    filt_infos[row] = coll_infos[row]
            return filt_infos
        # end filter_rows

        def get_fq_name_uuid_list(obj_uuids):
            ret_list = []
            for obj_uuid in obj_uuids:
                try:
                    obj_fq_name = self.uuid_to_fq_name(obj_uuid)
                    ret_list.append((obj_fq_name, obj_uuid))
                except cfgm_common.exceptions.NoIdError:
                    pass
            return ret_list
        # end get_fq_name_uuid_list

        if parent_uuids:
            # go from parent to child
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'children:analytics_node:'
            col_fin = 'children:analytics_node;'
            try:
                obj_rows = obj_uuid_cf.multiget(parent_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_parent_anchor(sort=False):
                # flatten to [('children:<type>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_child_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    child_uuid = col_name.split(':')[2]
                    if obj_uuids and child_uuid not in obj_uuids:
                        continue
                    all_child_infos[child_uuid] = {'uuid': child_uuid, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_child_infos = filter_rows(all_child_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_child_infos = all_child_infos

                if not sort:
                    ret_child_infos = filt_child_infos.values()
                else:
                    ret_child_infos = sorted(filt_child_infos.values(), key=itemgetter('tstamp'))

                return get_fq_name_uuid_list(r['uuid'] for r in ret_child_infos)
            # end filter_rows_parent_anchor

            if count:
                return (True, len(filter_rows_parent_anchor()))

            children_fq_names_uuids = filter_rows_parent_anchor(sort=True)

        if back_ref_uuids:
            # go from anchor to backrefs
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'backref:analytics_node:'
            col_fin = 'backref:analytics_node;'
            try:
                obj_rows = obj_uuid_cf.multiget(back_ref_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_backref_anchor():
                # flatten to [('<fqnstr>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_backref_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    col_name_arr = col_name.split(':')
                    fq_name = col_name_arr[:-1]
                    obj_uuid = col_name_arr[-1]
                    if obj_uuids and obj_uuid not in obj_uuids:
                        continue
                    all_backref_infos[obj_uuid] = \
                        {'uuid': obj_uuid, 'fq_name': fq_name, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_backref_infos = filter_rows(all_backref_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_backref_infos = all_backref_infos

                return [(br_info['fq_name'], br_info['uuid']) for br_info in filt_backref_infos.values()]
            # end filter_rows_backref_anchor

            if count:
                return (True, len(filter_rows_backref_anchor()))

            children_fq_names_uuids = filter_rows_backref_anchor()

        if not parent_uuids and not back_ref_uuids:
            obj_uuid_cf = self._obj_uuid_cf
            if obj_uuids:
                # exact objects specified
                def filter_rows_object_list():
                    all_obj_infos = {}
                    for obj_uuid in obj_uuids:
                        all_obj_infos[obj_uuid] = None

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return get_fq_name_uuid_list(filt_obj_infos.keys())
                # end filter_rows_object_list

                if count:
                    return (True, len(filter_rows_object_list()))
                children_fq_names_uuids = filter_rows_object_list()

            else: # grab all resources of this type
                obj_fq_name_cf = self._obj_fq_name_cf
                try:
                    cols = obj_fq_name_cf.get('analytics_node', column_count = 10000000)
                except pycassa.NotFoundException:
                    if count:
                        return (True, 0)
                    else:
                        return (True, children_fq_names_uuids)

                def filter_rows_no_anchor():
                    all_obj_infos = {}
                    for col_name, col_val in cols.items():
                        # give chance for zk heartbeat/ping
                        gevent.sleep(0)
                        col_name_arr = utils.decode_string(col_name).split(':')
                        obj_uuid = col_name_arr[-1]
                        all_obj_infos[obj_uuid] = (col_name_arr[:-1], obj_uuid)

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return filt_obj_infos.values()
                # end filter_rows_no_anchor

                if count:
                    return (True, len(filter_rows_no_anchor()))

                children_fq_names_uuids = filter_rows_no_anchor()

        return (True, children_fq_names_uuids)
    #end _cassandra_analytics_node_list

    def _cassandra_analytics_node_delete(self, obj_uuid):
        obj_uuid_cf = self._obj_uuid_cf
        fq_name = json.loads(obj_uuid_cf.get(obj_uuid, columns = ['fq_name'])['fq_name'])
        bch = obj_uuid_cf.batch()

        # unlink from parent
        col_start = 'parent:'
        col_fin = 'parent;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, parent_type, parent_uuid) = col_name.split(':')
            self._delete_child(bch, parent_type, parent_uuid, 'analytics_node', obj_uuid)

        # remove refs
        col_start = 'ref:'
        col_fin = 'ref;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, ref_type, ref_uuid) = col_name.split(':')
            self._delete_ref(bch, 'analytics_node', obj_uuid, ref_type, ref_uuid)

        bch.remove(obj_uuid)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(fq_name)
        fq_name_col = utils.encode_string(fq_name_str) + ':' + obj_uuid
        self._obj_fq_name_cf.remove('analytics_node', columns = [fq_name_col])


        return (True, '')
    #end _cassandra_analytics_node_delete

    def _cassandra_virtual_DNS_alloc(self, fq_name):
        return (True, '')
    #end _cassandra_virtual_DNS_alloc

    def _cassandra_virtual_DNS_create(self, obj_ids, obj_dict):
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        bch = self._obj_uuid_cf.batch()

        obj_cols = {}
        obj_cols['fq_name'] = json.dumps(obj_dict['fq_name'])
        obj_cols['type'] = json.dumps('virtual_DNS')
        if 'parent_type' in obj_dict:
            # non config-root child
            parent_type = obj_dict['parent_type']
            parent_method_type = parent_type.replace('-', '_')
            parent_fq_name = obj_dict['fq_name'][:-1]
            obj_cols['parent_type'] = json.dumps(parent_type)
            parent_uuid = self.fq_name_to_uuid(parent_method_type, parent_fq_name)
            self._create_child(bch, parent_method_type, parent_uuid, 'virtual_DNS', obj_ids['uuid'])

        # Properties
        field = obj_dict.get('virtual_DNS_data', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'virtual_DNS_data', field)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            field['created'] = datetime.datetime.utcnow().isoformat()
            field['last_modified'] = field['created']
            self._create_prop(bch, obj_ids['uuid'], 'id_perms', field)

        field = obj_dict.get('display_name', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'display_name', field)


        # References

        bch.insert(obj_ids['uuid'], obj_cols)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(obj_dict['fq_name'])
        fq_name_cols = {utils.encode_string(fq_name_str) + ':' + obj_ids['uuid']: json.dumps(None)}
        self._obj_fq_name_cf.insert('virtual_DNS', fq_name_cols)

        return (True, '')
    #end _cassandra_virtual_DNS_create

    def _cassandra_virtual_DNS_read(self, obj_uuids, field_names = None):
        # if field_names = None, all fields will be read/returned

        obj_uuid_cf = self._obj_uuid_cf

        # optimize for common case of reading non-backref, non-children fields
        # ignoring columns starting from 'b' and 'c' - significant performance
        # impact in scaled setting. e.g. read of project
        if (field_names is None or
            (set(field_names) & (VirtualDns.backref_fields | VirtualDns.children_fields))):
            # atleast one backref/children field is needed
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_count = 10000000,
                                       include_timestamp = True)
        else: # ignore reading backref + children columns
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_start = 'd',
                                       column_count = 10000000,
                                       include_timestamp = True)

        if (len(obj_uuids) == 1) and not obj_rows:
            raise cfgm_common.exceptions.NoIdError(obj_uuids[0])

        results = []
        for row_key in obj_rows:
            # give chance for zk heartbeat/ping
            gevent.sleep(0)
            obj_uuid = row_key
            obj_cols = obj_rows[obj_uuid]
            result = {}
            result['uuid'] = obj_uuid
            result['fq_name'] = json.loads(obj_cols['fq_name'][0])
            for col_name in obj_cols.keys():
                if self._re_match_parent.match(col_name):
                    # non config-root child
                    (_, _, parent_uuid) = col_name.split(':')
                    parent_type = json.loads(obj_cols['parent_type'][0])
                    result['parent_type'] = parent_type
                    try:
                        result['parent_uuid'] = parent_uuid
                        result['parent_href'] = self._generate_url(parent_type, parent_uuid)
                    except cfgm_common.exceptions.NoIdError:
                        err_msg = 'Unknown uuid for parent ' + result['fq_name'][-2]
                        return (False, err_msg)

                # TODO use compiled RE
                if self._re_match_prop.match(col_name):
                    (_, prop_name) = col_name.split(':')
                    result[prop_name] = json.loads(obj_cols[col_name][0])

                # TODO use compiled RE
                if self._re_match_children.match(col_name):
                    (_, child_type, child_uuid) = col_name.split(':')
                    if field_names and '%ss' %(child_type) not in field_names:
                        continue

                    child_tstamp = obj_cols[col_name][1]
                    try:
                        self._read_child(result, obj_uuid, child_type, child_uuid, child_tstamp)
                    except cfgm_common.exceptions.NoIdError:
                        continue

                # TODO use compiled RE
                if self._re_match_ref.match(col_name):
                    (_, ref_type, ref_uuid) = col_name.split(':')
                    self._read_ref(result, obj_uuid, ref_type, ref_uuid, obj_cols[col_name][0])

                if self._re_match_backref.match(col_name):
                    (_, back_ref_type, back_ref_uuid) = col_name.split(':')
                    if field_names and '%s_back_refs' %(back_ref_type) not in field_names:
                        continue

                    try:
                        self._read_back_ref(result, obj_uuid, back_ref_type, back_ref_uuid,
                                            obj_cols[col_name][0])
                    except cfgm_common.exceptions.NoIdError:
                        continue

            # for all column names

            if 'virtual_DNS_records' in result:
                # sort children; TODO do this based on schema
                sorted_children = sorted(result['virtual_DNS_records'], key = itemgetter('tstamp'))
                # re-write result's children without timestamp
                result['virtual_DNS_records'] = sorted_children
                [child.pop('tstamp') for child in result['virtual_DNS_records']]

            results.append(result)
        # end for all rows

        return (True, results)
    #end _cassandra_virtual_DNS_read

    def _cassandra_virtual_DNS_count_children(self, obj_uuid, child_type):
        # if child_type = None, return
        if child_type is None:
            return (False, '')

        obj_uuid_cf = self._obj_uuid_cf
        if child_type not in VirtualDns.children_fields:
            return (False, '%s is not a valid children of VirtualDns' %(child_type))

        col_start = 'children:'+child_type[:-1]+':'
        col_finish = 'children:'+child_type[:-1]+';'
        num_children = obj_uuid_cf.get_count(obj_uuid,
                                   column_start = col_start,
                                   column_finish = col_finish,
                                   max_count = 10000000)
        return (True, num_children)
    #end _cassandra_virtual_DNS_count_children

    def _cassandra_virtual_DNS_update(self, obj_uuid, new_obj_dict):
        # Grab ref-uuids and properties in new version
        new_ref_infos = {}

        new_props = {}
        if 'virtual_DNS_data' in new_obj_dict:
            new_props['virtual_DNS_data'] = new_obj_dict['virtual_DNS_data']
        if 'id_perms' in new_obj_dict:
            new_props['id_perms'] = new_obj_dict['id_perms']
        if 'display_name' in new_obj_dict:
            new_props['display_name'] = new_obj_dict['display_name']
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        obj_uuid_cf = self._obj_uuid_cf
        obj_cols_iter = obj_uuid_cf.xget(obj_uuid)
        # TODO optimize this (converts tuple to dict)
        obj_cols = {}
        for col_info in obj_cols_iter:
            obj_cols[col_info[0]] = col_info[1]

        bch = obj_uuid_cf.batch()
        for col_name in obj_cols.keys():
            # TODO use compiled RE
            if re.match('prop:', col_name):
                (_, prop_name) = col_name.split(':')
                if prop_name == 'id_perms':
                    # id-perms always has to be updated for last-mod timestamp
                    # get it from request dict(or from db if not in request dict)
                    new_id_perms = new_obj_dict.get(prop_name, json.loads(obj_cols[col_name]))
                    self.update_last_modified(bch, obj_uuid, new_id_perms)
                elif prop_name in new_obj_dict:
                    self._update_prop(bch, obj_uuid, prop_name, new_props)

            # TODO use compiled RE
            if re.match('ref:', col_name):
                (_, ref_type, ref_uuid) = col_name.split(':')
                self._update_ref(bch, 'virtual_DNS', obj_uuid, ref_type, ref_uuid, new_ref_infos)
        # for all column names

        # create new refs
        for ref_type in new_ref_infos.keys():
            for ref_uuid in new_ref_infos[ref_type].keys():
                ref_data = new_ref_infos[ref_type][ref_uuid]
                self._create_ref(bch, 'virtual_DNS', obj_uuid, ref_type, ref_uuid, ref_data)

        # create new props
        for prop_name in new_props.keys():
            self._create_prop(bch, obj_uuid, prop_name, new_props[prop_name])

        bch.send()

        return (True, '')
    #end _cassandra_virtual_DNS_update

    def _cassandra_virtual_DNS_list(self, parent_uuids=None, back_ref_uuids=None,
                           obj_uuids=None, count=False, filters=None):
        children_fq_names_uuids = []
        if filters:
            fnames = filters.get('field_names', [])
            fvalues = filters.get('field_values', [])
            filter_fields = [(fnames[i], fvalues[i]) for i in range(len(fnames))]
        else:
            filter_fields = []

        def filter_rows(coll_infos, filter_cols, filter_params):
            filt_infos = {}
            coll_rows = obj_uuid_cf.multiget(coll_infos.keys(),
                                   columns=filter_cols,
                                   column_count=10000000)
            for row in coll_rows:
                # give chance for zk heartbeat/ping
                gevent.sleep(0)
                full_match = True
                for fname, fval in filter_params:
                    if coll_rows[row]['prop:%s' %(fname)] != fval:
                        full_match = False
                        break
                if full_match:
                    filt_infos[row] = coll_infos[row]
            return filt_infos
        # end filter_rows

        def get_fq_name_uuid_list(obj_uuids):
            ret_list = []
            for obj_uuid in obj_uuids:
                try:
                    obj_fq_name = self.uuid_to_fq_name(obj_uuid)
                    ret_list.append((obj_fq_name, obj_uuid))
                except cfgm_common.exceptions.NoIdError:
                    pass
            return ret_list
        # end get_fq_name_uuid_list

        if parent_uuids:
            # go from parent to child
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'children:virtual_DNS:'
            col_fin = 'children:virtual_DNS;'
            try:
                obj_rows = obj_uuid_cf.multiget(parent_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_parent_anchor(sort=False):
                # flatten to [('children:<type>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_child_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    child_uuid = col_name.split(':')[2]
                    if obj_uuids and child_uuid not in obj_uuids:
                        continue
                    all_child_infos[child_uuid] = {'uuid': child_uuid, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_child_infos = filter_rows(all_child_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_child_infos = all_child_infos

                if not sort:
                    ret_child_infos = filt_child_infos.values()
                else:
                    ret_child_infos = sorted(filt_child_infos.values(), key=itemgetter('tstamp'))

                return get_fq_name_uuid_list(r['uuid'] for r in ret_child_infos)
            # end filter_rows_parent_anchor

            if count:
                return (True, len(filter_rows_parent_anchor()))

            children_fq_names_uuids = filter_rows_parent_anchor(sort=True)

        if back_ref_uuids:
            # go from anchor to backrefs
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'backref:virtual_DNS:'
            col_fin = 'backref:virtual_DNS;'
            try:
                obj_rows = obj_uuid_cf.multiget(back_ref_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_backref_anchor():
                # flatten to [('<fqnstr>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_backref_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    col_name_arr = col_name.split(':')
                    fq_name = col_name_arr[:-1]
                    obj_uuid = col_name_arr[-1]
                    if obj_uuids and obj_uuid not in obj_uuids:
                        continue
                    all_backref_infos[obj_uuid] = \
                        {'uuid': obj_uuid, 'fq_name': fq_name, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_backref_infos = filter_rows(all_backref_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_backref_infos = all_backref_infos

                return [(br_info['fq_name'], br_info['uuid']) for br_info in filt_backref_infos.values()]
            # end filter_rows_backref_anchor

            if count:
                return (True, len(filter_rows_backref_anchor()))

            children_fq_names_uuids = filter_rows_backref_anchor()

        if not parent_uuids and not back_ref_uuids:
            obj_uuid_cf = self._obj_uuid_cf
            if obj_uuids:
                # exact objects specified
                def filter_rows_object_list():
                    all_obj_infos = {}
                    for obj_uuid in obj_uuids:
                        all_obj_infos[obj_uuid] = None

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return get_fq_name_uuid_list(filt_obj_infos.keys())
                # end filter_rows_object_list

                if count:
                    return (True, len(filter_rows_object_list()))
                children_fq_names_uuids = filter_rows_object_list()

            else: # grab all resources of this type
                obj_fq_name_cf = self._obj_fq_name_cf
                try:
                    cols = obj_fq_name_cf.get('virtual_DNS', column_count = 10000000)
                except pycassa.NotFoundException:
                    if count:
                        return (True, 0)
                    else:
                        return (True, children_fq_names_uuids)

                def filter_rows_no_anchor():
                    all_obj_infos = {}
                    for col_name, col_val in cols.items():
                        # give chance for zk heartbeat/ping
                        gevent.sleep(0)
                        col_name_arr = utils.decode_string(col_name).split(':')
                        obj_uuid = col_name_arr[-1]
                        all_obj_infos[obj_uuid] = (col_name_arr[:-1], obj_uuid)

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return filt_obj_infos.values()
                # end filter_rows_no_anchor

                if count:
                    return (True, len(filter_rows_no_anchor()))

                children_fq_names_uuids = filter_rows_no_anchor()

        return (True, children_fq_names_uuids)
    #end _cassandra_virtual_DNS_list

    def _cassandra_virtual_DNS_delete(self, obj_uuid):
        obj_uuid_cf = self._obj_uuid_cf
        fq_name = json.loads(obj_uuid_cf.get(obj_uuid, columns = ['fq_name'])['fq_name'])
        bch = obj_uuid_cf.batch()

        # unlink from parent
        col_start = 'parent:'
        col_fin = 'parent;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, parent_type, parent_uuid) = col_name.split(':')
            self._delete_child(bch, parent_type, parent_uuid, 'virtual_DNS', obj_uuid)

        # remove refs
        col_start = 'ref:'
        col_fin = 'ref;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, ref_type, ref_uuid) = col_name.split(':')
            self._delete_ref(bch, 'virtual_DNS', obj_uuid, ref_type, ref_uuid)

        bch.remove(obj_uuid)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(fq_name)
        fq_name_col = utils.encode_string(fq_name_str) + ':' + obj_uuid
        self._obj_fq_name_cf.remove('virtual_DNS', columns = [fq_name_col])


        return (True, '')
    #end _cassandra_virtual_DNS_delete

    def _cassandra_customer_attachment_alloc(self, fq_name):
        return (True, '')
    #end _cassandra_customer_attachment_alloc

    def _cassandra_customer_attachment_create(self, obj_ids, obj_dict):
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        bch = self._obj_uuid_cf.batch()

        obj_cols = {}
        obj_cols['fq_name'] = json.dumps(obj_dict['fq_name'])
        obj_cols['type'] = json.dumps('customer_attachment')

        # Properties
        field = obj_dict.get('attachment_address', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'attachment_address', field)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            field['created'] = datetime.datetime.utcnow().isoformat()
            field['last_modified'] = field['created']
            self._create_prop(bch, obj_ids['uuid'], 'id_perms', field)

        field = obj_dict.get('display_name', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'display_name', field)


        # References
        refs = obj_dict.get('virtual_machine_interface_refs', [])
        for ref in refs:
            ref_uuid = self.fq_name_to_uuid('virtual_machine_interface', ref['to'])
            ref_attr = ref.get('attr', None)
            ref_data = {'attr': ref_attr, 'is_weakref': False}
            self._create_ref(bch, 'customer_attachment', obj_ids['uuid'], 'virtual_machine_interface', ref_uuid, ref_data)
        refs = obj_dict.get('floating_ip_refs', [])
        for ref in refs:
            ref_uuid = self.fq_name_to_uuid('floating_ip', ref['to'])
            ref_attr = ref.get('attr', None)
            ref_data = {'attr': ref_attr, 'is_weakref': False}
            self._create_ref(bch, 'customer_attachment', obj_ids['uuid'], 'floating_ip', ref_uuid, ref_data)

        bch.insert(obj_ids['uuid'], obj_cols)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(obj_dict['fq_name'])
        fq_name_cols = {utils.encode_string(fq_name_str) + ':' + obj_ids['uuid']: json.dumps(None)}
        self._obj_fq_name_cf.insert('customer_attachment', fq_name_cols)

        return (True, '')
    #end _cassandra_customer_attachment_create

    def _cassandra_customer_attachment_read(self, obj_uuids, field_names = None):
        # if field_names = None, all fields will be read/returned

        obj_uuid_cf = self._obj_uuid_cf

        # optimize for common case of reading non-backref, non-children fields
        # ignoring columns starting from 'b' and 'c' - significant performance
        # impact in scaled setting. e.g. read of project
        if (field_names is None or
            (set(field_names) & (CustomerAttachment.backref_fields | CustomerAttachment.children_fields))):
            # atleast one backref/children field is needed
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_count = 10000000,
                                       include_timestamp = True)
        else: # ignore reading backref + children columns
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_start = 'd',
                                       column_count = 10000000,
                                       include_timestamp = True)

        if (len(obj_uuids) == 1) and not obj_rows:
            raise cfgm_common.exceptions.NoIdError(obj_uuids[0])

        results = []
        for row_key in obj_rows:
            # give chance for zk heartbeat/ping
            gevent.sleep(0)
            obj_uuid = row_key
            obj_cols = obj_rows[obj_uuid]
            result = {}
            result['uuid'] = obj_uuid
            result['fq_name'] = json.loads(obj_cols['fq_name'][0])
            for col_name in obj_cols.keys():
                if self._re_match_parent.match(col_name):
                    # non config-root child
                    (_, _, parent_uuid) = col_name.split(':')
                    parent_type = json.loads(obj_cols['parent_type'][0])
                    result['parent_type'] = parent_type
                    try:
                        result['parent_uuid'] = parent_uuid
                        result['parent_href'] = self._generate_url(parent_type, parent_uuid)
                    except cfgm_common.exceptions.NoIdError:
                        err_msg = 'Unknown uuid for parent ' + result['fq_name'][-2]
                        return (False, err_msg)

                # TODO use compiled RE
                if self._re_match_prop.match(col_name):
                    (_, prop_name) = col_name.split(':')
                    result[prop_name] = json.loads(obj_cols[col_name][0])

                # TODO use compiled RE
                if self._re_match_children.match(col_name):
                    (_, child_type, child_uuid) = col_name.split(':')
                    if field_names and '%ss' %(child_type) not in field_names:
                        continue

                    child_tstamp = obj_cols[col_name][1]
                    try:
                        self._read_child(result, obj_uuid, child_type, child_uuid, child_tstamp)
                    except cfgm_common.exceptions.NoIdError:
                        continue

                # TODO use compiled RE
                if self._re_match_ref.match(col_name):
                    (_, ref_type, ref_uuid) = col_name.split(':')
                    self._read_ref(result, obj_uuid, ref_type, ref_uuid, obj_cols[col_name][0])

                if self._re_match_backref.match(col_name):
                    (_, back_ref_type, back_ref_uuid) = col_name.split(':')
                    if field_names and '%s_back_refs' %(back_ref_type) not in field_names:
                        continue

                    try:
                        self._read_back_ref(result, obj_uuid, back_ref_type, back_ref_uuid,
                                            obj_cols[col_name][0])
                    except cfgm_common.exceptions.NoIdError:
                        continue

            # for all column names


            results.append(result)
        # end for all rows

        return (True, results)
    #end _cassandra_customer_attachment_read

    def _cassandra_customer_attachment_count_children(self, obj_uuid, child_type):
        # if child_type = None, return
        if child_type is None:
            return (False, '')

        obj_uuid_cf = self._obj_uuid_cf
        if child_type not in CustomerAttachment.children_fields:
            return (False, '%s is not a valid children of CustomerAttachment' %(child_type))

        col_start = 'children:'+child_type[:-1]+':'
        col_finish = 'children:'+child_type[:-1]+';'
        num_children = obj_uuid_cf.get_count(obj_uuid,
                                   column_start = col_start,
                                   column_finish = col_finish,
                                   max_count = 10000000)
        return (True, num_children)
    #end _cassandra_customer_attachment_count_children

    def _cassandra_customer_attachment_update(self, obj_uuid, new_obj_dict):
        # Grab ref-uuids and properties in new version
        new_ref_infos = {}

        if 'virtual_machine_interface_refs' in new_obj_dict:
            new_ref_infos['virtual_machine_interface'] = {}
            new_refs = new_obj_dict['virtual_machine_interface_refs']
            if new_refs:
                for new_ref in new_refs:
                    new_ref_uuid = self.fq_name_to_uuid('virtual_machine_interface', new_ref['to'])
                    new_ref_attr = new_ref.get('attr', None)
                    new_ref_data = {'attr': new_ref_attr, 'is_weakref': False}
                    new_ref_infos['virtual_machine_interface'][new_ref_uuid] = new_ref_data

        if 'floating_ip_refs' in new_obj_dict:
            new_ref_infos['floating_ip'] = {}
            new_refs = new_obj_dict['floating_ip_refs']
            if new_refs:
                for new_ref in new_refs:
                    new_ref_uuid = self.fq_name_to_uuid('floating_ip', new_ref['to'])
                    new_ref_attr = new_ref.get('attr', None)
                    new_ref_data = {'attr': new_ref_attr, 'is_weakref': False}
                    new_ref_infos['floating_ip'][new_ref_uuid] = new_ref_data

        new_props = {}
        if 'attachment_address' in new_obj_dict:
            new_props['attachment_address'] = new_obj_dict['attachment_address']
        if 'id_perms' in new_obj_dict:
            new_props['id_perms'] = new_obj_dict['id_perms']
        if 'display_name' in new_obj_dict:
            new_props['display_name'] = new_obj_dict['display_name']
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        obj_uuid_cf = self._obj_uuid_cf
        obj_cols_iter = obj_uuid_cf.xget(obj_uuid)
        # TODO optimize this (converts tuple to dict)
        obj_cols = {}
        for col_info in obj_cols_iter:
            obj_cols[col_info[0]] = col_info[1]

        bch = obj_uuid_cf.batch()
        for col_name in obj_cols.keys():
            # TODO use compiled RE
            if re.match('prop:', col_name):
                (_, prop_name) = col_name.split(':')
                if prop_name == 'id_perms':
                    # id-perms always has to be updated for last-mod timestamp
                    # get it from request dict(or from db if not in request dict)
                    new_id_perms = new_obj_dict.get(prop_name, json.loads(obj_cols[col_name]))
                    self.update_last_modified(bch, obj_uuid, new_id_perms)
                elif prop_name in new_obj_dict:
                    self._update_prop(bch, obj_uuid, prop_name, new_props)

            # TODO use compiled RE
            if re.match('ref:', col_name):
                (_, ref_type, ref_uuid) = col_name.split(':')
                self._update_ref(bch, 'customer_attachment', obj_uuid, ref_type, ref_uuid, new_ref_infos)
        # for all column names

        # create new refs
        for ref_type in new_ref_infos.keys():
            for ref_uuid in new_ref_infos[ref_type].keys():
                ref_data = new_ref_infos[ref_type][ref_uuid]
                self._create_ref(bch, 'customer_attachment', obj_uuid, ref_type, ref_uuid, ref_data)

        # create new props
        for prop_name in new_props.keys():
            self._create_prop(bch, obj_uuid, prop_name, new_props[prop_name])

        bch.send()

        return (True, '')
    #end _cassandra_customer_attachment_update

    def _cassandra_customer_attachment_list(self, parent_uuids=None, back_ref_uuids=None,
                           obj_uuids=None, count=False, filters=None):
        children_fq_names_uuids = []
        if filters:
            fnames = filters.get('field_names', [])
            fvalues = filters.get('field_values', [])
            filter_fields = [(fnames[i], fvalues[i]) for i in range(len(fnames))]
        else:
            filter_fields = []

        def filter_rows(coll_infos, filter_cols, filter_params):
            filt_infos = {}
            coll_rows = obj_uuid_cf.multiget(coll_infos.keys(),
                                   columns=filter_cols,
                                   column_count=10000000)
            for row in coll_rows:
                # give chance for zk heartbeat/ping
                gevent.sleep(0)
                full_match = True
                for fname, fval in filter_params:
                    if coll_rows[row]['prop:%s' %(fname)] != fval:
                        full_match = False
                        break
                if full_match:
                    filt_infos[row] = coll_infos[row]
            return filt_infos
        # end filter_rows

        def get_fq_name_uuid_list(obj_uuids):
            ret_list = []
            for obj_uuid in obj_uuids:
                try:
                    obj_fq_name = self.uuid_to_fq_name(obj_uuid)
                    ret_list.append((obj_fq_name, obj_uuid))
                except cfgm_common.exceptions.NoIdError:
                    pass
            return ret_list
        # end get_fq_name_uuid_list

        if parent_uuids:
            # go from parent to child
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'children:customer_attachment:'
            col_fin = 'children:customer_attachment;'
            try:
                obj_rows = obj_uuid_cf.multiget(parent_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_parent_anchor(sort=False):
                # flatten to [('children:<type>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_child_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    child_uuid = col_name.split(':')[2]
                    if obj_uuids and child_uuid not in obj_uuids:
                        continue
                    all_child_infos[child_uuid] = {'uuid': child_uuid, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_child_infos = filter_rows(all_child_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_child_infos = all_child_infos

                if not sort:
                    ret_child_infos = filt_child_infos.values()
                else:
                    ret_child_infos = sorted(filt_child_infos.values(), key=itemgetter('tstamp'))

                return get_fq_name_uuid_list(r['uuid'] for r in ret_child_infos)
            # end filter_rows_parent_anchor

            if count:
                return (True, len(filter_rows_parent_anchor()))

            children_fq_names_uuids = filter_rows_parent_anchor(sort=True)

        if back_ref_uuids:
            # go from anchor to backrefs
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'backref:customer_attachment:'
            col_fin = 'backref:customer_attachment;'
            try:
                obj_rows = obj_uuid_cf.multiget(back_ref_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_backref_anchor():
                # flatten to [('<fqnstr>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_backref_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    col_name_arr = col_name.split(':')
                    fq_name = col_name_arr[:-1]
                    obj_uuid = col_name_arr[-1]
                    if obj_uuids and obj_uuid not in obj_uuids:
                        continue
                    all_backref_infos[obj_uuid] = \
                        {'uuid': obj_uuid, 'fq_name': fq_name, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_backref_infos = filter_rows(all_backref_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_backref_infos = all_backref_infos

                return [(br_info['fq_name'], br_info['uuid']) for br_info in filt_backref_infos.values()]
            # end filter_rows_backref_anchor

            if count:
                return (True, len(filter_rows_backref_anchor()))

            children_fq_names_uuids = filter_rows_backref_anchor()

        if not parent_uuids and not back_ref_uuids:
            obj_uuid_cf = self._obj_uuid_cf
            if obj_uuids:
                # exact objects specified
                def filter_rows_object_list():
                    all_obj_infos = {}
                    for obj_uuid in obj_uuids:
                        all_obj_infos[obj_uuid] = None

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return get_fq_name_uuid_list(filt_obj_infos.keys())
                # end filter_rows_object_list

                if count:
                    return (True, len(filter_rows_object_list()))
                children_fq_names_uuids = filter_rows_object_list()

            else: # grab all resources of this type
                obj_fq_name_cf = self._obj_fq_name_cf
                try:
                    cols = obj_fq_name_cf.get('customer_attachment', column_count = 10000000)
                except pycassa.NotFoundException:
                    if count:
                        return (True, 0)
                    else:
                        return (True, children_fq_names_uuids)

                def filter_rows_no_anchor():
                    all_obj_infos = {}
                    for col_name, col_val in cols.items():
                        # give chance for zk heartbeat/ping
                        gevent.sleep(0)
                        col_name_arr = utils.decode_string(col_name).split(':')
                        obj_uuid = col_name_arr[-1]
                        all_obj_infos[obj_uuid] = (col_name_arr[:-1], obj_uuid)

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return filt_obj_infos.values()
                # end filter_rows_no_anchor

                if count:
                    return (True, len(filter_rows_no_anchor()))

                children_fq_names_uuids = filter_rows_no_anchor()

        return (True, children_fq_names_uuids)
    #end _cassandra_customer_attachment_list

    def _cassandra_customer_attachment_delete(self, obj_uuid):
        obj_uuid_cf = self._obj_uuid_cf
        fq_name = json.loads(obj_uuid_cf.get(obj_uuid, columns = ['fq_name'])['fq_name'])
        bch = obj_uuid_cf.batch()

        # unlink from parent
        col_start = 'parent:'
        col_fin = 'parent;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, parent_type, parent_uuid) = col_name.split(':')
            self._delete_child(bch, parent_type, parent_uuid, 'customer_attachment', obj_uuid)

        # remove refs
        col_start = 'ref:'
        col_fin = 'ref;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, ref_type, ref_uuid) = col_name.split(':')
            self._delete_ref(bch, 'customer_attachment', obj_uuid, ref_type, ref_uuid)

        bch.remove(obj_uuid)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(fq_name)
        fq_name_col = utils.encode_string(fq_name_str) + ':' + obj_uuid
        self._obj_fq_name_cf.remove('customer_attachment', columns = [fq_name_col])


        return (True, '')
    #end _cassandra_customer_attachment_delete

    def _cassandra_service_appliance_set_alloc(self, fq_name):
        return (True, '')
    #end _cassandra_service_appliance_set_alloc

    def _cassandra_service_appliance_set_create(self, obj_ids, obj_dict):
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        bch = self._obj_uuid_cf.batch()

        obj_cols = {}
        obj_cols['fq_name'] = json.dumps(obj_dict['fq_name'])
        obj_cols['type'] = json.dumps('service_appliance_set')
        if 'parent_type' in obj_dict:
            # non config-root child
            parent_type = obj_dict['parent_type']
            parent_method_type = parent_type.replace('-', '_')
            parent_fq_name = obj_dict['fq_name'][:-1]
            obj_cols['parent_type'] = json.dumps(parent_type)
            parent_uuid = self.fq_name_to_uuid(parent_method_type, parent_fq_name)
            self._create_child(bch, parent_method_type, parent_uuid, 'service_appliance_set', obj_ids['uuid'])

        # Properties
        field = obj_dict.get('service_appliance_set_properties', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'service_appliance_set_properties', field)

        field = obj_dict.get('service_appliance_driver', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'service_appliance_driver', field)

        field = obj_dict.get('service_appliance_ha_mode', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'service_appliance_ha_mode', field)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            field['created'] = datetime.datetime.utcnow().isoformat()
            field['last_modified'] = field['created']
            self._create_prop(bch, obj_ids['uuid'], 'id_perms', field)

        field = obj_dict.get('display_name', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'display_name', field)


        # References

        bch.insert(obj_ids['uuid'], obj_cols)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(obj_dict['fq_name'])
        fq_name_cols = {utils.encode_string(fq_name_str) + ':' + obj_ids['uuid']: json.dumps(None)}
        self._obj_fq_name_cf.insert('service_appliance_set', fq_name_cols)

        return (True, '')
    #end _cassandra_service_appliance_set_create

    def _cassandra_service_appliance_set_read(self, obj_uuids, field_names = None):
        # if field_names = None, all fields will be read/returned

        obj_uuid_cf = self._obj_uuid_cf

        # optimize for common case of reading non-backref, non-children fields
        # ignoring columns starting from 'b' and 'c' - significant performance
        # impact in scaled setting. e.g. read of project
        if (field_names is None or
            (set(field_names) & (ServiceApplianceSet.backref_fields | ServiceApplianceSet.children_fields))):
            # atleast one backref/children field is needed
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_count = 10000000,
                                       include_timestamp = True)
        else: # ignore reading backref + children columns
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_start = 'd',
                                       column_count = 10000000,
                                       include_timestamp = True)

        if (len(obj_uuids) == 1) and not obj_rows:
            raise cfgm_common.exceptions.NoIdError(obj_uuids[0])

        results = []
        for row_key in obj_rows:
            # give chance for zk heartbeat/ping
            gevent.sleep(0)
            obj_uuid = row_key
            obj_cols = obj_rows[obj_uuid]
            result = {}
            result['uuid'] = obj_uuid
            result['fq_name'] = json.loads(obj_cols['fq_name'][0])
            for col_name in obj_cols.keys():
                if self._re_match_parent.match(col_name):
                    # non config-root child
                    (_, _, parent_uuid) = col_name.split(':')
                    parent_type = json.loads(obj_cols['parent_type'][0])
                    result['parent_type'] = parent_type
                    try:
                        result['parent_uuid'] = parent_uuid
                        result['parent_href'] = self._generate_url(parent_type, parent_uuid)
                    except cfgm_common.exceptions.NoIdError:
                        err_msg = 'Unknown uuid for parent ' + result['fq_name'][-2]
                        return (False, err_msg)

                # TODO use compiled RE
                if self._re_match_prop.match(col_name):
                    (_, prop_name) = col_name.split(':')
                    result[prop_name] = json.loads(obj_cols[col_name][0])

                # TODO use compiled RE
                if self._re_match_children.match(col_name):
                    (_, child_type, child_uuid) = col_name.split(':')
                    if field_names and '%ss' %(child_type) not in field_names:
                        continue

                    child_tstamp = obj_cols[col_name][1]
                    try:
                        self._read_child(result, obj_uuid, child_type, child_uuid, child_tstamp)
                    except cfgm_common.exceptions.NoIdError:
                        continue

                # TODO use compiled RE
                if self._re_match_ref.match(col_name):
                    (_, ref_type, ref_uuid) = col_name.split(':')
                    self._read_ref(result, obj_uuid, ref_type, ref_uuid, obj_cols[col_name][0])

                if self._re_match_backref.match(col_name):
                    (_, back_ref_type, back_ref_uuid) = col_name.split(':')
                    if field_names and '%s_back_refs' %(back_ref_type) not in field_names:
                        continue

                    try:
                        self._read_back_ref(result, obj_uuid, back_ref_type, back_ref_uuid,
                                            obj_cols[col_name][0])
                    except cfgm_common.exceptions.NoIdError:
                        continue

            # for all column names

            if 'service_appliances' in result:
                # sort children; TODO do this based on schema
                sorted_children = sorted(result['service_appliances'], key = itemgetter('tstamp'))
                # re-write result's children without timestamp
                result['service_appliances'] = sorted_children
                [child.pop('tstamp') for child in result['service_appliances']]

            results.append(result)
        # end for all rows

        return (True, results)
    #end _cassandra_service_appliance_set_read

    def _cassandra_service_appliance_set_count_children(self, obj_uuid, child_type):
        # if child_type = None, return
        if child_type is None:
            return (False, '')

        obj_uuid_cf = self._obj_uuid_cf
        if child_type not in ServiceApplianceSet.children_fields:
            return (False, '%s is not a valid children of ServiceApplianceSet' %(child_type))

        col_start = 'children:'+child_type[:-1]+':'
        col_finish = 'children:'+child_type[:-1]+';'
        num_children = obj_uuid_cf.get_count(obj_uuid,
                                   column_start = col_start,
                                   column_finish = col_finish,
                                   max_count = 10000000)
        return (True, num_children)
    #end _cassandra_service_appliance_set_count_children

    def _cassandra_service_appliance_set_update(self, obj_uuid, new_obj_dict):
        # Grab ref-uuids and properties in new version
        new_ref_infos = {}

        new_props = {}
        if 'service_appliance_set_properties' in new_obj_dict:
            new_props['service_appliance_set_properties'] = new_obj_dict['service_appliance_set_properties']
        if 'service_appliance_driver' in new_obj_dict:
            new_props['service_appliance_driver'] = new_obj_dict['service_appliance_driver']
        if 'service_appliance_ha_mode' in new_obj_dict:
            new_props['service_appliance_ha_mode'] = new_obj_dict['service_appliance_ha_mode']
        if 'id_perms' in new_obj_dict:
            new_props['id_perms'] = new_obj_dict['id_perms']
        if 'display_name' in new_obj_dict:
            new_props['display_name'] = new_obj_dict['display_name']
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        obj_uuid_cf = self._obj_uuid_cf
        obj_cols_iter = obj_uuid_cf.xget(obj_uuid)
        # TODO optimize this (converts tuple to dict)
        obj_cols = {}
        for col_info in obj_cols_iter:
            obj_cols[col_info[0]] = col_info[1]

        bch = obj_uuid_cf.batch()
        for col_name in obj_cols.keys():
            # TODO use compiled RE
            if re.match('prop:', col_name):
                (_, prop_name) = col_name.split(':')
                if prop_name == 'id_perms':
                    # id-perms always has to be updated for last-mod timestamp
                    # get it from request dict(or from db if not in request dict)
                    new_id_perms = new_obj_dict.get(prop_name, json.loads(obj_cols[col_name]))
                    self.update_last_modified(bch, obj_uuid, new_id_perms)
                elif prop_name in new_obj_dict:
                    self._update_prop(bch, obj_uuid, prop_name, new_props)

            # TODO use compiled RE
            if re.match('ref:', col_name):
                (_, ref_type, ref_uuid) = col_name.split(':')
                self._update_ref(bch, 'service_appliance_set', obj_uuid, ref_type, ref_uuid, new_ref_infos)
        # for all column names

        # create new refs
        for ref_type in new_ref_infos.keys():
            for ref_uuid in new_ref_infos[ref_type].keys():
                ref_data = new_ref_infos[ref_type][ref_uuid]
                self._create_ref(bch, 'service_appliance_set', obj_uuid, ref_type, ref_uuid, ref_data)

        # create new props
        for prop_name in new_props.keys():
            self._create_prop(bch, obj_uuid, prop_name, new_props[prop_name])

        bch.send()

        return (True, '')
    #end _cassandra_service_appliance_set_update

    def _cassandra_service_appliance_set_list(self, parent_uuids=None, back_ref_uuids=None,
                           obj_uuids=None, count=False, filters=None):
        children_fq_names_uuids = []
        if filters:
            fnames = filters.get('field_names', [])
            fvalues = filters.get('field_values', [])
            filter_fields = [(fnames[i], fvalues[i]) for i in range(len(fnames))]
        else:
            filter_fields = []

        def filter_rows(coll_infos, filter_cols, filter_params):
            filt_infos = {}
            coll_rows = obj_uuid_cf.multiget(coll_infos.keys(),
                                   columns=filter_cols,
                                   column_count=10000000)
            for row in coll_rows:
                # give chance for zk heartbeat/ping
                gevent.sleep(0)
                full_match = True
                for fname, fval in filter_params:
                    if coll_rows[row]['prop:%s' %(fname)] != fval:
                        full_match = False
                        break
                if full_match:
                    filt_infos[row] = coll_infos[row]
            return filt_infos
        # end filter_rows

        def get_fq_name_uuid_list(obj_uuids):
            ret_list = []
            for obj_uuid in obj_uuids:
                try:
                    obj_fq_name = self.uuid_to_fq_name(obj_uuid)
                    ret_list.append((obj_fq_name, obj_uuid))
                except cfgm_common.exceptions.NoIdError:
                    pass
            return ret_list
        # end get_fq_name_uuid_list

        if parent_uuids:
            # go from parent to child
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'children:service_appliance_set:'
            col_fin = 'children:service_appliance_set;'
            try:
                obj_rows = obj_uuid_cf.multiget(parent_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_parent_anchor(sort=False):
                # flatten to [('children:<type>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_child_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    child_uuid = col_name.split(':')[2]
                    if obj_uuids and child_uuid not in obj_uuids:
                        continue
                    all_child_infos[child_uuid] = {'uuid': child_uuid, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_child_infos = filter_rows(all_child_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_child_infos = all_child_infos

                if not sort:
                    ret_child_infos = filt_child_infos.values()
                else:
                    ret_child_infos = sorted(filt_child_infos.values(), key=itemgetter('tstamp'))

                return get_fq_name_uuid_list(r['uuid'] for r in ret_child_infos)
            # end filter_rows_parent_anchor

            if count:
                return (True, len(filter_rows_parent_anchor()))

            children_fq_names_uuids = filter_rows_parent_anchor(sort=True)

        if back_ref_uuids:
            # go from anchor to backrefs
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'backref:service_appliance_set:'
            col_fin = 'backref:service_appliance_set;'
            try:
                obj_rows = obj_uuid_cf.multiget(back_ref_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_backref_anchor():
                # flatten to [('<fqnstr>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_backref_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    col_name_arr = col_name.split(':')
                    fq_name = col_name_arr[:-1]
                    obj_uuid = col_name_arr[-1]
                    if obj_uuids and obj_uuid not in obj_uuids:
                        continue
                    all_backref_infos[obj_uuid] = \
                        {'uuid': obj_uuid, 'fq_name': fq_name, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_backref_infos = filter_rows(all_backref_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_backref_infos = all_backref_infos

                return [(br_info['fq_name'], br_info['uuid']) for br_info in filt_backref_infos.values()]
            # end filter_rows_backref_anchor

            if count:
                return (True, len(filter_rows_backref_anchor()))

            children_fq_names_uuids = filter_rows_backref_anchor()

        if not parent_uuids and not back_ref_uuids:
            obj_uuid_cf = self._obj_uuid_cf
            if obj_uuids:
                # exact objects specified
                def filter_rows_object_list():
                    all_obj_infos = {}
                    for obj_uuid in obj_uuids:
                        all_obj_infos[obj_uuid] = None

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return get_fq_name_uuid_list(filt_obj_infos.keys())
                # end filter_rows_object_list

                if count:
                    return (True, len(filter_rows_object_list()))
                children_fq_names_uuids = filter_rows_object_list()

            else: # grab all resources of this type
                obj_fq_name_cf = self._obj_fq_name_cf
                try:
                    cols = obj_fq_name_cf.get('service_appliance_set', column_count = 10000000)
                except pycassa.NotFoundException:
                    if count:
                        return (True, 0)
                    else:
                        return (True, children_fq_names_uuids)

                def filter_rows_no_anchor():
                    all_obj_infos = {}
                    for col_name, col_val in cols.items():
                        # give chance for zk heartbeat/ping
                        gevent.sleep(0)
                        col_name_arr = utils.decode_string(col_name).split(':')
                        obj_uuid = col_name_arr[-1]
                        all_obj_infos[obj_uuid] = (col_name_arr[:-1], obj_uuid)

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return filt_obj_infos.values()
                # end filter_rows_no_anchor

                if count:
                    return (True, len(filter_rows_no_anchor()))

                children_fq_names_uuids = filter_rows_no_anchor()

        return (True, children_fq_names_uuids)
    #end _cassandra_service_appliance_set_list

    def _cassandra_service_appliance_set_delete(self, obj_uuid):
        obj_uuid_cf = self._obj_uuid_cf
        fq_name = json.loads(obj_uuid_cf.get(obj_uuid, columns = ['fq_name'])['fq_name'])
        bch = obj_uuid_cf.batch()

        # unlink from parent
        col_start = 'parent:'
        col_fin = 'parent;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, parent_type, parent_uuid) = col_name.split(':')
            self._delete_child(bch, parent_type, parent_uuid, 'service_appliance_set', obj_uuid)

        # remove refs
        col_start = 'ref:'
        col_fin = 'ref;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, ref_type, ref_uuid) = col_name.split(':')
            self._delete_ref(bch, 'service_appliance_set', obj_uuid, ref_type, ref_uuid)

        bch.remove(obj_uuid)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(fq_name)
        fq_name_col = utils.encode_string(fq_name_str) + ':' + obj_uuid
        self._obj_fq_name_cf.remove('service_appliance_set', columns = [fq_name_col])


        return (True, '')
    #end _cassandra_service_appliance_set_delete

    def _cassandra_config_node_alloc(self, fq_name):
        return (True, '')
    #end _cassandra_config_node_alloc

    def _cassandra_config_node_create(self, obj_ids, obj_dict):
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        bch = self._obj_uuid_cf.batch()

        obj_cols = {}
        obj_cols['fq_name'] = json.dumps(obj_dict['fq_name'])
        obj_cols['type'] = json.dumps('config_node')
        if 'parent_type' in obj_dict:
            # non config-root child
            parent_type = obj_dict['parent_type']
            parent_method_type = parent_type.replace('-', '_')
            parent_fq_name = obj_dict['fq_name'][:-1]
            obj_cols['parent_type'] = json.dumps(parent_type)
            parent_uuid = self.fq_name_to_uuid(parent_method_type, parent_fq_name)
            self._create_child(bch, parent_method_type, parent_uuid, 'config_node', obj_ids['uuid'])

        # Properties
        field = obj_dict.get('config_node_ip_address', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'config_node_ip_address', field)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            field['created'] = datetime.datetime.utcnow().isoformat()
            field['last_modified'] = field['created']
            self._create_prop(bch, obj_ids['uuid'], 'id_perms', field)

        field = obj_dict.get('display_name', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'display_name', field)


        # References

        bch.insert(obj_ids['uuid'], obj_cols)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(obj_dict['fq_name'])
        fq_name_cols = {utils.encode_string(fq_name_str) + ':' + obj_ids['uuid']: json.dumps(None)}
        self._obj_fq_name_cf.insert('config_node', fq_name_cols)

        return (True, '')
    #end _cassandra_config_node_create

    def _cassandra_config_node_read(self, obj_uuids, field_names = None):
        # if field_names = None, all fields will be read/returned

        obj_uuid_cf = self._obj_uuid_cf

        # optimize for common case of reading non-backref, non-children fields
        # ignoring columns starting from 'b' and 'c' - significant performance
        # impact in scaled setting. e.g. read of project
        if (field_names is None or
            (set(field_names) & (ConfigNode.backref_fields | ConfigNode.children_fields))):
            # atleast one backref/children field is needed
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_count = 10000000,
                                       include_timestamp = True)
        else: # ignore reading backref + children columns
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_start = 'd',
                                       column_count = 10000000,
                                       include_timestamp = True)

        if (len(obj_uuids) == 1) and not obj_rows:
            raise cfgm_common.exceptions.NoIdError(obj_uuids[0])

        results = []
        for row_key in obj_rows:
            # give chance for zk heartbeat/ping
            gevent.sleep(0)
            obj_uuid = row_key
            obj_cols = obj_rows[obj_uuid]
            result = {}
            result['uuid'] = obj_uuid
            result['fq_name'] = json.loads(obj_cols['fq_name'][0])
            for col_name in obj_cols.keys():
                if self._re_match_parent.match(col_name):
                    # non config-root child
                    (_, _, parent_uuid) = col_name.split(':')
                    parent_type = json.loads(obj_cols['parent_type'][0])
                    result['parent_type'] = parent_type
                    try:
                        result['parent_uuid'] = parent_uuid
                        result['parent_href'] = self._generate_url(parent_type, parent_uuid)
                    except cfgm_common.exceptions.NoIdError:
                        err_msg = 'Unknown uuid for parent ' + result['fq_name'][-2]
                        return (False, err_msg)

                # TODO use compiled RE
                if self._re_match_prop.match(col_name):
                    (_, prop_name) = col_name.split(':')
                    result[prop_name] = json.loads(obj_cols[col_name][0])

                # TODO use compiled RE
                if self._re_match_children.match(col_name):
                    (_, child_type, child_uuid) = col_name.split(':')
                    if field_names and '%ss' %(child_type) not in field_names:
                        continue

                    child_tstamp = obj_cols[col_name][1]
                    try:
                        self._read_child(result, obj_uuid, child_type, child_uuid, child_tstamp)
                    except cfgm_common.exceptions.NoIdError:
                        continue

                # TODO use compiled RE
                if self._re_match_ref.match(col_name):
                    (_, ref_type, ref_uuid) = col_name.split(':')
                    self._read_ref(result, obj_uuid, ref_type, ref_uuid, obj_cols[col_name][0])

                if self._re_match_backref.match(col_name):
                    (_, back_ref_type, back_ref_uuid) = col_name.split(':')
                    if field_names and '%s_back_refs' %(back_ref_type) not in field_names:
                        continue

                    try:
                        self._read_back_ref(result, obj_uuid, back_ref_type, back_ref_uuid,
                                            obj_cols[col_name][0])
                    except cfgm_common.exceptions.NoIdError:
                        continue

            # for all column names


            results.append(result)
        # end for all rows

        return (True, results)
    #end _cassandra_config_node_read

    def _cassandra_config_node_count_children(self, obj_uuid, child_type):
        # if child_type = None, return
        if child_type is None:
            return (False, '')

        obj_uuid_cf = self._obj_uuid_cf
        if child_type not in ConfigNode.children_fields:
            return (False, '%s is not a valid children of ConfigNode' %(child_type))

        col_start = 'children:'+child_type[:-1]+':'
        col_finish = 'children:'+child_type[:-1]+';'
        num_children = obj_uuid_cf.get_count(obj_uuid,
                                   column_start = col_start,
                                   column_finish = col_finish,
                                   max_count = 10000000)
        return (True, num_children)
    #end _cassandra_config_node_count_children

    def _cassandra_config_node_update(self, obj_uuid, new_obj_dict):
        # Grab ref-uuids and properties in new version
        new_ref_infos = {}

        new_props = {}
        if 'config_node_ip_address' in new_obj_dict:
            new_props['config_node_ip_address'] = new_obj_dict['config_node_ip_address']
        if 'id_perms' in new_obj_dict:
            new_props['id_perms'] = new_obj_dict['id_perms']
        if 'display_name' in new_obj_dict:
            new_props['display_name'] = new_obj_dict['display_name']
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        obj_uuid_cf = self._obj_uuid_cf
        obj_cols_iter = obj_uuid_cf.xget(obj_uuid)
        # TODO optimize this (converts tuple to dict)
        obj_cols = {}
        for col_info in obj_cols_iter:
            obj_cols[col_info[0]] = col_info[1]

        bch = obj_uuid_cf.batch()
        for col_name in obj_cols.keys():
            # TODO use compiled RE
            if re.match('prop:', col_name):
                (_, prop_name) = col_name.split(':')
                if prop_name == 'id_perms':
                    # id-perms always has to be updated for last-mod timestamp
                    # get it from request dict(or from db if not in request dict)
                    new_id_perms = new_obj_dict.get(prop_name, json.loads(obj_cols[col_name]))
                    self.update_last_modified(bch, obj_uuid, new_id_perms)
                elif prop_name in new_obj_dict:
                    self._update_prop(bch, obj_uuid, prop_name, new_props)

            # TODO use compiled RE
            if re.match('ref:', col_name):
                (_, ref_type, ref_uuid) = col_name.split(':')
                self._update_ref(bch, 'config_node', obj_uuid, ref_type, ref_uuid, new_ref_infos)
        # for all column names

        # create new refs
        for ref_type in new_ref_infos.keys():
            for ref_uuid in new_ref_infos[ref_type].keys():
                ref_data = new_ref_infos[ref_type][ref_uuid]
                self._create_ref(bch, 'config_node', obj_uuid, ref_type, ref_uuid, ref_data)

        # create new props
        for prop_name in new_props.keys():
            self._create_prop(bch, obj_uuid, prop_name, new_props[prop_name])

        bch.send()

        return (True, '')
    #end _cassandra_config_node_update

    def _cassandra_config_node_list(self, parent_uuids=None, back_ref_uuids=None,
                           obj_uuids=None, count=False, filters=None):
        children_fq_names_uuids = []
        if filters:
            fnames = filters.get('field_names', [])
            fvalues = filters.get('field_values', [])
            filter_fields = [(fnames[i], fvalues[i]) for i in range(len(fnames))]
        else:
            filter_fields = []

        def filter_rows(coll_infos, filter_cols, filter_params):
            filt_infos = {}
            coll_rows = obj_uuid_cf.multiget(coll_infos.keys(),
                                   columns=filter_cols,
                                   column_count=10000000)
            for row in coll_rows:
                # give chance for zk heartbeat/ping
                gevent.sleep(0)
                full_match = True
                for fname, fval in filter_params:
                    if coll_rows[row]['prop:%s' %(fname)] != fval:
                        full_match = False
                        break
                if full_match:
                    filt_infos[row] = coll_infos[row]
            return filt_infos
        # end filter_rows

        def get_fq_name_uuid_list(obj_uuids):
            ret_list = []
            for obj_uuid in obj_uuids:
                try:
                    obj_fq_name = self.uuid_to_fq_name(obj_uuid)
                    ret_list.append((obj_fq_name, obj_uuid))
                except cfgm_common.exceptions.NoIdError:
                    pass
            return ret_list
        # end get_fq_name_uuid_list

        if parent_uuids:
            # go from parent to child
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'children:config_node:'
            col_fin = 'children:config_node;'
            try:
                obj_rows = obj_uuid_cf.multiget(parent_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_parent_anchor(sort=False):
                # flatten to [('children:<type>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_child_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    child_uuid = col_name.split(':')[2]
                    if obj_uuids and child_uuid not in obj_uuids:
                        continue
                    all_child_infos[child_uuid] = {'uuid': child_uuid, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_child_infos = filter_rows(all_child_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_child_infos = all_child_infos

                if not sort:
                    ret_child_infos = filt_child_infos.values()
                else:
                    ret_child_infos = sorted(filt_child_infos.values(), key=itemgetter('tstamp'))

                return get_fq_name_uuid_list(r['uuid'] for r in ret_child_infos)
            # end filter_rows_parent_anchor

            if count:
                return (True, len(filter_rows_parent_anchor()))

            children_fq_names_uuids = filter_rows_parent_anchor(sort=True)

        if back_ref_uuids:
            # go from anchor to backrefs
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'backref:config_node:'
            col_fin = 'backref:config_node;'
            try:
                obj_rows = obj_uuid_cf.multiget(back_ref_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_backref_anchor():
                # flatten to [('<fqnstr>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_backref_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    col_name_arr = col_name.split(':')
                    fq_name = col_name_arr[:-1]
                    obj_uuid = col_name_arr[-1]
                    if obj_uuids and obj_uuid not in obj_uuids:
                        continue
                    all_backref_infos[obj_uuid] = \
                        {'uuid': obj_uuid, 'fq_name': fq_name, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_backref_infos = filter_rows(all_backref_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_backref_infos = all_backref_infos

                return [(br_info['fq_name'], br_info['uuid']) for br_info in filt_backref_infos.values()]
            # end filter_rows_backref_anchor

            if count:
                return (True, len(filter_rows_backref_anchor()))

            children_fq_names_uuids = filter_rows_backref_anchor()

        if not parent_uuids and not back_ref_uuids:
            obj_uuid_cf = self._obj_uuid_cf
            if obj_uuids:
                # exact objects specified
                def filter_rows_object_list():
                    all_obj_infos = {}
                    for obj_uuid in obj_uuids:
                        all_obj_infos[obj_uuid] = None

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return get_fq_name_uuid_list(filt_obj_infos.keys())
                # end filter_rows_object_list

                if count:
                    return (True, len(filter_rows_object_list()))
                children_fq_names_uuids = filter_rows_object_list()

            else: # grab all resources of this type
                obj_fq_name_cf = self._obj_fq_name_cf
                try:
                    cols = obj_fq_name_cf.get('config_node', column_count = 10000000)
                except pycassa.NotFoundException:
                    if count:
                        return (True, 0)
                    else:
                        return (True, children_fq_names_uuids)

                def filter_rows_no_anchor():
                    all_obj_infos = {}
                    for col_name, col_val in cols.items():
                        # give chance for zk heartbeat/ping
                        gevent.sleep(0)
                        col_name_arr = utils.decode_string(col_name).split(':')
                        obj_uuid = col_name_arr[-1]
                        all_obj_infos[obj_uuid] = (col_name_arr[:-1], obj_uuid)

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return filt_obj_infos.values()
                # end filter_rows_no_anchor

                if count:
                    return (True, len(filter_rows_no_anchor()))

                children_fq_names_uuids = filter_rows_no_anchor()

        return (True, children_fq_names_uuids)
    #end _cassandra_config_node_list

    def _cassandra_config_node_delete(self, obj_uuid):
        obj_uuid_cf = self._obj_uuid_cf
        fq_name = json.loads(obj_uuid_cf.get(obj_uuid, columns = ['fq_name'])['fq_name'])
        bch = obj_uuid_cf.batch()

        # unlink from parent
        col_start = 'parent:'
        col_fin = 'parent;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, parent_type, parent_uuid) = col_name.split(':')
            self._delete_child(bch, parent_type, parent_uuid, 'config_node', obj_uuid)

        # remove refs
        col_start = 'ref:'
        col_fin = 'ref;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, ref_type, ref_uuid) = col_name.split(':')
            self._delete_ref(bch, 'config_node', obj_uuid, ref_type, ref_uuid)

        bch.remove(obj_uuid)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(fq_name)
        fq_name_col = utils.encode_string(fq_name_str) + ':' + obj_uuid
        self._obj_fq_name_cf.remove('config_node', columns = [fq_name_col])


        return (True, '')
    #end _cassandra_config_node_delete

    def _cassandra_qos_queue_alloc(self, fq_name):
        return (True, '')
    #end _cassandra_qos_queue_alloc

    def _cassandra_qos_queue_create(self, obj_ids, obj_dict):
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        bch = self._obj_uuid_cf.batch()

        obj_cols = {}
        obj_cols['fq_name'] = json.dumps(obj_dict['fq_name'])
        obj_cols['type'] = json.dumps('qos_queue')
        if 'parent_type' in obj_dict:
            # non config-root child
            parent_type = obj_dict['parent_type']
            parent_method_type = parent_type.replace('-', '_')
            parent_fq_name = obj_dict['fq_name'][:-1]
            obj_cols['parent_type'] = json.dumps(parent_type)
            parent_uuid = self.fq_name_to_uuid(parent_method_type, parent_fq_name)
            self._create_child(bch, parent_method_type, parent_uuid, 'qos_queue', obj_ids['uuid'])

        # Properties
        field = obj_dict.get('min_bandwidth', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'min_bandwidth', field)

        field = obj_dict.get('max_bandwidth', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'max_bandwidth', field)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            field['created'] = datetime.datetime.utcnow().isoformat()
            field['last_modified'] = field['created']
            self._create_prop(bch, obj_ids['uuid'], 'id_perms', field)

        field = obj_dict.get('display_name', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'display_name', field)


        # References

        bch.insert(obj_ids['uuid'], obj_cols)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(obj_dict['fq_name'])
        fq_name_cols = {utils.encode_string(fq_name_str) + ':' + obj_ids['uuid']: json.dumps(None)}
        self._obj_fq_name_cf.insert('qos_queue', fq_name_cols)

        return (True, '')
    #end _cassandra_qos_queue_create

    def _cassandra_qos_queue_read(self, obj_uuids, field_names = None):
        # if field_names = None, all fields will be read/returned

        obj_uuid_cf = self._obj_uuid_cf

        # optimize for common case of reading non-backref, non-children fields
        # ignoring columns starting from 'b' and 'c' - significant performance
        # impact in scaled setting. e.g. read of project
        if (field_names is None or
            (set(field_names) & (QosQueue.backref_fields | QosQueue.children_fields))):
            # atleast one backref/children field is needed
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_count = 10000000,
                                       include_timestamp = True)
        else: # ignore reading backref + children columns
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_start = 'd',
                                       column_count = 10000000,
                                       include_timestamp = True)

        if (len(obj_uuids) == 1) and not obj_rows:
            raise cfgm_common.exceptions.NoIdError(obj_uuids[0])

        results = []
        for row_key in obj_rows:
            # give chance for zk heartbeat/ping
            gevent.sleep(0)
            obj_uuid = row_key
            obj_cols = obj_rows[obj_uuid]
            result = {}
            result['uuid'] = obj_uuid
            result['fq_name'] = json.loads(obj_cols['fq_name'][0])
            for col_name in obj_cols.keys():
                if self._re_match_parent.match(col_name):
                    # non config-root child
                    (_, _, parent_uuid) = col_name.split(':')
                    parent_type = json.loads(obj_cols['parent_type'][0])
                    result['parent_type'] = parent_type
                    try:
                        result['parent_uuid'] = parent_uuid
                        result['parent_href'] = self._generate_url(parent_type, parent_uuid)
                    except cfgm_common.exceptions.NoIdError:
                        err_msg = 'Unknown uuid for parent ' + result['fq_name'][-2]
                        return (False, err_msg)

                # TODO use compiled RE
                if self._re_match_prop.match(col_name):
                    (_, prop_name) = col_name.split(':')
                    result[prop_name] = json.loads(obj_cols[col_name][0])

                # TODO use compiled RE
                if self._re_match_children.match(col_name):
                    (_, child_type, child_uuid) = col_name.split(':')
                    if field_names and '%ss' %(child_type) not in field_names:
                        continue

                    child_tstamp = obj_cols[col_name][1]
                    try:
                        self._read_child(result, obj_uuid, child_type, child_uuid, child_tstamp)
                    except cfgm_common.exceptions.NoIdError:
                        continue

                # TODO use compiled RE
                if self._re_match_ref.match(col_name):
                    (_, ref_type, ref_uuid) = col_name.split(':')
                    self._read_ref(result, obj_uuid, ref_type, ref_uuid, obj_cols[col_name][0])

                if self._re_match_backref.match(col_name):
                    (_, back_ref_type, back_ref_uuid) = col_name.split(':')
                    if field_names and '%s_back_refs' %(back_ref_type) not in field_names:
                        continue

                    try:
                        self._read_back_ref(result, obj_uuid, back_ref_type, back_ref_uuid,
                                            obj_cols[col_name][0])
                    except cfgm_common.exceptions.NoIdError:
                        continue

            # for all column names


            results.append(result)
        # end for all rows

        return (True, results)
    #end _cassandra_qos_queue_read

    def _cassandra_qos_queue_count_children(self, obj_uuid, child_type):
        # if child_type = None, return
        if child_type is None:
            return (False, '')

        obj_uuid_cf = self._obj_uuid_cf
        if child_type not in QosQueue.children_fields:
            return (False, '%s is not a valid children of QosQueue' %(child_type))

        col_start = 'children:'+child_type[:-1]+':'
        col_finish = 'children:'+child_type[:-1]+';'
        num_children = obj_uuid_cf.get_count(obj_uuid,
                                   column_start = col_start,
                                   column_finish = col_finish,
                                   max_count = 10000000)
        return (True, num_children)
    #end _cassandra_qos_queue_count_children

    def _cassandra_qos_queue_update(self, obj_uuid, new_obj_dict):
        # Grab ref-uuids and properties in new version
        new_ref_infos = {}

        new_props = {}
        if 'min_bandwidth' in new_obj_dict:
            new_props['min_bandwidth'] = new_obj_dict['min_bandwidth']
        if 'max_bandwidth' in new_obj_dict:
            new_props['max_bandwidth'] = new_obj_dict['max_bandwidth']
        if 'id_perms' in new_obj_dict:
            new_props['id_perms'] = new_obj_dict['id_perms']
        if 'display_name' in new_obj_dict:
            new_props['display_name'] = new_obj_dict['display_name']
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        obj_uuid_cf = self._obj_uuid_cf
        obj_cols_iter = obj_uuid_cf.xget(obj_uuid)
        # TODO optimize this (converts tuple to dict)
        obj_cols = {}
        for col_info in obj_cols_iter:
            obj_cols[col_info[0]] = col_info[1]

        bch = obj_uuid_cf.batch()
        for col_name in obj_cols.keys():
            # TODO use compiled RE
            if re.match('prop:', col_name):
                (_, prop_name) = col_name.split(':')
                if prop_name == 'id_perms':
                    # id-perms always has to be updated for last-mod timestamp
                    # get it from request dict(or from db if not in request dict)
                    new_id_perms = new_obj_dict.get(prop_name, json.loads(obj_cols[col_name]))
                    self.update_last_modified(bch, obj_uuid, new_id_perms)
                elif prop_name in new_obj_dict:
                    self._update_prop(bch, obj_uuid, prop_name, new_props)

            # TODO use compiled RE
            if re.match('ref:', col_name):
                (_, ref_type, ref_uuid) = col_name.split(':')
                self._update_ref(bch, 'qos_queue', obj_uuid, ref_type, ref_uuid, new_ref_infos)
        # for all column names

        # create new refs
        for ref_type in new_ref_infos.keys():
            for ref_uuid in new_ref_infos[ref_type].keys():
                ref_data = new_ref_infos[ref_type][ref_uuid]
                self._create_ref(bch, 'qos_queue', obj_uuid, ref_type, ref_uuid, ref_data)

        # create new props
        for prop_name in new_props.keys():
            self._create_prop(bch, obj_uuid, prop_name, new_props[prop_name])

        bch.send()

        return (True, '')
    #end _cassandra_qos_queue_update

    def _cassandra_qos_queue_list(self, parent_uuids=None, back_ref_uuids=None,
                           obj_uuids=None, count=False, filters=None):
        children_fq_names_uuids = []
        if filters:
            fnames = filters.get('field_names', [])
            fvalues = filters.get('field_values', [])
            filter_fields = [(fnames[i], fvalues[i]) for i in range(len(fnames))]
        else:
            filter_fields = []

        def filter_rows(coll_infos, filter_cols, filter_params):
            filt_infos = {}
            coll_rows = obj_uuid_cf.multiget(coll_infos.keys(),
                                   columns=filter_cols,
                                   column_count=10000000)
            for row in coll_rows:
                # give chance for zk heartbeat/ping
                gevent.sleep(0)
                full_match = True
                for fname, fval in filter_params:
                    if coll_rows[row]['prop:%s' %(fname)] != fval:
                        full_match = False
                        break
                if full_match:
                    filt_infos[row] = coll_infos[row]
            return filt_infos
        # end filter_rows

        def get_fq_name_uuid_list(obj_uuids):
            ret_list = []
            for obj_uuid in obj_uuids:
                try:
                    obj_fq_name = self.uuid_to_fq_name(obj_uuid)
                    ret_list.append((obj_fq_name, obj_uuid))
                except cfgm_common.exceptions.NoIdError:
                    pass
            return ret_list
        # end get_fq_name_uuid_list

        if parent_uuids:
            # go from parent to child
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'children:qos_queue:'
            col_fin = 'children:qos_queue;'
            try:
                obj_rows = obj_uuid_cf.multiget(parent_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_parent_anchor(sort=False):
                # flatten to [('children:<type>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_child_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    child_uuid = col_name.split(':')[2]
                    if obj_uuids and child_uuid not in obj_uuids:
                        continue
                    all_child_infos[child_uuid] = {'uuid': child_uuid, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_child_infos = filter_rows(all_child_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_child_infos = all_child_infos

                if not sort:
                    ret_child_infos = filt_child_infos.values()
                else:
                    ret_child_infos = sorted(filt_child_infos.values(), key=itemgetter('tstamp'))

                return get_fq_name_uuid_list(r['uuid'] for r in ret_child_infos)
            # end filter_rows_parent_anchor

            if count:
                return (True, len(filter_rows_parent_anchor()))

            children_fq_names_uuids = filter_rows_parent_anchor(sort=True)

        if back_ref_uuids:
            # go from anchor to backrefs
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'backref:qos_queue:'
            col_fin = 'backref:qos_queue;'
            try:
                obj_rows = obj_uuid_cf.multiget(back_ref_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_backref_anchor():
                # flatten to [('<fqnstr>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_backref_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    col_name_arr = col_name.split(':')
                    fq_name = col_name_arr[:-1]
                    obj_uuid = col_name_arr[-1]
                    if obj_uuids and obj_uuid not in obj_uuids:
                        continue
                    all_backref_infos[obj_uuid] = \
                        {'uuid': obj_uuid, 'fq_name': fq_name, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_backref_infos = filter_rows(all_backref_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_backref_infos = all_backref_infos

                return [(br_info['fq_name'], br_info['uuid']) for br_info in filt_backref_infos.values()]
            # end filter_rows_backref_anchor

            if count:
                return (True, len(filter_rows_backref_anchor()))

            children_fq_names_uuids = filter_rows_backref_anchor()

        if not parent_uuids and not back_ref_uuids:
            obj_uuid_cf = self._obj_uuid_cf
            if obj_uuids:
                # exact objects specified
                def filter_rows_object_list():
                    all_obj_infos = {}
                    for obj_uuid in obj_uuids:
                        all_obj_infos[obj_uuid] = None

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return get_fq_name_uuid_list(filt_obj_infos.keys())
                # end filter_rows_object_list

                if count:
                    return (True, len(filter_rows_object_list()))
                children_fq_names_uuids = filter_rows_object_list()

            else: # grab all resources of this type
                obj_fq_name_cf = self._obj_fq_name_cf
                try:
                    cols = obj_fq_name_cf.get('qos_queue', column_count = 10000000)
                except pycassa.NotFoundException:
                    if count:
                        return (True, 0)
                    else:
                        return (True, children_fq_names_uuids)

                def filter_rows_no_anchor():
                    all_obj_infos = {}
                    for col_name, col_val in cols.items():
                        # give chance for zk heartbeat/ping
                        gevent.sleep(0)
                        col_name_arr = utils.decode_string(col_name).split(':')
                        obj_uuid = col_name_arr[-1]
                        all_obj_infos[obj_uuid] = (col_name_arr[:-1], obj_uuid)

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return filt_obj_infos.values()
                # end filter_rows_no_anchor

                if count:
                    return (True, len(filter_rows_no_anchor()))

                children_fq_names_uuids = filter_rows_no_anchor()

        return (True, children_fq_names_uuids)
    #end _cassandra_qos_queue_list

    def _cassandra_qos_queue_delete(self, obj_uuid):
        obj_uuid_cf = self._obj_uuid_cf
        fq_name = json.loads(obj_uuid_cf.get(obj_uuid, columns = ['fq_name'])['fq_name'])
        bch = obj_uuid_cf.batch()

        # unlink from parent
        col_start = 'parent:'
        col_fin = 'parent;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, parent_type, parent_uuid) = col_name.split(':')
            self._delete_child(bch, parent_type, parent_uuid, 'qos_queue', obj_uuid)

        # remove refs
        col_start = 'ref:'
        col_fin = 'ref;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, ref_type, ref_uuid) = col_name.split(':')
            self._delete_ref(bch, 'qos_queue', obj_uuid, ref_type, ref_uuid)

        bch.remove(obj_uuid)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(fq_name)
        fq_name_col = utils.encode_string(fq_name_str) + ':' + obj_uuid
        self._obj_fq_name_cf.remove('qos_queue', columns = [fq_name_col])


        return (True, '')
    #end _cassandra_qos_queue_delete

    def _cassandra_virtual_machine_alloc(self, fq_name):
        return (True, '')
    #end _cassandra_virtual_machine_alloc

    def _cassandra_virtual_machine_create(self, obj_ids, obj_dict):
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        bch = self._obj_uuid_cf.batch()

        obj_cols = {}
        obj_cols['fq_name'] = json.dumps(obj_dict['fq_name'])
        obj_cols['type'] = json.dumps('virtual_machine')

        # Properties
        field = obj_dict.get('id_perms', None)
        if field is not None:
            field['created'] = datetime.datetime.utcnow().isoformat()
            field['last_modified'] = field['created']
            self._create_prop(bch, obj_ids['uuid'], 'id_perms', field)

        field = obj_dict.get('display_name', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'display_name', field)


        # References
        refs = obj_dict.get('service_instance_refs', [])
        for ref in refs:
            ref_uuid = self.fq_name_to_uuid('service_instance', ref['to'])
            ref_attr = ref.get('attr', None)
            ref_data = {'attr': ref_attr, 'is_weakref': True}
            self._create_ref(bch, 'virtual_machine', obj_ids['uuid'], 'service_instance', ref_uuid, ref_data)

        bch.insert(obj_ids['uuid'], obj_cols)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(obj_dict['fq_name'])
        fq_name_cols = {utils.encode_string(fq_name_str) + ':' + obj_ids['uuid']: json.dumps(None)}
        self._obj_fq_name_cf.insert('virtual_machine', fq_name_cols)

        return (True, '')
    #end _cassandra_virtual_machine_create

    def _cassandra_virtual_machine_read(self, obj_uuids, field_names = None):
        # if field_names = None, all fields will be read/returned

        obj_uuid_cf = self._obj_uuid_cf

        # optimize for common case of reading non-backref, non-children fields
        # ignoring columns starting from 'b' and 'c' - significant performance
        # impact in scaled setting. e.g. read of project
        if (field_names is None or
            (set(field_names) & (VirtualMachine.backref_fields | VirtualMachine.children_fields))):
            # atleast one backref/children field is needed
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_count = 10000000,
                                       include_timestamp = True)
        else: # ignore reading backref + children columns
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_start = 'd',
                                       column_count = 10000000,
                                       include_timestamp = True)

        if (len(obj_uuids) == 1) and not obj_rows:
            raise cfgm_common.exceptions.NoIdError(obj_uuids[0])

        results = []
        for row_key in obj_rows:
            # give chance for zk heartbeat/ping
            gevent.sleep(0)
            obj_uuid = row_key
            obj_cols = obj_rows[obj_uuid]
            result = {}
            result['uuid'] = obj_uuid
            result['fq_name'] = json.loads(obj_cols['fq_name'][0])
            for col_name in obj_cols.keys():
                if self._re_match_parent.match(col_name):
                    # non config-root child
                    (_, _, parent_uuid) = col_name.split(':')
                    parent_type = json.loads(obj_cols['parent_type'][0])
                    result['parent_type'] = parent_type
                    try:
                        result['parent_uuid'] = parent_uuid
                        result['parent_href'] = self._generate_url(parent_type, parent_uuid)
                    except cfgm_common.exceptions.NoIdError:
                        err_msg = 'Unknown uuid for parent ' + result['fq_name'][-2]
                        return (False, err_msg)

                # TODO use compiled RE
                if self._re_match_prop.match(col_name):
                    (_, prop_name) = col_name.split(':')
                    result[prop_name] = json.loads(obj_cols[col_name][0])

                # TODO use compiled RE
                if self._re_match_children.match(col_name):
                    (_, child_type, child_uuid) = col_name.split(':')
                    if field_names and '%ss' %(child_type) not in field_names:
                        continue

                    child_tstamp = obj_cols[col_name][1]
                    try:
                        self._read_child(result, obj_uuid, child_type, child_uuid, child_tstamp)
                    except cfgm_common.exceptions.NoIdError:
                        continue

                # TODO use compiled RE
                if self._re_match_ref.match(col_name):
                    (_, ref_type, ref_uuid) = col_name.split(':')
                    self._read_ref(result, obj_uuid, ref_type, ref_uuid, obj_cols[col_name][0])

                if self._re_match_backref.match(col_name):
                    (_, back_ref_type, back_ref_uuid) = col_name.split(':')
                    if field_names and '%s_back_refs' %(back_ref_type) not in field_names:
                        continue

                    try:
                        self._read_back_ref(result, obj_uuid, back_ref_type, back_ref_uuid,
                                            obj_cols[col_name][0])
                    except cfgm_common.exceptions.NoIdError:
                        continue

            # for all column names

            if 'virtual_machine_interfaces' in result:
                # sort children; TODO do this based on schema
                sorted_children = sorted(result['virtual_machine_interfaces'], key = itemgetter('tstamp'))
                # re-write result's children without timestamp
                result['virtual_machine_interfaces'] = sorted_children
                [child.pop('tstamp') for child in result['virtual_machine_interfaces']]

            results.append(result)
        # end for all rows

        return (True, results)
    #end _cassandra_virtual_machine_read

    def _cassandra_virtual_machine_count_children(self, obj_uuid, child_type):
        # if child_type = None, return
        if child_type is None:
            return (False, '')

        obj_uuid_cf = self._obj_uuid_cf
        if child_type not in VirtualMachine.children_fields:
            return (False, '%s is not a valid children of VirtualMachine' %(child_type))

        col_start = 'children:'+child_type[:-1]+':'
        col_finish = 'children:'+child_type[:-1]+';'
        num_children = obj_uuid_cf.get_count(obj_uuid,
                                   column_start = col_start,
                                   column_finish = col_finish,
                                   max_count = 10000000)
        return (True, num_children)
    #end _cassandra_virtual_machine_count_children

    def _cassandra_virtual_machine_update(self, obj_uuid, new_obj_dict):
        # Grab ref-uuids and properties in new version
        new_ref_infos = {}

        if 'service_instance_refs' in new_obj_dict:
            new_ref_infos['service_instance'] = {}
            new_refs = new_obj_dict['service_instance_refs']
            if new_refs:
                for new_ref in new_refs:
                    new_ref_uuid = self.fq_name_to_uuid('service_instance', new_ref['to'])
                    new_ref_attr = new_ref.get('attr', None)
                    new_ref_data = {'attr': new_ref_attr, 'is_weakref': True}
                    new_ref_infos['service_instance'][new_ref_uuid] = new_ref_data

        new_props = {}
        if 'id_perms' in new_obj_dict:
            new_props['id_perms'] = new_obj_dict['id_perms']
        if 'display_name' in new_obj_dict:
            new_props['display_name'] = new_obj_dict['display_name']
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        obj_uuid_cf = self._obj_uuid_cf
        obj_cols_iter = obj_uuid_cf.xget(obj_uuid)
        # TODO optimize this (converts tuple to dict)
        obj_cols = {}
        for col_info in obj_cols_iter:
            obj_cols[col_info[0]] = col_info[1]

        bch = obj_uuid_cf.batch()
        for col_name in obj_cols.keys():
            # TODO use compiled RE
            if re.match('prop:', col_name):
                (_, prop_name) = col_name.split(':')
                if prop_name == 'id_perms':
                    # id-perms always has to be updated for last-mod timestamp
                    # get it from request dict(or from db if not in request dict)
                    new_id_perms = new_obj_dict.get(prop_name, json.loads(obj_cols[col_name]))
                    self.update_last_modified(bch, obj_uuid, new_id_perms)
                elif prop_name in new_obj_dict:
                    self._update_prop(bch, obj_uuid, prop_name, new_props)

            # TODO use compiled RE
            if re.match('ref:', col_name):
                (_, ref_type, ref_uuid) = col_name.split(':')
                self._update_ref(bch, 'virtual_machine', obj_uuid, ref_type, ref_uuid, new_ref_infos)
        # for all column names

        # create new refs
        for ref_type in new_ref_infos.keys():
            for ref_uuid in new_ref_infos[ref_type].keys():
                ref_data = new_ref_infos[ref_type][ref_uuid]
                self._create_ref(bch, 'virtual_machine', obj_uuid, ref_type, ref_uuid, ref_data)

        # create new props
        for prop_name in new_props.keys():
            self._create_prop(bch, obj_uuid, prop_name, new_props[prop_name])

        bch.send()

        return (True, '')
    #end _cassandra_virtual_machine_update

    def _cassandra_virtual_machine_list(self, parent_uuids=None, back_ref_uuids=None,
                           obj_uuids=None, count=False, filters=None):
        children_fq_names_uuids = []
        if filters:
            fnames = filters.get('field_names', [])
            fvalues = filters.get('field_values', [])
            filter_fields = [(fnames[i], fvalues[i]) for i in range(len(fnames))]
        else:
            filter_fields = []

        def filter_rows(coll_infos, filter_cols, filter_params):
            filt_infos = {}
            coll_rows = obj_uuid_cf.multiget(coll_infos.keys(),
                                   columns=filter_cols,
                                   column_count=10000000)
            for row in coll_rows:
                # give chance for zk heartbeat/ping
                gevent.sleep(0)
                full_match = True
                for fname, fval in filter_params:
                    if coll_rows[row]['prop:%s' %(fname)] != fval:
                        full_match = False
                        break
                if full_match:
                    filt_infos[row] = coll_infos[row]
            return filt_infos
        # end filter_rows

        def get_fq_name_uuid_list(obj_uuids):
            ret_list = []
            for obj_uuid in obj_uuids:
                try:
                    obj_fq_name = self.uuid_to_fq_name(obj_uuid)
                    ret_list.append((obj_fq_name, obj_uuid))
                except cfgm_common.exceptions.NoIdError:
                    pass
            return ret_list
        # end get_fq_name_uuid_list

        if parent_uuids:
            # go from parent to child
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'children:virtual_machine:'
            col_fin = 'children:virtual_machine;'
            try:
                obj_rows = obj_uuid_cf.multiget(parent_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_parent_anchor(sort=False):
                # flatten to [('children:<type>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_child_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    child_uuid = col_name.split(':')[2]
                    if obj_uuids and child_uuid not in obj_uuids:
                        continue
                    all_child_infos[child_uuid] = {'uuid': child_uuid, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_child_infos = filter_rows(all_child_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_child_infos = all_child_infos

                if not sort:
                    ret_child_infos = filt_child_infos.values()
                else:
                    ret_child_infos = sorted(filt_child_infos.values(), key=itemgetter('tstamp'))

                return get_fq_name_uuid_list(r['uuid'] for r in ret_child_infos)
            # end filter_rows_parent_anchor

            if count:
                return (True, len(filter_rows_parent_anchor()))

            children_fq_names_uuids = filter_rows_parent_anchor(sort=True)

        if back_ref_uuids:
            # go from anchor to backrefs
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'backref:virtual_machine:'
            col_fin = 'backref:virtual_machine;'
            try:
                obj_rows = obj_uuid_cf.multiget(back_ref_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_backref_anchor():
                # flatten to [('<fqnstr>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_backref_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    col_name_arr = col_name.split(':')
                    fq_name = col_name_arr[:-1]
                    obj_uuid = col_name_arr[-1]
                    if obj_uuids and obj_uuid not in obj_uuids:
                        continue
                    all_backref_infos[obj_uuid] = \
                        {'uuid': obj_uuid, 'fq_name': fq_name, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_backref_infos = filter_rows(all_backref_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_backref_infos = all_backref_infos

                return [(br_info['fq_name'], br_info['uuid']) for br_info in filt_backref_infos.values()]
            # end filter_rows_backref_anchor

            if count:
                return (True, len(filter_rows_backref_anchor()))

            children_fq_names_uuids = filter_rows_backref_anchor()

        if not parent_uuids and not back_ref_uuids:
            obj_uuid_cf = self._obj_uuid_cf
            if obj_uuids:
                # exact objects specified
                def filter_rows_object_list():
                    all_obj_infos = {}
                    for obj_uuid in obj_uuids:
                        all_obj_infos[obj_uuid] = None

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return get_fq_name_uuid_list(filt_obj_infos.keys())
                # end filter_rows_object_list

                if count:
                    return (True, len(filter_rows_object_list()))
                children_fq_names_uuids = filter_rows_object_list()

            else: # grab all resources of this type
                obj_fq_name_cf = self._obj_fq_name_cf
                try:
                    cols = obj_fq_name_cf.get('virtual_machine', column_count = 10000000)
                except pycassa.NotFoundException:
                    if count:
                        return (True, 0)
                    else:
                        return (True, children_fq_names_uuids)

                def filter_rows_no_anchor():
                    all_obj_infos = {}
                    for col_name, col_val in cols.items():
                        # give chance for zk heartbeat/ping
                        gevent.sleep(0)
                        col_name_arr = utils.decode_string(col_name).split(':')
                        obj_uuid = col_name_arr[-1]
                        all_obj_infos[obj_uuid] = (col_name_arr[:-1], obj_uuid)

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return filt_obj_infos.values()
                # end filter_rows_no_anchor

                if count:
                    return (True, len(filter_rows_no_anchor()))

                children_fq_names_uuids = filter_rows_no_anchor()

        return (True, children_fq_names_uuids)
    #end _cassandra_virtual_machine_list

    def _cassandra_virtual_machine_delete(self, obj_uuid):
        obj_uuid_cf = self._obj_uuid_cf
        fq_name = json.loads(obj_uuid_cf.get(obj_uuid, columns = ['fq_name'])['fq_name'])
        bch = obj_uuid_cf.batch()

        # unlink from parent
        col_start = 'parent:'
        col_fin = 'parent;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, parent_type, parent_uuid) = col_name.split(':')
            self._delete_child(bch, parent_type, parent_uuid, 'virtual_machine', obj_uuid)

        # remove refs
        col_start = 'ref:'
        col_fin = 'ref;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, ref_type, ref_uuid) = col_name.split(':')
            self._delete_ref(bch, 'virtual_machine', obj_uuid, ref_type, ref_uuid)

        bch.remove(obj_uuid)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(fq_name)
        fq_name_col = utils.encode_string(fq_name_str) + ':' + obj_uuid
        self._obj_fq_name_cf.remove('virtual_machine', columns = [fq_name_col])


        return (True, '')
    #end _cassandra_virtual_machine_delete

    def _cassandra_interface_route_table_alloc(self, fq_name):
        return (True, '')
    #end _cassandra_interface_route_table_alloc

    def _cassandra_interface_route_table_create(self, obj_ids, obj_dict):
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        bch = self._obj_uuid_cf.batch()

        obj_cols = {}
        obj_cols['fq_name'] = json.dumps(obj_dict['fq_name'])
        obj_cols['type'] = json.dumps('interface_route_table')
        if 'parent_type' in obj_dict:
            # non config-root child
            parent_type = obj_dict['parent_type']
            parent_method_type = parent_type.replace('-', '_')
            parent_fq_name = obj_dict['fq_name'][:-1]
            obj_cols['parent_type'] = json.dumps(parent_type)
            parent_uuid = self.fq_name_to_uuid(parent_method_type, parent_fq_name)
            self._create_child(bch, parent_method_type, parent_uuid, 'interface_route_table', obj_ids['uuid'])

        # Properties
        field = obj_dict.get('interface_route_table_routes', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'interface_route_table_routes', field)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            field['created'] = datetime.datetime.utcnow().isoformat()
            field['last_modified'] = field['created']
            self._create_prop(bch, obj_ids['uuid'], 'id_perms', field)

        field = obj_dict.get('display_name', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'display_name', field)


        # References

        bch.insert(obj_ids['uuid'], obj_cols)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(obj_dict['fq_name'])
        fq_name_cols = {utils.encode_string(fq_name_str) + ':' + obj_ids['uuid']: json.dumps(None)}
        self._obj_fq_name_cf.insert('interface_route_table', fq_name_cols)

        return (True, '')
    #end _cassandra_interface_route_table_create

    def _cassandra_interface_route_table_read(self, obj_uuids, field_names = None):
        # if field_names = None, all fields will be read/returned

        obj_uuid_cf = self._obj_uuid_cf

        # optimize for common case of reading non-backref, non-children fields
        # ignoring columns starting from 'b' and 'c' - significant performance
        # impact in scaled setting. e.g. read of project
        if (field_names is None or
            (set(field_names) & (InterfaceRouteTable.backref_fields | InterfaceRouteTable.children_fields))):
            # atleast one backref/children field is needed
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_count = 10000000,
                                       include_timestamp = True)
        else: # ignore reading backref + children columns
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_start = 'd',
                                       column_count = 10000000,
                                       include_timestamp = True)

        if (len(obj_uuids) == 1) and not obj_rows:
            raise cfgm_common.exceptions.NoIdError(obj_uuids[0])

        results = []
        for row_key in obj_rows:
            # give chance for zk heartbeat/ping
            gevent.sleep(0)
            obj_uuid = row_key
            obj_cols = obj_rows[obj_uuid]
            result = {}
            result['uuid'] = obj_uuid
            result['fq_name'] = json.loads(obj_cols['fq_name'][0])
            for col_name in obj_cols.keys():
                if self._re_match_parent.match(col_name):
                    # non config-root child
                    (_, _, parent_uuid) = col_name.split(':')
                    parent_type = json.loads(obj_cols['parent_type'][0])
                    result['parent_type'] = parent_type
                    try:
                        result['parent_uuid'] = parent_uuid
                        result['parent_href'] = self._generate_url(parent_type, parent_uuid)
                    except cfgm_common.exceptions.NoIdError:
                        err_msg = 'Unknown uuid for parent ' + result['fq_name'][-2]
                        return (False, err_msg)

                # TODO use compiled RE
                if self._re_match_prop.match(col_name):
                    (_, prop_name) = col_name.split(':')
                    result[prop_name] = json.loads(obj_cols[col_name][0])

                # TODO use compiled RE
                if self._re_match_children.match(col_name):
                    (_, child_type, child_uuid) = col_name.split(':')
                    if field_names and '%ss' %(child_type) not in field_names:
                        continue

                    child_tstamp = obj_cols[col_name][1]
                    try:
                        self._read_child(result, obj_uuid, child_type, child_uuid, child_tstamp)
                    except cfgm_common.exceptions.NoIdError:
                        continue

                # TODO use compiled RE
                if self._re_match_ref.match(col_name):
                    (_, ref_type, ref_uuid) = col_name.split(':')
                    self._read_ref(result, obj_uuid, ref_type, ref_uuid, obj_cols[col_name][0])

                if self._re_match_backref.match(col_name):
                    (_, back_ref_type, back_ref_uuid) = col_name.split(':')
                    if field_names and '%s_back_refs' %(back_ref_type) not in field_names:
                        continue

                    try:
                        self._read_back_ref(result, obj_uuid, back_ref_type, back_ref_uuid,
                                            obj_cols[col_name][0])
                    except cfgm_common.exceptions.NoIdError:
                        continue

            # for all column names


            results.append(result)
        # end for all rows

        return (True, results)
    #end _cassandra_interface_route_table_read

    def _cassandra_interface_route_table_count_children(self, obj_uuid, child_type):
        # if child_type = None, return
        if child_type is None:
            return (False, '')

        obj_uuid_cf = self._obj_uuid_cf
        if child_type not in InterfaceRouteTable.children_fields:
            return (False, '%s is not a valid children of InterfaceRouteTable' %(child_type))

        col_start = 'children:'+child_type[:-1]+':'
        col_finish = 'children:'+child_type[:-1]+';'
        num_children = obj_uuid_cf.get_count(obj_uuid,
                                   column_start = col_start,
                                   column_finish = col_finish,
                                   max_count = 10000000)
        return (True, num_children)
    #end _cassandra_interface_route_table_count_children

    def _cassandra_interface_route_table_update(self, obj_uuid, new_obj_dict):
        # Grab ref-uuids and properties in new version
        new_ref_infos = {}

        new_props = {}
        if 'interface_route_table_routes' in new_obj_dict:
            new_props['interface_route_table_routes'] = new_obj_dict['interface_route_table_routes']
        if 'id_perms' in new_obj_dict:
            new_props['id_perms'] = new_obj_dict['id_perms']
        if 'display_name' in new_obj_dict:
            new_props['display_name'] = new_obj_dict['display_name']
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        obj_uuid_cf = self._obj_uuid_cf
        obj_cols_iter = obj_uuid_cf.xget(obj_uuid)
        # TODO optimize this (converts tuple to dict)
        obj_cols = {}
        for col_info in obj_cols_iter:
            obj_cols[col_info[0]] = col_info[1]

        bch = obj_uuid_cf.batch()
        for col_name in obj_cols.keys():
            # TODO use compiled RE
            if re.match('prop:', col_name):
                (_, prop_name) = col_name.split(':')
                if prop_name == 'id_perms':
                    # id-perms always has to be updated for last-mod timestamp
                    # get it from request dict(or from db if not in request dict)
                    new_id_perms = new_obj_dict.get(prop_name, json.loads(obj_cols[col_name]))
                    self.update_last_modified(bch, obj_uuid, new_id_perms)
                elif prop_name in new_obj_dict:
                    self._update_prop(bch, obj_uuid, prop_name, new_props)

            # TODO use compiled RE
            if re.match('ref:', col_name):
                (_, ref_type, ref_uuid) = col_name.split(':')
                self._update_ref(bch, 'interface_route_table', obj_uuid, ref_type, ref_uuid, new_ref_infos)
        # for all column names

        # create new refs
        for ref_type in new_ref_infos.keys():
            for ref_uuid in new_ref_infos[ref_type].keys():
                ref_data = new_ref_infos[ref_type][ref_uuid]
                self._create_ref(bch, 'interface_route_table', obj_uuid, ref_type, ref_uuid, ref_data)

        # create new props
        for prop_name in new_props.keys():
            self._create_prop(bch, obj_uuid, prop_name, new_props[prop_name])

        bch.send()

        return (True, '')
    #end _cassandra_interface_route_table_update

    def _cassandra_interface_route_table_list(self, parent_uuids=None, back_ref_uuids=None,
                           obj_uuids=None, count=False, filters=None):
        children_fq_names_uuids = []
        if filters:
            fnames = filters.get('field_names', [])
            fvalues = filters.get('field_values', [])
            filter_fields = [(fnames[i], fvalues[i]) for i in range(len(fnames))]
        else:
            filter_fields = []

        def filter_rows(coll_infos, filter_cols, filter_params):
            filt_infos = {}
            coll_rows = obj_uuid_cf.multiget(coll_infos.keys(),
                                   columns=filter_cols,
                                   column_count=10000000)
            for row in coll_rows:
                # give chance for zk heartbeat/ping
                gevent.sleep(0)
                full_match = True
                for fname, fval in filter_params:
                    if coll_rows[row]['prop:%s' %(fname)] != fval:
                        full_match = False
                        break
                if full_match:
                    filt_infos[row] = coll_infos[row]
            return filt_infos
        # end filter_rows

        def get_fq_name_uuid_list(obj_uuids):
            ret_list = []
            for obj_uuid in obj_uuids:
                try:
                    obj_fq_name = self.uuid_to_fq_name(obj_uuid)
                    ret_list.append((obj_fq_name, obj_uuid))
                except cfgm_common.exceptions.NoIdError:
                    pass
            return ret_list
        # end get_fq_name_uuid_list

        if parent_uuids:
            # go from parent to child
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'children:interface_route_table:'
            col_fin = 'children:interface_route_table;'
            try:
                obj_rows = obj_uuid_cf.multiget(parent_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_parent_anchor(sort=False):
                # flatten to [('children:<type>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_child_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    child_uuid = col_name.split(':')[2]
                    if obj_uuids and child_uuid not in obj_uuids:
                        continue
                    all_child_infos[child_uuid] = {'uuid': child_uuid, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_child_infos = filter_rows(all_child_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_child_infos = all_child_infos

                if not sort:
                    ret_child_infos = filt_child_infos.values()
                else:
                    ret_child_infos = sorted(filt_child_infos.values(), key=itemgetter('tstamp'))

                return get_fq_name_uuid_list(r['uuid'] for r in ret_child_infos)
            # end filter_rows_parent_anchor

            if count:
                return (True, len(filter_rows_parent_anchor()))

            children_fq_names_uuids = filter_rows_parent_anchor(sort=True)

        if back_ref_uuids:
            # go from anchor to backrefs
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'backref:interface_route_table:'
            col_fin = 'backref:interface_route_table;'
            try:
                obj_rows = obj_uuid_cf.multiget(back_ref_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_backref_anchor():
                # flatten to [('<fqnstr>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_backref_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    col_name_arr = col_name.split(':')
                    fq_name = col_name_arr[:-1]
                    obj_uuid = col_name_arr[-1]
                    if obj_uuids and obj_uuid not in obj_uuids:
                        continue
                    all_backref_infos[obj_uuid] = \
                        {'uuid': obj_uuid, 'fq_name': fq_name, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_backref_infos = filter_rows(all_backref_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_backref_infos = all_backref_infos

                return [(br_info['fq_name'], br_info['uuid']) for br_info in filt_backref_infos.values()]
            # end filter_rows_backref_anchor

            if count:
                return (True, len(filter_rows_backref_anchor()))

            children_fq_names_uuids = filter_rows_backref_anchor()

        if not parent_uuids and not back_ref_uuids:
            obj_uuid_cf = self._obj_uuid_cf
            if obj_uuids:
                # exact objects specified
                def filter_rows_object_list():
                    all_obj_infos = {}
                    for obj_uuid in obj_uuids:
                        all_obj_infos[obj_uuid] = None

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return get_fq_name_uuid_list(filt_obj_infos.keys())
                # end filter_rows_object_list

                if count:
                    return (True, len(filter_rows_object_list()))
                children_fq_names_uuids = filter_rows_object_list()

            else: # grab all resources of this type
                obj_fq_name_cf = self._obj_fq_name_cf
                try:
                    cols = obj_fq_name_cf.get('interface_route_table', column_count = 10000000)
                except pycassa.NotFoundException:
                    if count:
                        return (True, 0)
                    else:
                        return (True, children_fq_names_uuids)

                def filter_rows_no_anchor():
                    all_obj_infos = {}
                    for col_name, col_val in cols.items():
                        # give chance for zk heartbeat/ping
                        gevent.sleep(0)
                        col_name_arr = utils.decode_string(col_name).split(':')
                        obj_uuid = col_name_arr[-1]
                        all_obj_infos[obj_uuid] = (col_name_arr[:-1], obj_uuid)

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return filt_obj_infos.values()
                # end filter_rows_no_anchor

                if count:
                    return (True, len(filter_rows_no_anchor()))

                children_fq_names_uuids = filter_rows_no_anchor()

        return (True, children_fq_names_uuids)
    #end _cassandra_interface_route_table_list

    def _cassandra_interface_route_table_delete(self, obj_uuid):
        obj_uuid_cf = self._obj_uuid_cf
        fq_name = json.loads(obj_uuid_cf.get(obj_uuid, columns = ['fq_name'])['fq_name'])
        bch = obj_uuid_cf.batch()

        # unlink from parent
        col_start = 'parent:'
        col_fin = 'parent;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, parent_type, parent_uuid) = col_name.split(':')
            self._delete_child(bch, parent_type, parent_uuid, 'interface_route_table', obj_uuid)

        # remove refs
        col_start = 'ref:'
        col_fin = 'ref;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, ref_type, ref_uuid) = col_name.split(':')
            self._delete_ref(bch, 'interface_route_table', obj_uuid, ref_type, ref_uuid)

        bch.remove(obj_uuid)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(fq_name)
        fq_name_col = utils.encode_string(fq_name_str) + ':' + obj_uuid
        self._obj_fq_name_cf.remove('interface_route_table', columns = [fq_name_col])


        return (True, '')
    #end _cassandra_interface_route_table_delete

    def _cassandra_service_template_alloc(self, fq_name):
        return (True, '')
    #end _cassandra_service_template_alloc

    def _cassandra_service_template_create(self, obj_ids, obj_dict):
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        bch = self._obj_uuid_cf.batch()

        obj_cols = {}
        obj_cols['fq_name'] = json.dumps(obj_dict['fq_name'])
        obj_cols['type'] = json.dumps('service_template')
        if 'parent_type' in obj_dict:
            # non config-root child
            parent_type = obj_dict['parent_type']
            parent_method_type = parent_type.replace('-', '_')
            parent_fq_name = obj_dict['fq_name'][:-1]
            obj_cols['parent_type'] = json.dumps(parent_type)
            parent_uuid = self.fq_name_to_uuid(parent_method_type, parent_fq_name)
            self._create_child(bch, parent_method_type, parent_uuid, 'service_template', obj_ids['uuid'])

        # Properties
        field = obj_dict.get('service_template_properties', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'service_template_properties', field)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            field['created'] = datetime.datetime.utcnow().isoformat()
            field['last_modified'] = field['created']
            self._create_prop(bch, obj_ids['uuid'], 'id_perms', field)

        field = obj_dict.get('display_name', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'display_name', field)


        # References

        bch.insert(obj_ids['uuid'], obj_cols)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(obj_dict['fq_name'])
        fq_name_cols = {utils.encode_string(fq_name_str) + ':' + obj_ids['uuid']: json.dumps(None)}
        self._obj_fq_name_cf.insert('service_template', fq_name_cols)

        return (True, '')
    #end _cassandra_service_template_create

    def _cassandra_service_template_read(self, obj_uuids, field_names = None):
        # if field_names = None, all fields will be read/returned

        obj_uuid_cf = self._obj_uuid_cf

        # optimize for common case of reading non-backref, non-children fields
        # ignoring columns starting from 'b' and 'c' - significant performance
        # impact in scaled setting. e.g. read of project
        if (field_names is None or
            (set(field_names) & (ServiceTemplate.backref_fields | ServiceTemplate.children_fields))):
            # atleast one backref/children field is needed
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_count = 10000000,
                                       include_timestamp = True)
        else: # ignore reading backref + children columns
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_start = 'd',
                                       column_count = 10000000,
                                       include_timestamp = True)

        if (len(obj_uuids) == 1) and not obj_rows:
            raise cfgm_common.exceptions.NoIdError(obj_uuids[0])

        results = []
        for row_key in obj_rows:
            # give chance for zk heartbeat/ping
            gevent.sleep(0)
            obj_uuid = row_key
            obj_cols = obj_rows[obj_uuid]
            result = {}
            result['uuid'] = obj_uuid
            result['fq_name'] = json.loads(obj_cols['fq_name'][0])
            for col_name in obj_cols.keys():
                if self._re_match_parent.match(col_name):
                    # non config-root child
                    (_, _, parent_uuid) = col_name.split(':')
                    parent_type = json.loads(obj_cols['parent_type'][0])
                    result['parent_type'] = parent_type
                    try:
                        result['parent_uuid'] = parent_uuid
                        result['parent_href'] = self._generate_url(parent_type, parent_uuid)
                    except cfgm_common.exceptions.NoIdError:
                        err_msg = 'Unknown uuid for parent ' + result['fq_name'][-2]
                        return (False, err_msg)

                # TODO use compiled RE
                if self._re_match_prop.match(col_name):
                    (_, prop_name) = col_name.split(':')
                    result[prop_name] = json.loads(obj_cols[col_name][0])

                # TODO use compiled RE
                if self._re_match_children.match(col_name):
                    (_, child_type, child_uuid) = col_name.split(':')
                    if field_names and '%ss' %(child_type) not in field_names:
                        continue

                    child_tstamp = obj_cols[col_name][1]
                    try:
                        self._read_child(result, obj_uuid, child_type, child_uuid, child_tstamp)
                    except cfgm_common.exceptions.NoIdError:
                        continue

                # TODO use compiled RE
                if self._re_match_ref.match(col_name):
                    (_, ref_type, ref_uuid) = col_name.split(':')
                    self._read_ref(result, obj_uuid, ref_type, ref_uuid, obj_cols[col_name][0])

                if self._re_match_backref.match(col_name):
                    (_, back_ref_type, back_ref_uuid) = col_name.split(':')
                    if field_names and '%s_back_refs' %(back_ref_type) not in field_names:
                        continue

                    try:
                        self._read_back_ref(result, obj_uuid, back_ref_type, back_ref_uuid,
                                            obj_cols[col_name][0])
                    except cfgm_common.exceptions.NoIdError:
                        continue

            # for all column names


            results.append(result)
        # end for all rows

        return (True, results)
    #end _cassandra_service_template_read

    def _cassandra_service_template_count_children(self, obj_uuid, child_type):
        # if child_type = None, return
        if child_type is None:
            return (False, '')

        obj_uuid_cf = self._obj_uuid_cf
        if child_type not in ServiceTemplate.children_fields:
            return (False, '%s is not a valid children of ServiceTemplate' %(child_type))

        col_start = 'children:'+child_type[:-1]+':'
        col_finish = 'children:'+child_type[:-1]+';'
        num_children = obj_uuid_cf.get_count(obj_uuid,
                                   column_start = col_start,
                                   column_finish = col_finish,
                                   max_count = 10000000)
        return (True, num_children)
    #end _cassandra_service_template_count_children

    def _cassandra_service_template_update(self, obj_uuid, new_obj_dict):
        # Grab ref-uuids and properties in new version
        new_ref_infos = {}

        new_props = {}
        if 'service_template_properties' in new_obj_dict:
            new_props['service_template_properties'] = new_obj_dict['service_template_properties']
        if 'id_perms' in new_obj_dict:
            new_props['id_perms'] = new_obj_dict['id_perms']
        if 'display_name' in new_obj_dict:
            new_props['display_name'] = new_obj_dict['display_name']
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        obj_uuid_cf = self._obj_uuid_cf
        obj_cols_iter = obj_uuid_cf.xget(obj_uuid)
        # TODO optimize this (converts tuple to dict)
        obj_cols = {}
        for col_info in obj_cols_iter:
            obj_cols[col_info[0]] = col_info[1]

        bch = obj_uuid_cf.batch()
        for col_name in obj_cols.keys():
            # TODO use compiled RE
            if re.match('prop:', col_name):
                (_, prop_name) = col_name.split(':')
                if prop_name == 'id_perms':
                    # id-perms always has to be updated for last-mod timestamp
                    # get it from request dict(or from db if not in request dict)
                    new_id_perms = new_obj_dict.get(prop_name, json.loads(obj_cols[col_name]))
                    self.update_last_modified(bch, obj_uuid, new_id_perms)
                elif prop_name in new_obj_dict:
                    self._update_prop(bch, obj_uuid, prop_name, new_props)

            # TODO use compiled RE
            if re.match('ref:', col_name):
                (_, ref_type, ref_uuid) = col_name.split(':')
                self._update_ref(bch, 'service_template', obj_uuid, ref_type, ref_uuid, new_ref_infos)
        # for all column names

        # create new refs
        for ref_type in new_ref_infos.keys():
            for ref_uuid in new_ref_infos[ref_type].keys():
                ref_data = new_ref_infos[ref_type][ref_uuid]
                self._create_ref(bch, 'service_template', obj_uuid, ref_type, ref_uuid, ref_data)

        # create new props
        for prop_name in new_props.keys():
            self._create_prop(bch, obj_uuid, prop_name, new_props[prop_name])

        bch.send()

        return (True, '')
    #end _cassandra_service_template_update

    def _cassandra_service_template_list(self, parent_uuids=None, back_ref_uuids=None,
                           obj_uuids=None, count=False, filters=None):
        children_fq_names_uuids = []
        if filters:
            fnames = filters.get('field_names', [])
            fvalues = filters.get('field_values', [])
            filter_fields = [(fnames[i], fvalues[i]) for i in range(len(fnames))]
        else:
            filter_fields = []

        def filter_rows(coll_infos, filter_cols, filter_params):
            filt_infos = {}
            coll_rows = obj_uuid_cf.multiget(coll_infos.keys(),
                                   columns=filter_cols,
                                   column_count=10000000)
            for row in coll_rows:
                # give chance for zk heartbeat/ping
                gevent.sleep(0)
                full_match = True
                for fname, fval in filter_params:
                    if coll_rows[row]['prop:%s' %(fname)] != fval:
                        full_match = False
                        break
                if full_match:
                    filt_infos[row] = coll_infos[row]
            return filt_infos
        # end filter_rows

        def get_fq_name_uuid_list(obj_uuids):
            ret_list = []
            for obj_uuid in obj_uuids:
                try:
                    obj_fq_name = self.uuid_to_fq_name(obj_uuid)
                    ret_list.append((obj_fq_name, obj_uuid))
                except cfgm_common.exceptions.NoIdError:
                    pass
            return ret_list
        # end get_fq_name_uuid_list

        if parent_uuids:
            # go from parent to child
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'children:service_template:'
            col_fin = 'children:service_template;'
            try:
                obj_rows = obj_uuid_cf.multiget(parent_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_parent_anchor(sort=False):
                # flatten to [('children:<type>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_child_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    child_uuid = col_name.split(':')[2]
                    if obj_uuids and child_uuid not in obj_uuids:
                        continue
                    all_child_infos[child_uuid] = {'uuid': child_uuid, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_child_infos = filter_rows(all_child_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_child_infos = all_child_infos

                if not sort:
                    ret_child_infos = filt_child_infos.values()
                else:
                    ret_child_infos = sorted(filt_child_infos.values(), key=itemgetter('tstamp'))

                return get_fq_name_uuid_list(r['uuid'] for r in ret_child_infos)
            # end filter_rows_parent_anchor

            if count:
                return (True, len(filter_rows_parent_anchor()))

            children_fq_names_uuids = filter_rows_parent_anchor(sort=True)

        if back_ref_uuids:
            # go from anchor to backrefs
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'backref:service_template:'
            col_fin = 'backref:service_template;'
            try:
                obj_rows = obj_uuid_cf.multiget(back_ref_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_backref_anchor():
                # flatten to [('<fqnstr>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_backref_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    col_name_arr = col_name.split(':')
                    fq_name = col_name_arr[:-1]
                    obj_uuid = col_name_arr[-1]
                    if obj_uuids and obj_uuid not in obj_uuids:
                        continue
                    all_backref_infos[obj_uuid] = \
                        {'uuid': obj_uuid, 'fq_name': fq_name, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_backref_infos = filter_rows(all_backref_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_backref_infos = all_backref_infos

                return [(br_info['fq_name'], br_info['uuid']) for br_info in filt_backref_infos.values()]
            # end filter_rows_backref_anchor

            if count:
                return (True, len(filter_rows_backref_anchor()))

            children_fq_names_uuids = filter_rows_backref_anchor()

        if not parent_uuids and not back_ref_uuids:
            obj_uuid_cf = self._obj_uuid_cf
            if obj_uuids:
                # exact objects specified
                def filter_rows_object_list():
                    all_obj_infos = {}
                    for obj_uuid in obj_uuids:
                        all_obj_infos[obj_uuid] = None

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return get_fq_name_uuid_list(filt_obj_infos.keys())
                # end filter_rows_object_list

                if count:
                    return (True, len(filter_rows_object_list()))
                children_fq_names_uuids = filter_rows_object_list()

            else: # grab all resources of this type
                obj_fq_name_cf = self._obj_fq_name_cf
                try:
                    cols = obj_fq_name_cf.get('service_template', column_count = 10000000)
                except pycassa.NotFoundException:
                    if count:
                        return (True, 0)
                    else:
                        return (True, children_fq_names_uuids)

                def filter_rows_no_anchor():
                    all_obj_infos = {}
                    for col_name, col_val in cols.items():
                        # give chance for zk heartbeat/ping
                        gevent.sleep(0)
                        col_name_arr = utils.decode_string(col_name).split(':')
                        obj_uuid = col_name_arr[-1]
                        all_obj_infos[obj_uuid] = (col_name_arr[:-1], obj_uuid)

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return filt_obj_infos.values()
                # end filter_rows_no_anchor

                if count:
                    return (True, len(filter_rows_no_anchor()))

                children_fq_names_uuids = filter_rows_no_anchor()

        return (True, children_fq_names_uuids)
    #end _cassandra_service_template_list

    def _cassandra_service_template_delete(self, obj_uuid):
        obj_uuid_cf = self._obj_uuid_cf
        fq_name = json.loads(obj_uuid_cf.get(obj_uuid, columns = ['fq_name'])['fq_name'])
        bch = obj_uuid_cf.batch()

        # unlink from parent
        col_start = 'parent:'
        col_fin = 'parent;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, parent_type, parent_uuid) = col_name.split(':')
            self._delete_child(bch, parent_type, parent_uuid, 'service_template', obj_uuid)

        # remove refs
        col_start = 'ref:'
        col_fin = 'ref;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, ref_type, ref_uuid) = col_name.split(':')
            self._delete_ref(bch, 'service_template', obj_uuid, ref_type, ref_uuid)

        bch.remove(obj_uuid)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(fq_name)
        fq_name_col = utils.encode_string(fq_name_str) + ':' + obj_uuid
        self._obj_fq_name_cf.remove('service_template', columns = [fq_name_col])


        return (True, '')
    #end _cassandra_service_template_delete

    def _cassandra_virtual_ip_alloc(self, fq_name):
        return (True, '')
    #end _cassandra_virtual_ip_alloc

    def _cassandra_virtual_ip_create(self, obj_ids, obj_dict):
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        bch = self._obj_uuid_cf.batch()

        obj_cols = {}
        obj_cols['fq_name'] = json.dumps(obj_dict['fq_name'])
        obj_cols['type'] = json.dumps('virtual_ip')
        if 'parent_type' in obj_dict:
            # non config-root child
            parent_type = obj_dict['parent_type']
            parent_method_type = parent_type.replace('-', '_')
            parent_fq_name = obj_dict['fq_name'][:-1]
            obj_cols['parent_type'] = json.dumps(parent_type)
            parent_uuid = self.fq_name_to_uuid(parent_method_type, parent_fq_name)
            self._create_child(bch, parent_method_type, parent_uuid, 'virtual_ip', obj_ids['uuid'])

        # Properties
        field = obj_dict.get('virtual_ip_properties', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'virtual_ip_properties', field)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            field['created'] = datetime.datetime.utcnow().isoformat()
            field['last_modified'] = field['created']
            self._create_prop(bch, obj_ids['uuid'], 'id_perms', field)

        field = obj_dict.get('display_name', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'display_name', field)


        # References
        refs = obj_dict.get('loadbalancer_pool_refs', [])
        for ref in refs:
            ref_uuid = self.fq_name_to_uuid('loadbalancer_pool', ref['to'])
            ref_attr = ref.get('attr', None)
            ref_data = {'attr': ref_attr, 'is_weakref': False}
            self._create_ref(bch, 'virtual_ip', obj_ids['uuid'], 'loadbalancer_pool', ref_uuid, ref_data)
        refs = obj_dict.get('virtual_machine_interface_refs', [])
        for ref in refs:
            ref_uuid = self.fq_name_to_uuid('virtual_machine_interface', ref['to'])
            ref_attr = ref.get('attr', None)
            ref_data = {'attr': ref_attr, 'is_weakref': False}
            self._create_ref(bch, 'virtual_ip', obj_ids['uuid'], 'virtual_machine_interface', ref_uuid, ref_data)

        bch.insert(obj_ids['uuid'], obj_cols)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(obj_dict['fq_name'])
        fq_name_cols = {utils.encode_string(fq_name_str) + ':' + obj_ids['uuid']: json.dumps(None)}
        self._obj_fq_name_cf.insert('virtual_ip', fq_name_cols)

        return (True, '')
    #end _cassandra_virtual_ip_create

    def _cassandra_virtual_ip_read(self, obj_uuids, field_names = None):
        # if field_names = None, all fields will be read/returned

        obj_uuid_cf = self._obj_uuid_cf

        # optimize for common case of reading non-backref, non-children fields
        # ignoring columns starting from 'b' and 'c' - significant performance
        # impact in scaled setting. e.g. read of project
        if (field_names is None or
            (set(field_names) & (VirtualIp.backref_fields | VirtualIp.children_fields))):
            # atleast one backref/children field is needed
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_count = 10000000,
                                       include_timestamp = True)
        else: # ignore reading backref + children columns
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_start = 'd',
                                       column_count = 10000000,
                                       include_timestamp = True)

        if (len(obj_uuids) == 1) and not obj_rows:
            raise cfgm_common.exceptions.NoIdError(obj_uuids[0])

        results = []
        for row_key in obj_rows:
            # give chance for zk heartbeat/ping
            gevent.sleep(0)
            obj_uuid = row_key
            obj_cols = obj_rows[obj_uuid]
            result = {}
            result['uuid'] = obj_uuid
            result['fq_name'] = json.loads(obj_cols['fq_name'][0])
            for col_name in obj_cols.keys():
                if self._re_match_parent.match(col_name):
                    # non config-root child
                    (_, _, parent_uuid) = col_name.split(':')
                    parent_type = json.loads(obj_cols['parent_type'][0])
                    result['parent_type'] = parent_type
                    try:
                        result['parent_uuid'] = parent_uuid
                        result['parent_href'] = self._generate_url(parent_type, parent_uuid)
                    except cfgm_common.exceptions.NoIdError:
                        err_msg = 'Unknown uuid for parent ' + result['fq_name'][-2]
                        return (False, err_msg)

                # TODO use compiled RE
                if self._re_match_prop.match(col_name):
                    (_, prop_name) = col_name.split(':')
                    result[prop_name] = json.loads(obj_cols[col_name][0])

                # TODO use compiled RE
                if self._re_match_children.match(col_name):
                    (_, child_type, child_uuid) = col_name.split(':')
                    if field_names and '%ss' %(child_type) not in field_names:
                        continue

                    child_tstamp = obj_cols[col_name][1]
                    try:
                        self._read_child(result, obj_uuid, child_type, child_uuid, child_tstamp)
                    except cfgm_common.exceptions.NoIdError:
                        continue

                # TODO use compiled RE
                if self._re_match_ref.match(col_name):
                    (_, ref_type, ref_uuid) = col_name.split(':')
                    self._read_ref(result, obj_uuid, ref_type, ref_uuid, obj_cols[col_name][0])

                if self._re_match_backref.match(col_name):
                    (_, back_ref_type, back_ref_uuid) = col_name.split(':')
                    if field_names and '%s_back_refs' %(back_ref_type) not in field_names:
                        continue

                    try:
                        self._read_back_ref(result, obj_uuid, back_ref_type, back_ref_uuid,
                                            obj_cols[col_name][0])
                    except cfgm_common.exceptions.NoIdError:
                        continue

            # for all column names


            results.append(result)
        # end for all rows

        return (True, results)
    #end _cassandra_virtual_ip_read

    def _cassandra_virtual_ip_count_children(self, obj_uuid, child_type):
        # if child_type = None, return
        if child_type is None:
            return (False, '')

        obj_uuid_cf = self._obj_uuid_cf
        if child_type not in VirtualIp.children_fields:
            return (False, '%s is not a valid children of VirtualIp' %(child_type))

        col_start = 'children:'+child_type[:-1]+':'
        col_finish = 'children:'+child_type[:-1]+';'
        num_children = obj_uuid_cf.get_count(obj_uuid,
                                   column_start = col_start,
                                   column_finish = col_finish,
                                   max_count = 10000000)
        return (True, num_children)
    #end _cassandra_virtual_ip_count_children

    def _cassandra_virtual_ip_update(self, obj_uuid, new_obj_dict):
        # Grab ref-uuids and properties in new version
        new_ref_infos = {}

        if 'loadbalancer_pool_refs' in new_obj_dict:
            new_ref_infos['loadbalancer_pool'] = {}
            new_refs = new_obj_dict['loadbalancer_pool_refs']
            if new_refs:
                for new_ref in new_refs:
                    new_ref_uuid = self.fq_name_to_uuid('loadbalancer_pool', new_ref['to'])
                    new_ref_attr = new_ref.get('attr', None)
                    new_ref_data = {'attr': new_ref_attr, 'is_weakref': False}
                    new_ref_infos['loadbalancer_pool'][new_ref_uuid] = new_ref_data

        if 'virtual_machine_interface_refs' in new_obj_dict:
            new_ref_infos['virtual_machine_interface'] = {}
            new_refs = new_obj_dict['virtual_machine_interface_refs']
            if new_refs:
                for new_ref in new_refs:
                    new_ref_uuid = self.fq_name_to_uuid('virtual_machine_interface', new_ref['to'])
                    new_ref_attr = new_ref.get('attr', None)
                    new_ref_data = {'attr': new_ref_attr, 'is_weakref': False}
                    new_ref_infos['virtual_machine_interface'][new_ref_uuid] = new_ref_data

        new_props = {}
        if 'virtual_ip_properties' in new_obj_dict:
            new_props['virtual_ip_properties'] = new_obj_dict['virtual_ip_properties']
        if 'id_perms' in new_obj_dict:
            new_props['id_perms'] = new_obj_dict['id_perms']
        if 'display_name' in new_obj_dict:
            new_props['display_name'] = new_obj_dict['display_name']
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        obj_uuid_cf = self._obj_uuid_cf
        obj_cols_iter = obj_uuid_cf.xget(obj_uuid)
        # TODO optimize this (converts tuple to dict)
        obj_cols = {}
        for col_info in obj_cols_iter:
            obj_cols[col_info[0]] = col_info[1]

        bch = obj_uuid_cf.batch()
        for col_name in obj_cols.keys():
            # TODO use compiled RE
            if re.match('prop:', col_name):
                (_, prop_name) = col_name.split(':')
                if prop_name == 'id_perms':
                    # id-perms always has to be updated for last-mod timestamp
                    # get it from request dict(or from db if not in request dict)
                    new_id_perms = new_obj_dict.get(prop_name, json.loads(obj_cols[col_name]))
                    self.update_last_modified(bch, obj_uuid, new_id_perms)
                elif prop_name in new_obj_dict:
                    self._update_prop(bch, obj_uuid, prop_name, new_props)

            # TODO use compiled RE
            if re.match('ref:', col_name):
                (_, ref_type, ref_uuid) = col_name.split(':')
                self._update_ref(bch, 'virtual_ip', obj_uuid, ref_type, ref_uuid, new_ref_infos)
        # for all column names

        # create new refs
        for ref_type in new_ref_infos.keys():
            for ref_uuid in new_ref_infos[ref_type].keys():
                ref_data = new_ref_infos[ref_type][ref_uuid]
                self._create_ref(bch, 'virtual_ip', obj_uuid, ref_type, ref_uuid, ref_data)

        # create new props
        for prop_name in new_props.keys():
            self._create_prop(bch, obj_uuid, prop_name, new_props[prop_name])

        bch.send()

        return (True, '')
    #end _cassandra_virtual_ip_update

    def _cassandra_virtual_ip_list(self, parent_uuids=None, back_ref_uuids=None,
                           obj_uuids=None, count=False, filters=None):
        children_fq_names_uuids = []
        if filters:
            fnames = filters.get('field_names', [])
            fvalues = filters.get('field_values', [])
            filter_fields = [(fnames[i], fvalues[i]) for i in range(len(fnames))]
        else:
            filter_fields = []

        def filter_rows(coll_infos, filter_cols, filter_params):
            filt_infos = {}
            coll_rows = obj_uuid_cf.multiget(coll_infos.keys(),
                                   columns=filter_cols,
                                   column_count=10000000)
            for row in coll_rows:
                # give chance for zk heartbeat/ping
                gevent.sleep(0)
                full_match = True
                for fname, fval in filter_params:
                    if coll_rows[row]['prop:%s' %(fname)] != fval:
                        full_match = False
                        break
                if full_match:
                    filt_infos[row] = coll_infos[row]
            return filt_infos
        # end filter_rows

        def get_fq_name_uuid_list(obj_uuids):
            ret_list = []
            for obj_uuid in obj_uuids:
                try:
                    obj_fq_name = self.uuid_to_fq_name(obj_uuid)
                    ret_list.append((obj_fq_name, obj_uuid))
                except cfgm_common.exceptions.NoIdError:
                    pass
            return ret_list
        # end get_fq_name_uuid_list

        if parent_uuids:
            # go from parent to child
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'children:virtual_ip:'
            col_fin = 'children:virtual_ip;'
            try:
                obj_rows = obj_uuid_cf.multiget(parent_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_parent_anchor(sort=False):
                # flatten to [('children:<type>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_child_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    child_uuid = col_name.split(':')[2]
                    if obj_uuids and child_uuid not in obj_uuids:
                        continue
                    all_child_infos[child_uuid] = {'uuid': child_uuid, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_child_infos = filter_rows(all_child_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_child_infos = all_child_infos

                if not sort:
                    ret_child_infos = filt_child_infos.values()
                else:
                    ret_child_infos = sorted(filt_child_infos.values(), key=itemgetter('tstamp'))

                return get_fq_name_uuid_list(r['uuid'] for r in ret_child_infos)
            # end filter_rows_parent_anchor

            if count:
                return (True, len(filter_rows_parent_anchor()))

            children_fq_names_uuids = filter_rows_parent_anchor(sort=True)

        if back_ref_uuids:
            # go from anchor to backrefs
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'backref:virtual_ip:'
            col_fin = 'backref:virtual_ip;'
            try:
                obj_rows = obj_uuid_cf.multiget(back_ref_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_backref_anchor():
                # flatten to [('<fqnstr>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_backref_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    col_name_arr = col_name.split(':')
                    fq_name = col_name_arr[:-1]
                    obj_uuid = col_name_arr[-1]
                    if obj_uuids and obj_uuid not in obj_uuids:
                        continue
                    all_backref_infos[obj_uuid] = \
                        {'uuid': obj_uuid, 'fq_name': fq_name, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_backref_infos = filter_rows(all_backref_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_backref_infos = all_backref_infos

                return [(br_info['fq_name'], br_info['uuid']) for br_info in filt_backref_infos.values()]
            # end filter_rows_backref_anchor

            if count:
                return (True, len(filter_rows_backref_anchor()))

            children_fq_names_uuids = filter_rows_backref_anchor()

        if not parent_uuids and not back_ref_uuids:
            obj_uuid_cf = self._obj_uuid_cf
            if obj_uuids:
                # exact objects specified
                def filter_rows_object_list():
                    all_obj_infos = {}
                    for obj_uuid in obj_uuids:
                        all_obj_infos[obj_uuid] = None

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return get_fq_name_uuid_list(filt_obj_infos.keys())
                # end filter_rows_object_list

                if count:
                    return (True, len(filter_rows_object_list()))
                children_fq_names_uuids = filter_rows_object_list()

            else: # grab all resources of this type
                obj_fq_name_cf = self._obj_fq_name_cf
                try:
                    cols = obj_fq_name_cf.get('virtual_ip', column_count = 10000000)
                except pycassa.NotFoundException:
                    if count:
                        return (True, 0)
                    else:
                        return (True, children_fq_names_uuids)

                def filter_rows_no_anchor():
                    all_obj_infos = {}
                    for col_name, col_val in cols.items():
                        # give chance for zk heartbeat/ping
                        gevent.sleep(0)
                        col_name_arr = utils.decode_string(col_name).split(':')
                        obj_uuid = col_name_arr[-1]
                        all_obj_infos[obj_uuid] = (col_name_arr[:-1], obj_uuid)

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return filt_obj_infos.values()
                # end filter_rows_no_anchor

                if count:
                    return (True, len(filter_rows_no_anchor()))

                children_fq_names_uuids = filter_rows_no_anchor()

        return (True, children_fq_names_uuids)
    #end _cassandra_virtual_ip_list

    def _cassandra_virtual_ip_delete(self, obj_uuid):
        obj_uuid_cf = self._obj_uuid_cf
        fq_name = json.loads(obj_uuid_cf.get(obj_uuid, columns = ['fq_name'])['fq_name'])
        bch = obj_uuid_cf.batch()

        # unlink from parent
        col_start = 'parent:'
        col_fin = 'parent;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, parent_type, parent_uuid) = col_name.split(':')
            self._delete_child(bch, parent_type, parent_uuid, 'virtual_ip', obj_uuid)

        # remove refs
        col_start = 'ref:'
        col_fin = 'ref;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, ref_type, ref_uuid) = col_name.split(':')
            self._delete_ref(bch, 'virtual_ip', obj_uuid, ref_type, ref_uuid)

        bch.remove(obj_uuid)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(fq_name)
        fq_name_col = utils.encode_string(fq_name_str) + ':' + obj_uuid
        self._obj_fq_name_cf.remove('virtual_ip', columns = [fq_name_col])


        return (True, '')
    #end _cassandra_virtual_ip_delete

    def _cassandra_loadbalancer_member_alloc(self, fq_name):
        return (True, '')
    #end _cassandra_loadbalancer_member_alloc

    def _cassandra_loadbalancer_member_create(self, obj_ids, obj_dict):
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        bch = self._obj_uuid_cf.batch()

        obj_cols = {}
        obj_cols['fq_name'] = json.dumps(obj_dict['fq_name'])
        obj_cols['type'] = json.dumps('loadbalancer_member')
        if 'parent_type' in obj_dict:
            # non config-root child
            parent_type = obj_dict['parent_type']
            parent_method_type = parent_type.replace('-', '_')
            parent_fq_name = obj_dict['fq_name'][:-1]
            obj_cols['parent_type'] = json.dumps(parent_type)
            parent_uuid = self.fq_name_to_uuid(parent_method_type, parent_fq_name)
            self._create_child(bch, parent_method_type, parent_uuid, 'loadbalancer_member', obj_ids['uuid'])

        # Properties
        field = obj_dict.get('loadbalancer_member_properties', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'loadbalancer_member_properties', field)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            field['created'] = datetime.datetime.utcnow().isoformat()
            field['last_modified'] = field['created']
            self._create_prop(bch, obj_ids['uuid'], 'id_perms', field)

        field = obj_dict.get('display_name', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'display_name', field)


        # References

        bch.insert(obj_ids['uuid'], obj_cols)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(obj_dict['fq_name'])
        fq_name_cols = {utils.encode_string(fq_name_str) + ':' + obj_ids['uuid']: json.dumps(None)}
        self._obj_fq_name_cf.insert('loadbalancer_member', fq_name_cols)

        return (True, '')
    #end _cassandra_loadbalancer_member_create

    def _cassandra_loadbalancer_member_read(self, obj_uuids, field_names = None):
        # if field_names = None, all fields will be read/returned

        obj_uuid_cf = self._obj_uuid_cf

        # optimize for common case of reading non-backref, non-children fields
        # ignoring columns starting from 'b' and 'c' - significant performance
        # impact in scaled setting. e.g. read of project
        if (field_names is None or
            (set(field_names) & (LoadbalancerMember.backref_fields | LoadbalancerMember.children_fields))):
            # atleast one backref/children field is needed
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_count = 10000000,
                                       include_timestamp = True)
        else: # ignore reading backref + children columns
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_start = 'd',
                                       column_count = 10000000,
                                       include_timestamp = True)

        if (len(obj_uuids) == 1) and not obj_rows:
            raise cfgm_common.exceptions.NoIdError(obj_uuids[0])

        results = []
        for row_key in obj_rows:
            # give chance for zk heartbeat/ping
            gevent.sleep(0)
            obj_uuid = row_key
            obj_cols = obj_rows[obj_uuid]
            result = {}
            result['uuid'] = obj_uuid
            result['fq_name'] = json.loads(obj_cols['fq_name'][0])
            for col_name in obj_cols.keys():
                if self._re_match_parent.match(col_name):
                    # non config-root child
                    (_, _, parent_uuid) = col_name.split(':')
                    parent_type = json.loads(obj_cols['parent_type'][0])
                    result['parent_type'] = parent_type
                    try:
                        result['parent_uuid'] = parent_uuid
                        result['parent_href'] = self._generate_url(parent_type, parent_uuid)
                    except cfgm_common.exceptions.NoIdError:
                        err_msg = 'Unknown uuid for parent ' + result['fq_name'][-2]
                        return (False, err_msg)

                # TODO use compiled RE
                if self._re_match_prop.match(col_name):
                    (_, prop_name) = col_name.split(':')
                    result[prop_name] = json.loads(obj_cols[col_name][0])

                # TODO use compiled RE
                if self._re_match_children.match(col_name):
                    (_, child_type, child_uuid) = col_name.split(':')
                    if field_names and '%ss' %(child_type) not in field_names:
                        continue

                    child_tstamp = obj_cols[col_name][1]
                    try:
                        self._read_child(result, obj_uuid, child_type, child_uuid, child_tstamp)
                    except cfgm_common.exceptions.NoIdError:
                        continue

                # TODO use compiled RE
                if self._re_match_ref.match(col_name):
                    (_, ref_type, ref_uuid) = col_name.split(':')
                    self._read_ref(result, obj_uuid, ref_type, ref_uuid, obj_cols[col_name][0])

                if self._re_match_backref.match(col_name):
                    (_, back_ref_type, back_ref_uuid) = col_name.split(':')
                    if field_names and '%s_back_refs' %(back_ref_type) not in field_names:
                        continue

                    try:
                        self._read_back_ref(result, obj_uuid, back_ref_type, back_ref_uuid,
                                            obj_cols[col_name][0])
                    except cfgm_common.exceptions.NoIdError:
                        continue

            # for all column names


            results.append(result)
        # end for all rows

        return (True, results)
    #end _cassandra_loadbalancer_member_read

    def _cassandra_loadbalancer_member_count_children(self, obj_uuid, child_type):
        # if child_type = None, return
        if child_type is None:
            return (False, '')

        obj_uuid_cf = self._obj_uuid_cf
        if child_type not in LoadbalancerMember.children_fields:
            return (False, '%s is not a valid children of LoadbalancerMember' %(child_type))

        col_start = 'children:'+child_type[:-1]+':'
        col_finish = 'children:'+child_type[:-1]+';'
        num_children = obj_uuid_cf.get_count(obj_uuid,
                                   column_start = col_start,
                                   column_finish = col_finish,
                                   max_count = 10000000)
        return (True, num_children)
    #end _cassandra_loadbalancer_member_count_children

    def _cassandra_loadbalancer_member_update(self, obj_uuid, new_obj_dict):
        # Grab ref-uuids and properties in new version
        new_ref_infos = {}

        new_props = {}
        if 'loadbalancer_member_properties' in new_obj_dict:
            new_props['loadbalancer_member_properties'] = new_obj_dict['loadbalancer_member_properties']
        if 'id_perms' in new_obj_dict:
            new_props['id_perms'] = new_obj_dict['id_perms']
        if 'display_name' in new_obj_dict:
            new_props['display_name'] = new_obj_dict['display_name']
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        obj_uuid_cf = self._obj_uuid_cf
        obj_cols_iter = obj_uuid_cf.xget(obj_uuid)
        # TODO optimize this (converts tuple to dict)
        obj_cols = {}
        for col_info in obj_cols_iter:
            obj_cols[col_info[0]] = col_info[1]

        bch = obj_uuid_cf.batch()
        for col_name in obj_cols.keys():
            # TODO use compiled RE
            if re.match('prop:', col_name):
                (_, prop_name) = col_name.split(':')
                if prop_name == 'id_perms':
                    # id-perms always has to be updated for last-mod timestamp
                    # get it from request dict(or from db if not in request dict)
                    new_id_perms = new_obj_dict.get(prop_name, json.loads(obj_cols[col_name]))
                    self.update_last_modified(bch, obj_uuid, new_id_perms)
                elif prop_name in new_obj_dict:
                    self._update_prop(bch, obj_uuid, prop_name, new_props)

            # TODO use compiled RE
            if re.match('ref:', col_name):
                (_, ref_type, ref_uuid) = col_name.split(':')
                self._update_ref(bch, 'loadbalancer_member', obj_uuid, ref_type, ref_uuid, new_ref_infos)
        # for all column names

        # create new refs
        for ref_type in new_ref_infos.keys():
            for ref_uuid in new_ref_infos[ref_type].keys():
                ref_data = new_ref_infos[ref_type][ref_uuid]
                self._create_ref(bch, 'loadbalancer_member', obj_uuid, ref_type, ref_uuid, ref_data)

        # create new props
        for prop_name in new_props.keys():
            self._create_prop(bch, obj_uuid, prop_name, new_props[prop_name])

        bch.send()

        return (True, '')
    #end _cassandra_loadbalancer_member_update

    def _cassandra_loadbalancer_member_list(self, parent_uuids=None, back_ref_uuids=None,
                           obj_uuids=None, count=False, filters=None):
        children_fq_names_uuids = []
        if filters:
            fnames = filters.get('field_names', [])
            fvalues = filters.get('field_values', [])
            filter_fields = [(fnames[i], fvalues[i]) for i in range(len(fnames))]
        else:
            filter_fields = []

        def filter_rows(coll_infos, filter_cols, filter_params):
            filt_infos = {}
            coll_rows = obj_uuid_cf.multiget(coll_infos.keys(),
                                   columns=filter_cols,
                                   column_count=10000000)
            for row in coll_rows:
                # give chance for zk heartbeat/ping
                gevent.sleep(0)
                full_match = True
                for fname, fval in filter_params:
                    if coll_rows[row]['prop:%s' %(fname)] != fval:
                        full_match = False
                        break
                if full_match:
                    filt_infos[row] = coll_infos[row]
            return filt_infos
        # end filter_rows

        def get_fq_name_uuid_list(obj_uuids):
            ret_list = []
            for obj_uuid in obj_uuids:
                try:
                    obj_fq_name = self.uuid_to_fq_name(obj_uuid)
                    ret_list.append((obj_fq_name, obj_uuid))
                except cfgm_common.exceptions.NoIdError:
                    pass
            return ret_list
        # end get_fq_name_uuid_list

        if parent_uuids:
            # go from parent to child
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'children:loadbalancer_member:'
            col_fin = 'children:loadbalancer_member;'
            try:
                obj_rows = obj_uuid_cf.multiget(parent_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_parent_anchor(sort=False):
                # flatten to [('children:<type>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_child_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    child_uuid = col_name.split(':')[2]
                    if obj_uuids and child_uuid not in obj_uuids:
                        continue
                    all_child_infos[child_uuid] = {'uuid': child_uuid, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_child_infos = filter_rows(all_child_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_child_infos = all_child_infos

                if not sort:
                    ret_child_infos = filt_child_infos.values()
                else:
                    ret_child_infos = sorted(filt_child_infos.values(), key=itemgetter('tstamp'))

                return get_fq_name_uuid_list(r['uuid'] for r in ret_child_infos)
            # end filter_rows_parent_anchor

            if count:
                return (True, len(filter_rows_parent_anchor()))

            children_fq_names_uuids = filter_rows_parent_anchor(sort=True)

        if back_ref_uuids:
            # go from anchor to backrefs
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'backref:loadbalancer_member:'
            col_fin = 'backref:loadbalancer_member;'
            try:
                obj_rows = obj_uuid_cf.multiget(back_ref_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_backref_anchor():
                # flatten to [('<fqnstr>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_backref_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    col_name_arr = col_name.split(':')
                    fq_name = col_name_arr[:-1]
                    obj_uuid = col_name_arr[-1]
                    if obj_uuids and obj_uuid not in obj_uuids:
                        continue
                    all_backref_infos[obj_uuid] = \
                        {'uuid': obj_uuid, 'fq_name': fq_name, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_backref_infos = filter_rows(all_backref_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_backref_infos = all_backref_infos

                return [(br_info['fq_name'], br_info['uuid']) for br_info in filt_backref_infos.values()]
            # end filter_rows_backref_anchor

            if count:
                return (True, len(filter_rows_backref_anchor()))

            children_fq_names_uuids = filter_rows_backref_anchor()

        if not parent_uuids and not back_ref_uuids:
            obj_uuid_cf = self._obj_uuid_cf
            if obj_uuids:
                # exact objects specified
                def filter_rows_object_list():
                    all_obj_infos = {}
                    for obj_uuid in obj_uuids:
                        all_obj_infos[obj_uuid] = None

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return get_fq_name_uuid_list(filt_obj_infos.keys())
                # end filter_rows_object_list

                if count:
                    return (True, len(filter_rows_object_list()))
                children_fq_names_uuids = filter_rows_object_list()

            else: # grab all resources of this type
                obj_fq_name_cf = self._obj_fq_name_cf
                try:
                    cols = obj_fq_name_cf.get('loadbalancer_member', column_count = 10000000)
                except pycassa.NotFoundException:
                    if count:
                        return (True, 0)
                    else:
                        return (True, children_fq_names_uuids)

                def filter_rows_no_anchor():
                    all_obj_infos = {}
                    for col_name, col_val in cols.items():
                        # give chance for zk heartbeat/ping
                        gevent.sleep(0)
                        col_name_arr = utils.decode_string(col_name).split(':')
                        obj_uuid = col_name_arr[-1]
                        all_obj_infos[obj_uuid] = (col_name_arr[:-1], obj_uuid)

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return filt_obj_infos.values()
                # end filter_rows_no_anchor

                if count:
                    return (True, len(filter_rows_no_anchor()))

                children_fq_names_uuids = filter_rows_no_anchor()

        return (True, children_fq_names_uuids)
    #end _cassandra_loadbalancer_member_list

    def _cassandra_loadbalancer_member_delete(self, obj_uuid):
        obj_uuid_cf = self._obj_uuid_cf
        fq_name = json.loads(obj_uuid_cf.get(obj_uuid, columns = ['fq_name'])['fq_name'])
        bch = obj_uuid_cf.batch()

        # unlink from parent
        col_start = 'parent:'
        col_fin = 'parent;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, parent_type, parent_uuid) = col_name.split(':')
            self._delete_child(bch, parent_type, parent_uuid, 'loadbalancer_member', obj_uuid)

        # remove refs
        col_start = 'ref:'
        col_fin = 'ref;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, ref_type, ref_uuid) = col_name.split(':')
            self._delete_ref(bch, 'loadbalancer_member', obj_uuid, ref_type, ref_uuid)

        bch.remove(obj_uuid)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(fq_name)
        fq_name_col = utils.encode_string(fq_name_str) + ':' + obj_uuid
        self._obj_fq_name_cf.remove('loadbalancer_member', columns = [fq_name_col])


        return (True, '')
    #end _cassandra_loadbalancer_member_delete

    def _cassandra_security_group_alloc(self, fq_name):
        return (True, '')
    #end _cassandra_security_group_alloc

    def _cassandra_security_group_create(self, obj_ids, obj_dict):
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        bch = self._obj_uuid_cf.batch()

        obj_cols = {}
        obj_cols['fq_name'] = json.dumps(obj_dict['fq_name'])
        obj_cols['type'] = json.dumps('security_group')
        if 'parent_type' in obj_dict:
            # non config-root child
            parent_type = obj_dict['parent_type']
            parent_method_type = parent_type.replace('-', '_')
            parent_fq_name = obj_dict['fq_name'][:-1]
            obj_cols['parent_type'] = json.dumps(parent_type)
            parent_uuid = self.fq_name_to_uuid(parent_method_type, parent_fq_name)
            self._create_child(bch, parent_method_type, parent_uuid, 'security_group', obj_ids['uuid'])

        # Properties
        field = obj_dict.get('security_group_id', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'security_group_id', field)

        field = obj_dict.get('configured_security_group_id', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'configured_security_group_id', field)

        field = obj_dict.get('security_group_entries', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'security_group_entries', field)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            field['created'] = datetime.datetime.utcnow().isoformat()
            field['last_modified'] = field['created']
            self._create_prop(bch, obj_ids['uuid'], 'id_perms', field)

        field = obj_dict.get('display_name', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'display_name', field)


        # References

        bch.insert(obj_ids['uuid'], obj_cols)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(obj_dict['fq_name'])
        fq_name_cols = {utils.encode_string(fq_name_str) + ':' + obj_ids['uuid']: json.dumps(None)}
        self._obj_fq_name_cf.insert('security_group', fq_name_cols)

        return (True, '')
    #end _cassandra_security_group_create

    def _cassandra_security_group_read(self, obj_uuids, field_names = None):
        # if field_names = None, all fields will be read/returned

        obj_uuid_cf = self._obj_uuid_cf

        # optimize for common case of reading non-backref, non-children fields
        # ignoring columns starting from 'b' and 'c' - significant performance
        # impact in scaled setting. e.g. read of project
        if (field_names is None or
            (set(field_names) & (SecurityGroup.backref_fields | SecurityGroup.children_fields))):
            # atleast one backref/children field is needed
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_count = 10000000,
                                       include_timestamp = True)
        else: # ignore reading backref + children columns
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_start = 'd',
                                       column_count = 10000000,
                                       include_timestamp = True)

        if (len(obj_uuids) == 1) and not obj_rows:
            raise cfgm_common.exceptions.NoIdError(obj_uuids[0])

        results = []
        for row_key in obj_rows:
            # give chance for zk heartbeat/ping
            gevent.sleep(0)
            obj_uuid = row_key
            obj_cols = obj_rows[obj_uuid]
            result = {}
            result['uuid'] = obj_uuid
            result['fq_name'] = json.loads(obj_cols['fq_name'][0])
            for col_name in obj_cols.keys():
                if self._re_match_parent.match(col_name):
                    # non config-root child
                    (_, _, parent_uuid) = col_name.split(':')
                    parent_type = json.loads(obj_cols['parent_type'][0])
                    result['parent_type'] = parent_type
                    try:
                        result['parent_uuid'] = parent_uuid
                        result['parent_href'] = self._generate_url(parent_type, parent_uuid)
                    except cfgm_common.exceptions.NoIdError:
                        err_msg = 'Unknown uuid for parent ' + result['fq_name'][-2]
                        return (False, err_msg)

                # TODO use compiled RE
                if self._re_match_prop.match(col_name):
                    (_, prop_name) = col_name.split(':')
                    result[prop_name] = json.loads(obj_cols[col_name][0])

                # TODO use compiled RE
                if self._re_match_children.match(col_name):
                    (_, child_type, child_uuid) = col_name.split(':')
                    if field_names and '%ss' %(child_type) not in field_names:
                        continue

                    child_tstamp = obj_cols[col_name][1]
                    try:
                        self._read_child(result, obj_uuid, child_type, child_uuid, child_tstamp)
                    except cfgm_common.exceptions.NoIdError:
                        continue

                # TODO use compiled RE
                if self._re_match_ref.match(col_name):
                    (_, ref_type, ref_uuid) = col_name.split(':')
                    self._read_ref(result, obj_uuid, ref_type, ref_uuid, obj_cols[col_name][0])

                if self._re_match_backref.match(col_name):
                    (_, back_ref_type, back_ref_uuid) = col_name.split(':')
                    if field_names and '%s_back_refs' %(back_ref_type) not in field_names:
                        continue

                    try:
                        self._read_back_ref(result, obj_uuid, back_ref_type, back_ref_uuid,
                                            obj_cols[col_name][0])
                    except cfgm_common.exceptions.NoIdError:
                        continue

            # for all column names

            if 'access_control_lists' in result:
                # sort children; TODO do this based on schema
                sorted_children = sorted(result['access_control_lists'], key = itemgetter('tstamp'))
                # re-write result's children without timestamp
                result['access_control_lists'] = sorted_children
                [child.pop('tstamp') for child in result['access_control_lists']]

            results.append(result)
        # end for all rows

        return (True, results)
    #end _cassandra_security_group_read

    def _cassandra_security_group_count_children(self, obj_uuid, child_type):
        # if child_type = None, return
        if child_type is None:
            return (False, '')

        obj_uuid_cf = self._obj_uuid_cf
        if child_type not in SecurityGroup.children_fields:
            return (False, '%s is not a valid children of SecurityGroup' %(child_type))

        col_start = 'children:'+child_type[:-1]+':'
        col_finish = 'children:'+child_type[:-1]+';'
        num_children = obj_uuid_cf.get_count(obj_uuid,
                                   column_start = col_start,
                                   column_finish = col_finish,
                                   max_count = 10000000)
        return (True, num_children)
    #end _cassandra_security_group_count_children

    def _cassandra_security_group_update(self, obj_uuid, new_obj_dict):
        # Grab ref-uuids and properties in new version
        new_ref_infos = {}

        new_props = {}
        if 'security_group_id' in new_obj_dict:
            new_props['security_group_id'] = new_obj_dict['security_group_id']
        if 'configured_security_group_id' in new_obj_dict:
            new_props['configured_security_group_id'] = new_obj_dict['configured_security_group_id']
        if 'security_group_entries' in new_obj_dict:
            new_props['security_group_entries'] = new_obj_dict['security_group_entries']
        if 'id_perms' in new_obj_dict:
            new_props['id_perms'] = new_obj_dict['id_perms']
        if 'display_name' in new_obj_dict:
            new_props['display_name'] = new_obj_dict['display_name']
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        obj_uuid_cf = self._obj_uuid_cf
        obj_cols_iter = obj_uuid_cf.xget(obj_uuid)
        # TODO optimize this (converts tuple to dict)
        obj_cols = {}
        for col_info in obj_cols_iter:
            obj_cols[col_info[0]] = col_info[1]

        bch = obj_uuid_cf.batch()
        for col_name in obj_cols.keys():
            # TODO use compiled RE
            if re.match('prop:', col_name):
                (_, prop_name) = col_name.split(':')
                if prop_name == 'id_perms':
                    # id-perms always has to be updated for last-mod timestamp
                    # get it from request dict(or from db if not in request dict)
                    new_id_perms = new_obj_dict.get(prop_name, json.loads(obj_cols[col_name]))
                    self.update_last_modified(bch, obj_uuid, new_id_perms)
                elif prop_name in new_obj_dict:
                    self._update_prop(bch, obj_uuid, prop_name, new_props)

            # TODO use compiled RE
            if re.match('ref:', col_name):
                (_, ref_type, ref_uuid) = col_name.split(':')
                self._update_ref(bch, 'security_group', obj_uuid, ref_type, ref_uuid, new_ref_infos)
        # for all column names

        # create new refs
        for ref_type in new_ref_infos.keys():
            for ref_uuid in new_ref_infos[ref_type].keys():
                ref_data = new_ref_infos[ref_type][ref_uuid]
                self._create_ref(bch, 'security_group', obj_uuid, ref_type, ref_uuid, ref_data)

        # create new props
        for prop_name in new_props.keys():
            self._create_prop(bch, obj_uuid, prop_name, new_props[prop_name])

        bch.send()

        return (True, '')
    #end _cassandra_security_group_update

    def _cassandra_security_group_list(self, parent_uuids=None, back_ref_uuids=None,
                           obj_uuids=None, count=False, filters=None):
        children_fq_names_uuids = []
        if filters:
            fnames = filters.get('field_names', [])
            fvalues = filters.get('field_values', [])
            filter_fields = [(fnames[i], fvalues[i]) for i in range(len(fnames))]
        else:
            filter_fields = []

        def filter_rows(coll_infos, filter_cols, filter_params):
            filt_infos = {}
            coll_rows = obj_uuid_cf.multiget(coll_infos.keys(),
                                   columns=filter_cols,
                                   column_count=10000000)
            for row in coll_rows:
                # give chance for zk heartbeat/ping
                gevent.sleep(0)
                full_match = True
                for fname, fval in filter_params:
                    if coll_rows[row]['prop:%s' %(fname)] != fval:
                        full_match = False
                        break
                if full_match:
                    filt_infos[row] = coll_infos[row]
            return filt_infos
        # end filter_rows

        def get_fq_name_uuid_list(obj_uuids):
            ret_list = []
            for obj_uuid in obj_uuids:
                try:
                    obj_fq_name = self.uuid_to_fq_name(obj_uuid)
                    ret_list.append((obj_fq_name, obj_uuid))
                except cfgm_common.exceptions.NoIdError:
                    pass
            return ret_list
        # end get_fq_name_uuid_list

        if parent_uuids:
            # go from parent to child
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'children:security_group:'
            col_fin = 'children:security_group;'
            try:
                obj_rows = obj_uuid_cf.multiget(parent_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_parent_anchor(sort=False):
                # flatten to [('children:<type>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_child_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    child_uuid = col_name.split(':')[2]
                    if obj_uuids and child_uuid not in obj_uuids:
                        continue
                    all_child_infos[child_uuid] = {'uuid': child_uuid, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_child_infos = filter_rows(all_child_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_child_infos = all_child_infos

                if not sort:
                    ret_child_infos = filt_child_infos.values()
                else:
                    ret_child_infos = sorted(filt_child_infos.values(), key=itemgetter('tstamp'))

                return get_fq_name_uuid_list(r['uuid'] for r in ret_child_infos)
            # end filter_rows_parent_anchor

            if count:
                return (True, len(filter_rows_parent_anchor()))

            children_fq_names_uuids = filter_rows_parent_anchor(sort=True)

        if back_ref_uuids:
            # go from anchor to backrefs
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'backref:security_group:'
            col_fin = 'backref:security_group;'
            try:
                obj_rows = obj_uuid_cf.multiget(back_ref_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_backref_anchor():
                # flatten to [('<fqnstr>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_backref_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    col_name_arr = col_name.split(':')
                    fq_name = col_name_arr[:-1]
                    obj_uuid = col_name_arr[-1]
                    if obj_uuids and obj_uuid not in obj_uuids:
                        continue
                    all_backref_infos[obj_uuid] = \
                        {'uuid': obj_uuid, 'fq_name': fq_name, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_backref_infos = filter_rows(all_backref_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_backref_infos = all_backref_infos

                return [(br_info['fq_name'], br_info['uuid']) for br_info in filt_backref_infos.values()]
            # end filter_rows_backref_anchor

            if count:
                return (True, len(filter_rows_backref_anchor()))

            children_fq_names_uuids = filter_rows_backref_anchor()

        if not parent_uuids and not back_ref_uuids:
            obj_uuid_cf = self._obj_uuid_cf
            if obj_uuids:
                # exact objects specified
                def filter_rows_object_list():
                    all_obj_infos = {}
                    for obj_uuid in obj_uuids:
                        all_obj_infos[obj_uuid] = None

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return get_fq_name_uuid_list(filt_obj_infos.keys())
                # end filter_rows_object_list

                if count:
                    return (True, len(filter_rows_object_list()))
                children_fq_names_uuids = filter_rows_object_list()

            else: # grab all resources of this type
                obj_fq_name_cf = self._obj_fq_name_cf
                try:
                    cols = obj_fq_name_cf.get('security_group', column_count = 10000000)
                except pycassa.NotFoundException:
                    if count:
                        return (True, 0)
                    else:
                        return (True, children_fq_names_uuids)

                def filter_rows_no_anchor():
                    all_obj_infos = {}
                    for col_name, col_val in cols.items():
                        # give chance for zk heartbeat/ping
                        gevent.sleep(0)
                        col_name_arr = utils.decode_string(col_name).split(':')
                        obj_uuid = col_name_arr[-1]
                        all_obj_infos[obj_uuid] = (col_name_arr[:-1], obj_uuid)

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return filt_obj_infos.values()
                # end filter_rows_no_anchor

                if count:
                    return (True, len(filter_rows_no_anchor()))

                children_fq_names_uuids = filter_rows_no_anchor()

        return (True, children_fq_names_uuids)
    #end _cassandra_security_group_list

    def _cassandra_security_group_delete(self, obj_uuid):
        obj_uuid_cf = self._obj_uuid_cf
        fq_name = json.loads(obj_uuid_cf.get(obj_uuid, columns = ['fq_name'])['fq_name'])
        bch = obj_uuid_cf.batch()

        # unlink from parent
        col_start = 'parent:'
        col_fin = 'parent;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, parent_type, parent_uuid) = col_name.split(':')
            self._delete_child(bch, parent_type, parent_uuid, 'security_group', obj_uuid)

        # remove refs
        col_start = 'ref:'
        col_fin = 'ref;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, ref_type, ref_uuid) = col_name.split(':')
            self._delete_ref(bch, 'security_group', obj_uuid, ref_type, ref_uuid)

        bch.remove(obj_uuid)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(fq_name)
        fq_name_col = utils.encode_string(fq_name_str) + ':' + obj_uuid
        self._obj_fq_name_cf.remove('security_group', columns = [fq_name_col])


        return (True, '')
    #end _cassandra_security_group_delete

    def _cassandra_provider_attachment_alloc(self, fq_name):
        return (True, '')
    #end _cassandra_provider_attachment_alloc

    def _cassandra_provider_attachment_create(self, obj_ids, obj_dict):
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        bch = self._obj_uuid_cf.batch()

        obj_cols = {}
        obj_cols['fq_name'] = json.dumps(obj_dict['fq_name'])
        obj_cols['type'] = json.dumps('provider_attachment')

        # Properties
        field = obj_dict.get('id_perms', None)
        if field is not None:
            field['created'] = datetime.datetime.utcnow().isoformat()
            field['last_modified'] = field['created']
            self._create_prop(bch, obj_ids['uuid'], 'id_perms', field)

        field = obj_dict.get('display_name', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'display_name', field)


        # References
        refs = obj_dict.get('virtual_router_refs', [])
        for ref in refs:
            ref_uuid = self.fq_name_to_uuid('virtual_router', ref['to'])
            ref_attr = ref.get('attr', None)
            ref_data = {'attr': ref_attr, 'is_weakref': False}
            self._create_ref(bch, 'provider_attachment', obj_ids['uuid'], 'virtual_router', ref_uuid, ref_data)

        bch.insert(obj_ids['uuid'], obj_cols)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(obj_dict['fq_name'])
        fq_name_cols = {utils.encode_string(fq_name_str) + ':' + obj_ids['uuid']: json.dumps(None)}
        self._obj_fq_name_cf.insert('provider_attachment', fq_name_cols)

        return (True, '')
    #end _cassandra_provider_attachment_create

    def _cassandra_provider_attachment_read(self, obj_uuids, field_names = None):
        # if field_names = None, all fields will be read/returned

        obj_uuid_cf = self._obj_uuid_cf

        # optimize for common case of reading non-backref, non-children fields
        # ignoring columns starting from 'b' and 'c' - significant performance
        # impact in scaled setting. e.g. read of project
        if (field_names is None or
            (set(field_names) & (ProviderAttachment.backref_fields | ProviderAttachment.children_fields))):
            # atleast one backref/children field is needed
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_count = 10000000,
                                       include_timestamp = True)
        else: # ignore reading backref + children columns
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_start = 'd',
                                       column_count = 10000000,
                                       include_timestamp = True)

        if (len(obj_uuids) == 1) and not obj_rows:
            raise cfgm_common.exceptions.NoIdError(obj_uuids[0])

        results = []
        for row_key in obj_rows:
            # give chance for zk heartbeat/ping
            gevent.sleep(0)
            obj_uuid = row_key
            obj_cols = obj_rows[obj_uuid]
            result = {}
            result['uuid'] = obj_uuid
            result['fq_name'] = json.loads(obj_cols['fq_name'][0])
            for col_name in obj_cols.keys():
                if self._re_match_parent.match(col_name):
                    # non config-root child
                    (_, _, parent_uuid) = col_name.split(':')
                    parent_type = json.loads(obj_cols['parent_type'][0])
                    result['parent_type'] = parent_type
                    try:
                        result['parent_uuid'] = parent_uuid
                        result['parent_href'] = self._generate_url(parent_type, parent_uuid)
                    except cfgm_common.exceptions.NoIdError:
                        err_msg = 'Unknown uuid for parent ' + result['fq_name'][-2]
                        return (False, err_msg)

                # TODO use compiled RE
                if self._re_match_prop.match(col_name):
                    (_, prop_name) = col_name.split(':')
                    result[prop_name] = json.loads(obj_cols[col_name][0])

                # TODO use compiled RE
                if self._re_match_children.match(col_name):
                    (_, child_type, child_uuid) = col_name.split(':')
                    if field_names and '%ss' %(child_type) not in field_names:
                        continue

                    child_tstamp = obj_cols[col_name][1]
                    try:
                        self._read_child(result, obj_uuid, child_type, child_uuid, child_tstamp)
                    except cfgm_common.exceptions.NoIdError:
                        continue

                # TODO use compiled RE
                if self._re_match_ref.match(col_name):
                    (_, ref_type, ref_uuid) = col_name.split(':')
                    self._read_ref(result, obj_uuid, ref_type, ref_uuid, obj_cols[col_name][0])

                if self._re_match_backref.match(col_name):
                    (_, back_ref_type, back_ref_uuid) = col_name.split(':')
                    if field_names and '%s_back_refs' %(back_ref_type) not in field_names:
                        continue

                    try:
                        self._read_back_ref(result, obj_uuid, back_ref_type, back_ref_uuid,
                                            obj_cols[col_name][0])
                    except cfgm_common.exceptions.NoIdError:
                        continue

            # for all column names


            results.append(result)
        # end for all rows

        return (True, results)
    #end _cassandra_provider_attachment_read

    def _cassandra_provider_attachment_count_children(self, obj_uuid, child_type):
        # if child_type = None, return
        if child_type is None:
            return (False, '')

        obj_uuid_cf = self._obj_uuid_cf
        if child_type not in ProviderAttachment.children_fields:
            return (False, '%s is not a valid children of ProviderAttachment' %(child_type))

        col_start = 'children:'+child_type[:-1]+':'
        col_finish = 'children:'+child_type[:-1]+';'
        num_children = obj_uuid_cf.get_count(obj_uuid,
                                   column_start = col_start,
                                   column_finish = col_finish,
                                   max_count = 10000000)
        return (True, num_children)
    #end _cassandra_provider_attachment_count_children

    def _cassandra_provider_attachment_update(self, obj_uuid, new_obj_dict):
        # Grab ref-uuids and properties in new version
        new_ref_infos = {}

        if 'virtual_router_refs' in new_obj_dict:
            new_ref_infos['virtual_router'] = {}
            new_refs = new_obj_dict['virtual_router_refs']
            if new_refs:
                for new_ref in new_refs:
                    new_ref_uuid = self.fq_name_to_uuid('virtual_router', new_ref['to'])
                    new_ref_attr = new_ref.get('attr', None)
                    new_ref_data = {'attr': new_ref_attr, 'is_weakref': False}
                    new_ref_infos['virtual_router'][new_ref_uuid] = new_ref_data

        new_props = {}
        if 'id_perms' in new_obj_dict:
            new_props['id_perms'] = new_obj_dict['id_perms']
        if 'display_name' in new_obj_dict:
            new_props['display_name'] = new_obj_dict['display_name']
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        obj_uuid_cf = self._obj_uuid_cf
        obj_cols_iter = obj_uuid_cf.xget(obj_uuid)
        # TODO optimize this (converts tuple to dict)
        obj_cols = {}
        for col_info in obj_cols_iter:
            obj_cols[col_info[0]] = col_info[1]

        bch = obj_uuid_cf.batch()
        for col_name in obj_cols.keys():
            # TODO use compiled RE
            if re.match('prop:', col_name):
                (_, prop_name) = col_name.split(':')
                if prop_name == 'id_perms':
                    # id-perms always has to be updated for last-mod timestamp
                    # get it from request dict(or from db if not in request dict)
                    new_id_perms = new_obj_dict.get(prop_name, json.loads(obj_cols[col_name]))
                    self.update_last_modified(bch, obj_uuid, new_id_perms)
                elif prop_name in new_obj_dict:
                    self._update_prop(bch, obj_uuid, prop_name, new_props)

            # TODO use compiled RE
            if re.match('ref:', col_name):
                (_, ref_type, ref_uuid) = col_name.split(':')
                self._update_ref(bch, 'provider_attachment', obj_uuid, ref_type, ref_uuid, new_ref_infos)
        # for all column names

        # create new refs
        for ref_type in new_ref_infos.keys():
            for ref_uuid in new_ref_infos[ref_type].keys():
                ref_data = new_ref_infos[ref_type][ref_uuid]
                self._create_ref(bch, 'provider_attachment', obj_uuid, ref_type, ref_uuid, ref_data)

        # create new props
        for prop_name in new_props.keys():
            self._create_prop(bch, obj_uuid, prop_name, new_props[prop_name])

        bch.send()

        return (True, '')
    #end _cassandra_provider_attachment_update

    def _cassandra_provider_attachment_list(self, parent_uuids=None, back_ref_uuids=None,
                           obj_uuids=None, count=False, filters=None):
        children_fq_names_uuids = []
        if filters:
            fnames = filters.get('field_names', [])
            fvalues = filters.get('field_values', [])
            filter_fields = [(fnames[i], fvalues[i]) for i in range(len(fnames))]
        else:
            filter_fields = []

        def filter_rows(coll_infos, filter_cols, filter_params):
            filt_infos = {}
            coll_rows = obj_uuid_cf.multiget(coll_infos.keys(),
                                   columns=filter_cols,
                                   column_count=10000000)
            for row in coll_rows:
                # give chance for zk heartbeat/ping
                gevent.sleep(0)
                full_match = True
                for fname, fval in filter_params:
                    if coll_rows[row]['prop:%s' %(fname)] != fval:
                        full_match = False
                        break
                if full_match:
                    filt_infos[row] = coll_infos[row]
            return filt_infos
        # end filter_rows

        def get_fq_name_uuid_list(obj_uuids):
            ret_list = []
            for obj_uuid in obj_uuids:
                try:
                    obj_fq_name = self.uuid_to_fq_name(obj_uuid)
                    ret_list.append((obj_fq_name, obj_uuid))
                except cfgm_common.exceptions.NoIdError:
                    pass
            return ret_list
        # end get_fq_name_uuid_list

        if parent_uuids:
            # go from parent to child
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'children:provider_attachment:'
            col_fin = 'children:provider_attachment;'
            try:
                obj_rows = obj_uuid_cf.multiget(parent_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_parent_anchor(sort=False):
                # flatten to [('children:<type>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_child_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    child_uuid = col_name.split(':')[2]
                    if obj_uuids and child_uuid not in obj_uuids:
                        continue
                    all_child_infos[child_uuid] = {'uuid': child_uuid, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_child_infos = filter_rows(all_child_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_child_infos = all_child_infos

                if not sort:
                    ret_child_infos = filt_child_infos.values()
                else:
                    ret_child_infos = sorted(filt_child_infos.values(), key=itemgetter('tstamp'))

                return get_fq_name_uuid_list(r['uuid'] for r in ret_child_infos)
            # end filter_rows_parent_anchor

            if count:
                return (True, len(filter_rows_parent_anchor()))

            children_fq_names_uuids = filter_rows_parent_anchor(sort=True)

        if back_ref_uuids:
            # go from anchor to backrefs
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'backref:provider_attachment:'
            col_fin = 'backref:provider_attachment;'
            try:
                obj_rows = obj_uuid_cf.multiget(back_ref_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_backref_anchor():
                # flatten to [('<fqnstr>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_backref_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    col_name_arr = col_name.split(':')
                    fq_name = col_name_arr[:-1]
                    obj_uuid = col_name_arr[-1]
                    if obj_uuids and obj_uuid not in obj_uuids:
                        continue
                    all_backref_infos[obj_uuid] = \
                        {'uuid': obj_uuid, 'fq_name': fq_name, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_backref_infos = filter_rows(all_backref_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_backref_infos = all_backref_infos

                return [(br_info['fq_name'], br_info['uuid']) for br_info in filt_backref_infos.values()]
            # end filter_rows_backref_anchor

            if count:
                return (True, len(filter_rows_backref_anchor()))

            children_fq_names_uuids = filter_rows_backref_anchor()

        if not parent_uuids and not back_ref_uuids:
            obj_uuid_cf = self._obj_uuid_cf
            if obj_uuids:
                # exact objects specified
                def filter_rows_object_list():
                    all_obj_infos = {}
                    for obj_uuid in obj_uuids:
                        all_obj_infos[obj_uuid] = None

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return get_fq_name_uuid_list(filt_obj_infos.keys())
                # end filter_rows_object_list

                if count:
                    return (True, len(filter_rows_object_list()))
                children_fq_names_uuids = filter_rows_object_list()

            else: # grab all resources of this type
                obj_fq_name_cf = self._obj_fq_name_cf
                try:
                    cols = obj_fq_name_cf.get('provider_attachment', column_count = 10000000)
                except pycassa.NotFoundException:
                    if count:
                        return (True, 0)
                    else:
                        return (True, children_fq_names_uuids)

                def filter_rows_no_anchor():
                    all_obj_infos = {}
                    for col_name, col_val in cols.items():
                        # give chance for zk heartbeat/ping
                        gevent.sleep(0)
                        col_name_arr = utils.decode_string(col_name).split(':')
                        obj_uuid = col_name_arr[-1]
                        all_obj_infos[obj_uuid] = (col_name_arr[:-1], obj_uuid)

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return filt_obj_infos.values()
                # end filter_rows_no_anchor

                if count:
                    return (True, len(filter_rows_no_anchor()))

                children_fq_names_uuids = filter_rows_no_anchor()

        return (True, children_fq_names_uuids)
    #end _cassandra_provider_attachment_list

    def _cassandra_provider_attachment_delete(self, obj_uuid):
        obj_uuid_cf = self._obj_uuid_cf
        fq_name = json.loads(obj_uuid_cf.get(obj_uuid, columns = ['fq_name'])['fq_name'])
        bch = obj_uuid_cf.batch()

        # unlink from parent
        col_start = 'parent:'
        col_fin = 'parent;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, parent_type, parent_uuid) = col_name.split(':')
            self._delete_child(bch, parent_type, parent_uuid, 'provider_attachment', obj_uuid)

        # remove refs
        col_start = 'ref:'
        col_fin = 'ref;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, ref_type, ref_uuid) = col_name.split(':')
            self._delete_ref(bch, 'provider_attachment', obj_uuid, ref_type, ref_uuid)

        bch.remove(obj_uuid)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(fq_name)
        fq_name_col = utils.encode_string(fq_name_str) + ':' + obj_uuid
        self._obj_fq_name_cf.remove('provider_attachment', columns = [fq_name_col])


        return (True, '')
    #end _cassandra_provider_attachment_delete

    def _cassandra_virtual_machine_interface_alloc(self, fq_name):
        return (True, '')
    #end _cassandra_virtual_machine_interface_alloc

    def _cassandra_virtual_machine_interface_create(self, obj_ids, obj_dict):
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        bch = self._obj_uuid_cf.batch()

        obj_cols = {}
        obj_cols['fq_name'] = json.dumps(obj_dict['fq_name'])
        obj_cols['type'] = json.dumps('virtual_machine_interface')
        if 'parent_type' in obj_dict:
            # non config-root child
            parent_type = obj_dict['parent_type']
            parent_method_type = parent_type.replace('-', '_')
            parent_fq_name = obj_dict['fq_name'][:-1]
            obj_cols['parent_type'] = json.dumps(parent_type)
            parent_uuid = self.fq_name_to_uuid(parent_method_type, parent_fq_name)
            self._create_child(bch, parent_method_type, parent_uuid, 'virtual_machine_interface', obj_ids['uuid'])

        # Properties
        field = obj_dict.get('virtual_machine_interface_mac_addresses', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'virtual_machine_interface_mac_addresses', field)

        field = obj_dict.get('virtual_machine_interface_dhcp_option_list', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'virtual_machine_interface_dhcp_option_list', field)

        field = obj_dict.get('virtual_machine_interface_host_routes', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'virtual_machine_interface_host_routes', field)

        field = obj_dict.get('virtual_machine_interface_allowed_address_pairs', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'virtual_machine_interface_allowed_address_pairs', field)

        field = obj_dict.get('vrf_assign_table', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'vrf_assign_table', field)

        field = obj_dict.get('virtual_machine_interface_device_owner', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'virtual_machine_interface_device_owner', field)

        field = obj_dict.get('virtual_machine_interface_properties', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'virtual_machine_interface_properties', field)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            field['created'] = datetime.datetime.utcnow().isoformat()
            field['last_modified'] = field['created']
            self._create_prop(bch, obj_ids['uuid'], 'id_perms', field)

        field = obj_dict.get('display_name', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'display_name', field)


        # References
        refs = obj_dict.get('qos_forwarding_class_refs', [])
        for ref in refs:
            ref_uuid = self.fq_name_to_uuid('qos_forwarding_class', ref['to'])
            ref_attr = ref.get('attr', None)
            ref_data = {'attr': ref_attr, 'is_weakref': False}
            self._create_ref(bch, 'virtual_machine_interface', obj_ids['uuid'], 'qos_forwarding_class', ref_uuid, ref_data)
        refs = obj_dict.get('security_group_refs', [])
        for ref in refs:
            ref_uuid = self.fq_name_to_uuid('security_group', ref['to'])
            ref_attr = ref.get('attr', None)
            ref_data = {'attr': ref_attr, 'is_weakref': False}
            self._create_ref(bch, 'virtual_machine_interface', obj_ids['uuid'], 'security_group', ref_uuid, ref_data)
        refs = obj_dict.get('virtual_machine_interface_refs', [])
        for ref in refs:
            ref_uuid = self.fq_name_to_uuid('virtual_machine_interface', ref['to'])
            ref_attr = ref.get('attr', None)
            ref_data = {'attr': ref_attr, 'is_weakref': False}
            self._create_ref(bch, 'virtual_machine_interface', obj_ids['uuid'], 'virtual_machine_interface', ref_uuid, ref_data)
        refs = obj_dict.get('virtual_machine_refs', [])
        for ref in refs:
            ref_uuid = self.fq_name_to_uuid('virtual_machine', ref['to'])
            ref_attr = ref.get('attr', None)
            ref_data = {'attr': ref_attr, 'is_weakref': False}
            self._create_ref(bch, 'virtual_machine_interface', obj_ids['uuid'], 'virtual_machine', ref_uuid, ref_data)
        refs = obj_dict.get('virtual_network_refs', [])
        for ref in refs:
            ref_uuid = self.fq_name_to_uuid('virtual_network', ref['to'])
            ref_attr = ref.get('attr', None)
            ref_data = {'attr': ref_attr, 'is_weakref': False}
            self._create_ref(bch, 'virtual_machine_interface', obj_ids['uuid'], 'virtual_network', ref_uuid, ref_data)
        refs = obj_dict.get('routing_instance_refs', [])
        for ref in refs:
            ref_uuid = self.fq_name_to_uuid('routing_instance', ref['to'])
            ref_attr = ref.get('attr', None)
            ref_data = {'attr': ref_attr, 'is_weakref': False}
            self._create_ref(bch, 'virtual_machine_interface', obj_ids['uuid'], 'routing_instance', ref_uuid, ref_data)
        refs = obj_dict.get('interface_route_table_refs', [])
        for ref in refs:
            ref_uuid = self.fq_name_to_uuid('interface_route_table', ref['to'])
            ref_attr = ref.get('attr', None)
            ref_data = {'attr': ref_attr, 'is_weakref': False}
            self._create_ref(bch, 'virtual_machine_interface', obj_ids['uuid'], 'interface_route_table', ref_uuid, ref_data)

        bch.insert(obj_ids['uuid'], obj_cols)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(obj_dict['fq_name'])
        fq_name_cols = {utils.encode_string(fq_name_str) + ':' + obj_ids['uuid']: json.dumps(None)}
        self._obj_fq_name_cf.insert('virtual_machine_interface', fq_name_cols)

        return (True, '')
    #end _cassandra_virtual_machine_interface_create

    def _cassandra_virtual_machine_interface_read(self, obj_uuids, field_names = None):
        # if field_names = None, all fields will be read/returned

        obj_uuid_cf = self._obj_uuid_cf

        # optimize for common case of reading non-backref, non-children fields
        # ignoring columns starting from 'b' and 'c' - significant performance
        # impact in scaled setting. e.g. read of project
        if (field_names is None or
            (set(field_names) & (VirtualMachineInterface.backref_fields | VirtualMachineInterface.children_fields))):
            # atleast one backref/children field is needed
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_count = 10000000,
                                       include_timestamp = True)
        else: # ignore reading backref + children columns
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_start = 'd',
                                       column_count = 10000000,
                                       include_timestamp = True)

        if (len(obj_uuids) == 1) and not obj_rows:
            raise cfgm_common.exceptions.NoIdError(obj_uuids[0])

        results = []
        for row_key in obj_rows:
            # give chance for zk heartbeat/ping
            gevent.sleep(0)
            obj_uuid = row_key
            obj_cols = obj_rows[obj_uuid]
            result = {}
            result['uuid'] = obj_uuid
            result['fq_name'] = json.loads(obj_cols['fq_name'][0])
            for col_name in obj_cols.keys():
                if self._re_match_parent.match(col_name):
                    # non config-root child
                    (_, _, parent_uuid) = col_name.split(':')
                    parent_type = json.loads(obj_cols['parent_type'][0])
                    result['parent_type'] = parent_type
                    try:
                        result['parent_uuid'] = parent_uuid
                        result['parent_href'] = self._generate_url(parent_type, parent_uuid)
                    except cfgm_common.exceptions.NoIdError:
                        err_msg = 'Unknown uuid for parent ' + result['fq_name'][-2]
                        return (False, err_msg)

                # TODO use compiled RE
                if self._re_match_prop.match(col_name):
                    (_, prop_name) = col_name.split(':')
                    result[prop_name] = json.loads(obj_cols[col_name][0])

                # TODO use compiled RE
                if self._re_match_children.match(col_name):
                    (_, child_type, child_uuid) = col_name.split(':')
                    if field_names and '%ss' %(child_type) not in field_names:
                        continue

                    child_tstamp = obj_cols[col_name][1]
                    try:
                        self._read_child(result, obj_uuid, child_type, child_uuid, child_tstamp)
                    except cfgm_common.exceptions.NoIdError:
                        continue

                # TODO use compiled RE
                if self._re_match_ref.match(col_name):
                    (_, ref_type, ref_uuid) = col_name.split(':')
                    self._read_ref(result, obj_uuid, ref_type, ref_uuid, obj_cols[col_name][0])

                if self._re_match_backref.match(col_name):
                    (_, back_ref_type, back_ref_uuid) = col_name.split(':')
                    if field_names and '%s_back_refs' %(back_ref_type) not in field_names:
                        continue

                    try:
                        self._read_back_ref(result, obj_uuid, back_ref_type, back_ref_uuid,
                                            obj_cols[col_name][0])
                    except cfgm_common.exceptions.NoIdError:
                        continue

            # for all column names


            results.append(result)
        # end for all rows

        return (True, results)
    #end _cassandra_virtual_machine_interface_read

    def _cassandra_virtual_machine_interface_count_children(self, obj_uuid, child_type):
        # if child_type = None, return
        if child_type is None:
            return (False, '')

        obj_uuid_cf = self._obj_uuid_cf
        if child_type not in VirtualMachineInterface.children_fields:
            return (False, '%s is not a valid children of VirtualMachineInterface' %(child_type))

        col_start = 'children:'+child_type[:-1]+':'
        col_finish = 'children:'+child_type[:-1]+';'
        num_children = obj_uuid_cf.get_count(obj_uuid,
                                   column_start = col_start,
                                   column_finish = col_finish,
                                   max_count = 10000000)
        return (True, num_children)
    #end _cassandra_virtual_machine_interface_count_children

    def _cassandra_virtual_machine_interface_update(self, obj_uuid, new_obj_dict):
        # Grab ref-uuids and properties in new version
        new_ref_infos = {}

        if 'qos_forwarding_class_refs' in new_obj_dict:
            new_ref_infos['qos_forwarding_class'] = {}
            new_refs = new_obj_dict['qos_forwarding_class_refs']
            if new_refs:
                for new_ref in new_refs:
                    new_ref_uuid = self.fq_name_to_uuid('qos_forwarding_class', new_ref['to'])
                    new_ref_attr = new_ref.get('attr', None)
                    new_ref_data = {'attr': new_ref_attr, 'is_weakref': False}
                    new_ref_infos['qos_forwarding_class'][new_ref_uuid] = new_ref_data

        if 'security_group_refs' in new_obj_dict:
            new_ref_infos['security_group'] = {}
            new_refs = new_obj_dict['security_group_refs']
            if new_refs:
                for new_ref in new_refs:
                    new_ref_uuid = self.fq_name_to_uuid('security_group', new_ref['to'])
                    new_ref_attr = new_ref.get('attr', None)
                    new_ref_data = {'attr': new_ref_attr, 'is_weakref': False}
                    new_ref_infos['security_group'][new_ref_uuid] = new_ref_data

        if 'virtual_machine_interface_refs' in new_obj_dict:
            new_ref_infos['virtual_machine_interface'] = {}
            new_refs = new_obj_dict['virtual_machine_interface_refs']
            if new_refs:
                for new_ref in new_refs:
                    new_ref_uuid = self.fq_name_to_uuid('virtual_machine_interface', new_ref['to'])
                    new_ref_attr = new_ref.get('attr', None)
                    new_ref_data = {'attr': new_ref_attr, 'is_weakref': False}
                    new_ref_infos['virtual_machine_interface'][new_ref_uuid] = new_ref_data

        if 'virtual_machine_refs' in new_obj_dict:
            new_ref_infos['virtual_machine'] = {}
            new_refs = new_obj_dict['virtual_machine_refs']
            if new_refs:
                for new_ref in new_refs:
                    new_ref_uuid = self.fq_name_to_uuid('virtual_machine', new_ref['to'])
                    new_ref_attr = new_ref.get('attr', None)
                    new_ref_data = {'attr': new_ref_attr, 'is_weakref': False}
                    new_ref_infos['virtual_machine'][new_ref_uuid] = new_ref_data

        if 'virtual_network_refs' in new_obj_dict:
            new_ref_infos['virtual_network'] = {}
            new_refs = new_obj_dict['virtual_network_refs']
            if new_refs:
                for new_ref in new_refs:
                    new_ref_uuid = self.fq_name_to_uuid('virtual_network', new_ref['to'])
                    new_ref_attr = new_ref.get('attr', None)
                    new_ref_data = {'attr': new_ref_attr, 'is_weakref': False}
                    new_ref_infos['virtual_network'][new_ref_uuid] = new_ref_data

        if 'routing_instance_refs' in new_obj_dict:
            new_ref_infos['routing_instance'] = {}
            new_refs = new_obj_dict['routing_instance_refs']
            if new_refs:
                for new_ref in new_refs:
                    new_ref_uuid = self.fq_name_to_uuid('routing_instance', new_ref['to'])
                    new_ref_attr = new_ref.get('attr', None)
                    new_ref_data = {'attr': new_ref_attr, 'is_weakref': False}
                    new_ref_infos['routing_instance'][new_ref_uuid] = new_ref_data

        if 'interface_route_table_refs' in new_obj_dict:
            new_ref_infos['interface_route_table'] = {}
            new_refs = new_obj_dict['interface_route_table_refs']
            if new_refs:
                for new_ref in new_refs:
                    new_ref_uuid = self.fq_name_to_uuid('interface_route_table', new_ref['to'])
                    new_ref_attr = new_ref.get('attr', None)
                    new_ref_data = {'attr': new_ref_attr, 'is_weakref': False}
                    new_ref_infos['interface_route_table'][new_ref_uuid] = new_ref_data

        new_props = {}
        if 'virtual_machine_interface_mac_addresses' in new_obj_dict:
            new_props['virtual_machine_interface_mac_addresses'] = new_obj_dict['virtual_machine_interface_mac_addresses']
        if 'virtual_machine_interface_dhcp_option_list' in new_obj_dict:
            new_props['virtual_machine_interface_dhcp_option_list'] = new_obj_dict['virtual_machine_interface_dhcp_option_list']
        if 'virtual_machine_interface_host_routes' in new_obj_dict:
            new_props['virtual_machine_interface_host_routes'] = new_obj_dict['virtual_machine_interface_host_routes']
        if 'virtual_machine_interface_allowed_address_pairs' in new_obj_dict:
            new_props['virtual_machine_interface_allowed_address_pairs'] = new_obj_dict['virtual_machine_interface_allowed_address_pairs']
        if 'vrf_assign_table' in new_obj_dict:
            new_props['vrf_assign_table'] = new_obj_dict['vrf_assign_table']
        if 'virtual_machine_interface_device_owner' in new_obj_dict:
            new_props['virtual_machine_interface_device_owner'] = new_obj_dict['virtual_machine_interface_device_owner']
        if 'virtual_machine_interface_properties' in new_obj_dict:
            new_props['virtual_machine_interface_properties'] = new_obj_dict['virtual_machine_interface_properties']
        if 'id_perms' in new_obj_dict:
            new_props['id_perms'] = new_obj_dict['id_perms']
        if 'display_name' in new_obj_dict:
            new_props['display_name'] = new_obj_dict['display_name']
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        obj_uuid_cf = self._obj_uuid_cf
        obj_cols_iter = obj_uuid_cf.xget(obj_uuid)
        # TODO optimize this (converts tuple to dict)
        obj_cols = {}
        for col_info in obj_cols_iter:
            obj_cols[col_info[0]] = col_info[1]

        bch = obj_uuid_cf.batch()
        for col_name in obj_cols.keys():
            # TODO use compiled RE
            if re.match('prop:', col_name):
                (_, prop_name) = col_name.split(':')
                if prop_name == 'id_perms':
                    # id-perms always has to be updated for last-mod timestamp
                    # get it from request dict(or from db if not in request dict)
                    new_id_perms = new_obj_dict.get(prop_name, json.loads(obj_cols[col_name]))
                    self.update_last_modified(bch, obj_uuid, new_id_perms)
                elif prop_name in new_obj_dict:
                    self._update_prop(bch, obj_uuid, prop_name, new_props)

            # TODO use compiled RE
            if re.match('ref:', col_name):
                (_, ref_type, ref_uuid) = col_name.split(':')
                self._update_ref(bch, 'virtual_machine_interface', obj_uuid, ref_type, ref_uuid, new_ref_infos)
        # for all column names

        # create new refs
        for ref_type in new_ref_infos.keys():
            for ref_uuid in new_ref_infos[ref_type].keys():
                ref_data = new_ref_infos[ref_type][ref_uuid]
                self._create_ref(bch, 'virtual_machine_interface', obj_uuid, ref_type, ref_uuid, ref_data)

        # create new props
        for prop_name in new_props.keys():
            self._create_prop(bch, obj_uuid, prop_name, new_props[prop_name])

        bch.send()

        return (True, '')
    #end _cassandra_virtual_machine_interface_update

    def _cassandra_virtual_machine_interface_list(self, parent_uuids=None, back_ref_uuids=None,
                           obj_uuids=None, count=False, filters=None):
        children_fq_names_uuids = []
        if filters:
            fnames = filters.get('field_names', [])
            fvalues = filters.get('field_values', [])
            filter_fields = [(fnames[i], fvalues[i]) for i in range(len(fnames))]
        else:
            filter_fields = []

        def filter_rows(coll_infos, filter_cols, filter_params):
            filt_infos = {}
            coll_rows = obj_uuid_cf.multiget(coll_infos.keys(),
                                   columns=filter_cols,
                                   column_count=10000000)
            for row in coll_rows:
                # give chance for zk heartbeat/ping
                gevent.sleep(0)
                full_match = True
                for fname, fval in filter_params:
                    if coll_rows[row]['prop:%s' %(fname)] != fval:
                        full_match = False
                        break
                if full_match:
                    filt_infos[row] = coll_infos[row]
            return filt_infos
        # end filter_rows

        def get_fq_name_uuid_list(obj_uuids):
            ret_list = []
            for obj_uuid in obj_uuids:
                try:
                    obj_fq_name = self.uuid_to_fq_name(obj_uuid)
                    ret_list.append((obj_fq_name, obj_uuid))
                except cfgm_common.exceptions.NoIdError:
                    pass
            return ret_list
        # end get_fq_name_uuid_list

        if parent_uuids:
            # go from parent to child
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'children:virtual_machine_interface:'
            col_fin = 'children:virtual_machine_interface;'
            try:
                obj_rows = obj_uuid_cf.multiget(parent_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_parent_anchor(sort=False):
                # flatten to [('children:<type>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_child_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    child_uuid = col_name.split(':')[2]
                    if obj_uuids and child_uuid not in obj_uuids:
                        continue
                    all_child_infos[child_uuid] = {'uuid': child_uuid, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_child_infos = filter_rows(all_child_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_child_infos = all_child_infos

                if not sort:
                    ret_child_infos = filt_child_infos.values()
                else:
                    ret_child_infos = sorted(filt_child_infos.values(), key=itemgetter('tstamp'))

                return get_fq_name_uuid_list(r['uuid'] for r in ret_child_infos)
            # end filter_rows_parent_anchor

            if count:
                return (True, len(filter_rows_parent_anchor()))

            children_fq_names_uuids = filter_rows_parent_anchor(sort=True)

        if back_ref_uuids:
            # go from anchor to backrefs
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'backref:virtual_machine_interface:'
            col_fin = 'backref:virtual_machine_interface;'
            try:
                obj_rows = obj_uuid_cf.multiget(back_ref_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_backref_anchor():
                # flatten to [('<fqnstr>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_backref_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    col_name_arr = col_name.split(':')
                    fq_name = col_name_arr[:-1]
                    obj_uuid = col_name_arr[-1]
                    if obj_uuids and obj_uuid not in obj_uuids:
                        continue
                    all_backref_infos[obj_uuid] = \
                        {'uuid': obj_uuid, 'fq_name': fq_name, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_backref_infos = filter_rows(all_backref_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_backref_infos = all_backref_infos

                return [(br_info['fq_name'], br_info['uuid']) for br_info in filt_backref_infos.values()]
            # end filter_rows_backref_anchor

            if count:
                return (True, len(filter_rows_backref_anchor()))

            children_fq_names_uuids = filter_rows_backref_anchor()

        if not parent_uuids and not back_ref_uuids:
            obj_uuid_cf = self._obj_uuid_cf
            if obj_uuids:
                # exact objects specified
                def filter_rows_object_list():
                    all_obj_infos = {}
                    for obj_uuid in obj_uuids:
                        all_obj_infos[obj_uuid] = None

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return get_fq_name_uuid_list(filt_obj_infos.keys())
                # end filter_rows_object_list

                if count:
                    return (True, len(filter_rows_object_list()))
                children_fq_names_uuids = filter_rows_object_list()

            else: # grab all resources of this type
                obj_fq_name_cf = self._obj_fq_name_cf
                try:
                    cols = obj_fq_name_cf.get('virtual_machine_interface', column_count = 10000000)
                except pycassa.NotFoundException:
                    if count:
                        return (True, 0)
                    else:
                        return (True, children_fq_names_uuids)

                def filter_rows_no_anchor():
                    all_obj_infos = {}
                    for col_name, col_val in cols.items():
                        # give chance for zk heartbeat/ping
                        gevent.sleep(0)
                        col_name_arr = utils.decode_string(col_name).split(':')
                        obj_uuid = col_name_arr[-1]
                        all_obj_infos[obj_uuid] = (col_name_arr[:-1], obj_uuid)

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return filt_obj_infos.values()
                # end filter_rows_no_anchor

                if count:
                    return (True, len(filter_rows_no_anchor()))

                children_fq_names_uuids = filter_rows_no_anchor()

        return (True, children_fq_names_uuids)
    #end _cassandra_virtual_machine_interface_list

    def _cassandra_virtual_machine_interface_delete(self, obj_uuid):
        obj_uuid_cf = self._obj_uuid_cf
        fq_name = json.loads(obj_uuid_cf.get(obj_uuid, columns = ['fq_name'])['fq_name'])
        bch = obj_uuid_cf.batch()

        # unlink from parent
        col_start = 'parent:'
        col_fin = 'parent;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, parent_type, parent_uuid) = col_name.split(':')
            self._delete_child(bch, parent_type, parent_uuid, 'virtual_machine_interface', obj_uuid)

        # remove refs
        col_start = 'ref:'
        col_fin = 'ref;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, ref_type, ref_uuid) = col_name.split(':')
            self._delete_ref(bch, 'virtual_machine_interface', obj_uuid, ref_type, ref_uuid)

        bch.remove(obj_uuid)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(fq_name)
        fq_name_col = utils.encode_string(fq_name_str) + ':' + obj_uuid
        self._obj_fq_name_cf.remove('virtual_machine_interface', columns = [fq_name_col])


        return (True, '')
    #end _cassandra_virtual_machine_interface_delete

    def _cassandra_loadbalancer_healthmonitor_alloc(self, fq_name):
        return (True, '')
    #end _cassandra_loadbalancer_healthmonitor_alloc

    def _cassandra_loadbalancer_healthmonitor_create(self, obj_ids, obj_dict):
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        bch = self._obj_uuid_cf.batch()

        obj_cols = {}
        obj_cols['fq_name'] = json.dumps(obj_dict['fq_name'])
        obj_cols['type'] = json.dumps('loadbalancer_healthmonitor')
        if 'parent_type' in obj_dict:
            # non config-root child
            parent_type = obj_dict['parent_type']
            parent_method_type = parent_type.replace('-', '_')
            parent_fq_name = obj_dict['fq_name'][:-1]
            obj_cols['parent_type'] = json.dumps(parent_type)
            parent_uuid = self.fq_name_to_uuid(parent_method_type, parent_fq_name)
            self._create_child(bch, parent_method_type, parent_uuid, 'loadbalancer_healthmonitor', obj_ids['uuid'])

        # Properties
        field = obj_dict.get('loadbalancer_healthmonitor_properties', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'loadbalancer_healthmonitor_properties', field)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            field['created'] = datetime.datetime.utcnow().isoformat()
            field['last_modified'] = field['created']
            self._create_prop(bch, obj_ids['uuid'], 'id_perms', field)

        field = obj_dict.get('display_name', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'display_name', field)


        # References

        bch.insert(obj_ids['uuid'], obj_cols)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(obj_dict['fq_name'])
        fq_name_cols = {utils.encode_string(fq_name_str) + ':' + obj_ids['uuid']: json.dumps(None)}
        self._obj_fq_name_cf.insert('loadbalancer_healthmonitor', fq_name_cols)

        return (True, '')
    #end _cassandra_loadbalancer_healthmonitor_create

    def _cassandra_loadbalancer_healthmonitor_read(self, obj_uuids, field_names = None):
        # if field_names = None, all fields will be read/returned

        obj_uuid_cf = self._obj_uuid_cf

        # optimize for common case of reading non-backref, non-children fields
        # ignoring columns starting from 'b' and 'c' - significant performance
        # impact in scaled setting. e.g. read of project
        if (field_names is None or
            (set(field_names) & (LoadbalancerHealthmonitor.backref_fields | LoadbalancerHealthmonitor.children_fields))):
            # atleast one backref/children field is needed
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_count = 10000000,
                                       include_timestamp = True)
        else: # ignore reading backref + children columns
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_start = 'd',
                                       column_count = 10000000,
                                       include_timestamp = True)

        if (len(obj_uuids) == 1) and not obj_rows:
            raise cfgm_common.exceptions.NoIdError(obj_uuids[0])

        results = []
        for row_key in obj_rows:
            # give chance for zk heartbeat/ping
            gevent.sleep(0)
            obj_uuid = row_key
            obj_cols = obj_rows[obj_uuid]
            result = {}
            result['uuid'] = obj_uuid
            result['fq_name'] = json.loads(obj_cols['fq_name'][0])
            for col_name in obj_cols.keys():
                if self._re_match_parent.match(col_name):
                    # non config-root child
                    (_, _, parent_uuid) = col_name.split(':')
                    parent_type = json.loads(obj_cols['parent_type'][0])
                    result['parent_type'] = parent_type
                    try:
                        result['parent_uuid'] = parent_uuid
                        result['parent_href'] = self._generate_url(parent_type, parent_uuid)
                    except cfgm_common.exceptions.NoIdError:
                        err_msg = 'Unknown uuid for parent ' + result['fq_name'][-2]
                        return (False, err_msg)

                # TODO use compiled RE
                if self._re_match_prop.match(col_name):
                    (_, prop_name) = col_name.split(':')
                    result[prop_name] = json.loads(obj_cols[col_name][0])

                # TODO use compiled RE
                if self._re_match_children.match(col_name):
                    (_, child_type, child_uuid) = col_name.split(':')
                    if field_names and '%ss' %(child_type) not in field_names:
                        continue

                    child_tstamp = obj_cols[col_name][1]
                    try:
                        self._read_child(result, obj_uuid, child_type, child_uuid, child_tstamp)
                    except cfgm_common.exceptions.NoIdError:
                        continue

                # TODO use compiled RE
                if self._re_match_ref.match(col_name):
                    (_, ref_type, ref_uuid) = col_name.split(':')
                    self._read_ref(result, obj_uuid, ref_type, ref_uuid, obj_cols[col_name][0])

                if self._re_match_backref.match(col_name):
                    (_, back_ref_type, back_ref_uuid) = col_name.split(':')
                    if field_names and '%s_back_refs' %(back_ref_type) not in field_names:
                        continue

                    try:
                        self._read_back_ref(result, obj_uuid, back_ref_type, back_ref_uuid,
                                            obj_cols[col_name][0])
                    except cfgm_common.exceptions.NoIdError:
                        continue

            # for all column names


            results.append(result)
        # end for all rows

        return (True, results)
    #end _cassandra_loadbalancer_healthmonitor_read

    def _cassandra_loadbalancer_healthmonitor_count_children(self, obj_uuid, child_type):
        # if child_type = None, return
        if child_type is None:
            return (False, '')

        obj_uuid_cf = self._obj_uuid_cf
        if child_type not in LoadbalancerHealthmonitor.children_fields:
            return (False, '%s is not a valid children of LoadbalancerHealthmonitor' %(child_type))

        col_start = 'children:'+child_type[:-1]+':'
        col_finish = 'children:'+child_type[:-1]+';'
        num_children = obj_uuid_cf.get_count(obj_uuid,
                                   column_start = col_start,
                                   column_finish = col_finish,
                                   max_count = 10000000)
        return (True, num_children)
    #end _cassandra_loadbalancer_healthmonitor_count_children

    def _cassandra_loadbalancer_healthmonitor_update(self, obj_uuid, new_obj_dict):
        # Grab ref-uuids and properties in new version
        new_ref_infos = {}

        new_props = {}
        if 'loadbalancer_healthmonitor_properties' in new_obj_dict:
            new_props['loadbalancer_healthmonitor_properties'] = new_obj_dict['loadbalancer_healthmonitor_properties']
        if 'id_perms' in new_obj_dict:
            new_props['id_perms'] = new_obj_dict['id_perms']
        if 'display_name' in new_obj_dict:
            new_props['display_name'] = new_obj_dict['display_name']
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        obj_uuid_cf = self._obj_uuid_cf
        obj_cols_iter = obj_uuid_cf.xget(obj_uuid)
        # TODO optimize this (converts tuple to dict)
        obj_cols = {}
        for col_info in obj_cols_iter:
            obj_cols[col_info[0]] = col_info[1]

        bch = obj_uuid_cf.batch()
        for col_name in obj_cols.keys():
            # TODO use compiled RE
            if re.match('prop:', col_name):
                (_, prop_name) = col_name.split(':')
                if prop_name == 'id_perms':
                    # id-perms always has to be updated for last-mod timestamp
                    # get it from request dict(or from db if not in request dict)
                    new_id_perms = new_obj_dict.get(prop_name, json.loads(obj_cols[col_name]))
                    self.update_last_modified(bch, obj_uuid, new_id_perms)
                elif prop_name in new_obj_dict:
                    self._update_prop(bch, obj_uuid, prop_name, new_props)

            # TODO use compiled RE
            if re.match('ref:', col_name):
                (_, ref_type, ref_uuid) = col_name.split(':')
                self._update_ref(bch, 'loadbalancer_healthmonitor', obj_uuid, ref_type, ref_uuid, new_ref_infos)
        # for all column names

        # create new refs
        for ref_type in new_ref_infos.keys():
            for ref_uuid in new_ref_infos[ref_type].keys():
                ref_data = new_ref_infos[ref_type][ref_uuid]
                self._create_ref(bch, 'loadbalancer_healthmonitor', obj_uuid, ref_type, ref_uuid, ref_data)

        # create new props
        for prop_name in new_props.keys():
            self._create_prop(bch, obj_uuid, prop_name, new_props[prop_name])

        bch.send()

        return (True, '')
    #end _cassandra_loadbalancer_healthmonitor_update

    def _cassandra_loadbalancer_healthmonitor_list(self, parent_uuids=None, back_ref_uuids=None,
                           obj_uuids=None, count=False, filters=None):
        children_fq_names_uuids = []
        if filters:
            fnames = filters.get('field_names', [])
            fvalues = filters.get('field_values', [])
            filter_fields = [(fnames[i], fvalues[i]) for i in range(len(fnames))]
        else:
            filter_fields = []

        def filter_rows(coll_infos, filter_cols, filter_params):
            filt_infos = {}
            coll_rows = obj_uuid_cf.multiget(coll_infos.keys(),
                                   columns=filter_cols,
                                   column_count=10000000)
            for row in coll_rows:
                # give chance for zk heartbeat/ping
                gevent.sleep(0)
                full_match = True
                for fname, fval in filter_params:
                    if coll_rows[row]['prop:%s' %(fname)] != fval:
                        full_match = False
                        break
                if full_match:
                    filt_infos[row] = coll_infos[row]
            return filt_infos
        # end filter_rows

        def get_fq_name_uuid_list(obj_uuids):
            ret_list = []
            for obj_uuid in obj_uuids:
                try:
                    obj_fq_name = self.uuid_to_fq_name(obj_uuid)
                    ret_list.append((obj_fq_name, obj_uuid))
                except cfgm_common.exceptions.NoIdError:
                    pass
            return ret_list
        # end get_fq_name_uuid_list

        if parent_uuids:
            # go from parent to child
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'children:loadbalancer_healthmonitor:'
            col_fin = 'children:loadbalancer_healthmonitor;'
            try:
                obj_rows = obj_uuid_cf.multiget(parent_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_parent_anchor(sort=False):
                # flatten to [('children:<type>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_child_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    child_uuid = col_name.split(':')[2]
                    if obj_uuids and child_uuid not in obj_uuids:
                        continue
                    all_child_infos[child_uuid] = {'uuid': child_uuid, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_child_infos = filter_rows(all_child_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_child_infos = all_child_infos

                if not sort:
                    ret_child_infos = filt_child_infos.values()
                else:
                    ret_child_infos = sorted(filt_child_infos.values(), key=itemgetter('tstamp'))

                return get_fq_name_uuid_list(r['uuid'] for r in ret_child_infos)
            # end filter_rows_parent_anchor

            if count:
                return (True, len(filter_rows_parent_anchor()))

            children_fq_names_uuids = filter_rows_parent_anchor(sort=True)

        if back_ref_uuids:
            # go from anchor to backrefs
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'backref:loadbalancer_healthmonitor:'
            col_fin = 'backref:loadbalancer_healthmonitor;'
            try:
                obj_rows = obj_uuid_cf.multiget(back_ref_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_backref_anchor():
                # flatten to [('<fqnstr>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_backref_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    col_name_arr = col_name.split(':')
                    fq_name = col_name_arr[:-1]
                    obj_uuid = col_name_arr[-1]
                    if obj_uuids and obj_uuid not in obj_uuids:
                        continue
                    all_backref_infos[obj_uuid] = \
                        {'uuid': obj_uuid, 'fq_name': fq_name, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_backref_infos = filter_rows(all_backref_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_backref_infos = all_backref_infos

                return [(br_info['fq_name'], br_info['uuid']) for br_info in filt_backref_infos.values()]
            # end filter_rows_backref_anchor

            if count:
                return (True, len(filter_rows_backref_anchor()))

            children_fq_names_uuids = filter_rows_backref_anchor()

        if not parent_uuids and not back_ref_uuids:
            obj_uuid_cf = self._obj_uuid_cf
            if obj_uuids:
                # exact objects specified
                def filter_rows_object_list():
                    all_obj_infos = {}
                    for obj_uuid in obj_uuids:
                        all_obj_infos[obj_uuid] = None

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return get_fq_name_uuid_list(filt_obj_infos.keys())
                # end filter_rows_object_list

                if count:
                    return (True, len(filter_rows_object_list()))
                children_fq_names_uuids = filter_rows_object_list()

            else: # grab all resources of this type
                obj_fq_name_cf = self._obj_fq_name_cf
                try:
                    cols = obj_fq_name_cf.get('loadbalancer_healthmonitor', column_count = 10000000)
                except pycassa.NotFoundException:
                    if count:
                        return (True, 0)
                    else:
                        return (True, children_fq_names_uuids)

                def filter_rows_no_anchor():
                    all_obj_infos = {}
                    for col_name, col_val in cols.items():
                        # give chance for zk heartbeat/ping
                        gevent.sleep(0)
                        col_name_arr = utils.decode_string(col_name).split(':')
                        obj_uuid = col_name_arr[-1]
                        all_obj_infos[obj_uuid] = (col_name_arr[:-1], obj_uuid)

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return filt_obj_infos.values()
                # end filter_rows_no_anchor

                if count:
                    return (True, len(filter_rows_no_anchor()))

                children_fq_names_uuids = filter_rows_no_anchor()

        return (True, children_fq_names_uuids)
    #end _cassandra_loadbalancer_healthmonitor_list

    def _cassandra_loadbalancer_healthmonitor_delete(self, obj_uuid):
        obj_uuid_cf = self._obj_uuid_cf
        fq_name = json.loads(obj_uuid_cf.get(obj_uuid, columns = ['fq_name'])['fq_name'])
        bch = obj_uuid_cf.batch()

        # unlink from parent
        col_start = 'parent:'
        col_fin = 'parent;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, parent_type, parent_uuid) = col_name.split(':')
            self._delete_child(bch, parent_type, parent_uuid, 'loadbalancer_healthmonitor', obj_uuid)

        # remove refs
        col_start = 'ref:'
        col_fin = 'ref;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, ref_type, ref_uuid) = col_name.split(':')
            self._delete_ref(bch, 'loadbalancer_healthmonitor', obj_uuid, ref_type, ref_uuid)

        bch.remove(obj_uuid)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(fq_name)
        fq_name_col = utils.encode_string(fq_name_str) + ':' + obj_uuid
        self._obj_fq_name_cf.remove('loadbalancer_healthmonitor', columns = [fq_name_col])


        return (True, '')
    #end _cassandra_loadbalancer_healthmonitor_delete

    def _cassandra_virtual_network_alloc(self, fq_name):
        return (True, '')
    #end _cassandra_virtual_network_alloc

    def _cassandra_virtual_network_create(self, obj_ids, obj_dict):
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        bch = self._obj_uuid_cf.batch()

        obj_cols = {}
        obj_cols['fq_name'] = json.dumps(obj_dict['fq_name'])
        obj_cols['type'] = json.dumps('virtual_network')
        if 'parent_type' in obj_dict:
            # non config-root child
            parent_type = obj_dict['parent_type']
            parent_method_type = parent_type.replace('-', '_')
            parent_fq_name = obj_dict['fq_name'][:-1]
            obj_cols['parent_type'] = json.dumps(parent_type)
            parent_uuid = self.fq_name_to_uuid(parent_method_type, parent_fq_name)
            self._create_child(bch, parent_method_type, parent_uuid, 'virtual_network', obj_ids['uuid'])

        # Properties
        field = obj_dict.get('virtual_network_properties', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'virtual_network_properties', field)

        field = obj_dict.get('virtual_network_network_id', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'virtual_network_network_id', field)

        field = obj_dict.get('route_target_list', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'route_target_list', field)

        field = obj_dict.get('router_external', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'router_external', field)

        field = obj_dict.get('is_shared', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'is_shared', field)

        field = obj_dict.get('external_ipam', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'external_ipam', field)

        field = obj_dict.get('flood_unknown_unicast', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'flood_unknown_unicast', field)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            field['created'] = datetime.datetime.utcnow().isoformat()
            field['last_modified'] = field['created']
            self._create_prop(bch, obj_ids['uuid'], 'id_perms', field)

        field = obj_dict.get('display_name', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'display_name', field)


        # References
        refs = obj_dict.get('qos_forwarding_class_refs', [])
        for ref in refs:
            ref_uuid = self.fq_name_to_uuid('qos_forwarding_class', ref['to'])
            ref_attr = ref.get('attr', None)
            ref_data = {'attr': ref_attr, 'is_weakref': False}
            self._create_ref(bch, 'virtual_network', obj_ids['uuid'], 'qos_forwarding_class', ref_uuid, ref_data)
        refs = obj_dict.get('network_ipam_refs', [])
        for ref in refs:
            ref_uuid = self.fq_name_to_uuid('network_ipam', ref['to'])
            ref_attr = ref.get('attr', None)
            ref_data = {'attr': ref_attr, 'is_weakref': False}
            self._create_ref(bch, 'virtual_network', obj_ids['uuid'], 'network_ipam', ref_uuid, ref_data)
        refs = obj_dict.get('network_policy_refs', [])
        for ref in refs:
            ref_uuid = self.fq_name_to_uuid('network_policy', ref['to'])
            ref_attr = ref.get('attr', None)
            ref_data = {'attr': ref_attr, 'is_weakref': False}
            self._create_ref(bch, 'virtual_network', obj_ids['uuid'], 'network_policy', ref_uuid, ref_data)
        refs = obj_dict.get('route_table_refs', [])
        for ref in refs:
            ref_uuid = self.fq_name_to_uuid('route_table', ref['to'])
            ref_attr = ref.get('attr', None)
            ref_data = {'attr': ref_attr, 'is_weakref': False}
            self._create_ref(bch, 'virtual_network', obj_ids['uuid'], 'route_table', ref_uuid, ref_data)

        bch.insert(obj_ids['uuid'], obj_cols)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(obj_dict['fq_name'])
        fq_name_cols = {utils.encode_string(fq_name_str) + ':' + obj_ids['uuid']: json.dumps(None)}
        self._obj_fq_name_cf.insert('virtual_network', fq_name_cols)

        return (True, '')
    #end _cassandra_virtual_network_create

    def _cassandra_virtual_network_read(self, obj_uuids, field_names = None):
        # if field_names = None, all fields will be read/returned

        obj_uuid_cf = self._obj_uuid_cf

        # optimize for common case of reading non-backref, non-children fields
        # ignoring columns starting from 'b' and 'c' - significant performance
        # impact in scaled setting. e.g. read of project
        if (field_names is None or
            (set(field_names) & (VirtualNetwork.backref_fields | VirtualNetwork.children_fields))):
            # atleast one backref/children field is needed
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_count = 10000000,
                                       include_timestamp = True)
        else: # ignore reading backref + children columns
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_start = 'd',
                                       column_count = 10000000,
                                       include_timestamp = True)

        if (len(obj_uuids) == 1) and not obj_rows:
            raise cfgm_common.exceptions.NoIdError(obj_uuids[0])

        results = []
        for row_key in obj_rows:
            # give chance for zk heartbeat/ping
            gevent.sleep(0)
            obj_uuid = row_key
            obj_cols = obj_rows[obj_uuid]
            result = {}
            result['uuid'] = obj_uuid
            result['fq_name'] = json.loads(obj_cols['fq_name'][0])
            for col_name in obj_cols.keys():
                if self._re_match_parent.match(col_name):
                    # non config-root child
                    (_, _, parent_uuid) = col_name.split(':')
                    parent_type = json.loads(obj_cols['parent_type'][0])
                    result['parent_type'] = parent_type
                    try:
                        result['parent_uuid'] = parent_uuid
                        result['parent_href'] = self._generate_url(parent_type, parent_uuid)
                    except cfgm_common.exceptions.NoIdError:
                        err_msg = 'Unknown uuid for parent ' + result['fq_name'][-2]
                        return (False, err_msg)

                # TODO use compiled RE
                if self._re_match_prop.match(col_name):
                    (_, prop_name) = col_name.split(':')
                    result[prop_name] = json.loads(obj_cols[col_name][0])

                # TODO use compiled RE
                if self._re_match_children.match(col_name):
                    (_, child_type, child_uuid) = col_name.split(':')
                    if field_names and '%ss' %(child_type) not in field_names:
                        continue

                    child_tstamp = obj_cols[col_name][1]
                    try:
                        self._read_child(result, obj_uuid, child_type, child_uuid, child_tstamp)
                    except cfgm_common.exceptions.NoIdError:
                        continue

                # TODO use compiled RE
                if self._re_match_ref.match(col_name):
                    (_, ref_type, ref_uuid) = col_name.split(':')
                    self._read_ref(result, obj_uuid, ref_type, ref_uuid, obj_cols[col_name][0])

                if self._re_match_backref.match(col_name):
                    (_, back_ref_type, back_ref_uuid) = col_name.split(':')
                    if field_names and '%s_back_refs' %(back_ref_type) not in field_names:
                        continue

                    try:
                        self._read_back_ref(result, obj_uuid, back_ref_type, back_ref_uuid,
                                            obj_cols[col_name][0])
                    except cfgm_common.exceptions.NoIdError:
                        continue

            # for all column names

            if 'access_control_lists' in result:
                # sort children; TODO do this based on schema
                sorted_children = sorted(result['access_control_lists'], key = itemgetter('tstamp'))
                # re-write result's children without timestamp
                result['access_control_lists'] = sorted_children
                [child.pop('tstamp') for child in result['access_control_lists']]

            if 'floating_ip_pools' in result:
                # sort children; TODO do this based on schema
                sorted_children = sorted(result['floating_ip_pools'], key = itemgetter('tstamp'))
                # re-write result's children without timestamp
                result['floating_ip_pools'] = sorted_children
                [child.pop('tstamp') for child in result['floating_ip_pools']]

            if 'routing_instances' in result:
                # sort children; TODO do this based on schema
                sorted_children = sorted(result['routing_instances'], key = itemgetter('tstamp'))
                # re-write result's children without timestamp
                result['routing_instances'] = sorted_children
                [child.pop('tstamp') for child in result['routing_instances']]

            results.append(result)
        # end for all rows

        return (True, results)
    #end _cassandra_virtual_network_read

    def _cassandra_virtual_network_count_children(self, obj_uuid, child_type):
        # if child_type = None, return
        if child_type is None:
            return (False, '')

        obj_uuid_cf = self._obj_uuid_cf
        if child_type not in VirtualNetwork.children_fields:
            return (False, '%s is not a valid children of VirtualNetwork' %(child_type))

        col_start = 'children:'+child_type[:-1]+':'
        col_finish = 'children:'+child_type[:-1]+';'
        num_children = obj_uuid_cf.get_count(obj_uuid,
                                   column_start = col_start,
                                   column_finish = col_finish,
                                   max_count = 10000000)
        return (True, num_children)
    #end _cassandra_virtual_network_count_children

    def _cassandra_virtual_network_update(self, obj_uuid, new_obj_dict):
        # Grab ref-uuids and properties in new version
        new_ref_infos = {}

        if 'qos_forwarding_class_refs' in new_obj_dict:
            new_ref_infos['qos_forwarding_class'] = {}
            new_refs = new_obj_dict['qos_forwarding_class_refs']
            if new_refs:
                for new_ref in new_refs:
                    new_ref_uuid = self.fq_name_to_uuid('qos_forwarding_class', new_ref['to'])
                    new_ref_attr = new_ref.get('attr', None)
                    new_ref_data = {'attr': new_ref_attr, 'is_weakref': False}
                    new_ref_infos['qos_forwarding_class'][new_ref_uuid] = new_ref_data

        if 'network_ipam_refs' in new_obj_dict:
            new_ref_infos['network_ipam'] = {}
            new_refs = new_obj_dict['network_ipam_refs']
            if new_refs:
                for new_ref in new_refs:
                    new_ref_uuid = self.fq_name_to_uuid('network_ipam', new_ref['to'])
                    new_ref_attr = new_ref.get('attr', None)
                    new_ref_data = {'attr': new_ref_attr, 'is_weakref': False}
                    new_ref_infos['network_ipam'][new_ref_uuid] = new_ref_data

        if 'network_policy_refs' in new_obj_dict:
            new_ref_infos['network_policy'] = {}
            new_refs = new_obj_dict['network_policy_refs']
            if new_refs:
                for new_ref in new_refs:
                    new_ref_uuid = self.fq_name_to_uuid('network_policy', new_ref['to'])
                    new_ref_attr = new_ref.get('attr', None)
                    new_ref_data = {'attr': new_ref_attr, 'is_weakref': False}
                    new_ref_infos['network_policy'][new_ref_uuid] = new_ref_data

        if 'route_table_refs' in new_obj_dict:
            new_ref_infos['route_table'] = {}
            new_refs = new_obj_dict['route_table_refs']
            if new_refs:
                for new_ref in new_refs:
                    new_ref_uuid = self.fq_name_to_uuid('route_table', new_ref['to'])
                    new_ref_attr = new_ref.get('attr', None)
                    new_ref_data = {'attr': new_ref_attr, 'is_weakref': False}
                    new_ref_infos['route_table'][new_ref_uuid] = new_ref_data

        new_props = {}
        if 'virtual_network_properties' in new_obj_dict:
            new_props['virtual_network_properties'] = new_obj_dict['virtual_network_properties']
        if 'virtual_network_network_id' in new_obj_dict:
            new_props['virtual_network_network_id'] = new_obj_dict['virtual_network_network_id']
        if 'route_target_list' in new_obj_dict:
            new_props['route_target_list'] = new_obj_dict['route_target_list']
        if 'router_external' in new_obj_dict:
            new_props['router_external'] = new_obj_dict['router_external']
        if 'is_shared' in new_obj_dict:
            new_props['is_shared'] = new_obj_dict['is_shared']
        if 'external_ipam' in new_obj_dict:
            new_props['external_ipam'] = new_obj_dict['external_ipam']
        if 'flood_unknown_unicast' in new_obj_dict:
            new_props['flood_unknown_unicast'] = new_obj_dict['flood_unknown_unicast']
        if 'id_perms' in new_obj_dict:
            new_props['id_perms'] = new_obj_dict['id_perms']
        if 'display_name' in new_obj_dict:
            new_props['display_name'] = new_obj_dict['display_name']
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        obj_uuid_cf = self._obj_uuid_cf
        obj_cols_iter = obj_uuid_cf.xget(obj_uuid)
        # TODO optimize this (converts tuple to dict)
        obj_cols = {}
        for col_info in obj_cols_iter:
            obj_cols[col_info[0]] = col_info[1]

        bch = obj_uuid_cf.batch()
        for col_name in obj_cols.keys():
            # TODO use compiled RE
            if re.match('prop:', col_name):
                (_, prop_name) = col_name.split(':')
                if prop_name == 'id_perms':
                    # id-perms always has to be updated for last-mod timestamp
                    # get it from request dict(or from db if not in request dict)
                    new_id_perms = new_obj_dict.get(prop_name, json.loads(obj_cols[col_name]))
                    self.update_last_modified(bch, obj_uuid, new_id_perms)
                elif prop_name in new_obj_dict:
                    self._update_prop(bch, obj_uuid, prop_name, new_props)

            # TODO use compiled RE
            if re.match('ref:', col_name):
                (_, ref_type, ref_uuid) = col_name.split(':')
                self._update_ref(bch, 'virtual_network', obj_uuid, ref_type, ref_uuid, new_ref_infos)
        # for all column names

        # create new refs
        for ref_type in new_ref_infos.keys():
            for ref_uuid in new_ref_infos[ref_type].keys():
                ref_data = new_ref_infos[ref_type][ref_uuid]
                self._create_ref(bch, 'virtual_network', obj_uuid, ref_type, ref_uuid, ref_data)

        # create new props
        for prop_name in new_props.keys():
            self._create_prop(bch, obj_uuid, prop_name, new_props[prop_name])

        bch.send()

        return (True, '')
    #end _cassandra_virtual_network_update

    def _cassandra_virtual_network_list(self, parent_uuids=None, back_ref_uuids=None,
                           obj_uuids=None, count=False, filters=None):
        children_fq_names_uuids = []
        if filters:
            fnames = filters.get('field_names', [])
            fvalues = filters.get('field_values', [])
            filter_fields = [(fnames[i], fvalues[i]) for i in range(len(fnames))]
        else:
            filter_fields = []

        def filter_rows(coll_infos, filter_cols, filter_params):
            filt_infos = {}
            coll_rows = obj_uuid_cf.multiget(coll_infos.keys(),
                                   columns=filter_cols,
                                   column_count=10000000)
            for row in coll_rows:
                # give chance for zk heartbeat/ping
                gevent.sleep(0)
                full_match = True
                for fname, fval in filter_params:
                    if coll_rows[row]['prop:%s' %(fname)] != fval:
                        full_match = False
                        break
                if full_match:
                    filt_infos[row] = coll_infos[row]
            return filt_infos
        # end filter_rows

        def get_fq_name_uuid_list(obj_uuids):
            ret_list = []
            for obj_uuid in obj_uuids:
                try:
                    obj_fq_name = self.uuid_to_fq_name(obj_uuid)
                    ret_list.append((obj_fq_name, obj_uuid))
                except cfgm_common.exceptions.NoIdError:
                    pass
            return ret_list
        # end get_fq_name_uuid_list

        if parent_uuids:
            # go from parent to child
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'children:virtual_network:'
            col_fin = 'children:virtual_network;'
            try:
                obj_rows = obj_uuid_cf.multiget(parent_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_parent_anchor(sort=False):
                # flatten to [('children:<type>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_child_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    child_uuid = col_name.split(':')[2]
                    if obj_uuids and child_uuid not in obj_uuids:
                        continue
                    all_child_infos[child_uuid] = {'uuid': child_uuid, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_child_infos = filter_rows(all_child_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_child_infos = all_child_infos

                if not sort:
                    ret_child_infos = filt_child_infos.values()
                else:
                    ret_child_infos = sorted(filt_child_infos.values(), key=itemgetter('tstamp'))

                return get_fq_name_uuid_list(r['uuid'] for r in ret_child_infos)
            # end filter_rows_parent_anchor

            if count:
                return (True, len(filter_rows_parent_anchor()))

            children_fq_names_uuids = filter_rows_parent_anchor(sort=True)

        if back_ref_uuids:
            # go from anchor to backrefs
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'backref:virtual_network:'
            col_fin = 'backref:virtual_network;'
            try:
                obj_rows = obj_uuid_cf.multiget(back_ref_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_backref_anchor():
                # flatten to [('<fqnstr>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_backref_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    col_name_arr = col_name.split(':')
                    fq_name = col_name_arr[:-1]
                    obj_uuid = col_name_arr[-1]
                    if obj_uuids and obj_uuid not in obj_uuids:
                        continue
                    all_backref_infos[obj_uuid] = \
                        {'uuid': obj_uuid, 'fq_name': fq_name, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_backref_infos = filter_rows(all_backref_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_backref_infos = all_backref_infos

                return [(br_info['fq_name'], br_info['uuid']) for br_info in filt_backref_infos.values()]
            # end filter_rows_backref_anchor

            if count:
                return (True, len(filter_rows_backref_anchor()))

            children_fq_names_uuids = filter_rows_backref_anchor()

        if not parent_uuids and not back_ref_uuids:
            obj_uuid_cf = self._obj_uuid_cf
            if obj_uuids:
                # exact objects specified
                def filter_rows_object_list():
                    all_obj_infos = {}
                    for obj_uuid in obj_uuids:
                        all_obj_infos[obj_uuid] = None

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return get_fq_name_uuid_list(filt_obj_infos.keys())
                # end filter_rows_object_list

                if count:
                    return (True, len(filter_rows_object_list()))
                children_fq_names_uuids = filter_rows_object_list()

            else: # grab all resources of this type
                obj_fq_name_cf = self._obj_fq_name_cf
                try:
                    cols = obj_fq_name_cf.get('virtual_network', column_count = 10000000)
                except pycassa.NotFoundException:
                    if count:
                        return (True, 0)
                    else:
                        return (True, children_fq_names_uuids)

                def filter_rows_no_anchor():
                    all_obj_infos = {}
                    for col_name, col_val in cols.items():
                        # give chance for zk heartbeat/ping
                        gevent.sleep(0)
                        col_name_arr = utils.decode_string(col_name).split(':')
                        obj_uuid = col_name_arr[-1]
                        all_obj_infos[obj_uuid] = (col_name_arr[:-1], obj_uuid)

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return filt_obj_infos.values()
                # end filter_rows_no_anchor

                if count:
                    return (True, len(filter_rows_no_anchor()))

                children_fq_names_uuids = filter_rows_no_anchor()

        return (True, children_fq_names_uuids)
    #end _cassandra_virtual_network_list

    def _cassandra_virtual_network_delete(self, obj_uuid):
        obj_uuid_cf = self._obj_uuid_cf
        fq_name = json.loads(obj_uuid_cf.get(obj_uuid, columns = ['fq_name'])['fq_name'])
        bch = obj_uuid_cf.batch()

        # unlink from parent
        col_start = 'parent:'
        col_fin = 'parent;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, parent_type, parent_uuid) = col_name.split(':')
            self._delete_child(bch, parent_type, parent_uuid, 'virtual_network', obj_uuid)

        # remove refs
        col_start = 'ref:'
        col_fin = 'ref;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, ref_type, ref_uuid) = col_name.split(':')
            self._delete_ref(bch, 'virtual_network', obj_uuid, ref_type, ref_uuid)

        bch.remove(obj_uuid)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(fq_name)
        fq_name_col = utils.encode_string(fq_name_str) + ':' + obj_uuid
        self._obj_fq_name_cf.remove('virtual_network', columns = [fq_name_col])


        return (True, '')
    #end _cassandra_virtual_network_delete

    def _cassandra_project_alloc(self, fq_name):
        return (True, '')
    #end _cassandra_project_alloc

    def _cassandra_project_create(self, obj_ids, obj_dict):
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        bch = self._obj_uuid_cf.batch()

        obj_cols = {}
        obj_cols['fq_name'] = json.dumps(obj_dict['fq_name'])
        obj_cols['type'] = json.dumps('project')
        if 'parent_type' in obj_dict:
            # non config-root child
            parent_type = obj_dict['parent_type']
            parent_method_type = parent_type.replace('-', '_')
            parent_fq_name = obj_dict['fq_name'][:-1]
            obj_cols['parent_type'] = json.dumps(parent_type)
            parent_uuid = self.fq_name_to_uuid(parent_method_type, parent_fq_name)
            self._create_child(bch, parent_method_type, parent_uuid, 'project', obj_ids['uuid'])

        # Properties
        field = obj_dict.get('quota', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'quota', field)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            field['created'] = datetime.datetime.utcnow().isoformat()
            field['last_modified'] = field['created']
            self._create_prop(bch, obj_ids['uuid'], 'id_perms', field)

        field = obj_dict.get('display_name', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'display_name', field)


        # References
        refs = obj_dict.get('namespace_refs', [])
        for ref in refs:
            ref_uuid = self.fq_name_to_uuid('namespace', ref['to'])
            ref_attr = ref.get('attr', None)
            ref_data = {'attr': ref_attr, 'is_weakref': False}
            self._create_ref(bch, 'project', obj_ids['uuid'], 'namespace', ref_uuid, ref_data)
        refs = obj_dict.get('floating_ip_pool_refs', [])
        for ref in refs:
            ref_uuid = self.fq_name_to_uuid('floating_ip_pool', ref['to'])
            ref_attr = ref.get('attr', None)
            ref_data = {'attr': ref_attr, 'is_weakref': False}
            self._create_ref(bch, 'project', obj_ids['uuid'], 'floating_ip_pool', ref_uuid, ref_data)

        bch.insert(obj_ids['uuid'], obj_cols)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(obj_dict['fq_name'])
        fq_name_cols = {utils.encode_string(fq_name_str) + ':' + obj_ids['uuid']: json.dumps(None)}
        self._obj_fq_name_cf.insert('project', fq_name_cols)

        return (True, '')
    #end _cassandra_project_create

    def _cassandra_project_read(self, obj_uuids, field_names = None):
        # if field_names = None, all fields will be read/returned

        obj_uuid_cf = self._obj_uuid_cf

        # optimize for common case of reading non-backref, non-children fields
        # ignoring columns starting from 'b' and 'c' - significant performance
        # impact in scaled setting. e.g. read of project
        if (field_names is None or
            (set(field_names) & (Project.backref_fields | Project.children_fields))):
            # atleast one backref/children field is needed
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_count = 10000000,
                                       include_timestamp = True)
        else: # ignore reading backref + children columns
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_start = 'd',
                                       column_count = 10000000,
                                       include_timestamp = True)

        if (len(obj_uuids) == 1) and not obj_rows:
            raise cfgm_common.exceptions.NoIdError(obj_uuids[0])

        results = []
        for row_key in obj_rows:
            # give chance for zk heartbeat/ping
            gevent.sleep(0)
            obj_uuid = row_key
            obj_cols = obj_rows[obj_uuid]
            result = {}
            result['uuid'] = obj_uuid
            result['fq_name'] = json.loads(obj_cols['fq_name'][0])
            for col_name in obj_cols.keys():
                if self._re_match_parent.match(col_name):
                    # non config-root child
                    (_, _, parent_uuid) = col_name.split(':')
                    parent_type = json.loads(obj_cols['parent_type'][0])
                    result['parent_type'] = parent_type
                    try:
                        result['parent_uuid'] = parent_uuid
                        result['parent_href'] = self._generate_url(parent_type, parent_uuid)
                    except cfgm_common.exceptions.NoIdError:
                        err_msg = 'Unknown uuid for parent ' + result['fq_name'][-2]
                        return (False, err_msg)

                # TODO use compiled RE
                if self._re_match_prop.match(col_name):
                    (_, prop_name) = col_name.split(':')
                    result[prop_name] = json.loads(obj_cols[col_name][0])

                # TODO use compiled RE
                if self._re_match_children.match(col_name):
                    (_, child_type, child_uuid) = col_name.split(':')
                    if field_names and '%ss' %(child_type) not in field_names:
                        continue

                    child_tstamp = obj_cols[col_name][1]
                    try:
                        self._read_child(result, obj_uuid, child_type, child_uuid, child_tstamp)
                    except cfgm_common.exceptions.NoIdError:
                        continue

                # TODO use compiled RE
                if self._re_match_ref.match(col_name):
                    (_, ref_type, ref_uuid) = col_name.split(':')
                    self._read_ref(result, obj_uuid, ref_type, ref_uuid, obj_cols[col_name][0])

                if self._re_match_backref.match(col_name):
                    (_, back_ref_type, back_ref_uuid) = col_name.split(':')
                    if field_names and '%s_back_refs' %(back_ref_type) not in field_names:
                        continue

                    try:
                        self._read_back_ref(result, obj_uuid, back_ref_type, back_ref_uuid,
                                            obj_cols[col_name][0])
                    except cfgm_common.exceptions.NoIdError:
                        continue

            # for all column names

            if 'security_groups' in result:
                # sort children; TODO do this based on schema
                sorted_children = sorted(result['security_groups'], key = itemgetter('tstamp'))
                # re-write result's children without timestamp
                result['security_groups'] = sorted_children
                [child.pop('tstamp') for child in result['security_groups']]

            if 'virtual_networks' in result:
                # sort children; TODO do this based on schema
                sorted_children = sorted(result['virtual_networks'], key = itemgetter('tstamp'))
                # re-write result's children without timestamp
                result['virtual_networks'] = sorted_children
                [child.pop('tstamp') for child in result['virtual_networks']]

            if 'qos_queues' in result:
                # sort children; TODO do this based on schema
                sorted_children = sorted(result['qos_queues'], key = itemgetter('tstamp'))
                # re-write result's children without timestamp
                result['qos_queues'] = sorted_children
                [child.pop('tstamp') for child in result['qos_queues']]

            if 'qos_forwarding_classs' in result:
                # sort children; TODO do this based on schema
                sorted_children = sorted(result['qos_forwarding_classs'], key = itemgetter('tstamp'))
                # re-write result's children without timestamp
                result['qos_forwarding_classs'] = sorted_children
                [child.pop('tstamp') for child in result['qos_forwarding_classs']]

            if 'network_ipams' in result:
                # sort children; TODO do this based on schema
                sorted_children = sorted(result['network_ipams'], key = itemgetter('tstamp'))
                # re-write result's children without timestamp
                result['network_ipams'] = sorted_children
                [child.pop('tstamp') for child in result['network_ipams']]

            if 'network_policys' in result:
                # sort children; TODO do this based on schema
                sorted_children = sorted(result['network_policys'], key = itemgetter('tstamp'))
                # re-write result's children without timestamp
                result['network_policys'] = sorted_children
                [child.pop('tstamp') for child in result['network_policys']]

            if 'virtual_machine_interfaces' in result:
                # sort children; TODO do this based on schema
                sorted_children = sorted(result['virtual_machine_interfaces'], key = itemgetter('tstamp'))
                # re-write result's children without timestamp
                result['virtual_machine_interfaces'] = sorted_children
                [child.pop('tstamp') for child in result['virtual_machine_interfaces']]

            if 'service_instances' in result:
                # sort children; TODO do this based on schema
                sorted_children = sorted(result['service_instances'], key = itemgetter('tstamp'))
                # re-write result's children without timestamp
                result['service_instances'] = sorted_children
                [child.pop('tstamp') for child in result['service_instances']]

            if 'route_tables' in result:
                # sort children; TODO do this based on schema
                sorted_children = sorted(result['route_tables'], key = itemgetter('tstamp'))
                # re-write result's children without timestamp
                result['route_tables'] = sorted_children
                [child.pop('tstamp') for child in result['route_tables']]

            if 'interface_route_tables' in result:
                # sort children; TODO do this based on schema
                sorted_children = sorted(result['interface_route_tables'], key = itemgetter('tstamp'))
                # re-write result's children without timestamp
                result['interface_route_tables'] = sorted_children
                [child.pop('tstamp') for child in result['interface_route_tables']]

            if 'logical_routers' in result:
                # sort children; TODO do this based on schema
                sorted_children = sorted(result['logical_routers'], key = itemgetter('tstamp'))
                # re-write result's children without timestamp
                result['logical_routers'] = sorted_children
                [child.pop('tstamp') for child in result['logical_routers']]

            if 'loadbalancer_pools' in result:
                # sort children; TODO do this based on schema
                sorted_children = sorted(result['loadbalancer_pools'], key = itemgetter('tstamp'))
                # re-write result's children without timestamp
                result['loadbalancer_pools'] = sorted_children
                [child.pop('tstamp') for child in result['loadbalancer_pools']]

            if 'loadbalancer_healthmonitors' in result:
                # sort children; TODO do this based on schema
                sorted_children = sorted(result['loadbalancer_healthmonitors'], key = itemgetter('tstamp'))
                # re-write result's children without timestamp
                result['loadbalancer_healthmonitors'] = sorted_children
                [child.pop('tstamp') for child in result['loadbalancer_healthmonitors']]

            if 'virtual_ips' in result:
                # sort children; TODO do this based on schema
                sorted_children = sorted(result['virtual_ips'], key = itemgetter('tstamp'))
                # re-write result's children without timestamp
                result['virtual_ips'] = sorted_children
                [child.pop('tstamp') for child in result['virtual_ips']]

            results.append(result)
        # end for all rows

        return (True, results)
    #end _cassandra_project_read

    def _cassandra_project_count_children(self, obj_uuid, child_type):
        # if child_type = None, return
        if child_type is None:
            return (False, '')

        obj_uuid_cf = self._obj_uuid_cf
        if child_type not in Project.children_fields:
            return (False, '%s is not a valid children of Project' %(child_type))

        col_start = 'children:'+child_type[:-1]+':'
        col_finish = 'children:'+child_type[:-1]+';'
        num_children = obj_uuid_cf.get_count(obj_uuid,
                                   column_start = col_start,
                                   column_finish = col_finish,
                                   max_count = 10000000)
        return (True, num_children)
    #end _cassandra_project_count_children

    def _cassandra_project_update(self, obj_uuid, new_obj_dict):
        # Grab ref-uuids and properties in new version
        new_ref_infos = {}

        if 'namespace_refs' in new_obj_dict:
            new_ref_infos['namespace'] = {}
            new_refs = new_obj_dict['namespace_refs']
            if new_refs:
                for new_ref in new_refs:
                    new_ref_uuid = self.fq_name_to_uuid('namespace', new_ref['to'])
                    new_ref_attr = new_ref.get('attr', None)
                    new_ref_data = {'attr': new_ref_attr, 'is_weakref': False}
                    new_ref_infos['namespace'][new_ref_uuid] = new_ref_data

        if 'floating_ip_pool_refs' in new_obj_dict:
            new_ref_infos['floating_ip_pool'] = {}
            new_refs = new_obj_dict['floating_ip_pool_refs']
            if new_refs:
                for new_ref in new_refs:
                    new_ref_uuid = self.fq_name_to_uuid('floating_ip_pool', new_ref['to'])
                    new_ref_attr = new_ref.get('attr', None)
                    new_ref_data = {'attr': new_ref_attr, 'is_weakref': False}
                    new_ref_infos['floating_ip_pool'][new_ref_uuid] = new_ref_data

        new_props = {}
        if 'quota' in new_obj_dict:
            new_props['quota'] = new_obj_dict['quota']
        if 'id_perms' in new_obj_dict:
            new_props['id_perms'] = new_obj_dict['id_perms']
        if 'display_name' in new_obj_dict:
            new_props['display_name'] = new_obj_dict['display_name']
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        obj_uuid_cf = self._obj_uuid_cf
        obj_cols_iter = obj_uuid_cf.xget(obj_uuid)
        # TODO optimize this (converts tuple to dict)
        obj_cols = {}
        for col_info in obj_cols_iter:
            obj_cols[col_info[0]] = col_info[1]

        bch = obj_uuid_cf.batch()
        for col_name in obj_cols.keys():
            # TODO use compiled RE
            if re.match('prop:', col_name):
                (_, prop_name) = col_name.split(':')
                if prop_name == 'id_perms':
                    # id-perms always has to be updated for last-mod timestamp
                    # get it from request dict(or from db if not in request dict)
                    new_id_perms = new_obj_dict.get(prop_name, json.loads(obj_cols[col_name]))
                    self.update_last_modified(bch, obj_uuid, new_id_perms)
                elif prop_name in new_obj_dict:
                    self._update_prop(bch, obj_uuid, prop_name, new_props)

            # TODO use compiled RE
            if re.match('ref:', col_name):
                (_, ref_type, ref_uuid) = col_name.split(':')
                self._update_ref(bch, 'project', obj_uuid, ref_type, ref_uuid, new_ref_infos)
        # for all column names

        # create new refs
        for ref_type in new_ref_infos.keys():
            for ref_uuid in new_ref_infos[ref_type].keys():
                ref_data = new_ref_infos[ref_type][ref_uuid]
                self._create_ref(bch, 'project', obj_uuid, ref_type, ref_uuid, ref_data)

        # create new props
        for prop_name in new_props.keys():
            self._create_prop(bch, obj_uuid, prop_name, new_props[prop_name])

        bch.send()

        return (True, '')
    #end _cassandra_project_update

    def _cassandra_project_list(self, parent_uuids=None, back_ref_uuids=None,
                           obj_uuids=None, count=False, filters=None):
        children_fq_names_uuids = []
        if filters:
            fnames = filters.get('field_names', [])
            fvalues = filters.get('field_values', [])
            filter_fields = [(fnames[i], fvalues[i]) for i in range(len(fnames))]
        else:
            filter_fields = []

        def filter_rows(coll_infos, filter_cols, filter_params):
            filt_infos = {}
            coll_rows = obj_uuid_cf.multiget(coll_infos.keys(),
                                   columns=filter_cols,
                                   column_count=10000000)
            for row in coll_rows:
                # give chance for zk heartbeat/ping
                gevent.sleep(0)
                full_match = True
                for fname, fval in filter_params:
                    if coll_rows[row]['prop:%s' %(fname)] != fval:
                        full_match = False
                        break
                if full_match:
                    filt_infos[row] = coll_infos[row]
            return filt_infos
        # end filter_rows

        def get_fq_name_uuid_list(obj_uuids):
            ret_list = []
            for obj_uuid in obj_uuids:
                try:
                    obj_fq_name = self.uuid_to_fq_name(obj_uuid)
                    ret_list.append((obj_fq_name, obj_uuid))
                except cfgm_common.exceptions.NoIdError:
                    pass
            return ret_list
        # end get_fq_name_uuid_list

        if parent_uuids:
            # go from parent to child
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'children:project:'
            col_fin = 'children:project;'
            try:
                obj_rows = obj_uuid_cf.multiget(parent_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_parent_anchor(sort=False):
                # flatten to [('children:<type>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_child_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    child_uuid = col_name.split(':')[2]
                    if obj_uuids and child_uuid not in obj_uuids:
                        continue
                    all_child_infos[child_uuid] = {'uuid': child_uuid, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_child_infos = filter_rows(all_child_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_child_infos = all_child_infos

                if not sort:
                    ret_child_infos = filt_child_infos.values()
                else:
                    ret_child_infos = sorted(filt_child_infos.values(), key=itemgetter('tstamp'))

                return get_fq_name_uuid_list(r['uuid'] for r in ret_child_infos)
            # end filter_rows_parent_anchor

            if count:
                return (True, len(filter_rows_parent_anchor()))

            children_fq_names_uuids = filter_rows_parent_anchor(sort=True)

        if back_ref_uuids:
            # go from anchor to backrefs
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'backref:project:'
            col_fin = 'backref:project;'
            try:
                obj_rows = obj_uuid_cf.multiget(back_ref_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_backref_anchor():
                # flatten to [('<fqnstr>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_backref_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    col_name_arr = col_name.split(':')
                    fq_name = col_name_arr[:-1]
                    obj_uuid = col_name_arr[-1]
                    if obj_uuids and obj_uuid not in obj_uuids:
                        continue
                    all_backref_infos[obj_uuid] = \
                        {'uuid': obj_uuid, 'fq_name': fq_name, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_backref_infos = filter_rows(all_backref_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_backref_infos = all_backref_infos

                return [(br_info['fq_name'], br_info['uuid']) for br_info in filt_backref_infos.values()]
            # end filter_rows_backref_anchor

            if count:
                return (True, len(filter_rows_backref_anchor()))

            children_fq_names_uuids = filter_rows_backref_anchor()

        if not parent_uuids and not back_ref_uuids:
            obj_uuid_cf = self._obj_uuid_cf
            if obj_uuids:
                # exact objects specified
                def filter_rows_object_list():
                    all_obj_infos = {}
                    for obj_uuid in obj_uuids:
                        all_obj_infos[obj_uuid] = None

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return get_fq_name_uuid_list(filt_obj_infos.keys())
                # end filter_rows_object_list

                if count:
                    return (True, len(filter_rows_object_list()))
                children_fq_names_uuids = filter_rows_object_list()

            else: # grab all resources of this type
                obj_fq_name_cf = self._obj_fq_name_cf
                try:
                    cols = obj_fq_name_cf.get('project', column_count = 10000000)
                except pycassa.NotFoundException:
                    if count:
                        return (True, 0)
                    else:
                        return (True, children_fq_names_uuids)

                def filter_rows_no_anchor():
                    all_obj_infos = {}
                    for col_name, col_val in cols.items():
                        # give chance for zk heartbeat/ping
                        gevent.sleep(0)
                        col_name_arr = utils.decode_string(col_name).split(':')
                        obj_uuid = col_name_arr[-1]
                        all_obj_infos[obj_uuid] = (col_name_arr[:-1], obj_uuid)

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return filt_obj_infos.values()
                # end filter_rows_no_anchor

                if count:
                    return (True, len(filter_rows_no_anchor()))

                children_fq_names_uuids = filter_rows_no_anchor()

        return (True, children_fq_names_uuids)
    #end _cassandra_project_list

    def _cassandra_project_delete(self, obj_uuid):
        obj_uuid_cf = self._obj_uuid_cf
        fq_name = json.loads(obj_uuid_cf.get(obj_uuid, columns = ['fq_name'])['fq_name'])
        bch = obj_uuid_cf.batch()

        # unlink from parent
        col_start = 'parent:'
        col_fin = 'parent;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, parent_type, parent_uuid) = col_name.split(':')
            self._delete_child(bch, parent_type, parent_uuid, 'project', obj_uuid)

        # remove refs
        col_start = 'ref:'
        col_fin = 'ref;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, ref_type, ref_uuid) = col_name.split(':')
            self._delete_ref(bch, 'project', obj_uuid, ref_type, ref_uuid)

        bch.remove(obj_uuid)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(fq_name)
        fq_name_col = utils.encode_string(fq_name_str) + ':' + obj_uuid
        self._obj_fq_name_cf.remove('project', columns = [fq_name_col])


        return (True, '')
    #end _cassandra_project_delete

    def _cassandra_qos_forwarding_class_alloc(self, fq_name):
        return (True, '')
    #end _cassandra_qos_forwarding_class_alloc

    def _cassandra_qos_forwarding_class_create(self, obj_ids, obj_dict):
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        bch = self._obj_uuid_cf.batch()

        obj_cols = {}
        obj_cols['fq_name'] = json.dumps(obj_dict['fq_name'])
        obj_cols['type'] = json.dumps('qos_forwarding_class')
        if 'parent_type' in obj_dict:
            # non config-root child
            parent_type = obj_dict['parent_type']
            parent_method_type = parent_type.replace('-', '_')
            parent_fq_name = obj_dict['fq_name'][:-1]
            obj_cols['parent_type'] = json.dumps(parent_type)
            parent_uuid = self.fq_name_to_uuid(parent_method_type, parent_fq_name)
            self._create_child(bch, parent_method_type, parent_uuid, 'qos_forwarding_class', obj_ids['uuid'])

        # Properties
        field = obj_dict.get('dscp', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'dscp', field)

        field = obj_dict.get('trusted', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'trusted', field)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            field['created'] = datetime.datetime.utcnow().isoformat()
            field['last_modified'] = field['created']
            self._create_prop(bch, obj_ids['uuid'], 'id_perms', field)

        field = obj_dict.get('display_name', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'display_name', field)


        # References
        refs = obj_dict.get('qos_queue_refs', [])
        for ref in refs:
            ref_uuid = self.fq_name_to_uuid('qos_queue', ref['to'])
            ref_attr = ref.get('attr', None)
            ref_data = {'attr': ref_attr, 'is_weakref': False}
            self._create_ref(bch, 'qos_forwarding_class', obj_ids['uuid'], 'qos_queue', ref_uuid, ref_data)

        bch.insert(obj_ids['uuid'], obj_cols)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(obj_dict['fq_name'])
        fq_name_cols = {utils.encode_string(fq_name_str) + ':' + obj_ids['uuid']: json.dumps(None)}
        self._obj_fq_name_cf.insert('qos_forwarding_class', fq_name_cols)

        return (True, '')
    #end _cassandra_qos_forwarding_class_create

    def _cassandra_qos_forwarding_class_read(self, obj_uuids, field_names = None):
        # if field_names = None, all fields will be read/returned

        obj_uuid_cf = self._obj_uuid_cf

        # optimize for common case of reading non-backref, non-children fields
        # ignoring columns starting from 'b' and 'c' - significant performance
        # impact in scaled setting. e.g. read of project
        if (field_names is None or
            (set(field_names) & (QosForwardingClass.backref_fields | QosForwardingClass.children_fields))):
            # atleast one backref/children field is needed
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_count = 10000000,
                                       include_timestamp = True)
        else: # ignore reading backref + children columns
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_start = 'd',
                                       column_count = 10000000,
                                       include_timestamp = True)

        if (len(obj_uuids) == 1) and not obj_rows:
            raise cfgm_common.exceptions.NoIdError(obj_uuids[0])

        results = []
        for row_key in obj_rows:
            # give chance for zk heartbeat/ping
            gevent.sleep(0)
            obj_uuid = row_key
            obj_cols = obj_rows[obj_uuid]
            result = {}
            result['uuid'] = obj_uuid
            result['fq_name'] = json.loads(obj_cols['fq_name'][0])
            for col_name in obj_cols.keys():
                if self._re_match_parent.match(col_name):
                    # non config-root child
                    (_, _, parent_uuid) = col_name.split(':')
                    parent_type = json.loads(obj_cols['parent_type'][0])
                    result['parent_type'] = parent_type
                    try:
                        result['parent_uuid'] = parent_uuid
                        result['parent_href'] = self._generate_url(parent_type, parent_uuid)
                    except cfgm_common.exceptions.NoIdError:
                        err_msg = 'Unknown uuid for parent ' + result['fq_name'][-2]
                        return (False, err_msg)

                # TODO use compiled RE
                if self._re_match_prop.match(col_name):
                    (_, prop_name) = col_name.split(':')
                    result[prop_name] = json.loads(obj_cols[col_name][0])

                # TODO use compiled RE
                if self._re_match_children.match(col_name):
                    (_, child_type, child_uuid) = col_name.split(':')
                    if field_names and '%ss' %(child_type) not in field_names:
                        continue

                    child_tstamp = obj_cols[col_name][1]
                    try:
                        self._read_child(result, obj_uuid, child_type, child_uuid, child_tstamp)
                    except cfgm_common.exceptions.NoIdError:
                        continue

                # TODO use compiled RE
                if self._re_match_ref.match(col_name):
                    (_, ref_type, ref_uuid) = col_name.split(':')
                    self._read_ref(result, obj_uuid, ref_type, ref_uuid, obj_cols[col_name][0])

                if self._re_match_backref.match(col_name):
                    (_, back_ref_type, back_ref_uuid) = col_name.split(':')
                    if field_names and '%s_back_refs' %(back_ref_type) not in field_names:
                        continue

                    try:
                        self._read_back_ref(result, obj_uuid, back_ref_type, back_ref_uuid,
                                            obj_cols[col_name][0])
                    except cfgm_common.exceptions.NoIdError:
                        continue

            # for all column names


            results.append(result)
        # end for all rows

        return (True, results)
    #end _cassandra_qos_forwarding_class_read

    def _cassandra_qos_forwarding_class_count_children(self, obj_uuid, child_type):
        # if child_type = None, return
        if child_type is None:
            return (False, '')

        obj_uuid_cf = self._obj_uuid_cf
        if child_type not in QosForwardingClass.children_fields:
            return (False, '%s is not a valid children of QosForwardingClass' %(child_type))

        col_start = 'children:'+child_type[:-1]+':'
        col_finish = 'children:'+child_type[:-1]+';'
        num_children = obj_uuid_cf.get_count(obj_uuid,
                                   column_start = col_start,
                                   column_finish = col_finish,
                                   max_count = 10000000)
        return (True, num_children)
    #end _cassandra_qos_forwarding_class_count_children

    def _cassandra_qos_forwarding_class_update(self, obj_uuid, new_obj_dict):
        # Grab ref-uuids and properties in new version
        new_ref_infos = {}

        if 'qos_queue_refs' in new_obj_dict:
            new_ref_infos['qos_queue'] = {}
            new_refs = new_obj_dict['qos_queue_refs']
            if new_refs:
                for new_ref in new_refs:
                    new_ref_uuid = self.fq_name_to_uuid('qos_queue', new_ref['to'])
                    new_ref_attr = new_ref.get('attr', None)
                    new_ref_data = {'attr': new_ref_attr, 'is_weakref': False}
                    new_ref_infos['qos_queue'][new_ref_uuid] = new_ref_data

        new_props = {}
        if 'dscp' in new_obj_dict:
            new_props['dscp'] = new_obj_dict['dscp']
        if 'trusted' in new_obj_dict:
            new_props['trusted'] = new_obj_dict['trusted']
        if 'id_perms' in new_obj_dict:
            new_props['id_perms'] = new_obj_dict['id_perms']
        if 'display_name' in new_obj_dict:
            new_props['display_name'] = new_obj_dict['display_name']
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        obj_uuid_cf = self._obj_uuid_cf
        obj_cols_iter = obj_uuid_cf.xget(obj_uuid)
        # TODO optimize this (converts tuple to dict)
        obj_cols = {}
        for col_info in obj_cols_iter:
            obj_cols[col_info[0]] = col_info[1]

        bch = obj_uuid_cf.batch()
        for col_name in obj_cols.keys():
            # TODO use compiled RE
            if re.match('prop:', col_name):
                (_, prop_name) = col_name.split(':')
                if prop_name == 'id_perms':
                    # id-perms always has to be updated for last-mod timestamp
                    # get it from request dict(or from db if not in request dict)
                    new_id_perms = new_obj_dict.get(prop_name, json.loads(obj_cols[col_name]))
                    self.update_last_modified(bch, obj_uuid, new_id_perms)
                elif prop_name in new_obj_dict:
                    self._update_prop(bch, obj_uuid, prop_name, new_props)

            # TODO use compiled RE
            if re.match('ref:', col_name):
                (_, ref_type, ref_uuid) = col_name.split(':')
                self._update_ref(bch, 'qos_forwarding_class', obj_uuid, ref_type, ref_uuid, new_ref_infos)
        # for all column names

        # create new refs
        for ref_type in new_ref_infos.keys():
            for ref_uuid in new_ref_infos[ref_type].keys():
                ref_data = new_ref_infos[ref_type][ref_uuid]
                self._create_ref(bch, 'qos_forwarding_class', obj_uuid, ref_type, ref_uuid, ref_data)

        # create new props
        for prop_name in new_props.keys():
            self._create_prop(bch, obj_uuid, prop_name, new_props[prop_name])

        bch.send()

        return (True, '')
    #end _cassandra_qos_forwarding_class_update

    def _cassandra_qos_forwarding_class_list(self, parent_uuids=None, back_ref_uuids=None,
                           obj_uuids=None, count=False, filters=None):
        children_fq_names_uuids = []
        if filters:
            fnames = filters.get('field_names', [])
            fvalues = filters.get('field_values', [])
            filter_fields = [(fnames[i], fvalues[i]) for i in range(len(fnames))]
        else:
            filter_fields = []

        def filter_rows(coll_infos, filter_cols, filter_params):
            filt_infos = {}
            coll_rows = obj_uuid_cf.multiget(coll_infos.keys(),
                                   columns=filter_cols,
                                   column_count=10000000)
            for row in coll_rows:
                # give chance for zk heartbeat/ping
                gevent.sleep(0)
                full_match = True
                for fname, fval in filter_params:
                    if coll_rows[row]['prop:%s' %(fname)] != fval:
                        full_match = False
                        break
                if full_match:
                    filt_infos[row] = coll_infos[row]
            return filt_infos
        # end filter_rows

        def get_fq_name_uuid_list(obj_uuids):
            ret_list = []
            for obj_uuid in obj_uuids:
                try:
                    obj_fq_name = self.uuid_to_fq_name(obj_uuid)
                    ret_list.append((obj_fq_name, obj_uuid))
                except cfgm_common.exceptions.NoIdError:
                    pass
            return ret_list
        # end get_fq_name_uuid_list

        if parent_uuids:
            # go from parent to child
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'children:qos_forwarding_class:'
            col_fin = 'children:qos_forwarding_class;'
            try:
                obj_rows = obj_uuid_cf.multiget(parent_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_parent_anchor(sort=False):
                # flatten to [('children:<type>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_child_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    child_uuid = col_name.split(':')[2]
                    if obj_uuids and child_uuid not in obj_uuids:
                        continue
                    all_child_infos[child_uuid] = {'uuid': child_uuid, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_child_infos = filter_rows(all_child_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_child_infos = all_child_infos

                if not sort:
                    ret_child_infos = filt_child_infos.values()
                else:
                    ret_child_infos = sorted(filt_child_infos.values(), key=itemgetter('tstamp'))

                return get_fq_name_uuid_list(r['uuid'] for r in ret_child_infos)
            # end filter_rows_parent_anchor

            if count:
                return (True, len(filter_rows_parent_anchor()))

            children_fq_names_uuids = filter_rows_parent_anchor(sort=True)

        if back_ref_uuids:
            # go from anchor to backrefs
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'backref:qos_forwarding_class:'
            col_fin = 'backref:qos_forwarding_class;'
            try:
                obj_rows = obj_uuid_cf.multiget(back_ref_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_backref_anchor():
                # flatten to [('<fqnstr>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_backref_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    col_name_arr = col_name.split(':')
                    fq_name = col_name_arr[:-1]
                    obj_uuid = col_name_arr[-1]
                    if obj_uuids and obj_uuid not in obj_uuids:
                        continue
                    all_backref_infos[obj_uuid] = \
                        {'uuid': obj_uuid, 'fq_name': fq_name, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_backref_infos = filter_rows(all_backref_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_backref_infos = all_backref_infos

                return [(br_info['fq_name'], br_info['uuid']) for br_info in filt_backref_infos.values()]
            # end filter_rows_backref_anchor

            if count:
                return (True, len(filter_rows_backref_anchor()))

            children_fq_names_uuids = filter_rows_backref_anchor()

        if not parent_uuids and not back_ref_uuids:
            obj_uuid_cf = self._obj_uuid_cf
            if obj_uuids:
                # exact objects specified
                def filter_rows_object_list():
                    all_obj_infos = {}
                    for obj_uuid in obj_uuids:
                        all_obj_infos[obj_uuid] = None

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return get_fq_name_uuid_list(filt_obj_infos.keys())
                # end filter_rows_object_list

                if count:
                    return (True, len(filter_rows_object_list()))
                children_fq_names_uuids = filter_rows_object_list()

            else: # grab all resources of this type
                obj_fq_name_cf = self._obj_fq_name_cf
                try:
                    cols = obj_fq_name_cf.get('qos_forwarding_class', column_count = 10000000)
                except pycassa.NotFoundException:
                    if count:
                        return (True, 0)
                    else:
                        return (True, children_fq_names_uuids)

                def filter_rows_no_anchor():
                    all_obj_infos = {}
                    for col_name, col_val in cols.items():
                        # give chance for zk heartbeat/ping
                        gevent.sleep(0)
                        col_name_arr = utils.decode_string(col_name).split(':')
                        obj_uuid = col_name_arr[-1]
                        all_obj_infos[obj_uuid] = (col_name_arr[:-1], obj_uuid)

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return filt_obj_infos.values()
                # end filter_rows_no_anchor

                if count:
                    return (True, len(filter_rows_no_anchor()))

                children_fq_names_uuids = filter_rows_no_anchor()

        return (True, children_fq_names_uuids)
    #end _cassandra_qos_forwarding_class_list

    def _cassandra_qos_forwarding_class_delete(self, obj_uuid):
        obj_uuid_cf = self._obj_uuid_cf
        fq_name = json.loads(obj_uuid_cf.get(obj_uuid, columns = ['fq_name'])['fq_name'])
        bch = obj_uuid_cf.batch()

        # unlink from parent
        col_start = 'parent:'
        col_fin = 'parent;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, parent_type, parent_uuid) = col_name.split(':')
            self._delete_child(bch, parent_type, parent_uuid, 'qos_forwarding_class', obj_uuid)

        # remove refs
        col_start = 'ref:'
        col_fin = 'ref;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, ref_type, ref_uuid) = col_name.split(':')
            self._delete_ref(bch, 'qos_forwarding_class', obj_uuid, ref_type, ref_uuid)

        bch.remove(obj_uuid)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(fq_name)
        fq_name_col = utils.encode_string(fq_name_str) + ':' + obj_uuid
        self._obj_fq_name_cf.remove('qos_forwarding_class', columns = [fq_name_col])


        return (True, '')
    #end _cassandra_qos_forwarding_class_delete

    def _cassandra_database_node_alloc(self, fq_name):
        return (True, '')
    #end _cassandra_database_node_alloc

    def _cassandra_database_node_create(self, obj_ids, obj_dict):
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        bch = self._obj_uuid_cf.batch()

        obj_cols = {}
        obj_cols['fq_name'] = json.dumps(obj_dict['fq_name'])
        obj_cols['type'] = json.dumps('database_node')
        if 'parent_type' in obj_dict:
            # non config-root child
            parent_type = obj_dict['parent_type']
            parent_method_type = parent_type.replace('-', '_')
            parent_fq_name = obj_dict['fq_name'][:-1]
            obj_cols['parent_type'] = json.dumps(parent_type)
            parent_uuid = self.fq_name_to_uuid(parent_method_type, parent_fq_name)
            self._create_child(bch, parent_method_type, parent_uuid, 'database_node', obj_ids['uuid'])

        # Properties
        field = obj_dict.get('database_node_ip_address', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'database_node_ip_address', field)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            field['created'] = datetime.datetime.utcnow().isoformat()
            field['last_modified'] = field['created']
            self._create_prop(bch, obj_ids['uuid'], 'id_perms', field)

        field = obj_dict.get('display_name', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'display_name', field)


        # References

        bch.insert(obj_ids['uuid'], obj_cols)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(obj_dict['fq_name'])
        fq_name_cols = {utils.encode_string(fq_name_str) + ':' + obj_ids['uuid']: json.dumps(None)}
        self._obj_fq_name_cf.insert('database_node', fq_name_cols)

        return (True, '')
    #end _cassandra_database_node_create

    def _cassandra_database_node_read(self, obj_uuids, field_names = None):
        # if field_names = None, all fields will be read/returned

        obj_uuid_cf = self._obj_uuid_cf

        # optimize for common case of reading non-backref, non-children fields
        # ignoring columns starting from 'b' and 'c' - significant performance
        # impact in scaled setting. e.g. read of project
        if (field_names is None or
            (set(field_names) & (DatabaseNode.backref_fields | DatabaseNode.children_fields))):
            # atleast one backref/children field is needed
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_count = 10000000,
                                       include_timestamp = True)
        else: # ignore reading backref + children columns
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_start = 'd',
                                       column_count = 10000000,
                                       include_timestamp = True)

        if (len(obj_uuids) == 1) and not obj_rows:
            raise cfgm_common.exceptions.NoIdError(obj_uuids[0])

        results = []
        for row_key in obj_rows:
            # give chance for zk heartbeat/ping
            gevent.sleep(0)
            obj_uuid = row_key
            obj_cols = obj_rows[obj_uuid]
            result = {}
            result['uuid'] = obj_uuid
            result['fq_name'] = json.loads(obj_cols['fq_name'][0])
            for col_name in obj_cols.keys():
                if self._re_match_parent.match(col_name):
                    # non config-root child
                    (_, _, parent_uuid) = col_name.split(':')
                    parent_type = json.loads(obj_cols['parent_type'][0])
                    result['parent_type'] = parent_type
                    try:
                        result['parent_uuid'] = parent_uuid
                        result['parent_href'] = self._generate_url(parent_type, parent_uuid)
                    except cfgm_common.exceptions.NoIdError:
                        err_msg = 'Unknown uuid for parent ' + result['fq_name'][-2]
                        return (False, err_msg)

                # TODO use compiled RE
                if self._re_match_prop.match(col_name):
                    (_, prop_name) = col_name.split(':')
                    result[prop_name] = json.loads(obj_cols[col_name][0])

                # TODO use compiled RE
                if self._re_match_children.match(col_name):
                    (_, child_type, child_uuid) = col_name.split(':')
                    if field_names and '%ss' %(child_type) not in field_names:
                        continue

                    child_tstamp = obj_cols[col_name][1]
                    try:
                        self._read_child(result, obj_uuid, child_type, child_uuid, child_tstamp)
                    except cfgm_common.exceptions.NoIdError:
                        continue

                # TODO use compiled RE
                if self._re_match_ref.match(col_name):
                    (_, ref_type, ref_uuid) = col_name.split(':')
                    self._read_ref(result, obj_uuid, ref_type, ref_uuid, obj_cols[col_name][0])

                if self._re_match_backref.match(col_name):
                    (_, back_ref_type, back_ref_uuid) = col_name.split(':')
                    if field_names and '%s_back_refs' %(back_ref_type) not in field_names:
                        continue

                    try:
                        self._read_back_ref(result, obj_uuid, back_ref_type, back_ref_uuid,
                                            obj_cols[col_name][0])
                    except cfgm_common.exceptions.NoIdError:
                        continue

            # for all column names


            results.append(result)
        # end for all rows

        return (True, results)
    #end _cassandra_database_node_read

    def _cassandra_database_node_count_children(self, obj_uuid, child_type):
        # if child_type = None, return
        if child_type is None:
            return (False, '')

        obj_uuid_cf = self._obj_uuid_cf
        if child_type not in DatabaseNode.children_fields:
            return (False, '%s is not a valid children of DatabaseNode' %(child_type))

        col_start = 'children:'+child_type[:-1]+':'
        col_finish = 'children:'+child_type[:-1]+';'
        num_children = obj_uuid_cf.get_count(obj_uuid,
                                   column_start = col_start,
                                   column_finish = col_finish,
                                   max_count = 10000000)
        return (True, num_children)
    #end _cassandra_database_node_count_children

    def _cassandra_database_node_update(self, obj_uuid, new_obj_dict):
        # Grab ref-uuids and properties in new version
        new_ref_infos = {}

        new_props = {}
        if 'database_node_ip_address' in new_obj_dict:
            new_props['database_node_ip_address'] = new_obj_dict['database_node_ip_address']
        if 'id_perms' in new_obj_dict:
            new_props['id_perms'] = new_obj_dict['id_perms']
        if 'display_name' in new_obj_dict:
            new_props['display_name'] = new_obj_dict['display_name']
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        obj_uuid_cf = self._obj_uuid_cf
        obj_cols_iter = obj_uuid_cf.xget(obj_uuid)
        # TODO optimize this (converts tuple to dict)
        obj_cols = {}
        for col_info in obj_cols_iter:
            obj_cols[col_info[0]] = col_info[1]

        bch = obj_uuid_cf.batch()
        for col_name in obj_cols.keys():
            # TODO use compiled RE
            if re.match('prop:', col_name):
                (_, prop_name) = col_name.split(':')
                if prop_name == 'id_perms':
                    # id-perms always has to be updated for last-mod timestamp
                    # get it from request dict(or from db if not in request dict)
                    new_id_perms = new_obj_dict.get(prop_name, json.loads(obj_cols[col_name]))
                    self.update_last_modified(bch, obj_uuid, new_id_perms)
                elif prop_name in new_obj_dict:
                    self._update_prop(bch, obj_uuid, prop_name, new_props)

            # TODO use compiled RE
            if re.match('ref:', col_name):
                (_, ref_type, ref_uuid) = col_name.split(':')
                self._update_ref(bch, 'database_node', obj_uuid, ref_type, ref_uuid, new_ref_infos)
        # for all column names

        # create new refs
        for ref_type in new_ref_infos.keys():
            for ref_uuid in new_ref_infos[ref_type].keys():
                ref_data = new_ref_infos[ref_type][ref_uuid]
                self._create_ref(bch, 'database_node', obj_uuid, ref_type, ref_uuid, ref_data)

        # create new props
        for prop_name in new_props.keys():
            self._create_prop(bch, obj_uuid, prop_name, new_props[prop_name])

        bch.send()

        return (True, '')
    #end _cassandra_database_node_update

    def _cassandra_database_node_list(self, parent_uuids=None, back_ref_uuids=None,
                           obj_uuids=None, count=False, filters=None):
        children_fq_names_uuids = []
        if filters:
            fnames = filters.get('field_names', [])
            fvalues = filters.get('field_values', [])
            filter_fields = [(fnames[i], fvalues[i]) for i in range(len(fnames))]
        else:
            filter_fields = []

        def filter_rows(coll_infos, filter_cols, filter_params):
            filt_infos = {}
            coll_rows = obj_uuid_cf.multiget(coll_infos.keys(),
                                   columns=filter_cols,
                                   column_count=10000000)
            for row in coll_rows:
                # give chance for zk heartbeat/ping
                gevent.sleep(0)
                full_match = True
                for fname, fval in filter_params:
                    if coll_rows[row]['prop:%s' %(fname)] != fval:
                        full_match = False
                        break
                if full_match:
                    filt_infos[row] = coll_infos[row]
            return filt_infos
        # end filter_rows

        def get_fq_name_uuid_list(obj_uuids):
            ret_list = []
            for obj_uuid in obj_uuids:
                try:
                    obj_fq_name = self.uuid_to_fq_name(obj_uuid)
                    ret_list.append((obj_fq_name, obj_uuid))
                except cfgm_common.exceptions.NoIdError:
                    pass
            return ret_list
        # end get_fq_name_uuid_list

        if parent_uuids:
            # go from parent to child
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'children:database_node:'
            col_fin = 'children:database_node;'
            try:
                obj_rows = obj_uuid_cf.multiget(parent_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_parent_anchor(sort=False):
                # flatten to [('children:<type>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_child_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    child_uuid = col_name.split(':')[2]
                    if obj_uuids and child_uuid not in obj_uuids:
                        continue
                    all_child_infos[child_uuid] = {'uuid': child_uuid, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_child_infos = filter_rows(all_child_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_child_infos = all_child_infos

                if not sort:
                    ret_child_infos = filt_child_infos.values()
                else:
                    ret_child_infos = sorted(filt_child_infos.values(), key=itemgetter('tstamp'))

                return get_fq_name_uuid_list(r['uuid'] for r in ret_child_infos)
            # end filter_rows_parent_anchor

            if count:
                return (True, len(filter_rows_parent_anchor()))

            children_fq_names_uuids = filter_rows_parent_anchor(sort=True)

        if back_ref_uuids:
            # go from anchor to backrefs
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'backref:database_node:'
            col_fin = 'backref:database_node;'
            try:
                obj_rows = obj_uuid_cf.multiget(back_ref_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_backref_anchor():
                # flatten to [('<fqnstr>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_backref_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    col_name_arr = col_name.split(':')
                    fq_name = col_name_arr[:-1]
                    obj_uuid = col_name_arr[-1]
                    if obj_uuids and obj_uuid not in obj_uuids:
                        continue
                    all_backref_infos[obj_uuid] = \
                        {'uuid': obj_uuid, 'fq_name': fq_name, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_backref_infos = filter_rows(all_backref_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_backref_infos = all_backref_infos

                return [(br_info['fq_name'], br_info['uuid']) for br_info in filt_backref_infos.values()]
            # end filter_rows_backref_anchor

            if count:
                return (True, len(filter_rows_backref_anchor()))

            children_fq_names_uuids = filter_rows_backref_anchor()

        if not parent_uuids and not back_ref_uuids:
            obj_uuid_cf = self._obj_uuid_cf
            if obj_uuids:
                # exact objects specified
                def filter_rows_object_list():
                    all_obj_infos = {}
                    for obj_uuid in obj_uuids:
                        all_obj_infos[obj_uuid] = None

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return get_fq_name_uuid_list(filt_obj_infos.keys())
                # end filter_rows_object_list

                if count:
                    return (True, len(filter_rows_object_list()))
                children_fq_names_uuids = filter_rows_object_list()

            else: # grab all resources of this type
                obj_fq_name_cf = self._obj_fq_name_cf
                try:
                    cols = obj_fq_name_cf.get('database_node', column_count = 10000000)
                except pycassa.NotFoundException:
                    if count:
                        return (True, 0)
                    else:
                        return (True, children_fq_names_uuids)

                def filter_rows_no_anchor():
                    all_obj_infos = {}
                    for col_name, col_val in cols.items():
                        # give chance for zk heartbeat/ping
                        gevent.sleep(0)
                        col_name_arr = utils.decode_string(col_name).split(':')
                        obj_uuid = col_name_arr[-1]
                        all_obj_infos[obj_uuid] = (col_name_arr[:-1], obj_uuid)

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return filt_obj_infos.values()
                # end filter_rows_no_anchor

                if count:
                    return (True, len(filter_rows_no_anchor()))

                children_fq_names_uuids = filter_rows_no_anchor()

        return (True, children_fq_names_uuids)
    #end _cassandra_database_node_list

    def _cassandra_database_node_delete(self, obj_uuid):
        obj_uuid_cf = self._obj_uuid_cf
        fq_name = json.loads(obj_uuid_cf.get(obj_uuid, columns = ['fq_name'])['fq_name'])
        bch = obj_uuid_cf.batch()

        # unlink from parent
        col_start = 'parent:'
        col_fin = 'parent;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, parent_type, parent_uuid) = col_name.split(':')
            self._delete_child(bch, parent_type, parent_uuid, 'database_node', obj_uuid)

        # remove refs
        col_start = 'ref:'
        col_fin = 'ref;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, ref_type, ref_uuid) = col_name.split(':')
            self._delete_ref(bch, 'database_node', obj_uuid, ref_type, ref_uuid)

        bch.remove(obj_uuid)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(fq_name)
        fq_name_col = utils.encode_string(fq_name_str) + ':' + obj_uuid
        self._obj_fq_name_cf.remove('database_node', columns = [fq_name_col])


        return (True, '')
    #end _cassandra_database_node_delete

    def _cassandra_routing_instance_alloc(self, fq_name):
        return (True, '')
    #end _cassandra_routing_instance_alloc

    def _cassandra_routing_instance_create(self, obj_ids, obj_dict):
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        bch = self._obj_uuid_cf.batch()

        obj_cols = {}
        obj_cols['fq_name'] = json.dumps(obj_dict['fq_name'])
        obj_cols['type'] = json.dumps('routing_instance')
        if 'parent_type' in obj_dict:
            # non config-root child
            parent_type = obj_dict['parent_type']
            parent_method_type = parent_type.replace('-', '_')
            parent_fq_name = obj_dict['fq_name'][:-1]
            obj_cols['parent_type'] = json.dumps(parent_type)
            parent_uuid = self.fq_name_to_uuid(parent_method_type, parent_fq_name)
            self._create_child(bch, parent_method_type, parent_uuid, 'routing_instance', obj_ids['uuid'])

        # Properties
        field = obj_dict.get('service_chain_information', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'service_chain_information', field)

        field = obj_dict.get('routing_instance_is_default', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'routing_instance_is_default', field)

        field = obj_dict.get('static_route_entries', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'static_route_entries', field)

        field = obj_dict.get('default_ce_protocol', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'default_ce_protocol', field)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            field['created'] = datetime.datetime.utcnow().isoformat()
            field['last_modified'] = field['created']
            self._create_prop(bch, obj_ids['uuid'], 'id_perms', field)

        field = obj_dict.get('display_name', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'display_name', field)


        # References
        refs = obj_dict.get('routing_instance_refs', [])
        for ref in refs:
            ref_uuid = self.fq_name_to_uuid('routing_instance', ref['to'])
            ref_attr = ref.get('attr', None)
            ref_data = {'attr': ref_attr, 'is_weakref': False}
            self._create_ref(bch, 'routing_instance', obj_ids['uuid'], 'routing_instance', ref_uuid, ref_data)
        refs = obj_dict.get('route_target_refs', [])
        for ref in refs:
            ref_uuid = self.fq_name_to_uuid('route_target', ref['to'])
            ref_attr = ref.get('attr', None)
            ref_data = {'attr': ref_attr, 'is_weakref': False}
            self._create_ref(bch, 'routing_instance', obj_ids['uuid'], 'route_target', ref_uuid, ref_data)

        bch.insert(obj_ids['uuid'], obj_cols)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(obj_dict['fq_name'])
        fq_name_cols = {utils.encode_string(fq_name_str) + ':' + obj_ids['uuid']: json.dumps(None)}
        self._obj_fq_name_cf.insert('routing_instance', fq_name_cols)

        return (True, '')
    #end _cassandra_routing_instance_create

    def _cassandra_routing_instance_read(self, obj_uuids, field_names = None):
        # if field_names = None, all fields will be read/returned

        obj_uuid_cf = self._obj_uuid_cf

        # optimize for common case of reading non-backref, non-children fields
        # ignoring columns starting from 'b' and 'c' - significant performance
        # impact in scaled setting. e.g. read of project
        if (field_names is None or
            (set(field_names) & (RoutingInstance.backref_fields | RoutingInstance.children_fields))):
            # atleast one backref/children field is needed
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_count = 10000000,
                                       include_timestamp = True)
        else: # ignore reading backref + children columns
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_start = 'd',
                                       column_count = 10000000,
                                       include_timestamp = True)

        if (len(obj_uuids) == 1) and not obj_rows:
            raise cfgm_common.exceptions.NoIdError(obj_uuids[0])

        results = []
        for row_key in obj_rows:
            # give chance for zk heartbeat/ping
            gevent.sleep(0)
            obj_uuid = row_key
            obj_cols = obj_rows[obj_uuid]
            result = {}
            result['uuid'] = obj_uuid
            result['fq_name'] = json.loads(obj_cols['fq_name'][0])
            for col_name in obj_cols.keys():
                if self._re_match_parent.match(col_name):
                    # non config-root child
                    (_, _, parent_uuid) = col_name.split(':')
                    parent_type = json.loads(obj_cols['parent_type'][0])
                    result['parent_type'] = parent_type
                    try:
                        result['parent_uuid'] = parent_uuid
                        result['parent_href'] = self._generate_url(parent_type, parent_uuid)
                    except cfgm_common.exceptions.NoIdError:
                        err_msg = 'Unknown uuid for parent ' + result['fq_name'][-2]
                        return (False, err_msg)

                # TODO use compiled RE
                if self._re_match_prop.match(col_name):
                    (_, prop_name) = col_name.split(':')
                    result[prop_name] = json.loads(obj_cols[col_name][0])

                # TODO use compiled RE
                if self._re_match_children.match(col_name):
                    (_, child_type, child_uuid) = col_name.split(':')
                    if field_names and '%ss' %(child_type) not in field_names:
                        continue

                    child_tstamp = obj_cols[col_name][1]
                    try:
                        self._read_child(result, obj_uuid, child_type, child_uuid, child_tstamp)
                    except cfgm_common.exceptions.NoIdError:
                        continue

                # TODO use compiled RE
                if self._re_match_ref.match(col_name):
                    (_, ref_type, ref_uuid) = col_name.split(':')
                    self._read_ref(result, obj_uuid, ref_type, ref_uuid, obj_cols[col_name][0])

                if self._re_match_backref.match(col_name):
                    (_, back_ref_type, back_ref_uuid) = col_name.split(':')
                    if field_names and '%s_back_refs' %(back_ref_type) not in field_names:
                        continue

                    try:
                        self._read_back_ref(result, obj_uuid, back_ref_type, back_ref_uuid,
                                            obj_cols[col_name][0])
                    except cfgm_common.exceptions.NoIdError:
                        continue

            # for all column names

            if 'bgp_routers' in result:
                # sort children; TODO do this based on schema
                sorted_children = sorted(result['bgp_routers'], key = itemgetter('tstamp'))
                # re-write result's children without timestamp
                result['bgp_routers'] = sorted_children
                [child.pop('tstamp') for child in result['bgp_routers']]

            results.append(result)
        # end for all rows

        return (True, results)
    #end _cassandra_routing_instance_read

    def _cassandra_routing_instance_count_children(self, obj_uuid, child_type):
        # if child_type = None, return
        if child_type is None:
            return (False, '')

        obj_uuid_cf = self._obj_uuid_cf
        if child_type not in RoutingInstance.children_fields:
            return (False, '%s is not a valid children of RoutingInstance' %(child_type))

        col_start = 'children:'+child_type[:-1]+':'
        col_finish = 'children:'+child_type[:-1]+';'
        num_children = obj_uuid_cf.get_count(obj_uuid,
                                   column_start = col_start,
                                   column_finish = col_finish,
                                   max_count = 10000000)
        return (True, num_children)
    #end _cassandra_routing_instance_count_children

    def _cassandra_routing_instance_update(self, obj_uuid, new_obj_dict):
        # Grab ref-uuids and properties in new version
        new_ref_infos = {}

        if 'routing_instance_refs' in new_obj_dict:
            new_ref_infos['routing_instance'] = {}
            new_refs = new_obj_dict['routing_instance_refs']
            if new_refs:
                for new_ref in new_refs:
                    new_ref_uuid = self.fq_name_to_uuid('routing_instance', new_ref['to'])
                    new_ref_attr = new_ref.get('attr', None)
                    new_ref_data = {'attr': new_ref_attr, 'is_weakref': False}
                    new_ref_infos['routing_instance'][new_ref_uuid] = new_ref_data

        if 'route_target_refs' in new_obj_dict:
            new_ref_infos['route_target'] = {}
            new_refs = new_obj_dict['route_target_refs']
            if new_refs:
                for new_ref in new_refs:
                    new_ref_uuid = self.fq_name_to_uuid('route_target', new_ref['to'])
                    new_ref_attr = new_ref.get('attr', None)
                    new_ref_data = {'attr': new_ref_attr, 'is_weakref': False}
                    new_ref_infos['route_target'][new_ref_uuid] = new_ref_data

        new_props = {}
        if 'service_chain_information' in new_obj_dict:
            new_props['service_chain_information'] = new_obj_dict['service_chain_information']
        if 'routing_instance_is_default' in new_obj_dict:
            new_props['routing_instance_is_default'] = new_obj_dict['routing_instance_is_default']
        if 'static_route_entries' in new_obj_dict:
            new_props['static_route_entries'] = new_obj_dict['static_route_entries']
        if 'default_ce_protocol' in new_obj_dict:
            new_props['default_ce_protocol'] = new_obj_dict['default_ce_protocol']
        if 'id_perms' in new_obj_dict:
            new_props['id_perms'] = new_obj_dict['id_perms']
        if 'display_name' in new_obj_dict:
            new_props['display_name'] = new_obj_dict['display_name']
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        obj_uuid_cf = self._obj_uuid_cf
        obj_cols_iter = obj_uuid_cf.xget(obj_uuid)
        # TODO optimize this (converts tuple to dict)
        obj_cols = {}
        for col_info in obj_cols_iter:
            obj_cols[col_info[0]] = col_info[1]

        bch = obj_uuid_cf.batch()
        for col_name in obj_cols.keys():
            # TODO use compiled RE
            if re.match('prop:', col_name):
                (_, prop_name) = col_name.split(':')
                if prop_name == 'id_perms':
                    # id-perms always has to be updated for last-mod timestamp
                    # get it from request dict(or from db if not in request dict)
                    new_id_perms = new_obj_dict.get(prop_name, json.loads(obj_cols[col_name]))
                    self.update_last_modified(bch, obj_uuid, new_id_perms)
                elif prop_name in new_obj_dict:
                    self._update_prop(bch, obj_uuid, prop_name, new_props)

            # TODO use compiled RE
            if re.match('ref:', col_name):
                (_, ref_type, ref_uuid) = col_name.split(':')
                self._update_ref(bch, 'routing_instance', obj_uuid, ref_type, ref_uuid, new_ref_infos)
        # for all column names

        # create new refs
        for ref_type in new_ref_infos.keys():
            for ref_uuid in new_ref_infos[ref_type].keys():
                ref_data = new_ref_infos[ref_type][ref_uuid]
                self._create_ref(bch, 'routing_instance', obj_uuid, ref_type, ref_uuid, ref_data)

        # create new props
        for prop_name in new_props.keys():
            self._create_prop(bch, obj_uuid, prop_name, new_props[prop_name])

        bch.send()

        return (True, '')
    #end _cassandra_routing_instance_update

    def _cassandra_routing_instance_list(self, parent_uuids=None, back_ref_uuids=None,
                           obj_uuids=None, count=False, filters=None):
        children_fq_names_uuids = []
        if filters:
            fnames = filters.get('field_names', [])
            fvalues = filters.get('field_values', [])
            filter_fields = [(fnames[i], fvalues[i]) for i in range(len(fnames))]
        else:
            filter_fields = []

        def filter_rows(coll_infos, filter_cols, filter_params):
            filt_infos = {}
            coll_rows = obj_uuid_cf.multiget(coll_infos.keys(),
                                   columns=filter_cols,
                                   column_count=10000000)
            for row in coll_rows:
                # give chance for zk heartbeat/ping
                gevent.sleep(0)
                full_match = True
                for fname, fval in filter_params:
                    if coll_rows[row]['prop:%s' %(fname)] != fval:
                        full_match = False
                        break
                if full_match:
                    filt_infos[row] = coll_infos[row]
            return filt_infos
        # end filter_rows

        def get_fq_name_uuid_list(obj_uuids):
            ret_list = []
            for obj_uuid in obj_uuids:
                try:
                    obj_fq_name = self.uuid_to_fq_name(obj_uuid)
                    ret_list.append((obj_fq_name, obj_uuid))
                except cfgm_common.exceptions.NoIdError:
                    pass
            return ret_list
        # end get_fq_name_uuid_list

        if parent_uuids:
            # go from parent to child
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'children:routing_instance:'
            col_fin = 'children:routing_instance;'
            try:
                obj_rows = obj_uuid_cf.multiget(parent_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_parent_anchor(sort=False):
                # flatten to [('children:<type>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_child_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    child_uuid = col_name.split(':')[2]
                    if obj_uuids and child_uuid not in obj_uuids:
                        continue
                    all_child_infos[child_uuid] = {'uuid': child_uuid, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_child_infos = filter_rows(all_child_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_child_infos = all_child_infos

                if not sort:
                    ret_child_infos = filt_child_infos.values()
                else:
                    ret_child_infos = sorted(filt_child_infos.values(), key=itemgetter('tstamp'))

                return get_fq_name_uuid_list(r['uuid'] for r in ret_child_infos)
            # end filter_rows_parent_anchor

            if count:
                return (True, len(filter_rows_parent_anchor()))

            children_fq_names_uuids = filter_rows_parent_anchor(sort=True)

        if back_ref_uuids:
            # go from anchor to backrefs
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'backref:routing_instance:'
            col_fin = 'backref:routing_instance;'
            try:
                obj_rows = obj_uuid_cf.multiget(back_ref_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_backref_anchor():
                # flatten to [('<fqnstr>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_backref_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    col_name_arr = col_name.split(':')
                    fq_name = col_name_arr[:-1]
                    obj_uuid = col_name_arr[-1]
                    if obj_uuids and obj_uuid not in obj_uuids:
                        continue
                    all_backref_infos[obj_uuid] = \
                        {'uuid': obj_uuid, 'fq_name': fq_name, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_backref_infos = filter_rows(all_backref_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_backref_infos = all_backref_infos

                return [(br_info['fq_name'], br_info['uuid']) for br_info in filt_backref_infos.values()]
            # end filter_rows_backref_anchor

            if count:
                return (True, len(filter_rows_backref_anchor()))

            children_fq_names_uuids = filter_rows_backref_anchor()

        if not parent_uuids and not back_ref_uuids:
            obj_uuid_cf = self._obj_uuid_cf
            if obj_uuids:
                # exact objects specified
                def filter_rows_object_list():
                    all_obj_infos = {}
                    for obj_uuid in obj_uuids:
                        all_obj_infos[obj_uuid] = None

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return get_fq_name_uuid_list(filt_obj_infos.keys())
                # end filter_rows_object_list

                if count:
                    return (True, len(filter_rows_object_list()))
                children_fq_names_uuids = filter_rows_object_list()

            else: # grab all resources of this type
                obj_fq_name_cf = self._obj_fq_name_cf
                try:
                    cols = obj_fq_name_cf.get('routing_instance', column_count = 10000000)
                except pycassa.NotFoundException:
                    if count:
                        return (True, 0)
                    else:
                        return (True, children_fq_names_uuids)

                def filter_rows_no_anchor():
                    all_obj_infos = {}
                    for col_name, col_val in cols.items():
                        # give chance for zk heartbeat/ping
                        gevent.sleep(0)
                        col_name_arr = utils.decode_string(col_name).split(':')
                        obj_uuid = col_name_arr[-1]
                        all_obj_infos[obj_uuid] = (col_name_arr[:-1], obj_uuid)

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return filt_obj_infos.values()
                # end filter_rows_no_anchor

                if count:
                    return (True, len(filter_rows_no_anchor()))

                children_fq_names_uuids = filter_rows_no_anchor()

        return (True, children_fq_names_uuids)
    #end _cassandra_routing_instance_list

    def _cassandra_routing_instance_delete(self, obj_uuid):
        obj_uuid_cf = self._obj_uuid_cf
        fq_name = json.loads(obj_uuid_cf.get(obj_uuid, columns = ['fq_name'])['fq_name'])
        bch = obj_uuid_cf.batch()

        # unlink from parent
        col_start = 'parent:'
        col_fin = 'parent;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, parent_type, parent_uuid) = col_name.split(':')
            self._delete_child(bch, parent_type, parent_uuid, 'routing_instance', obj_uuid)

        # remove refs
        col_start = 'ref:'
        col_fin = 'ref;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, ref_type, ref_uuid) = col_name.split(':')
            self._delete_ref(bch, 'routing_instance', obj_uuid, ref_type, ref_uuid)

        bch.remove(obj_uuid)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(fq_name)
        fq_name_col = utils.encode_string(fq_name_str) + ':' + obj_uuid
        self._obj_fq_name_cf.remove('routing_instance', columns = [fq_name_col])


        return (True, '')
    #end _cassandra_routing_instance_delete

    def _cassandra_network_ipam_alloc(self, fq_name):
        return (True, '')
    #end _cassandra_network_ipam_alloc

    def _cassandra_network_ipam_create(self, obj_ids, obj_dict):
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        bch = self._obj_uuid_cf.batch()

        obj_cols = {}
        obj_cols['fq_name'] = json.dumps(obj_dict['fq_name'])
        obj_cols['type'] = json.dumps('network_ipam')
        if 'parent_type' in obj_dict:
            # non config-root child
            parent_type = obj_dict['parent_type']
            parent_method_type = parent_type.replace('-', '_')
            parent_fq_name = obj_dict['fq_name'][:-1]
            obj_cols['parent_type'] = json.dumps(parent_type)
            parent_uuid = self.fq_name_to_uuid(parent_method_type, parent_fq_name)
            self._create_child(bch, parent_method_type, parent_uuid, 'network_ipam', obj_ids['uuid'])

        # Properties
        field = obj_dict.get('network_ipam_mgmt', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'network_ipam_mgmt', field)

        field = obj_dict.get('id_perms', None)
        if field is not None:
            field['created'] = datetime.datetime.utcnow().isoformat()
            field['last_modified'] = field['created']
            self._create_prop(bch, obj_ids['uuid'], 'id_perms', field)

        field = obj_dict.get('display_name', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'display_name', field)


        # References
        refs = obj_dict.get('virtual_DNS_refs', [])
        for ref in refs:
            ref_uuid = self.fq_name_to_uuid('virtual_DNS', ref['to'])
            ref_attr = ref.get('attr', None)
            ref_data = {'attr': ref_attr, 'is_weakref': False}
            self._create_ref(bch, 'network_ipam', obj_ids['uuid'], 'virtual_DNS', ref_uuid, ref_data)

        bch.insert(obj_ids['uuid'], obj_cols)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(obj_dict['fq_name'])
        fq_name_cols = {utils.encode_string(fq_name_str) + ':' + obj_ids['uuid']: json.dumps(None)}
        self._obj_fq_name_cf.insert('network_ipam', fq_name_cols)

        return (True, '')
    #end _cassandra_network_ipam_create

    def _cassandra_network_ipam_read(self, obj_uuids, field_names = None):
        # if field_names = None, all fields will be read/returned

        obj_uuid_cf = self._obj_uuid_cf

        # optimize for common case of reading non-backref, non-children fields
        # ignoring columns starting from 'b' and 'c' - significant performance
        # impact in scaled setting. e.g. read of project
        if (field_names is None or
            (set(field_names) & (NetworkIpam.backref_fields | NetworkIpam.children_fields))):
            # atleast one backref/children field is needed
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_count = 10000000,
                                       include_timestamp = True)
        else: # ignore reading backref + children columns
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_start = 'd',
                                       column_count = 10000000,
                                       include_timestamp = True)

        if (len(obj_uuids) == 1) and not obj_rows:
            raise cfgm_common.exceptions.NoIdError(obj_uuids[0])

        results = []
        for row_key in obj_rows:
            # give chance for zk heartbeat/ping
            gevent.sleep(0)
            obj_uuid = row_key
            obj_cols = obj_rows[obj_uuid]
            result = {}
            result['uuid'] = obj_uuid
            result['fq_name'] = json.loads(obj_cols['fq_name'][0])
            for col_name in obj_cols.keys():
                if self._re_match_parent.match(col_name):
                    # non config-root child
                    (_, _, parent_uuid) = col_name.split(':')
                    parent_type = json.loads(obj_cols['parent_type'][0])
                    result['parent_type'] = parent_type
                    try:
                        result['parent_uuid'] = parent_uuid
                        result['parent_href'] = self._generate_url(parent_type, parent_uuid)
                    except cfgm_common.exceptions.NoIdError:
                        err_msg = 'Unknown uuid for parent ' + result['fq_name'][-2]
                        return (False, err_msg)

                # TODO use compiled RE
                if self._re_match_prop.match(col_name):
                    (_, prop_name) = col_name.split(':')
                    result[prop_name] = json.loads(obj_cols[col_name][0])

                # TODO use compiled RE
                if self._re_match_children.match(col_name):
                    (_, child_type, child_uuid) = col_name.split(':')
                    if field_names and '%ss' %(child_type) not in field_names:
                        continue

                    child_tstamp = obj_cols[col_name][1]
                    try:
                        self._read_child(result, obj_uuid, child_type, child_uuid, child_tstamp)
                    except cfgm_common.exceptions.NoIdError:
                        continue

                # TODO use compiled RE
                if self._re_match_ref.match(col_name):
                    (_, ref_type, ref_uuid) = col_name.split(':')
                    self._read_ref(result, obj_uuid, ref_type, ref_uuid, obj_cols[col_name][0])

                if self._re_match_backref.match(col_name):
                    (_, back_ref_type, back_ref_uuid) = col_name.split(':')
                    if field_names and '%s_back_refs' %(back_ref_type) not in field_names:
                        continue

                    try:
                        self._read_back_ref(result, obj_uuid, back_ref_type, back_ref_uuid,
                                            obj_cols[col_name][0])
                    except cfgm_common.exceptions.NoIdError:
                        continue

            # for all column names


            results.append(result)
        # end for all rows

        return (True, results)
    #end _cassandra_network_ipam_read

    def _cassandra_network_ipam_count_children(self, obj_uuid, child_type):
        # if child_type = None, return
        if child_type is None:
            return (False, '')

        obj_uuid_cf = self._obj_uuid_cf
        if child_type not in NetworkIpam.children_fields:
            return (False, '%s is not a valid children of NetworkIpam' %(child_type))

        col_start = 'children:'+child_type[:-1]+':'
        col_finish = 'children:'+child_type[:-1]+';'
        num_children = obj_uuid_cf.get_count(obj_uuid,
                                   column_start = col_start,
                                   column_finish = col_finish,
                                   max_count = 10000000)
        return (True, num_children)
    #end _cassandra_network_ipam_count_children

    def _cassandra_network_ipam_update(self, obj_uuid, new_obj_dict):
        # Grab ref-uuids and properties in new version
        new_ref_infos = {}

        if 'virtual_DNS_refs' in new_obj_dict:
            new_ref_infos['virtual_DNS'] = {}
            new_refs = new_obj_dict['virtual_DNS_refs']
            if new_refs:
                for new_ref in new_refs:
                    new_ref_uuid = self.fq_name_to_uuid('virtual_DNS', new_ref['to'])
                    new_ref_attr = new_ref.get('attr', None)
                    new_ref_data = {'attr': new_ref_attr, 'is_weakref': False}
                    new_ref_infos['virtual_DNS'][new_ref_uuid] = new_ref_data

        new_props = {}
        if 'network_ipam_mgmt' in new_obj_dict:
            new_props['network_ipam_mgmt'] = new_obj_dict['network_ipam_mgmt']
        if 'id_perms' in new_obj_dict:
            new_props['id_perms'] = new_obj_dict['id_perms']
        if 'display_name' in new_obj_dict:
            new_props['display_name'] = new_obj_dict['display_name']
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        obj_uuid_cf = self._obj_uuid_cf
        obj_cols_iter = obj_uuid_cf.xget(obj_uuid)
        # TODO optimize this (converts tuple to dict)
        obj_cols = {}
        for col_info in obj_cols_iter:
            obj_cols[col_info[0]] = col_info[1]

        bch = obj_uuid_cf.batch()
        for col_name in obj_cols.keys():
            # TODO use compiled RE
            if re.match('prop:', col_name):
                (_, prop_name) = col_name.split(':')
                if prop_name == 'id_perms':
                    # id-perms always has to be updated for last-mod timestamp
                    # get it from request dict(or from db if not in request dict)
                    new_id_perms = new_obj_dict.get(prop_name, json.loads(obj_cols[col_name]))
                    self.update_last_modified(bch, obj_uuid, new_id_perms)
                elif prop_name in new_obj_dict:
                    self._update_prop(bch, obj_uuid, prop_name, new_props)

            # TODO use compiled RE
            if re.match('ref:', col_name):
                (_, ref_type, ref_uuid) = col_name.split(':')
                self._update_ref(bch, 'network_ipam', obj_uuid, ref_type, ref_uuid, new_ref_infos)
        # for all column names

        # create new refs
        for ref_type in new_ref_infos.keys():
            for ref_uuid in new_ref_infos[ref_type].keys():
                ref_data = new_ref_infos[ref_type][ref_uuid]
                self._create_ref(bch, 'network_ipam', obj_uuid, ref_type, ref_uuid, ref_data)

        # create new props
        for prop_name in new_props.keys():
            self._create_prop(bch, obj_uuid, prop_name, new_props[prop_name])

        bch.send()

        return (True, '')
    #end _cassandra_network_ipam_update

    def _cassandra_network_ipam_list(self, parent_uuids=None, back_ref_uuids=None,
                           obj_uuids=None, count=False, filters=None):
        children_fq_names_uuids = []
        if filters:
            fnames = filters.get('field_names', [])
            fvalues = filters.get('field_values', [])
            filter_fields = [(fnames[i], fvalues[i]) for i in range(len(fnames))]
        else:
            filter_fields = []

        def filter_rows(coll_infos, filter_cols, filter_params):
            filt_infos = {}
            coll_rows = obj_uuid_cf.multiget(coll_infos.keys(),
                                   columns=filter_cols,
                                   column_count=10000000)
            for row in coll_rows:
                # give chance for zk heartbeat/ping
                gevent.sleep(0)
                full_match = True
                for fname, fval in filter_params:
                    if coll_rows[row]['prop:%s' %(fname)] != fval:
                        full_match = False
                        break
                if full_match:
                    filt_infos[row] = coll_infos[row]
            return filt_infos
        # end filter_rows

        def get_fq_name_uuid_list(obj_uuids):
            ret_list = []
            for obj_uuid in obj_uuids:
                try:
                    obj_fq_name = self.uuid_to_fq_name(obj_uuid)
                    ret_list.append((obj_fq_name, obj_uuid))
                except cfgm_common.exceptions.NoIdError:
                    pass
            return ret_list
        # end get_fq_name_uuid_list

        if parent_uuids:
            # go from parent to child
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'children:network_ipam:'
            col_fin = 'children:network_ipam;'
            try:
                obj_rows = obj_uuid_cf.multiget(parent_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_parent_anchor(sort=False):
                # flatten to [('children:<type>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_child_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    child_uuid = col_name.split(':')[2]
                    if obj_uuids and child_uuid not in obj_uuids:
                        continue
                    all_child_infos[child_uuid] = {'uuid': child_uuid, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_child_infos = filter_rows(all_child_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_child_infos = all_child_infos

                if not sort:
                    ret_child_infos = filt_child_infos.values()
                else:
                    ret_child_infos = sorted(filt_child_infos.values(), key=itemgetter('tstamp'))

                return get_fq_name_uuid_list(r['uuid'] for r in ret_child_infos)
            # end filter_rows_parent_anchor

            if count:
                return (True, len(filter_rows_parent_anchor()))

            children_fq_names_uuids = filter_rows_parent_anchor(sort=True)

        if back_ref_uuids:
            # go from anchor to backrefs
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'backref:network_ipam:'
            col_fin = 'backref:network_ipam;'
            try:
                obj_rows = obj_uuid_cf.multiget(back_ref_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_backref_anchor():
                # flatten to [('<fqnstr>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_backref_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    col_name_arr = col_name.split(':')
                    fq_name = col_name_arr[:-1]
                    obj_uuid = col_name_arr[-1]
                    if obj_uuids and obj_uuid not in obj_uuids:
                        continue
                    all_backref_infos[obj_uuid] = \
                        {'uuid': obj_uuid, 'fq_name': fq_name, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_backref_infos = filter_rows(all_backref_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_backref_infos = all_backref_infos

                return [(br_info['fq_name'], br_info['uuid']) for br_info in filt_backref_infos.values()]
            # end filter_rows_backref_anchor

            if count:
                return (True, len(filter_rows_backref_anchor()))

            children_fq_names_uuids = filter_rows_backref_anchor()

        if not parent_uuids and not back_ref_uuids:
            obj_uuid_cf = self._obj_uuid_cf
            if obj_uuids:
                # exact objects specified
                def filter_rows_object_list():
                    all_obj_infos = {}
                    for obj_uuid in obj_uuids:
                        all_obj_infos[obj_uuid] = None

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return get_fq_name_uuid_list(filt_obj_infos.keys())
                # end filter_rows_object_list

                if count:
                    return (True, len(filter_rows_object_list()))
                children_fq_names_uuids = filter_rows_object_list()

            else: # grab all resources of this type
                obj_fq_name_cf = self._obj_fq_name_cf
                try:
                    cols = obj_fq_name_cf.get('network_ipam', column_count = 10000000)
                except pycassa.NotFoundException:
                    if count:
                        return (True, 0)
                    else:
                        return (True, children_fq_names_uuids)

                def filter_rows_no_anchor():
                    all_obj_infos = {}
                    for col_name, col_val in cols.items():
                        # give chance for zk heartbeat/ping
                        gevent.sleep(0)
                        col_name_arr = utils.decode_string(col_name).split(':')
                        obj_uuid = col_name_arr[-1]
                        all_obj_infos[obj_uuid] = (col_name_arr[:-1], obj_uuid)

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return filt_obj_infos.values()
                # end filter_rows_no_anchor

                if count:
                    return (True, len(filter_rows_no_anchor()))

                children_fq_names_uuids = filter_rows_no_anchor()

        return (True, children_fq_names_uuids)
    #end _cassandra_network_ipam_list

    def _cassandra_network_ipam_delete(self, obj_uuid):
        obj_uuid_cf = self._obj_uuid_cf
        fq_name = json.loads(obj_uuid_cf.get(obj_uuid, columns = ['fq_name'])['fq_name'])
        bch = obj_uuid_cf.batch()

        # unlink from parent
        col_start = 'parent:'
        col_fin = 'parent;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, parent_type, parent_uuid) = col_name.split(':')
            self._delete_child(bch, parent_type, parent_uuid, 'network_ipam', obj_uuid)

        # remove refs
        col_start = 'ref:'
        col_fin = 'ref;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, ref_type, ref_uuid) = col_name.split(':')
            self._delete_ref(bch, 'network_ipam', obj_uuid, ref_type, ref_uuid)

        bch.remove(obj_uuid)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(fq_name)
        fq_name_col = utils.encode_string(fq_name_str) + ':' + obj_uuid
        self._obj_fq_name_cf.remove('network_ipam', columns = [fq_name_col])


        return (True, '')
    #end _cassandra_network_ipam_delete

    def _cassandra_logical_router_alloc(self, fq_name):
        return (True, '')
    #end _cassandra_logical_router_alloc

    def _cassandra_logical_router_create(self, obj_ids, obj_dict):
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        bch = self._obj_uuid_cf.batch()

        obj_cols = {}
        obj_cols['fq_name'] = json.dumps(obj_dict['fq_name'])
        obj_cols['type'] = json.dumps('logical_router')
        if 'parent_type' in obj_dict:
            # non config-root child
            parent_type = obj_dict['parent_type']
            parent_method_type = parent_type.replace('-', '_')
            parent_fq_name = obj_dict['fq_name'][:-1]
            obj_cols['parent_type'] = json.dumps(parent_type)
            parent_uuid = self.fq_name_to_uuid(parent_method_type, parent_fq_name)
            self._create_child(bch, parent_method_type, parent_uuid, 'logical_router', obj_ids['uuid'])

        # Properties
        field = obj_dict.get('id_perms', None)
        if field is not None:
            field['created'] = datetime.datetime.utcnow().isoformat()
            field['last_modified'] = field['created']
            self._create_prop(bch, obj_ids['uuid'], 'id_perms', field)

        field = obj_dict.get('display_name', None)
        if field is not None:
            self._create_prop(bch, obj_ids['uuid'], 'display_name', field)


        # References
        refs = obj_dict.get('virtual_machine_interface_refs', [])
        for ref in refs:
            ref_uuid = self.fq_name_to_uuid('virtual_machine_interface', ref['to'])
            ref_attr = ref.get('attr', None)
            ref_data = {'attr': ref_attr, 'is_weakref': False}
            self._create_ref(bch, 'logical_router', obj_ids['uuid'], 'virtual_machine_interface', ref_uuid, ref_data)
        refs = obj_dict.get('route_target_refs', [])
        for ref in refs:
            ref_uuid = self.fq_name_to_uuid('route_target', ref['to'])
            ref_attr = ref.get('attr', None)
            ref_data = {'attr': ref_attr, 'is_weakref': False}
            self._create_ref(bch, 'logical_router', obj_ids['uuid'], 'route_target', ref_uuid, ref_data)
        refs = obj_dict.get('virtual_network_refs', [])
        for ref in refs:
            ref_uuid = self.fq_name_to_uuid('virtual_network', ref['to'])
            ref_attr = ref.get('attr', None)
            ref_data = {'attr': ref_attr, 'is_weakref': False}
            self._create_ref(bch, 'logical_router', obj_ids['uuid'], 'virtual_network', ref_uuid, ref_data)
        refs = obj_dict.get('service_instance_refs', [])
        for ref in refs:
            ref_uuid = self.fq_name_to_uuid('service_instance', ref['to'])
            ref_attr = ref.get('attr', None)
            ref_data = {'attr': ref_attr, 'is_weakref': False}
            self._create_ref(bch, 'logical_router', obj_ids['uuid'], 'service_instance', ref_uuid, ref_data)

        bch.insert(obj_ids['uuid'], obj_cols)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(obj_dict['fq_name'])
        fq_name_cols = {utils.encode_string(fq_name_str) + ':' + obj_ids['uuid']: json.dumps(None)}
        self._obj_fq_name_cf.insert('logical_router', fq_name_cols)

        return (True, '')
    #end _cassandra_logical_router_create

    def _cassandra_logical_router_read(self, obj_uuids, field_names = None):
        # if field_names = None, all fields will be read/returned

        obj_uuid_cf = self._obj_uuid_cf

        # optimize for common case of reading non-backref, non-children fields
        # ignoring columns starting from 'b' and 'c' - significant performance
        # impact in scaled setting. e.g. read of project
        if (field_names is None or
            (set(field_names) & (LogicalRouter.backref_fields | LogicalRouter.children_fields))):
            # atleast one backref/children field is needed
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_count = 10000000,
                                       include_timestamp = True)
        else: # ignore reading backref + children columns
            obj_rows = obj_uuid_cf.multiget(obj_uuids,
                                       column_start = 'd',
                                       column_count = 10000000,
                                       include_timestamp = True)

        if (len(obj_uuids) == 1) and not obj_rows:
            raise cfgm_common.exceptions.NoIdError(obj_uuids[0])

        results = []
        for row_key in obj_rows:
            # give chance for zk heartbeat/ping
            gevent.sleep(0)
            obj_uuid = row_key
            obj_cols = obj_rows[obj_uuid]
            result = {}
            result['uuid'] = obj_uuid
            result['fq_name'] = json.loads(obj_cols['fq_name'][0])
            for col_name in obj_cols.keys():
                if self._re_match_parent.match(col_name):
                    # non config-root child
                    (_, _, parent_uuid) = col_name.split(':')
                    parent_type = json.loads(obj_cols['parent_type'][0])
                    result['parent_type'] = parent_type
                    try:
                        result['parent_uuid'] = parent_uuid
                        result['parent_href'] = self._generate_url(parent_type, parent_uuid)
                    except cfgm_common.exceptions.NoIdError:
                        err_msg = 'Unknown uuid for parent ' + result['fq_name'][-2]
                        return (False, err_msg)

                # TODO use compiled RE
                if self._re_match_prop.match(col_name):
                    (_, prop_name) = col_name.split(':')
                    result[prop_name] = json.loads(obj_cols[col_name][0])

                # TODO use compiled RE
                if self._re_match_children.match(col_name):
                    (_, child_type, child_uuid) = col_name.split(':')
                    if field_names and '%ss' %(child_type) not in field_names:
                        continue

                    child_tstamp = obj_cols[col_name][1]
                    try:
                        self._read_child(result, obj_uuid, child_type, child_uuid, child_tstamp)
                    except cfgm_common.exceptions.NoIdError:
                        continue

                # TODO use compiled RE
                if self._re_match_ref.match(col_name):
                    (_, ref_type, ref_uuid) = col_name.split(':')
                    self._read_ref(result, obj_uuid, ref_type, ref_uuid, obj_cols[col_name][0])

                if self._re_match_backref.match(col_name):
                    (_, back_ref_type, back_ref_uuid) = col_name.split(':')
                    if field_names and '%s_back_refs' %(back_ref_type) not in field_names:
                        continue

                    try:
                        self._read_back_ref(result, obj_uuid, back_ref_type, back_ref_uuid,
                                            obj_cols[col_name][0])
                    except cfgm_common.exceptions.NoIdError:
                        continue

            # for all column names


            results.append(result)
        # end for all rows

        return (True, results)
    #end _cassandra_logical_router_read

    def _cassandra_logical_router_count_children(self, obj_uuid, child_type):
        # if child_type = None, return
        if child_type is None:
            return (False, '')

        obj_uuid_cf = self._obj_uuid_cf
        if child_type not in LogicalRouter.children_fields:
            return (False, '%s is not a valid children of LogicalRouter' %(child_type))

        col_start = 'children:'+child_type[:-1]+':'
        col_finish = 'children:'+child_type[:-1]+';'
        num_children = obj_uuid_cf.get_count(obj_uuid,
                                   column_start = col_start,
                                   column_finish = col_finish,
                                   max_count = 10000000)
        return (True, num_children)
    #end _cassandra_logical_router_count_children

    def _cassandra_logical_router_update(self, obj_uuid, new_obj_dict):
        # Grab ref-uuids and properties in new version
        new_ref_infos = {}

        if 'virtual_machine_interface_refs' in new_obj_dict:
            new_ref_infos['virtual_machine_interface'] = {}
            new_refs = new_obj_dict['virtual_machine_interface_refs']
            if new_refs:
                for new_ref in new_refs:
                    new_ref_uuid = self.fq_name_to_uuid('virtual_machine_interface', new_ref['to'])
                    new_ref_attr = new_ref.get('attr', None)
                    new_ref_data = {'attr': new_ref_attr, 'is_weakref': False}
                    new_ref_infos['virtual_machine_interface'][new_ref_uuid] = new_ref_data

        if 'route_target_refs' in new_obj_dict:
            new_ref_infos['route_target'] = {}
            new_refs = new_obj_dict['route_target_refs']
            if new_refs:
                for new_ref in new_refs:
                    new_ref_uuid = self.fq_name_to_uuid('route_target', new_ref['to'])
                    new_ref_attr = new_ref.get('attr', None)
                    new_ref_data = {'attr': new_ref_attr, 'is_weakref': False}
                    new_ref_infos['route_target'][new_ref_uuid] = new_ref_data

        if 'virtual_network_refs' in new_obj_dict:
            new_ref_infos['virtual_network'] = {}
            new_refs = new_obj_dict['virtual_network_refs']
            if new_refs:
                for new_ref in new_refs:
                    new_ref_uuid = self.fq_name_to_uuid('virtual_network', new_ref['to'])
                    new_ref_attr = new_ref.get('attr', None)
                    new_ref_data = {'attr': new_ref_attr, 'is_weakref': False}
                    new_ref_infos['virtual_network'][new_ref_uuid] = new_ref_data

        if 'service_instance_refs' in new_obj_dict:
            new_ref_infos['service_instance'] = {}
            new_refs = new_obj_dict['service_instance_refs']
            if new_refs:
                for new_ref in new_refs:
                    new_ref_uuid = self.fq_name_to_uuid('service_instance', new_ref['to'])
                    new_ref_attr = new_ref.get('attr', None)
                    new_ref_data = {'attr': new_ref_attr, 'is_weakref': False}
                    new_ref_infos['service_instance'][new_ref_uuid] = new_ref_data

        new_props = {}
        if 'id_perms' in new_obj_dict:
            new_props['id_perms'] = new_obj_dict['id_perms']
        if 'display_name' in new_obj_dict:
            new_props['display_name'] = new_obj_dict['display_name']
        # Gather column values for obj and updates to backrefs
        # in a batch and write it at the end
        obj_uuid_cf = self._obj_uuid_cf
        obj_cols_iter = obj_uuid_cf.xget(obj_uuid)
        # TODO optimize this (converts tuple to dict)
        obj_cols = {}
        for col_info in obj_cols_iter:
            obj_cols[col_info[0]] = col_info[1]

        bch = obj_uuid_cf.batch()
        for col_name in obj_cols.keys():
            # TODO use compiled RE
            if re.match('prop:', col_name):
                (_, prop_name) = col_name.split(':')
                if prop_name == 'id_perms':
                    # id-perms always has to be updated for last-mod timestamp
                    # get it from request dict(or from db if not in request dict)
                    new_id_perms = new_obj_dict.get(prop_name, json.loads(obj_cols[col_name]))
                    self.update_last_modified(bch, obj_uuid, new_id_perms)
                elif prop_name in new_obj_dict:
                    self._update_prop(bch, obj_uuid, prop_name, new_props)

            # TODO use compiled RE
            if re.match('ref:', col_name):
                (_, ref_type, ref_uuid) = col_name.split(':')
                self._update_ref(bch, 'logical_router', obj_uuid, ref_type, ref_uuid, new_ref_infos)
        # for all column names

        # create new refs
        for ref_type in new_ref_infos.keys():
            for ref_uuid in new_ref_infos[ref_type].keys():
                ref_data = new_ref_infos[ref_type][ref_uuid]
                self._create_ref(bch, 'logical_router', obj_uuid, ref_type, ref_uuid, ref_data)

        # create new props
        for prop_name in new_props.keys():
            self._create_prop(bch, obj_uuid, prop_name, new_props[prop_name])

        bch.send()

        return (True, '')
    #end _cassandra_logical_router_update

    def _cassandra_logical_router_list(self, parent_uuids=None, back_ref_uuids=None,
                           obj_uuids=None, count=False, filters=None):
        children_fq_names_uuids = []
        if filters:
            fnames = filters.get('field_names', [])
            fvalues = filters.get('field_values', [])
            filter_fields = [(fnames[i], fvalues[i]) for i in range(len(fnames))]
        else:
            filter_fields = []

        def filter_rows(coll_infos, filter_cols, filter_params):
            filt_infos = {}
            coll_rows = obj_uuid_cf.multiget(coll_infos.keys(),
                                   columns=filter_cols,
                                   column_count=10000000)
            for row in coll_rows:
                # give chance for zk heartbeat/ping
                gevent.sleep(0)
                full_match = True
                for fname, fval in filter_params:
                    if coll_rows[row]['prop:%s' %(fname)] != fval:
                        full_match = False
                        break
                if full_match:
                    filt_infos[row] = coll_infos[row]
            return filt_infos
        # end filter_rows

        def get_fq_name_uuid_list(obj_uuids):
            ret_list = []
            for obj_uuid in obj_uuids:
                try:
                    obj_fq_name = self.uuid_to_fq_name(obj_uuid)
                    ret_list.append((obj_fq_name, obj_uuid))
                except cfgm_common.exceptions.NoIdError:
                    pass
            return ret_list
        # end get_fq_name_uuid_list

        if parent_uuids:
            # go from parent to child
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'children:logical_router:'
            col_fin = 'children:logical_router;'
            try:
                obj_rows = obj_uuid_cf.multiget(parent_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_parent_anchor(sort=False):
                # flatten to [('children:<type>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_child_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    child_uuid = col_name.split(':')[2]
                    if obj_uuids and child_uuid not in obj_uuids:
                        continue
                    all_child_infos[child_uuid] = {'uuid': child_uuid, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_child_infos = filter_rows(all_child_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_child_infos = all_child_infos

                if not sort:
                    ret_child_infos = filt_child_infos.values()
                else:
                    ret_child_infos = sorted(filt_child_infos.values(), key=itemgetter('tstamp'))

                return get_fq_name_uuid_list(r['uuid'] for r in ret_child_infos)
            # end filter_rows_parent_anchor

            if count:
                return (True, len(filter_rows_parent_anchor()))

            children_fq_names_uuids = filter_rows_parent_anchor(sort=True)

        if back_ref_uuids:
            # go from anchor to backrefs
            obj_uuid_cf = self._obj_uuid_cf
            col_start = 'backref:logical_router:'
            col_fin = 'backref:logical_router;'
            try:
                obj_rows = obj_uuid_cf.multiget(back_ref_uuids,
                                       column_start = col_start,
                                       column_finish = col_fin,
                                       column_count = 10000000,
                                       include_timestamp = True)
            except pycassa.NotFoundException:
                if count:
                    return (True, 0)
                else:
                    return (True, children_fq_names_uuids)

            def filter_rows_backref_anchor():
                # flatten to [('<fqnstr>:<uuid>', (<val>,<ts>), *]
                all_cols = [cols for obj_key in obj_rows.keys() for cols in obj_rows[obj_key].items()]
                all_backref_infos = {}
                for col_name, col_val_ts in all_cols:
                    # give chance for zk heartbeat/ping
                    gevent.sleep(0)
                    col_name_arr = col_name.split(':')
                    fq_name = col_name_arr[:-1]
                    obj_uuid = col_name_arr[-1]
                    if obj_uuids and obj_uuid not in obj_uuids:
                        continue
                    all_backref_infos[obj_uuid] = \
                        {'uuid': obj_uuid, 'fq_name': fq_name, 'tstamp': col_val_ts[1]}

                filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                if filter_cols:
                    filt_backref_infos = filter_rows(all_backref_infos, filter_cols, filter_fields)
                else: # no filter specified
                    filt_backref_infos = all_backref_infos

                return [(br_info['fq_name'], br_info['uuid']) for br_info in filt_backref_infos.values()]
            # end filter_rows_backref_anchor

            if count:
                return (True, len(filter_rows_backref_anchor()))

            children_fq_names_uuids = filter_rows_backref_anchor()

        if not parent_uuids and not back_ref_uuids:
            obj_uuid_cf = self._obj_uuid_cf
            if obj_uuids:
                # exact objects specified
                def filter_rows_object_list():
                    all_obj_infos = {}
                    for obj_uuid in obj_uuids:
                        all_obj_infos[obj_uuid] = None

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return get_fq_name_uuid_list(filt_obj_infos.keys())
                # end filter_rows_object_list

                if count:
                    return (True, len(filter_rows_object_list()))
                children_fq_names_uuids = filter_rows_object_list()

            else: # grab all resources of this type
                obj_fq_name_cf = self._obj_fq_name_cf
                try:
                    cols = obj_fq_name_cf.get('logical_router', column_count = 10000000)
                except pycassa.NotFoundException:
                    if count:
                        return (True, 0)
                    else:
                        return (True, children_fq_names_uuids)

                def filter_rows_no_anchor():
                    all_obj_infos = {}
                    for col_name, col_val in cols.items():
                        # give chance for zk heartbeat/ping
                        gevent.sleep(0)
                        col_name_arr = utils.decode_string(col_name).split(':')
                        obj_uuid = col_name_arr[-1]
                        all_obj_infos[obj_uuid] = (col_name_arr[:-1], obj_uuid)

                    filter_cols = ['prop:%s' %(fname) for fname, _ in filter_fields]
                    if filter_cols:
                        filt_obj_infos = filter_rows(all_obj_infos, filter_cols, filter_fields)
                    else: # no filters specified
                        filt_obj_infos = all_obj_infos

                    return filt_obj_infos.values()
                # end filter_rows_no_anchor

                if count:
                    return (True, len(filter_rows_no_anchor()))

                children_fq_names_uuids = filter_rows_no_anchor()

        return (True, children_fq_names_uuids)
    #end _cassandra_logical_router_list

    def _cassandra_logical_router_delete(self, obj_uuid):
        obj_uuid_cf = self._obj_uuid_cf
        fq_name = json.loads(obj_uuid_cf.get(obj_uuid, columns = ['fq_name'])['fq_name'])
        bch = obj_uuid_cf.batch()

        # unlink from parent
        col_start = 'parent:'
        col_fin = 'parent;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, parent_type, parent_uuid) = col_name.split(':')
            self._delete_child(bch, parent_type, parent_uuid, 'logical_router', obj_uuid)

        # remove refs
        col_start = 'ref:'
        col_fin = 'ref;'
        col_name_iter = obj_uuid_cf.xget(obj_uuid, column_start = col_start, column_finish = col_fin)
        for (col_name, col_val) in col_name_iter:
            (_, ref_type, ref_uuid) = col_name.split(':')
            self._delete_ref(bch, 'logical_router', obj_uuid, ref_type, ref_uuid)

        bch.remove(obj_uuid)
        bch.send()

        # Update fqname table
        fq_name_str = ':'.join(fq_name)
        fq_name_col = utils.encode_string(fq_name_str) + ':' + obj_uuid
        self._obj_fq_name_cf.remove('logical_router', columns = [fq_name_col])


        return (True, '')
    #end _cassandra_logical_router_delete

