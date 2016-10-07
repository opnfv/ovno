from collections import OrderedDict

import yaml

from charmhelpers.core.hookenv import (
    related_units,
    relation_get,
    relation_ids
)

def ordereddict_representer(dumper, data):
    return dumper.represent_mapping("tag:yaml.org,2002:map", data.items())

yaml.add_representer(OrderedDict, ordereddict_representer)

def contrail_analytics_api_units():
    """Return a list of contrail analytics api units"""
    return [ unit for rid in relation_ids("contrail-analytics-api")
                  for unit in related_units(rid)
                  if relation_get("port", unit, rid) ]

def units(relation):
    """Return a list of units for the specified relation"""
    return [ unit for rid in relation_ids(relation)
                  for unit in related_units(rid) ]
