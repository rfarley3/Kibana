#!/usr/bin/env python
from __future__ import absolute_import, unicode_literals, print_function

import re
try:
    from urllib2 import urlopen, HTTPError
except ImportError:
    # Python 3
    from urllib.request import urlopen
    from urllib.error import HTTPError
import json
import requests
import time
import sys


DEBUG = True
PY3 = False
if sys.version_info[0] >= 3:
    PY3 = True


def iteritems(d):
    if PY3:
        return d.items()
    else:
        return d.iteritems()


class KibanaMapping():
    def __init__(self, index, index_pattern, host):
        self.index = index
        self._index_pattern = index_pattern
        self._host = host
        self.update_urls()
        # from the js possible mappings are:
        #     { type, indexed, analyzed, doc_values }
        # but indexed and analyzed are .kibana specific,
        # determined by the value within ES's 'index', which could be:
        #     { analyzed, no, not_analyzed }
        self.mappings = ['type', 'doc_values']
        # ignore system fields:
        self.sys_mappings = ['_source', '_index', '_type', '_id']
        # .kibana has some fields to ignore too:
        self.mappings_ignore = ['count']

    def pr_dbg(self, msg):
        if DEBUG:
            print('[DBG] Mapping %s' % msg)

    def pr_inf(self, msg):
        print('[INF] Mapping %s' % msg)

    def pr_err(self, msg):
        print('[ERR] Mapping %s' % msg)

    def update_urls(self):
        # 'http://localhost:5601/elasticsearch/aaa*/_mapping/field/*?ignore_unavailable=false&allow_no_indices=false&include_defaults=true'
        # 'http://localhost:9200/aaa*/_mapping/field/*?ignore_unavailable=false&allow_no_indices=false&include_defaults=true'
        self.es_get_url = ('http://%s:%s/' % (self._host[0], self._host[1]) +
                           '%s/' % self._index_pattern +
                           '_mapping/field/' +
                           '*?ignore_unavailable=false&' +
                           'allow_no_indices=false&' +
                           'include_defaults=true')
        # 'http://localhost:5601/elasticsearch/.kibana/index-pattern/aaa*'
        # 'http://localhost:9200/.kibana/index-pattern/aaa*'
        self.post_url = ('http://%s:%s/' % (self._host[0], self._host[1]) +
                         '%s/' % self.index +
                         'index-pattern/%s' % self._index_pattern)
        # 'http://localhost:5601/elasticsearch/.kibana/index-pattern/aaa*'
        # 'http://localhost:9200/.kibana/index-pattern/_search/?id=aaa*'
        # 'http://localhost:9200/.kibana/index-pattern/aaa*/'
        self.get_url = ('http://%s:%s/' % (self._host[0], self._host[1]) +
                        '%s/' % self.index +
                        'index-pattern/%s/' % self._index_pattern)

    @property
    def index_pattern(self):
        return self._index_pattern

    @index_pattern.setter
    def index_pattern_setter(self, index_pattern):
        self._index_pattern = index_pattern
        self.update_urls()

    @property
    def host(self):
        return self._host

    @host.setter
    def host_setter(self, host):
        self._host = host
        self.update_urls()

    def get_field_cache(self, cache_type='es'):
        """Return a list of fields' mappings"""
        if cache_type == 'kibana':
            try:
                search_results = urlopen(self.get_url).read().decode('utf-8')
            except HTTPError:  # as e:
                # self.pr_err("get_field_cache(kibana), HTTPError: %s" % e)
                return []
            index_pattern = json.loads(search_results)
            # Results look like: {"_index":".kibana","_type":"index-pattern","_id":"aaa*","_version":6,"found":true,"_source":{"title":"aaa*","fields":"<what we want>"}}  # noqa
            fields_str = index_pattern['_source']['fields']
            return json.loads(fields_str)
        elif cache_type == 'es' or cache_type.startswith('elastic'):
            search_results = urlopen(self.es_get_url).read().decode('utf-8')
            es_mappings = json.loads(search_results)
            # Results look like: {"<index_name>":{"mappings":{"<doc_type>":{"<field_name>":{"full_name":"<field_name>","mapping":{"<sub-field_name>":{"type":"date","index_name":"<sub-field_name>","boost":1.0,"index":"not_analyzed","store":false,"doc_values":false,"term_vector":"no","norms":{"enabled":false},"index_options":"docs","index_analyzer":"_date/16","search_analyzer":"_date/max","postings_format":"default","doc_values_format":"default","similarity":"default","fielddata":{},"ignore_malformed":false,"coerce":true,"precision_step":16,"format":"dateOptionalTime","null_value":null,"include_in_all":false,"numeric_resolution":"milliseconds","locale":""}}},  # noqa
            # now convert the mappings into the .kibana format
            field_cache = []
            for (index_name, val) in iteritems(es_mappings):
                if index_name != self.index:  # only get non-'.kibana' indices
                    # self.pr_dbg("index: %s" % index_name)
                    m_dict = es_mappings[index_name]['mappings']
                    # self.pr_dbg('m_dict %s' % m_dict)
                    mappings = self.get_index_mappings(m_dict)
                    # self.pr_dbg('mappings %s' % mappings)
                    field_cache.extend(mappings)
            field_cache = self.dedup_field_cache(field_cache)
            return field_cache
        self.pr_err("Unknown cache type: %s" % cache_type)
        return None

    def dedup_field_cache(self, field_cache):
        deduped = []
        fields_found = {}
        for field in field_cache:
            name = field['name']
            if name not in fields_found:
                deduped.append(field)
                fields_found[name] = field
            elif fields_found[name] != field:
                self.pr_dbg("Dup field doesn't match")
                self.pr_dbg("1st found: %s" % fields_found[name])
                self.pr_dbg("  Dup one: %s" % field)
            # else ignore, pass
        return deduped

    def post_field_cache(self, field_cache):
        """Where field_cache is a list of fields' mappings"""
        index_pattern = self.field_cache_to_index_pattern(field_cache)
        # self.pr_dbg("request/post: %s" % index_pattern)
        resp = requests.post(self.post_url, data=index_pattern).text
        # resp = {"_index":".kibana","_type":"index-pattern","_id":"aaa*","_version":1,"created":true}  # noqa
        resp = json.loads(resp)
        return 0
        # TODO detect failure (return 1)

    def field_cache_to_index_pattern(self, field_cache):
        """Return a .kibana index-pattern doc_type"""
        mapping_dict = {}
        mapping_dict['customFormats'] = "{}"
        mapping_dict['title'] = self.index_pattern
        # now post the data into .kibana
        mapping_dict['fields'] = json.dumps(field_cache, separators=(',', ':'))
        # in order to post, we need to create the post string
        mapping_str = json.dumps(mapping_dict, separators=(',', ':'))
        return mapping_str

    def check_mapping(self, m):
        """Assert minimum set of fields in cache, does not validate contents"""
        if 'name' not in m:
            self.pr_dbg("Missing %s" % "name")
            return False
        # self.pr_dbg("Checking %s" % m['name'])
        for x in ['analyzed', 'indexed', 'type', 'scripted', 'count']:
            if x not in m or m[x] == "":
                self.pr_dbg("Missing %s" % x)
                self.pr_dbg("Full %s" % m)
                return False
        if 'doc_values' not in m or m['doc_values'] == "":
            if not m['name'].startswith('_'):
                self.pr_dbg("Missing %s" % "doc_values")
                return False
            m['doc_values'] = False
        return True

    def get_index_mappings(self, index):
        """Converts all index's doc_types to .kibana"""
        fields_arr = []
        for (key, val) in iteritems(index):
            # self.pr_dbg("\tdoc_type: %s" % key)
            doc_mapping = self.get_doc_type_mappings(index[key])
            # self.pr_dbg("\tdoc_mapping: %s" % doc_mapping)
            if doc_mapping is None:
                return None
            # keep adding to the fields array
            fields_arr.extend(doc_mapping)
        return fields_arr

    def get_doc_type_mappings(self, doc_type):
        """Converts all doc_types' fields to .kibana"""
        doc_fields_arr = []
        found_score = False
        for (key, val) in iteritems(doc_type):
            # self.pr_dbg("\t\tfield: %s" % key)
            # self.pr_dbg("\tval: %s" % val)
            add_it = False
            retdict = {}
            # _ are system
            if not key.startswith('_'):
                if 'mapping' not in doc_type[key]:
                    self.pr_err("No mapping in doc_type[%s]" % key)
                    return None
                if key in doc_type[key]['mapping']:
                    subkey_name = key
                else:
                    subkey_name = re.sub('.*\.', '', key)
                if subkey_name not in doc_type[key]['mapping']:
                    self.pr_err(
                        "Couldn't find subkey " +
                        "doc_type[%s]['mapping'][%s]" % (key, subkey_name))
                    return None
                # self.pr_dbg("\t\tsubkey_name: %s" % subkey_name)
                retdict = self.get_field_mappings(
                    doc_type[key]['mapping'][subkey_name])
                add_it = True
            # system mappings don't list a type,
            # but kibana makes them all strings
            if key in self.sys_mappings:
                retdict['analyzed'] = False
                retdict['indexed'] = False
                if key == '_source':
                    retdict = self.get_field_mappings(
                        doc_type[key]['mapping'][key])
                    retdict['type'] = "_source"
                elif key == '_score':
                    retdict['type'] = "number"
                elif 'type' not in retdict:
                    retdict['type'] = "string"
                add_it = True
            if add_it:
                retdict['name'] = key
                retdict['count'] = 0  # always init to 0
                retdict['scripted'] = False  # I haven't observed a True yet
                if not self.check_mapping(retdict):
                    self.pr_err("Error, invalid mapping")
                    return None
                # the fields element is an escaped array of json
                # make the array here, after all collected, then escape it
                doc_fields_arr.append(retdict)
        if not found_score:
            doc_fields_arr.append(
                {"name": "_score",
                 "type": "number",
                 "count": 0,
                 "scripted": False,
                 "indexed": False,
                 "analyzed": False,
                 "doc_values": False})
        return doc_fields_arr

    def get_field_mappings(self, field):
        """Converts ES field mappings to .kibana field mappings"""
        retdict = {}
        retdict['indexed'] = False
        retdict['analyzed'] = False
        for (key, val) in iteritems(field):
            if key in self.mappings:
                if (key == 'type' and
                    (val == "long" or
                     val == "integer" or
                     val == "double")):
                    val = "number"
                # self.pr_dbg("\t\t\tkey: %s" % key)
                # self.pr_dbg("\t\t\t\tval: %s" % val)
                retdict[key] = val
            if key == 'index' and val != "no":
                retdict['indexed'] = True
                # self.pr_dbg("\t\t\tkey: %s" % key)
                # self.pr_dbg("\t\t\t\tval: %s" % val)
                if val == "analyzed":
                    retdict['analyzed'] = True
        return retdict

    def refresh_poll(self, period):
        self.poll_another = True
        while self.poll_another:
            self.do_refresh()
            self.pr_inf("Polling again in %s secs" % period)
            try:
                time.sleep(period)
            except KeyboardInterrupt:
                self.poll_another = False

    def needs_refresh(self):
        es_cache = self.get_field_cache('es')
        k_cache = self.get_field_cache('kibana')
        if self.is_kibana_cache_incomplete(es_cache, k_cache):
            return True
        return False

    def do_refresh(self, force=False):
        es_cache = self.get_field_cache('es')
        if force:
            self.pr_inf("Forcing mapping update")
            # no need to get kibana if we are forcing it
            return self.post_field_cache(es_cache)
        k_cache = self.get_field_cache('kibana')
        if self.is_kibana_cache_incomplete(es_cache, k_cache):
            self.pr_inf("Mapping is incomplete, doing update")
            return self.post_field_cache(es_cache)
        self.pr_inf("Mapping is correct, no refresh needed")
        return 0

    def is_kibana_cache_incomplete(self, es_cache, k_cache):
        """Test if k_cache is incomplete

        Assume k_cache is always correct, but could be missing new
        fields that es_cache has
        """
        # convert list into dict, with each item's ['name'] as key
        k_dict = {}
        for field in k_cache:
            # self.pr_dbg("field: %s" % field)
            k_dict[field['name']] = field
            for ign_f in self.mappings_ignore:
                k_dict[field['name']][ign_f] = 0
        es_dict = {}
        for field in es_cache:
            es_dict[field['name']] = field
            for ign_f in self.mappings_ignore:
                es_dict[field['name']][ign_f] = 0
        es_set = set(es_dict.keys())
        k_set = set(k_dict.keys())
        # reasons why kibana cache could be incomplete:
        #     k_dict is missing keys that are within es_dict
        #     We don't care if k has keys that es doesn't
        # es {1,2} k {1,2,3}; intersection {1,2}; len(es-{}) 0
        # es {1,2} k {1,2};   intersection {1,2}; len(es-{}) 0
        # es {1,2} k {};      intersection {};    len(es-{}) 2
        # es {1,2} k {1};     intersection {1};   len(es-{}) 1
        # es {2,3} k {1};     intersection {};    len(es-{}) 2
        # es {2,3} k {1,2};   intersection {2};   len(es-{}) 1
        return len(es_set - k_set.intersection(es_set)) > 0

    def list_to_compare_dict(self, list_form):
        """Convert list into a data structure we can query easier"""
        compare_dict = {}
        for field in list_form:
            if field['name'] in compare_dict:
                self.pr_dbg("List has duplicate field %s:\n%s" %
                            (field['name'], compare_dict[field['name']]))
                if compare_dict[field['name']] != field:
                    self.pr_dbg("And values are different:\n%s" % field)
                return None
            compare_dict[field['name']] = field
            for ign_f in self.mappings_ignore:
                compare_dict[field['name']][ign_f] = 0
        return compare_dict

    def compare_field_caches(self, replica, original):
        """Verify original is subset of replica"""
        if original is None:
            original = []
        if replica is None:
            replica = []
        self.pr_dbg("Comparing orig with %s fields to replica with %s fields" %
                    (len(original), len(replica)))
        # convert list into dict, with each item's ['name'] as key
        orig = self.list_to_compare_dict(original)
        if orig is None:
            self.pr_dbg("Original has duplicate fields")
            return 1
        repl = self.list_to_compare_dict(replica)
        if repl is None:
            self.pr_dbg("Replica has duplicate fields")
            return 1
        # search orig for each item in repl
        # if any items in repl not within orig or vice versa, then complain
        # make sure contents of each item match
        orig_found = {}
        for (key, field) in iteritems(repl):
            field_name = field['name']
            if field_name not in orig:
                self.pr_dbg("Replica has field not found in orig %s: %s" %
                            (field_name, field))
                return 1
            orig_found[field_name] = True
            if orig[field_name] != field:
                self.pr_dbg("Field in replica doesn't match orig:")
                self.pr_dbg("orig:%s\nrepl:%s" % (orig[field_name], field))
                return 1
        unfound = set(orig_found.keys()) - set(repl.keys())
        if len(unfound) > 0:
            self.pr_dbg("Orig contains fields that were not in replica")
            self.pr_dbg('%s' % unfound)
            return 1
        # We don't care about case when replica has more fields than orig
        # unfound = set(repl.keys()) - set(orig_found.keys())
        # if len(unfound) > 0:
        #     self.pr_dbg("Replica contains fields that were not in orig")
        #     self.pr_dbg('%s' % unfound)
        #     return 1
        self.pr_dbg("Original matches replica")
        return 0

    def test_cache(self):
        """Test if this code is equiv to Kibana.refreshFields()

        Within Kibana GUI click refreshFields, then either:
            * self.test_cache()
            * vagrant ssh -c "python -c \"
                import kibana; kibana.DotKibana('aaa*').mapping.test_cache()\""
        """
        es_cache = self.get_field_cache(cache_type='es')
        # self.pr_dbg(json.dumps(es_cache))
        kibana_cache = self.get_field_cache(cache_type='kibana')
        # self.pr_dbg(json.dumps(kibana_cache))
        return self.compare_field_caches(es_cache, kibana_cache)


# end mapping.py
