#!/usr/bin/env python
from __future__ import absolute_import, unicode_literals, print_function

from elasticsearch import Elasticsearch, RequestError
from datetime import datetime
import json
import os
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


"""
Access all the internal kibana objects, like dashboards,
visualizations, saved searches, and config as json.

import-pkg:
    -for each element in [] do import

import:
    - loads json files into the internal kibana index
    - requires an additional command line argument of what file to read
    - requires the json to specify a _index, _id, and _type;
          with the actual document in _source

export-pkg:
    -[<all found objects>]

export:
    - search the internal kibana index for a specific type of document
         * 'all' or no additional CLA means all object types
         * 'config' means only the config document
         * <any ID of a dashboard> means only that dashboard and
           its visualizations/saved searches
    - write each found document to a separate json file
    - the document will appear in _source; with additional metadata needed
          to import it later in _index, _id, and _type.


This script makes requests (via ES API) for each object type specified.
Alternative is to skip the ES API and use urllib/curl on
    http://127.0.0.1:9200/.kibana/_search, json.loads the contents,
    and interact with the dict from there.
"""


class KibanaManager():
    """Import/Export Kibana objects"""
    def __init__(self, index, host):
        self._host_ip = host[0]
        self._host_port = host[1]
        self.index = index
        self.es = None
        self.max_hits = 9999

    def pr_dbg(self, msg):
        if DEBUG:
            print('[DBG] Manager %s' % msg)

    def pr_inf(self, msg):
        print('[INF] Manager %s' % msg)

    def pr_err(self, msg):
        print('[ERR] Manager %s' % msg)

    @property
    def host(self):
        return (self._host_ip, self._host_port)

    @host.setter
    def host_setter(self, host):
        self._host_ip = host[0]
        self._host_port = host[1]

    def connect_es(self):
        if self.es is not None:
            return
        self.es = Elasticsearch(
            [{'host': self._host_ip, 'port': self._host_port}])

    def read_object_from_file(self, filename):
        self.pr_inf("Reading object from file: " + filename)
        obj = {}
        with open(filename, 'r') as f:
            obj = json.loads(f.read().decode('utf-8'))
        return obj

    def read_pkg_from_file(self, filename):
        obj = {}
        with open(filename, 'r') as f:
            obj = json.loads(f.read().decode('utf-8'))
        return obj

    def put_object(self, obj):
        # TODO consider putting into a ES class
        self.pr_dbg('put_obj: %s' % self.json_dumps(obj))
        """
        Wrapper for es.index, determines metadata needed to index from obj.
        If you have a raw object json string you can hard code these:
        index is .kibana (as of kibana4);
        id can be A-Za-z0-9\- and must be unique;
        doc_type is either visualization, dashboard, search
            or for settings docs: config, or index-pattern.
        """
        if obj['_index'] is None or obj['_index'] == "":
            raise Exception("Invalid Object, no index")
        if obj['_id'] is None or obj['_id'] == "":
            raise Exception("Invalid Object, no _id")
        if obj['_type'] is None or obj['_type'] == "":
            raise Exception("Invalid Object, no _type")
        if obj['_source'] is None or obj['_source'] == "":
            raise Exception("Invalid Object, no _source")
        self.connect_es()
        self.es.indices.create(index=obj['_index'], ignore=400, timeout="2m")
        try:
            resp = self.es.index(index=obj['_index'],
                                 id=obj['_id'],
                                 doc_type=obj['_type'],
                                 body=obj['_source'], timeout="2m")
        except RequestError as e:
            self.pr_err('RequestError: %s, info: %s' % (e.error, e.info))
            raise
        return resp

    def put_pkg(self, objs):
        for obj in objs:
            self.put_object(obj)

    def put_objects(self, objects):
        for name, obj in iteritems(objects):
            self.put_object(obj)

    def del_object(self, obj):
        """Debug deletes obj of obj[_type] with id of obj['_id']"""
        if obj['_index'] is None or obj['_index'] == "":
            raise Exception("Invalid Object")
        if obj['_id'] is None or obj['_id'] == "":
            raise Exception("Invalid Object")
        if obj['_type'] is None or obj['_type'] == "":
            raise Exception("Invalid Object")
        self.connect_es()
        self.es.delete(index=obj['_index'],
                       id=obj['_id'],
                       doc_type=obj['_type'])

    def del_objects(self, objects):
        for name, obj in iteritems(objects):
            self.del_object(obj)

    def json_dumps(self, obj):
        """Serializer for consistency"""
        return json.dumps(obj, sort_keys=True, indent=4, separators=(',', ': '))

    def safe_filename(self, otype, oid):
        """Santize obj name into fname and verify doesn't already exist"""
        permitted = set(['_', '-', '(', ')'])
        oid = ''.join([c for c in oid if c.isalnum() or c in permitted])
        while oid.find('--') != -1:
            oid = oid.replace('--', '-')
        ext = 'json'
        ts = datetime.now().strftime("%Y%m%dT%H%M%S")
        fname = ''
        is_new = False
        while not is_new:
            oid_len = 255 - len('%s--%s.%s' % (otype, ts, ext))
            fname = '%s-%s-%s.%s' % (otype, oid[:oid_len], ts, ext)
            is_new = True
            if os.path.exists(fname):
                is_new = False
                ts += '-bck'
        return fname

    def write_object_to_file(self, obj, path='.'):
        """Convert obj (dict) to json string and write to file"""
        output = self.json_dumps(obj) + '\n'
        filename = self.safe_filename(obj['_type'], obj['_id'])
        filename = os.path.join(path, filename)
        self.pr_inf("Writing to file: " + filename)
        with open(filename, 'w') as f:
            f.write(output)
        # self.pr_dbg("Contents: " + output)
        return filename

    def write_objects_to_file(self, objects, path='.'):
        for name, obj in iteritems(objects):
            self.write_object_to_file(obj, path)

    def write_pkg_to_file(self, name, objects, path='.'):
        """Write a list of related objs to file"""
        # Kibana uses an array of docs, do the same
        # as opposed to a dict of docs
        pkg_objs = []
        for _, obj in iteritems(objects):
            pkg_objs.append(obj)
        output = self.json_dumps(pkg_objs) + '\n'
        filename = self.safe_filename('Pkg', name)
        filename = os.path.join(path, filename)
        self.pr_inf("Writing to file: " + filename)
        with open(filename, 'w') as f:
            f.write(output)
        # self.pr_dbg("Contents: " + output)
        return filename

    def get_objects(self, search_field, search_val):
        """Return all objects of type (assumes < MAX_HITS)"""
        query = ("{ size: " + str(self.max_hits) + ", " +
                 "query: { filtered: { filter: { " +
                 search_field + ": { value: \"" + search_val + "\"" +
                 " } } } } } }")
        self.connect_es()
        res = self.es.search(index=self.index, body=query)
        # self.pr_dbg("%d Hits:" % res['hits']['total'])
        objects = {}
        for doc in res['hits']['hits']:
            objects[doc['_id']] = {}
            # To make uploading easier in the future:
            # Record all those bits into the backup.
            # Mimics how ES returns the result.
            # Prevents having to store this in some external, contrived, format
            objects[doc['_id']]['_index'] = self.index  # also in doc['_index']
            objects[doc['_id']]['_type'] = doc['_type']
            objects[doc['_id']]['_id'] = doc['_id']
            objects[doc['_id']]['_source'] = doc['_source']  # the actual result
        return objects

    def get_config(self):
        """ Wrapper for get_objects to collect config; skips index-pattern"""
        return self.get_objects("type", "config")

    def get_visualizations(self):
        """Wrapper for get_objects to collect all visualizations"""
        return self.get_objects("type", "visualization")

    def get_dashboards(self):
        """Wrapper for get_objects to collect all dashboards"""
        return self.get_objects("type", "dashboard")

    def get_searches(self):
        """Wrapper for get_objects to collect all saved searches"""
        return self.get_objects("type", "search")

    def get_dashboard_full(self, db_name):
        """Get DB and all objs needed to duplicate it"""
        objects = {}
        dashboards = self.get_objects("type", "dashboard")
        vizs = self.get_objects("type", "visualization")
        searches = self.get_objects("type", "search")
        if db_name not in dashboards:
            return None
        self.pr_inf("Found dashboard: " + db_name)
        objects[db_name] = dashboards[db_name]
        panels = json.loads(dashboards[db_name]['_source']['panelsJSON'])
        for panel in panels:
            if 'id' not in panel:
                continue
            pid = panel['id']
            if pid in searches:
                self.pr_inf("Found search:    " + pid)
                objects[pid] = searches[pid]
            elif pid in vizs:
                self.pr_inf("Found vis:       " + pid)
                objects[pid] = vizs[pid]
                emb = vizs[pid].get('_source', {}).get('savedSearchId', None)
                if emb is not None and emb not in objects:
                    if emb not in searches:
                        self.pr_err('Missing search %s' % emb)
                        return objects
                    objects[emb] = searches[emb]
        return objects

# end manager.py
