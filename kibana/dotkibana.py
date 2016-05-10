#!/usr/bin/env python
from __future__ import absolute_import, unicode_literals, print_function

from .mapping import KibanaMapping
from .manager import KibanaManager


class DotKibana():
    def __init__(self, index_pattern='*', host=('localhost', 9200), index='.kibana'):
        self._host = host
        self.index = index
        self._index_pattern = index_pattern
        self.mapping = KibanaMapping(
            self.index,
            self._index_pattern,
            self._host)
        self.manager = KibanaManager(self.index, self._host)

    @property
    def index_pattern(self):
        return self._index_pattern

    @index_pattern.setter
    def index_pattern_setter(self, index_pattern):
        self._index_pattern = index_pattern
        self.mapping.index_pattern(index_pattern)

    @property
    def host(self):
        return self._host

    @host.setter
    def host_setter(self, host):
        self._host = host
        self.mapping.host(host)
        self.manager.host(host)

    def do_mapping_refresh(self):
        return self.mapping.do_refresh()

    def poll_mapping_refresh(self, period=15):
        return self.mapping.refresh_poll(period)

    def needs_mapping_refresh(self):
        return self.mapping.needs_refresh()

    def do_file_import(self, fname):
        obj = self.manager.read_object_from_file(fname)
        return self.do_import(obj)

    def do_pkg_import(self, fname):
        objs = self.manager.read_pkg_from_file(fname)
        return self.manager.put_pkg(objs)

    def do_import(self, obj):
        self.manager.put_object(obj)
        # TODO test return value for success
        return 0

    def do_export(self, mode, path='.', pkg=False):
        print("Exporting from %s to %s" % (self.index, path))
        if mode == 'all':
            print("Exporting all objects")
            vizs = self.manager.get_visualizations()
            boards = self.manager.get_dashboards()
            searches = self.manager.get_searches()
            config = self.manager.get_config()
            print("Writing %d dashboards, %d visualizations, %d searches, "
                  "as well as the config, to disk" %
                  (len(boards), len(vizs), len(searches)))
            objects = {}
            objects.update(searches)
            objects.update(vizs)
            objects.update(boards)
            objects.update(config)
        elif mode == 'config':
            print("Exporting config object")
            objects = self.manager.get_config()
            print("Writing the config to disk")
        else:
            board_name = mode
            print("Exporting dashboard %s" % board_name)
            objects = self.manager.get_dashboard_full(board_name)
            if objects is None:
                print("Error, could not find %s" % board_name)
                return 1
            print("%d dashboard objects found" % len(objects))
        if pkg:
            print("Writing package to disk")
            self.manager.write_pkg_to_file(mode, objects, path)
        else:
            print("Writing %d objects to disk" % len(objects))
            self.manager.write_objects_to_file(objects, path)
        print("Export complete")
        return 0

# end dotkibana.py
