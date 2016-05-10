#!/usr/bin/env python
from __future__ import absolute_import, unicode_literals, print_function

import sys
import argparse

from .dotkibana import DotKibana


def handle_mapping(dotk, sub_mode):
    if sub_mode.startswith('refresh'):
        print("Mimicking Kibana GUI refreshFields")
        return dotk.do_mapping_refresh()
    elif sub_mode.startswith('poll'):
        return dotk.poll_mapping_refresh()
    elif dotk.needs_mapping_refresh():
        print("Mapping needs refresh")
        return 1
    print("Mapping is correct, no refresh needed")
    return 0


def handle_import(dotk, infile, pkg=False):
    print("Importing Kibana object from json file %s" % infile)
    if pkg:
        return dotk.do_pkg_import(infile)
    return dotk.do_file_import(infile)


def handle_export(dotk, exp_obj, path, pkg=False):
    return dotk.do_export(exp_obj, path, pkg)


def getargs():
    # '--poll "%s"'    % idx_patt
    # '--status "%s"'  % idx_patt
    # '--refresh "%s"' % idx_patt
    # '--import %s' --pkg' % inp_file
    # '--import %s'        % inp_file
    # '--export %s --pkg --outdir %s' % [all|config|dashboard], outpath
    # '--export %s --outdir %s'       % [all|config|dashboard], outpath
    parser = argparse.ArgumentParser(description='.kibana interaction module')
    parser.add_argument(
        '--status', '-s',
        action='store',
        dest='status_idx',
        help='exit code is mapping status')
    parser.add_argument(
        '--refresh', '-r',
        action='store',
        dest='refresh_idx',
        help='refreshes mapping')
    parser.add_argument(
        '--poll', '-p',
        action='store',
        dest='poll_idx',
        help='periodically polls mapping and refreshes if necessary')
    parser.add_argument(
        '--export', '-e',
        action='store',
        dest='export_obj',
        default='all',
        help='[all|config|dashboard name] to json individual/pkg')
    parser.add_argument(
        '--import', '-i',
        action='store',
        dest='import_file',
        help='import .kibana json obj/pkg')
    parser.add_argument(
        '--pkg',
        action='store_true',
        dest='pkg_flag',
        default=False,
        help='use pkg mode for import/export')
    parser.add_argument(
        '--outdir', '-o',
        action='store',
        dest='output_path',
        default='.',
        help='export only: output file(s) directory')
    parser.add_argument(
        '--host',
        action='store',
        dest='host',
        default='localhost:9200',
        help='ES host to use, format ip:port')
    parser.add_argument(
        '--index',
        action='store',
        dest='index',
        default='.kibana',
        help='Kibana index to work on')
    infile = None
    exp_obj = None
    map_cmd = None
    idx_pattern = None
    results = parser.parse_args()
    if results.status_idx is not None:
        idx_pattern = results.status_idx
        mode = 'mapping'
        map_cmd = 'status'
    elif results.refresh_idx is not None:
        idx_pattern = results.refresh_idx
        mode = 'mapping'
        map_cmd = 'refresh'
    elif results.poll_idx is not None:
        idx_pattern = results.poll_idx
        mode = 'mapping'
        map_cmd = 'poll'
    elif results.import_file is not None:
        infile = results.import_file
        mode = 'import'
    # export_obj has a default value, so this is always true
    elif results.export_obj is not None:
        exp_obj = results.export_obj
        mode = 'export'
    if mode is None:
        # usage
        pass
    is_pkg = results.pkg_flag
    host_arr = results.host.split(':')
    host = (host_arr[0], int(host_arr[1]))
    outdir = results.output_path
    index = results.index
    args = {}
    args['host'] = host
    args['idx_pattern'] = idx_pattern
    args['mode'] = mode
    args['is_pkg'] = is_pkg
    args['map_cmd'] = map_cmd
    args['infile'] = infile
    args['exp_obj'] = exp_obj
    args['outdir'] = outdir
    args['index'] = index
    return args


def main():
    args = getargs()
    dotk = DotKibana(index_pattern=args['idx_pattern'], host=args['host'], index=args['index'])
    if args['mode'] == 'mapping':
        return handle_mapping(dotk, args['map_cmd'])
    elif args['mode'] == 'export':
        return handle_export(
            dotk,
            args['exp_obj'],
            args['outdir'],
            args['is_pkg'])
    elif args['mode'] == 'import':
        return handle_import(
            dotk,
            args['infile'],
            args['is_pkg'])
    # else print usage


if __name__ == "__main__":
    sys.exit(main())


# end __main__.py
