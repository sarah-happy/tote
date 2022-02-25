import argparse
import sys
import os

from pathlib import Path

import tote


def cmd_blob_cat(args):
    conn = tote.connect()
    blob = conn.store.load_blob(args.data)
    sys.stdout.buffer.write(blob)


def cmd_show_workdir(args):
    conn = tote.connect(args.path)
    print('workdir_path =', conn.workdir_path)
    print('store_path =', conn.store_path)
    print('store =', conn.store)
    

def cmd_put(args):
    conn = tote.connect()

    out = conn.write_stream(sys.stdout)
    
    if args.path:
        paths = [ Path(path) for path in args.path ]
        for path in tote.list_trees(paths, recurse=args.recursive):
            out.write(conn.put_file(path))
    else:
        out.write(conn.put_stream(sys.stdin.buffer))


def cmd_cat(args):
    conn = tote.connect()
    
    out = sys.stdout.buffer
    
    if args.tote:
        for file in args.tote:
            with conn.read_file(file) as items_in:
                for item in items_in:
                    for chunk in conn.get_chunks(item):
                        out.write(chunk)
    else:
        for item in conn.read_stream(sys.stdin):
            for chunk in conn.get_chunks(item):
                out.write(chunk)


def cmd_scan(args):
    paths = [ Path(path) for path in args.path ]
    for item in tote.scan_trees(paths):
        print(item)
    
    
def cmd_echo(args):
    print(args)

    
def cmd_append(args):
    arc = args.tote
    files = args.file
    recursive = args.recursive
    u = sys.stdout

    conn = tote.connect()
    with conn.append_file(arc) as o:
        for file in tote.list_trees(files, recurse=recursive):
            print('append', file, file=u)
            f = conn.put_file(file)
            o.write(f)


def cmd_list(args):
    arc = args.tote
    files = args.file
    
    conn = tote.connect(arc)
    with conn.read_file(arc) as items:
        
        if files:
            items = tote._filter_items_by_names(items, files)
        
        for item in items:
            print(item.type, item.size, item.name)
            
            
def cmd_fold_pipe(args):
    arc = args.tote
    out = sys.stdout.buffer

    conn = tote.connect()
    with conn.read_file(arc, unfold=False) as i:
        with conn.write_stream(out) as o:
            f = conn.fold(i)
            o.writeall(f)


def cmd_refold_pipe(args):
    arc = args.tote
    out = sys.stdout
    
    conn = tote.connect(arc)
    with conn.read_file(arc) as items_in:
        with conn.write_stream(out) as items_out:
            items = conn.fold(items_in)
            items_out.writeall(items)

def cmd_refold(args):
    arc = args.tote

    conn = tote.connect(arc)

    with conn.read_file(arc) as items_in:
        with conn.write_file(arc + '.part') as items_out:
            items = conn.fold(items_in)
            items_out.writeall(items)

    with conn.append_file(arc + '.history') as items_out:
        items_out.write(conn.put_file(arc))

    os.rename(arc + '.part', arc)


def cmd_unfold_pipe(args):
    arc = args.tote
    out = sys.stdout
    
    conn = tote.connect(arc)
    with conn.read_file(arc) as items_in:
        with conn.write_stream(out) as items_out:
            items_out.writeall(items_in)

def cmd_unfold(args):
    arc = args.tote
    
    conn = tote.connect(arc)

    with conn.read_file(arc) as items_in:
        with conn.write_file(arc + '.part') as items_out:
            items_out.writeall(items_in)

    with conn.append_file(arc + '.history') as items_out:
        items_out.write(conn.put_file(arc))

    os.rename(arc + '.part', arc)

    
def cmd_status(args):
    conn = tote.connect()
    
    last_checkin = conn._most_recent_checkin()
    
    tote.tote_update(
        arc=last_checkin,
        paths=[conn.workdir_path], 
        relative_to=conn.workdir_path,
        base_path=conn.workdir_path,
        conn=conn,
        dryrun=True,
        verbose=True,
    )


import subprocess

def cmd_checkin(args):
    conn = tote.connect()
    timestamp = tote.format_timestamp(safe=True)
    
    # pre checkin hook
    pre_hook = conn.tote_path / "checkin-pre"
    if pre_hook.exists():
        subprocess.run([pre_hook, timestamp], check=True, cwd=conn.workdir_path)
    
    arc_output = conn.tote_path / 'checkin' / 'default' / (timestamp + '.tote')
    arc_output.parent.mkdir(parents=True, exist_ok=True)
    
    last_checkin = conn._most_recent_checkin()
    
    tote.tote_update(
        arc=last_checkin,
        arc_output=arc_output,
        paths=[conn.workdir_path], 
        relative_to=conn.workdir_path,
        base_path=conn.workdir_path,
        conn=conn,
        verbose=args.verbose,
    )
    
    # post checkin hook (arc_output)
    post_hook = conn.tote_path / "checkin-post"
    if post_hook.exists():
        subprocess.run([post_hook, arc_output], check=True, cwd=conn.workdir_path)


def cmd_add(args):
    tote.tote_update(
        arc=args.tote,
        paths=args.file,
        update=False,
    )


def cmd_refresh(args):
    tote.tote_update(
        arc=args.tote,
        paths=args.file,
        delete=True,
    )


def cmd_extract(args):
    '''extract files from archive'''
    arc = args.tote
    files = args.file
    to = args.to

    conn = tote.connect(arc)
    with conn.read_file(arc) as items_in:
        if files:
            items_in = tote._filter_items_by_names(items_in, files)

        for item in items_in:
            print(item.name)
            conn.get_file(item, out_base=to)

def cmd_import_blobs(args):
    conn = tote.connect()
    for f in args.file:
        print(f, '...')
        p = Path(f)
        with open(p, 'rb') as i:
            b = i.read()
        conn.store.save(b)
    
    
def main(argv=None):
    p = argparse.ArgumentParser(prog='tote')
    s = p.add_subparsers()

    c = s.add_parser('blob-cat', help='copy a blob to stdout')
    c.add_argument('data', help='key of the blob')
    c.set_defaults(func=cmd_blob_cat)

    c = s.add_parser('show-workdir', help='print workdir')
    c.add_argument('path', nargs='?', default=None)
    c.set_defaults(func=cmd_show_workdir)
    
    c = s.add_parser('put', help='save stdin or files and print stream')
    c.add_argument('path', nargs='*')
    c.add_argument('--recursive', action='store_true', help='recursively decend into directories')
    c.set_defaults(func=cmd_put)
    
    c = s.add_parser('scan', help='show all the included files')
    c.add_argument('path', nargs='+')
    c.set_defaults(func=cmd_scan)
    
    c = s.add_parser('cat', help='copy content of files in stream to stdout')
    c.add_argument('tote', nargs='*')
    c.set_defaults(func=cmd_cat)
    
    c = s.add_parser('echo', help='print args object')
    c.add_argument('args', nargs='*')
    c.add_argument('--arg')
    c.add_argument('--recursive', action='store_true')
    c.set_defaults(func=cmd_echo)
    
    c = s.add_parser('append', help='append files to list')
    c.add_argument('tote')
    c.add_argument('file', nargs='+')
    c.add_argument('--recursive', action='store_true', help='recursively decend into directories')
    c.set_defaults(func=cmd_append)
    
    c = s.add_parser('list', help='list files in list')
    c.add_argument('tote')
    c.add_argument('file', nargs='*')
    c.set_defaults(func=cmd_list)
    
    c = s.add_parser('fold-pipe', help='fold list to stdout')
    c.add_argument('tote')
    c.set_defaults(func=cmd_fold_pipe)

    c = s.add_parser('refold-pipe', help='refold list to stdout')
    c.add_argument('tote')
    c.set_defaults(func=cmd_refold_pipe)
    
    c = s.add_parser('refold', help='refold list')
    c.add_argument('tote')
    c.set_defaults(func=cmd_refold)
    
    c = s.add_parser('unfold-pipe', help='unfold list to stdout')
    c.add_argument('tote')
    c.set_defaults(func=cmd_unfold_pipe)

    c = s.add_parser('unfold', help='unfold list')
    c.add_argument('tote')
    c.set_defaults(func=cmd_unfold)
    
    c = s.add_parser('status', help='show what changed since the last checkin')
    c.set_defaults(func=cmd_status)

    c = s.add_parser('checkin', help='checkin the current state')
    c.add_argument('--verbose', action='store_true', help='verbose output')
    c.set_defaults(func=cmd_checkin)
    
    c = s.add_parser('add', help='add and updates files in list')
    c.add_argument('tote')
    c.add_argument('file', nargs='+')
#     c.add_argument('--recursive', action='store_true', help='recursively decend into directories')
    c.set_defaults(func=cmd_add)

    c = s.add_parser('refresh', help='update to files in list')
    c.add_argument('tote')
    c.add_argument('file', nargs='+')
#     c.add_argument('--recursive', action='store_true', help='recursively decend into directories')
    c.set_defaults(func=cmd_refresh)

    c = s.add_parser('extract', help='extract files from archive')
    c.add_argument('tote')
    c.add_argument('file', nargs='*')
    c.add_argument('--to')
    c.set_defaults(func=cmd_extract)

    c = s.add_parser('import-blobs', help='import a directory of blobs')
    c.add_argument('file', nargs='+', help='file to import')
#     c.add_argument('--recursive', action='store_true', help='recursively decend into directories')
    c.set_defaults(func=cmd_import_blobs)
    
    args = p.parse_args(argv)
    if 'func' not in args:
        p.print_usage()
        return

    args.func(args)
