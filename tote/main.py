import sys
import argparse
import tote


def cmd_blob_cat(args):
    conn = tote.connect()
    blob = conn.store.load_blob(blob_name)
    sys.stdout.buffer.write(blob)


def cmd_show_workdir(args):
    conn = tote.connect(args.path)
    print('workdir_path =', conn.workdir_path)
    print('store_path =', conn.store_path)
    print('get_store() =', conn.store)
    

def cmd_put(args):
    from tote import tojsons
    from tote.scan import treescan
    from itertools import chain
    
    conn = tote.connect()
    
    files = args.path
    if files:
        if args.recursive:
            files = chain.from_iterable(map(treescan, files))
        for file in files:
            f = conn.put_file(file)
            print(tojsons(f))
    else:
        f = conn.put_stream(sys.stdin.buffer)
        print(tojsons(f))


def cmd_cat(args):
    from tote import readtote, fromjsons
    
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
    from tote import treescan
    from itertools import chain
    files = args.path
    files = chain.from_iterable(map(treescan, files))
    for file in files:
        print(file)
    
    
def cmd_echo(args):
    print(args)

    
def cmd_append(args):
    from tote import tojsons
    from tote.scan import scan_trees
    from itertools import chain
    
    arc = args.tote
    files = args.file
    recursive = args.recursive
    u = sys.stdout

    conn = tote.connect()

    with open(arc, 'at') as o:
        if recursive:
            files = list_trees(files)
        for file in files:
            f = conn.put_file(file)
            o.write(tojsons(f))
            print('append', file, file=u)


def cmd_list(args):
    arc = args.tote
    files = args.file
    
    conn = tote.connect(arc)
    with conn.read_file(arc) as f:
        for item in f:
            print(item.get('type'), item.get('size', None), item.get('name', None))
            
            
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

    import os
    os.rename(arc + '.part', arc)

    
def cmd_unfold(args):
    arc = args.tote
    
    conn = tote.connect(arc)

    with conn.read_file(arc) as items_in:
        with conn.write_file(arc + '.part') as items_out:
            items_out.writeall(items_in)

    with conn.append_file(arc + '.history') as items_out:
        items_out.write(conn.put_file(arc))

    import os
    os.rename(arc + '.part', arc)

    
def cmd_status(args):
    conn = tote.connect()
    
    from tote.workdir import checkin_status
    checkin_status(conn)

    
def cmd_checkin(args):
    from tote.save import ts
    from tote.workdir import checkin_save

    conn = tote.connect()
    
    update = checkin_save(conn)
    
    folds = conn.fold(update)

    path = conn.tote_path / 'checkin' / 'default'
    path.mkdir(parents=True, exist_ok=True)

    path = path / (ts() + '.tote')
    path_part = path.with_name(path.name + '.part')
    
    with conn.write_file(path_part) as f:
        # save_new_checkin looks for the most recent checkin here
        f.writeall(folds)
    
    path_part.rename(target=path)


def cmd_add(args):
    from tote.scan import scan_trees, merge_sorted
    from pathlib import Path, PurePosixPath
    
    arc = Path(args.tote)
    paths = args.file

    def do_add(m, conn):
        for a, b in m:
            if b is None:
                yield a
                continue

            print('add' if a is None else 'update', b['name'])

            path = PurePosixPath(b['name'])
            path = Path(path)
            if path.is_file():
                with open(path, 'rb') as file:
                    b.update(conn.put_stream(file))

            yield b

    conn = tote.connect(arc)
    
    try:
        with conn.read_file(arc, unfold=False) as f:
            items_in = list(f)
    except FileNotFoundError:
        items_in = []
    items_in = conn.unfold(items_in)
    b = scan_trees(paths)
    m = merge_sorted(items_in, b)

    o = do_add(m, conn)

    arc_part = arc.with_name(arc.name + '.part')
    with conn.write_file(arc_part) as w:
        w.writeall(conn.fold(o))

    arc_history = arc.with_name(arc.name + '.history')
    if arc.is_file():
        with conn.append_file(arc_history) as w:
            w.write(conn.put_file(arc))

    arc_part.rename(target=arc)


def cmd_extract(args):
    '''extract files from archive'''
    arc = args.tote
    members = args.file
    out_base = args.to

    if members:
        print('not implemented')
        return

    conn = tote.connect(arc)
    from tote.save import extract_file
    with conn.read_file(arc) as items_in:
        for item in items_in:
            print(item['name'])
            extract_file(item, conn.store, out_base)


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
    
    c = s.add_parser('unfold', help='unfold list')
    c.add_argument('tote')
    c.set_defaults(func=cmd_unfold)
    
    c = s.add_parser('status', help='show what changed since the last checkin')
    c.set_defaults(func=cmd_status)

    c = s.add_parser('checkin', help='checkin the current state')
    c.set_defaults(func=cmd_checkin)
    
    c = s.add_parser('add', help='add and updates files in list')
    c.add_argument('tote')
    c.add_argument('file', nargs='+')
    c.add_argument('--recursive', action='store_true', help='recursively decend into directories')
    c.set_defaults(func=cmd_add)
    
    c = s.add_parser('extract', help='extract files from archive')
    c.add_argument('tote')
    c.add_argument('file', nargs='*')
    c.add_argument('--to')
    c.set_defaults(func=cmd_extract)

    args = p.parse_args(argv)
    if 'func' not in args:
        p.print_usage()
        return

    args.func(args)
