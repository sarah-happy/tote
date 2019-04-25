import sys
import argparse

from .store import Store, load_blob

def blob_cat(args):
    from tote import workdir
    w = workdir.attach()
    store = w.get_store()
    b = store.load_blob(args.data)
    sys.stdout.buffer.write(b)

def show_workdir(args):
    import tote.workdir
    wd = tote.workdir.attach(args.path)
    print('path =', wd.path)
    print('store.path =', wd.config.get('store', 'path', fallback=None))
    print('get_store() =', wd.get_store())

def put(args):
    from tote import workdir, treescan, save_file, save_stream, tojsons
    import sys
    from itertools import chain
    
    wd = workdir.attach()
    store = wd.get_store()
    
    files = args.path
    if files:
        if args.recursive:
            files = chain.from_iterable(map(treescan, files))
        for file in files:
            f = save_file(file, store)
            print(tojsons(f))
    else:
        f = save_stream(sys.stdin.buffer, store)
        print(tojsons(f))
        
def cat(args):
    from tote import workdir
    from tote.text import fromjsons
    from tote.save import load_content
    
    wd = workdir.attach()
    store = wd.get_store()
    
    out = sys.stdout
    
    if args.tote:
        for file in args.tote:
            with open(file) as f:
                for item in fromjsons(f):
                    for chunk in load_content(item, store):
                        out.write(chunk)
    else:
        for item in fromjsons(sys.stdin):
            for chunk in load_content(item, store):
                out.write(chunk)

def scan(args):
    from tote import treescan
    from itertools import chain
    files = args.path
    files = chain.from_iterable(map(treescan, files))
    for file in files:
        print(file)
                
def echo(args):
    print(args)
    
def append(args):
    import sys
    from tote import treescan, save_file, tojsons
    from itertools import chain
    
    from tote import workdir
    w = workdir.attach()
    store = w.get_store()

    arc = args.tote
    files = args.file
    recursive = args.recursive
    u = sys.stdout

    with open(arc, 'at') as o:
        if recursive:
            files = chain.from_iterable(map(treescan, files))
        for file in files:
            f = save_file(file, store)
            o.write(tojsons(f))
            print('append', file, file=u)

def cmd_list(args):
    import sys
    from tote import fromjsons
    from tote import workdir
    
    arc = args.tote

    with open(arc, 'rt') as i:
        for item in fromjsons(i):
            print(item['type'], item.get('size', None), item['name'])
            
def fold_pipe(args):
    from tote import workdir, save_chunk, fromjsons, tojsons, Fold

    arc = args.tote
    
    w = workdir.attach()
    store = w.get_store()

    def output(item):
        sys.stdout.write(tojsons(item))

    with open(arc, 'rt') as f:
        with Fold(store, func=output) as fold:
            for item in fromjsons(f):
                fold.append(item)

def main(argv=None):
    p = argparse.ArgumentParser(prog='tote')
    s = p.add_subparsers()

    c = s.add_parser('blob-cat', help='copy a blob to stdout')
    c.add_argument('data', help='key of the blob')
    c.set_defaults(func=blob_cat)

    c = s.add_parser('show-workdir', help='print workdir')
    c.add_argument('path', nargs='?', default=None)
    c.set_defaults(func=show_workdir)
    
    c = s.add_parser('put', help='save stdin or files and print stream')
    c.add_argument('path', nargs='*')
    c.add_argument('--recursive', action='store_true', help='recursively decend into directories')
    c.set_defaults(func=put)
    
    c = s.add_parser('scan', help='show all the included files')
    c.add_argument('path', nargs='+')
    c.set_defaults(func=scan)
    
    c = s.add_parser('cat', help='copy content of files in stream to stdout')
    c.add_argument('tote', nargs='*')
    c.set_defaults(func=cat)
    
    c = s.add_parser('echo', help='print args object')
    c.add_argument('args', nargs='*')
    c.add_argument('--arg')
    c.add_argument('--recursive', action='store_true')
    c.set_defaults(func=echo)
    
    c = s.add_parser('append', help='append files to list')
    c.add_argument('tote')
    c.add_argument('file', nargs='+')
    c.add_argument('--recursive', action='store_true', help='recursively decend into directories')
    c.set_defaults(func=append)
    
    c = s.add_parser('list', help='list files in list')
    c.add_argument('tote')
    c.set_defaults(func=cmd_list)
    
    c = s.add_parser('fold-pipe', help='fold list to stdout')
    c.add_argument('tote')
    c.set_defaults(func=fold_pipe)

    args = p.parse_args(argv)
    
    if 'func' not in args:
        p.print_usage()
        return

    args.func(args)
