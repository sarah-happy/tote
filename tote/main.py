import sys
import argparse
import tote

def cmd_blob_cat(args):
    store = tote.get_store()
    b = store.load_blob(args.data)
    sys.stdout.buffer.write(b)

def cmd_show_workdir(args):
    wd = tote.get_workdir(args.path)
    print('path =', wd.path)
    print('store.path =', wd.config.get('store', 'path', fallback=None))
    print('get_store() =', wd.get_store())

def cmd_put(args):
    from tote import treescan, save_file, save_stream, tojsons
    from itertools import chain
    
    store = tote.get_store()
    
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
        
def cmd_cat(args):
    from tote import readtote, fromjsons
    from tote.save import load_content
    
    store = tote.get_store()
    
    out = sys.stdout.buffer
    
    if args.tote:
        for file in args.tote:
            with readtote(file) as items:
                for item in items:
                    for chunk in load_content(item, store):
                        out.write(chunk)
    else:
        for item in fromjsons(sys.stdin):
            for chunk in load_content(item, store):
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
    from tote import treescan, save_file, tojsons
    from itertools import chain
    
    store = tote.get_store()

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
    from tote import unfold, readtote
    
    arc = args.tote

    store = tote.get_store()
    with readtote(arc) as i:
        for item in unfold(i, store):
            print(item.get('type'), item.get('size', None), item.get('name', None))
            
def cmd_fold_pipe(args):
    from tote import fold, readtote, ToteWriter

    arc = args.tote

    store = tote.get_store()
    with readtote(arc) as i, ToteWriter() as o:
        f = fold(i, store)
        o.writeall(f)

def cmd_refold_pipe(args):
    store = tote.get_store()

    from tote import fold, unfold, readtote, ToteWriter

    with readtote(args.tote) as i, ToteWriter() as o:
        l = unfold(i, store)
        f = fold(l, store)
        o.writeall(f)

def cmd_refold(args):
    store = tote.get_store()

    from tote import fold, unfold, readtote, writetote, appendtote
    import os

    name=args.tote
    with readtote(name) as i, writetote(name + '.part') as o:
        l = unfold(i, store)
        f = fold(l, store)
        o.writeall(f)

    from tote import save_file
    with appendtote(name + '.history') as o:
        o.write(save_file(name, store))

    os.rename(name + '.part', name)

    
def cmd_status(args):
    from tote.workdir import checkin_status
    wd = tote.get_workdir()
    checkin_status(wd)

    
def cmd_checkin(args):
    from tote.save import ts
    from tote.workdir import checkin_save
    from os.path import join
    import os

    wd = tote.get_workdir()
    update = checkin_save(wd)
    folds = tote.save.fold(update, wd.get_store())

    path = join(wd.path, '.tote', 'checkin', 'default')
    os.makedirs(path, exist_ok=True)

    path = join(path, ts() + '.tote')
    path_part = path + '.part'
    with tote.writetote(path_part) as f:
        # save_new_checkin looks for the most recent checkin here
        f.writeall(folds)
    os.rename(path_part, path)


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
    
    c = s.add_parser('status', help='show what changed since the last checkin')
    c.set_defaults(func=cmd_status)

    c = s.add_parser('checkin', help='checkin the current state')
    c.set_defaults(func=cmd_checkin)

    args = p.parse_args(argv)
    if 'func' not in args:
        p.print_usage()
        return

    args.func(args)
