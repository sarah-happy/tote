import configparser
import os
from os.path import join, isdir, dirname, abspath, isfile
from os.path import expanduser, expandvars

import tote
import tote.store
import tote.scan

def is_workdir(path):
    return isdir(join(path, '.tote'))

def parents(path):
    path = abspath(path)
    parent = dirname(path)
    while parent != path:
        path = parent
        yield path
        parent = dirname(path)
    return

def find_workdir(path='.'):
    if is_workdir(path):
        return path
    for p in parents(path):
        if is_workdir(p):
            return p
    raise FileNotFoundError("no .tote in current or parent folders")

def load_config(file):
    c = configparser.ConfigParser()
    c.read(file)
    return c

def attach(path=None):
    if path is None:
        path = find_workdir()
    if is_workdir(path):
        return WorkDir(path)
    raise FileNotFoundError(join(path, '.tote'))

class WorkDir:
    def __init__(self, path):
        self.path = path
        self.config = load_config(join(path, '.tote/config'))

    def get_store(self):
        p = self.config.get('store', 'path', fallback=None)
        if p is None:
            return tote.store.attach(join(self.path, '.tote'))
        p = expanduser(p)
        p = expandvars(p)
        return tote.store.attach(p)
    
    def get_ignore(self):
        return tote.scan.make_ignore(self.path)
    
    def most_recent_checkin(self):
        return most_recent_checkin(self.path)

    def read_most_recent_checkin(self, store=None):
        if store is None:
            store = self.get_store()
        return read_most_recent_checkin(self.path, store)
    
    def __repr__(self):
        return "[WorkDir: %s]" % (self.path)
    

def most_recent_checkin(workdir):
    """
    find the most recent checkin for the workdir

    returns a path to the list, or None
    """
    try:
        l = os.listdir(join(workdir, '.tote', 'checkin', 'default'))
    except FileNotFoundError:
        return None

    for name in sorted(l, reverse=True):
        path = join(workdir, '.tote', 'checkin', 'default', name)
        if os.path.getsize(path) == 0:
            continue
        return path
    return None


def read_most_recent_checkin(workdir, store):
    """
    yield item objects from most recent checkin sorted by path parts
    """
    path = most_recent_checkin(workdir)
    if path is None:
        return
    items = tote.loadtote(path)
    yield from tote.unfold(items, store)


def checkin_status(wd):
    lista = wd.read_most_recent_checkin()
    listb = tote.scan.scan_tree_relative(wd.path, ignore=wd.get_ignore(), one_filesystem=True)
    for a, b in tote.scan.merge_sorted(lista, listb):
        if a is None:
            print('new', b['name'])
            continue
        
        if b is None:
            print('del', a['name'])
            continue
        
        if a == b:
            continue
        
        if b['type'] == 'file':
            same = (
                a.get(f, None) == b.get(f, None) 
                for f in ('type', 'size', 'mtime')
            )
            if all(same):
                continue
        
        print('update', b['name'])


def checkin_save(wd):
    lista = wd.read_most_recent_checkin()
    listb = tote.scan.scan_tree_relative(wd.path, ignore=wd.get_ignore(), one_filesystem=True)
    for a, b in tote.scan.merge_sorted(lista, listb):
        if a is None:
            print('new', b['name'])
            if isfile(join(wd.path, b['name'])):
                with open(join(wd.path, b['name']), 'rb') as file:
                    b.update(tote.save_stream(file, wd.get_store()))
            yield b
            continue
        
        if b is None:
            print('del', a['name'])
            continue
        
        # neigher a nor b are None
        
        if a == b:
            yield a
            continue
        
        if b['type'] == 'file':
            same = (
                a.get(f, None) == b.get(f, None) 
                for f in ('type', 'size', 'mtime')
            )
            if all(same):
                yield a
                continue
            with open(join(wd.path, b['name']), 'rb') as file:
                b.update(tote.save_stream(file, wd.get_store()))

        print('update', b['name'])
        yield b
