import configparser
import os
import pathlib

from pathlib import Path

from os.path import join, isdir, dirname, abspath, isfile
from os.path import expanduser, expandvars

import tote
import tote.store
import tote.scan


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
        l = (workdir / '.tote' / 'checkin' / 'default').iterdir()
    except FileNotFoundError:
        return None

    for path in sorted(l, reverse=True):
        if path.name.endswith('.tote') and path.lstat().st_size:
            return path
    return None


def read_most_recent_checkin(conn):
    """
    yield item objects from most recent checkin sorted by path parts
    """
    path = most_recent_checkin(conn.workdir_path)
    if path is None:
        return tuple()
    
    with conn.read_file(path, unfold=False) as items:
        return conn.unfold(list(items))


def checkin_status(conn):
    lista = read_most_recent_checkin(conn)
    listb = tote.scan_trees(
        paths=[conn.workdir_path], 
        relative_to=conn.workdir_path,
        base_path=conn.workdir_path,
    )
    
    for a, b in tote.scan.merge_sorted(lista, listb):
        if a is None:
            print('new', b.name)
            continue
        
        if b is None:
            print('del', a.name)
            continue
        
        if a == b:
            continue
        
        if b.type == 'file':
            changes = {
                f 
                for f in ('type', 'size', 'mtime')
                if getattr(a, f, None) != getattr(b, f, None)
            }
            if not changes:
                continue
        
            print('changes', changes)
            for f in changes:
                print(f, getattr(a, f, None), getattr(b, f, None))

        print('update', b.name)


def checkin_save(conn):
    lista = read_most_recent_checkin(conn)
    listb = tote.scan_trees(
        paths=[conn.workdir_path], 
        relative_to=conn.workdir_path,
        base_path=conn.workdir_path,
    )
    
    for a, b in tote.scan.merge_sorted(lista, listb):
        if a is None:
            print('new', b.name)
            path = conn.workdir_path / b.name
            if path.is_file():
                try:
                    with open(path, 'rb') as file:
                        b.update(conn.put_stream(file))
                except OSError as e:
                    b.error = str(e)
            yield b
            continue
        
        if b is None:
            print('del', a.name)
            continue
        
        # neigher a nor b are None
        
        if a == b:
            yield a
            continue
        
        if b.type == 'file':
            changes = {
                f 
                for f in ('type', 'size', 'mtime')
                if getattr(a, f, None) != getattr(b, f, None)
            }
            if not changes:
                yield a
                continue
        
            print('changes', changes)
            for f in changes:
                print(f, getattr(a, f, None), getattr(b, f, None))

            path = conn.workdir_path / b.name
            try:
                with open(path, 'rb') as file:
                    b.update(conn.put_stream(file))
            except OSError as e:
                b.error = str(e)

        print('update', b.name)
        yield b
