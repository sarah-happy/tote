import configparser
from os.path import join, isdir, dirname, abspath
from os.path import expanduser, expandvars

import tote
import tote.store

class WorkDirNotFoundError(FileNotFoundError):
    pass
    
def is_workdir(path):
    return isdir(join(path, '.tote'))

def parents(path):
    path = abspath(path)
    parent = dirname(path)
    while parent != path:
        path = parent
        parent = dirname(path)
        yield path
    return

def find_workdir(path='.'):
    if is_workdir(path):
        return path
    for p in parents(path):
        if is_workdir(p):
            return p
    raise WorkDirNotFoundError("no .tote in current or parent folders")

def load_config(file):
    c = configparser.ConfigParser()
    c.read(file)
    return c

def attach(path=None):
    if path is None:
        path = find_workdir()
    if is_workdir(path):
        return WorkDir(path)
    raise WorkDirNotFoundError(join(path, '.tote'))

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
        
    def __repr__(self):
        return "[WorkDir: %s]" % (self.path)
    
    