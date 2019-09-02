import sys

from contextlib import contextmanager

from .save import (
    get_file_info,
    fold,
    itemkey, 
    load_content,
    load_chunk, 
    save_file,
    save_stream,
    save_chunk, 
    ts,
    unfold
)

from .text import tojsons, fromjsons

from . import workdir, scan

from os.path import expanduser, expandvars


def get_workdir(path=None):
    return workdir.attach(path)


def get_store(path=None):
    wd = get_workdir(path)
    return wd.get_store()


@contextmanager
def readtote(name):
    with open(name, 'rt') as f:
        yield fromjsons(f)
    return


def loadtote(name):
    with readtote(name) as f:
        return list(f)


class ToteWriter:
    def __init__(self, fd=sys.stdout):
        self.fd = fd
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_value, traceback):
        pass
    def write(self, item):
        self.fd.write(tojsons(item))
    def writeall(self, items):
        for item in items:
            self.fd.write(tojsons(item))


@contextmanager
def writetote(name):
    with open(name, 'wt') as o:
        with ToteWriter(fd=o) as w:
            yield w


@contextmanager
def appendtote(name):
    with open(name, 'at') as o:
        with ToteWriter(fd=o) as w:
            yield w


from pathlib import Path
from itertools import chain
import configparser



def _find_workdir(path=None):
    '''
    search for a working directory (the directory with the .tote directory in it) starting from path, defaulting
    to the current directory, and trying all the parent directories to the root.
    '''
    if path is None:
        path = Path()
    else:
        path = Path(path)
    
    path = path.absolute()
    
    for p in chain([ path ], path.parents):
        if (p / '.tote').is_dir():
            return p
    else:
        raise FileNotFoundError("no .tote folder in path or parents of path")


def _load_config(config_path):
    c = configparser.ConfigParser()
    c.read(config_path)
    return c

        
class _ToteConnection:
    def __init__(self, workdir_path):
        self.workdir_path = Path(workdir_path)
        
        self.tote_path = self.workdir_path / '.tote'
        
        self.config = _load_config(self.tote_path / 'config')

        store_path = self.config.get('store', 'path', fallback=None)
        if store_path is None:
            self.store_path = self.workdir_path / '.tote'
        else:
            store_path = expandvars(store_path)
            store_path = expanduser(store_path)
            self.store_path = Path(store_path)
        
        self.store = FileStore(self.store_path)
    
#     with conn.read_file(file_name) as items_in:
#         for item in items_in:
#             pass

    @contextmanager
    def read_file(self, file_name, unfold=True):
        with open(file_name, 'rt') as f:
            items_in = self.read_stream(f, unfold)
            yield items_in
    
#     items_in = conn.read_stream(stream)

    def read_stream(self, stream, unfold=True):
        items_in = fromjsons(stream)

        if unfold:
            items_in = self.unfold(items_in)
        
        return items_in

#     items_in = conn.parse(bytes)

#     bytes = conn.format(item)

#     with conn.write_file(file_name) as items_out:
#         items_out.write(item)

    @contextmanager
    def write_file(self, file_name):
        with open(file_name, 'wt') as f:
            with ToteWriter(fd=f) as w:
                yield w

    @contextmanager
    def append_file(self, name):
        with open(name, 'at') as out:
            with ToteWriter(fd=out) as w:
                yield w

    def write_stream(self, stream):
        return ToteWriter(fd=stream)
        
#     items_out = conn.write_stream(stream)
#     items_out.write(item)

    def fold(self, items):
        return fold(items, self.store)

    def unfold(self, items):
        return unfold(items, self.store)

#     get -- read item into memory
#     get_file -- read item into file
#     get_stream -- read item into stream
#     get_chunks -- a generator of the chunks

    def get_chunks(self, item):
        return load_content(item, self.store)
    
#     put -- store item from memory
#     put_file - store item from file
#     put_stream - store item from stream

    def put_file(self, file_name):
        return save_file(file_name, self.store)

    def put_stream(self, stream):
        return save_stream(stream, self.store)
    
#     FileItem
#     FoldItem
#     Content


def connect(path=None):
    '''
    connect to a workspace.
    
    The workspace is the folder that has the .tote folder in it.
    
    The repository is the .tote folder in the workspace.
    '''
    path = _find_workdir(path)

    return _ToteConnection(
        workdir_path=path
    )


from .store import FileStore
