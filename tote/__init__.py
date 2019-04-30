import sys

from contextlib import contextmanager

from .save import save_file, save_stream, save_chunk, fold, itemkey
from .scan import treescan
from .text import tojsons, fromjsons
from .save import load_content
from .save import unfold

from . import workdir

def get_workdir(path=None):
    return workdir.attach(path)

def get_store(path=None):
    wd = get_workdir(path)
    return wd.get_store()


@contextmanager
def readtote(name):
    with open(name) as f:
        yield fromjsons(f)

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
