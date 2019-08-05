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


