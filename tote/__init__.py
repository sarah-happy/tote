from .save import save_file, save_stream, save_chunk, fold, itemkey
from .scan import treescan
from .text import tojsons, fromjsons
from .save import load_content
from .save import unfold

from . import workdir

def get_store(path=None):
    wd = workdir.attach(path)
    return wd.get_store()

def get_workdir(path=None):
    return workdir.attach(path)