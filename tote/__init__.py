import configparser
import sys
import json
import os
import time
import zlib

from Crypto.Cipher import AES
from Crypto.Util import Counter
from datetime import datetime, timezone
from dataclasses import dataclass, field
from fnmatch import fnmatch
from functools import lru_cache
from hashlib import sha256
from heapq import heapify, heappop, heappush
from itertools import chain, groupby
from collections import deque, namedtuple
from contextlib import contextmanager
from functools import partial
from os.path import expanduser, expandvars, ismount
from pathlib import Path, PurePosixPath

from .store import FileStore


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
    
    search = [ path ]
    search.extend(path.parents)
    
    for p in search:
        if (p / '.tote').is_dir():
            return p
    else:
        raise FileNotFoundError("no .tote folder in path or parents of path")


def _load_config(config_path):
    c = configparser.ConfigParser()
    c.read([ config_path ])
    return c

        
class _ToteConnection:
    def __init__(self, workdir_path):
        self.workdir_path = Path(workdir_path)
        
        self.tote_path = self.workdir_path / '.tote'
        
        self.config = _load_config(self.tote_path / 'config')

        store_path = self.config.get('store', 'path', fallback=None)
        if store_path is not None:
            store_path = expandvars(store_path)
            store_path = expanduser(store_path)
            store_path = Path(store_path)
            if not store_path.is_absolute():
                store_path = workdir_path / store_path
        else:
            store_path = workdir_path / '.tote'
        
        self.store_path = store_path
        self.store = FileStore(self.store_path)
    
    @contextmanager
    def read_file(self, file_name, unfold=True):
        with open(file_name, 'rt') as f:
            yield self.read_stream(f, unfold)
    
    def read_stream(self, stream, unfold=True):
        items_in = decode_item_stream(stream)
        
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

    def fold(self, items, fold_size=2**22):
        page = list()
        page_size = 0
        for item in items:
            part = encode_item_text(item).encode()
            if len(part) + page_size > fold_size:
                yield self._save_fold(page)
                page.clear()
                page_size = 0
            page.append(item)
            page_size += len(part)
        if page:
            yield self._save_fold(page)

        return
    
    
    def _save_fold(self, items):
        items = sorted(items, key=_item_sort_key)
        return FoldItem(
            type='fold',
            content=[ self._put_chunk(encode_items_bytes(items)) ],
            count=len(items),
            name_min=items[0].name,
            name_max=items[-1].name,
        )


    def unfold(self, items):
        work = deque(sorted(items, key=_item_sort_key))
        while work:
            item = work.popleft()
            if item.type == 'fold':
                for chunk in self.get_chunks(item):
                    lines = chunk.decode().splitlines()
                    work.extend(decode_item_stream(lines))
                work = deque(sorted(work, key=_item_sort_key))
            else:
                yield item
        return

#     get -- read item into memory
#     get_file -- read item into file
#     get_stream -- read item into stream
#     get_chunks -- a generator of the chunks

    def get_file(self, item, out_base=None):
        # clean up the path: make relative, remove '.' and '..'
        if out_base is None:
            name = Path(item.name)
        else:
            name = Path(out_base) / item.name

        if item.type == 'dir':
            name.mkdir(parents=True, exist_ok=True)

        if item.type == 'file':
            with open(name, 'wb') as f:
                for chunk in self.get_chunks(item):
                    f.write(chunk)


    def get_chunks(self, item):
        for part in item.content:
            yield self.get_chunk(part)
    
    def get_chunk(self, part):
        blob = self.store.load(part.data)
        key = bytes.fromhex(part.key)
        blob = _decrypt_blob(blob=blob, lock=part.lock, key=key)
        blob = _decompress_blob(blob)
        data = _parse_blob(blob)
        return data

#     put -- store item from memory
#     put_file - store item from file
#     put_stream - store item from stream

    def put_file(self, path):
        path = Path(path)
        item = get_file_info(path)
        if item.type == 'file':
            with path.open('rb') as f:
                item.update(self.put_stream(f))
        return item

    def put_stream(self, stream, chunk_size=2**24, lock='aes256ctr'):
        content = list()
        h = sha256()
        size = 0
        
        for chunk in iter(partial(stream.read, chunk_size), b''):
            h.update(chunk)
            size += len(chunk)
            c = self._put_chunk(chunk, lock)
            content.append(c)

        return FileItem(
            content=content,
            sha256=h.hexdigest(),
            size=size,
        )
    
    
    def _put_chunk(self, chunk, lock='aes256ctr'):
        blob = _format_blob(chunk)
        blob = _compress_blob(blob)
        key = sha256(blob)
        blob = _encrypt_blob(blob, lock=lock, key=key.digest())
        data = self.store.save(blob)
        return Chunk(
            size=len(chunk),
            sha256=sha256(chunk).hexdigest(),
            lock=lock,
            key=key.hexdigest(),
            data=data,
        )
    
    
    def _most_recent_checkin(self):
        """
        find the most recent checkin for the workdir

        returns a path to the list, or None
        """
        try:
            l = (self.tote_path / 'checkin' / 'default').iterdir()
            paths = sorted(l, reverse=True)
        except FileNotFoundError:
            return None

        for path in paths:
            if path.name.endswith('.tote') and path.lstat().st_size:
                return path
        return None


    def _load_most_recent_checkin(self):
        """
        yield item objects from most recent checkin sorted by path parts
        """
        path = self._most_recent_checkin()
        if path is None:
            return tuple()

        with self.read_file(path, unfold=False) as items:
            return self.unfold(list(items))


class ToteWriter:
    def __init__(self, fd=sys.stdout):
        self.fd = fd
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_value, traceback):
        pass
    def write(self, item):
        self.fd.write(encode_item_text(item))
    def writeall(self, items):
        for item in items:
            self.write(item)


def _compress_blob(data):
    out = b'zlib\n' + zlib.compress(data, 9)
    if len(out) < len(data):
        return out    
    else:
        return data

    
def _decompress_blob(blob):
    if not blob.startswith(b'zlib\n'):
        return blob
    return zlib.decompress(blob[5:])


def _format_blob(data):
    return b'blob\n' + data

def _parse_blob(blob):
    """unwrap a blob, raise TypeError if it is not a blob."""
    if not blob.startswith(b'blob\n'):
        raise TypeError('not a blob')
    return blob[5:]


def _encrypt_blob(data, lock, key):
    if lock == 'aes256ctr':
        c = Counter.new(nbits=128)
        alg = AES.new(key, mode=AES.MODE_CTR, counter=c)
    else:
        raise TypeError('unknown lock type', lock)
    return _format_blob(alg.encrypt(data))


def _decrypt_blob(blob, lock, key):
    if lock == 'aes256ctr':
        ctr = Counter.new(nbits=128)
        alg = AES.new(key, mode=AES.MODE_CTR, counter=ctr)
    else:
        raise TypeError('unknown lock type: ', lock)
    data = _parse_blob(blob)
    return alg.decrypt(data)


def _item_sort_key(item):
    if item.type == 'fold':
        return item.name_min
    return item.name


def format_timestamp(secs=None, safe=False):
    if secs == None:
        t = datetime.now()
    else:
        t = datetime.fromtimestamp(secs, tz=timezone.utc)
    
    if safe:
        return t.strftime('%Y-%m-%dT%H-%M-%S.%f%z')
    
    return t.strftime('%Y-%m-%dT%H:%M:%S.%f%z')


@dataclass
class Chunk:
    size: int = None
    sha256: str = None
    lock: str = None
    key: str = None
    data: str = None


@dataclass
class FileItem:
    name: str = None
    type: str = None
    mtime: datetime = None
    size: int = None
    content: list = None
    sha256: str = None
    target: str = None
    error: str = None

    def update(self, item):
        for field in (
            'name', 'type', 'mtime', 'size', 'content', 'sha256', 'target', 'error'
        ):
            value = getattr(item, field, None)
            if value is not None:
                setattr(self, field, value)


@dataclass
class FoldItem:
    name_min: str = None
    name_max: str = None
    type: str = None
    content: list = None
    count: int = None


def decode_item_stream(stream):
    """
    text -> obj iterable
    """
    for key, group in groupby(stream, key=lambda l: l.rstrip() == '---'):
        if not key:
            body = ''.join(group)
            item = json.loads(body)
            if not 'type' in item:
                item['type'] = 'stream'
            yield decode_item(item)


def decode_item(obj):
    if 'type' in obj:
        if obj['type'] == 'fold':
            return _decode_fold_item(obj)
        else:
            return _decode_file_item(obj)
    
    return obj


def _decode_fold_item(obj):
    fields = {
        'name_min': _decode_name,
        'name_max': _decode_name,
        'type': _decode_str,
        'content': _decode_content,
        'count': _decode_int,
    }
    kwargs = { field: func(obj.get(field, None)) for field, func in fields.items() }
    return FoldItem(**kwargs)


def _decode_str(obj):
    if obj is None:
        return None
    return str(obj)


def _decode_int(obj):
    if obj is None:
        return None
    return int(obj)


def _decode_file_item(obj):
    fields = {
        'name': _decode_name,
        'type': _decode_str,
        'mtime': _decode_timestamp,
        'size': _decode_int,
        'content': _decode_content,
        'sha256': _decode_str,
        'target': _decode_str,
        'error': _decode_str,
    }
    kwargs = { field: func(obj.get(field, None)) for field, func in fields.items() }
    return FileItem(**kwargs)


def _decode_content(content):
    if content is None:
        return None
    return [ _decode_chunk(i) for i in content ]


def _decode_chunk(obj):
    fields = {
        'size': _decode_int,
        'sha256': _decode_str,
        'lock': _decode_str,
        'key': _decode_str,
        'data': _decode_str,
    }
    kwargs = { field: func(obj.get(field, None)) for field, func in fields.items() }
    return Chunk(**kwargs)


def _decode_name(name):
    if name is None:
        return None
    path = PurePosixPath(name)
    if path.is_absolute():
        path = path.relative_to(path.root)
    parts = [ i for i in path.parts if i not in ('.', '..') ]
    return PurePosixPath(*parts)


def _encode_timestamp(timestamp):
    return datetime.strftime(timestamp, '%Y-%m-%dT%H:%M:%S.%f%z')


def _decode_timestamp(timestamp):
    if timestamp is None:
        return None

    try:
        return datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%fZ')
    except ValueError:
        pass
    
    try:
        return datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%SZ')
    except ValueError:
        pass

    return datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%f%z')


def encode_items_bytes(items):
    return b''.join(encode_item_text(item).encode() for item in items)


def encode_item_text(item):
    body = json.dumps(item, default=encode_item, indent=4)
    return f'---\n{body}\n'


def encode_item(item):
    _encoders = (
        (FileItem, _encode_file_item),
        (FoldItem, _encode_fold_item),
        (Chunk, _encode_chunk),
        (datetime, _encode_timestamp),
        (PurePosixPath, _encode_name),
    )
    
    for t, func in _encoders:
        if isinstance(item, t):
            return func(item)

    raise ValueError(type(item), item)

    
def _encode_file_item(item):
    out = {}
    for field in (
        'name', 'type', 'mtime', 'size', 'content', 'sha256', 'target', 'error'
    ):
        if getattr(item, field, None) is not None:
            out[field] = getattr(item, field)
    return out


def _encode_fold_item(item):
    out = {}
    for field in ('name_min', 'name_max', 'type', 'content', 'count'):
        if getattr(item, field, None) is not None:
            out[field] = getattr(item, field)
    return out


def _encode_content(content):
    return [ _encode_chunk(chunk) for chunk in content ]


def _encode_chunk(chunk):
    out = {}
    for field in ('size', 'sha256', 'lock', 'key', 'data'):
        if getattr(chunk, field, None) is not None:
            out[field] = getattr(chunk, field)
    return out


def _encode_name(name):
    return name.as_posix()



class PathQueue:
    '''
    A priority queue of paths, the paths are returned in min-order, equal entries are collapsed.
    '''
    def __init__(self, paths=None):
        self.heap = []
        if paths:
            self.heap.extend(paths)
            heapify(self.heap)

    def __next__(self):
        try:
            return self.pop()
        except IndexError:
            raise StopIteration()
    
    def __iter__(self):
        return self
    
    def pop(self):
        out = heappop(self.heap)
        while self.heap and self.heap[0] == out:
            heappop(self.heap)
        return out
        
    def add(self, path):
        heappush(self.heap, path)
    
    def update(self, paths):
        self.heap.extend(paths)
        heapify(self.heap)
    



@dataclass
class _Ignore_Rule:
    invert: bool = False
    anchored: bool = False
    pattern: tuple = ()

    def matches(self, path):
        if self.anchored and len(self.pattern) != len(path.parts):
            return None
        
        for a, b in zip(path.parts[-len(self.pattern):], self.pattern):
            if not fnmatch(a, b):
                return None
        else:
            if self.invert:
                return False
            else:
                return True
        
        return None


@dataclass
class _Ignore_Rules:
    rules: list = field(default_factory=list)
    
    def matches(self, path):
        for rule in self.rules:
            ignore = rule.matches(path)
            if ignore is not None:
                return ignore
        else:
            return None
    
    def append(self, rule):
        self.rules.append(rule)


def _load_ignore_rules(path):
    rules = _Ignore_Rules()
    
    try:
        with Path(path, '.toteignore').open('rt') as lines:
            for line in lines:
                rule = _Ignore_Rule()

                line = line.rstrip()

                if line.startswith('#'):
                    continue
                
                if line.startswith('!'):
                    rule.invert = True
                    line = line[1:]

                if line.startswith('/'):
                    rule.anchored = True
                    line = line[1:]

                rule.pattern = tuple(i for i in line.split('/') if i != '.')
                if not rule.pattern:
                    continue

                rules.append(rule)
    except FileNotFoundError:
        pass

    return rules


@dataclass
class _Ignore_Manager:
    
    # do not go above here, and raise an error if given a path to check outside here
    base_path: Path = None
    
    def __post_init__(self):
        self._rules_for = lru_cache()(_load_ignore_rules)
        
    def _check_rules(self, path):
        check_path = path
        check_name = Path()
        
        ignore = None
        while ignore is None:
            if check_path == self.base_path:
                break # we hit the base
            
            # step up
            last_path = check_path
            check_path = last_path.parent
            check_name = Path(last_path.name) / check_name

            if check_path == last_path:
                break # we hit the root
            
            ignore = self._rules_for(check_path).matches(check_name)
        return ignore

    def matches(self, path):
        path = Path(path)
        
        if self.base_path is not None:
            # check path is in base_path
            path.relative_to(self.base_path)
        
        ignore = self._check_rules(path)
        if ignore is not None:
            return ignore
        
        if path.name == '.tote':
            return True

        return None


def list_trees(paths, recurse=True, one_filesystem=True, base_path=None):
    paths = [ Path(path) for path in paths ]

    def should_decend(path):
        if not recurse:
            return False
        
        try:
            if not path.is_dir():
                return False
        except OSError as e:
            print(e)
            return False

        try:
            if one_filesystem and ismount(path) and path not in paths:
                return False
        except NotImplementedError:
            pass
        
        if path.is_symlink():
            return False
        
        return True

    ignore_rules = _Ignore_Manager(base_path=base_path)
    
    pq = PathQueue(paths)

    for path in pq:
        if ignore_rules.matches(path):
            continue
        
        if should_decend(path):
            try:
                pq.update(path.iterdir())
            except PermissionError as e:
                print(e)

        yield path


def list_tree(path, **kwargs):
    return list_trees([path], **kwargs)


def scan_trees(paths, relative_to=None, **kwargs):
    for path in list_trees(paths, **kwargs):
        try:
            item = get_file_info(path)
        except OSError as e:
            print(e)
            continue
        
        if relative_to is not None:
            item.name = PurePosixPath(path.relative_to(relative_to))
        yield item


def get_file_info(path):
    path = Path(path)
    item = FileItem()
    item.name = PurePosixPath(path)
    
    try:
        st = path.lstat()
        item.mtime = datetime.fromtimestamp(st.st_mtime, tz=timezone.utc)
    except FileNotFoundError:
        item.type = 'missing'
        return item
    
    if path.is_symlink():
        item.type = 'link'
        item.target = os.readlink(path)
        return item
    
    if path.is_dir():
        item.type = 'dir'
        return item
    
    if path.is_file():
        item.type = 'file'
        item.size = st.st_size
        return item
    
    if path.exists():
        item.type = 'other'
        return item
    
    item.type = 'missing'
    return item


def checkin_status(conn):
    lista = conn._load_most_recent_checkin()
    listb = scan_trees(
        paths=[conn.workdir_path], 
        relative_to=conn.workdir_path,
        base_path=conn.workdir_path,
    )
    
    for a, b in merge_sorted(lista, listb):
        if a is None:
            print('a', b.name)
            continue
        
        if b is None:
            print('d', a.name)
            continue
        
        if a == b:
            continue
        
        changes = set()
        
        if b.type == 'file':
            changes = {
                f 
                for f in ('type', 'size', 'mtime')
                if getattr(a, f, None) != getattr(b, f, None)
            }
            if not changes:
                continue
        
#             print('changes', changes)
#             for f in changes:
#                 print(f, getattr(a, f, None), getattr(b, f, None))

        if changes:
            print('u', b.name, changes)


def checkin_save(conn):
    lista = conn._load_most_recent_checkin()
    listb = scan_trees(
        paths=[conn.workdir_path], 
        relative_to=conn.workdir_path,
        base_path=conn.workdir_path,
    )
    
    for a, b in merge_sorted(lista, listb):
        if a is None:
            print('a', b.name)
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
            print('d', a.name)
            continue
        
        # neigher a nor b are None
        
        if a == b:
            yield a
            continue
        
        changes = set()
        
        if b.type == 'file':
            changes = {
                f 
                for f in ('type', 'size', 'mtime')
                if getattr(a, f, None) != getattr(b, f, None)
            }
            if not changes:
                yield a
                continue
        
#             print('changes', changes)
#             for f in changes:
#                 print(f, getattr(a, f, None), getattr(b, f, None))

            path = conn.workdir_path / b.name
            try:
                with open(path, 'rb') as file:
                    b.update(conn.put_stream(file))
            except OSError as e:
                b.error = str(e)

        if changes:
            print('u', b.name, changes)
        
        yield b


def merge_sorted(a, b):
    """
    yields pairs (item object a, item object b) where the names match,
    for unmatched names the item object is None.
    if duplicate names exist, each item is only output once.
    assumes the lists are sorted by path parts.
    """
    itera = iter(a)
    iterb = iter(b)
    
    itema = next(itera, None)
    itemb = next(iterb, None)
    
    while itema is not None and itemb is not None:
        namea = itema.name
        nameb = itemb.name
        
        if namea < nameb:
            yield (itema, None)
            itema = next(itera, None)
            continue

        if nameb < namea:
            yield (None, itemb)
            itemb = next(iterb, None)
            continue

        yield (itema, itemb)
        itema = next(itera, None)
        itemb = next(iterb, None)

    while itema is not None:
        yield (itema, None)
        itema = next(itera, None)

    while itemb is not None:
        yield (None, itemb)
        itemb = next(iterb, None)


def _filter_items_by_names(items, patterns):
    """
    Return a generator of items with names that start with any of patterns.
    """
    
    def make_filter(pattern):
        pattern_path = PurePosixPath(pattern)
        pattern_parts = pattern_path.parts
        def filter_func(item):
            name_parts = item.name.parts
            if len(name_parts) < len(pattern_parts):
                return False
            for a, b in zip(name_parts, pattern_parts):
                if not fnmatch(a, b):
                    return False
            return True
        return filter_func
    
    filters = [ make_filter(name) for name in patterns ]
    
    for item in items:
        matched = any(f(item) for f in filters)
        if matched:
            yield item

            
def encode_chunk(chunk, lock='aes256ctr'):
    blob = _format_blob(chunk)
    blob = _compress_blob(blob)
    key = sha256(blob)
    blob = _encrypt_blob(blob, lock=lock, key=key.digest())
    
    BlobInfo = namedtuple('BlobInfo', ['data', 'size', 'sha256', 'lock', 'key', 'blob'])
    return BlobInfo(
        data=sha256(blob).hexdigest(),
        size=len(chunk),
        sha256=sha256(chunk).hexdigest(),
        lock=lock,
        key=key.hexdigest(),
        blob=blob,
   )
