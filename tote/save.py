import binascii
import os
import re
import time
import zlib

from Crypto.Cipher import AES
from Crypto.Util import Counter
from collections import OrderedDict, deque
from functools import partial
from hashlib import sha256
from io import StringIO
from os import lstat, readlink
from os.path import islink, isfile, isdir, exists
from pathlib import Path, PurePosixPath

from .text import dump, dumps, fromjsons, tojsons

def load_tree(store, hexdigest):
    blob = store.load_blob(hexdigest)
    return load_all(blob)

class TooBigError(Exception): pass

def chunks(file, size=2**24):
    return iter(partial(file.read, size), b'')

def ts(secs=None):
    t = time.gmtime(secs)
    return time.strftime('%Y-%m-%dT%H:%M:%SZ', t)

def save_file(name, store, *args, **kwargs):
    out = OrderedDict()
    out['name'] = name
    
    try:
        st = lstat(name)
        out['mtime'] = ts(st.st_mtime)
    
    except FileNotFoundError:
        out['type'] = 'missing'
        return out
    
    if islink(name):
        out['type'] = 'link'
        out['target'] = readlink(name)
        return out
    
    if isdir(name):
        out['type'] = 'dir'
        return out
    
    if isfile(name):
        out['type'] = 'file'
        with open(name, 'rb') as file:
            out.update(save_stream(file, store, *args, **kwargs))
        return out
    
    if exists(name):
        out.update(type='unknown')
        return out
    
    out.update(type='missing')
    return out

def save_stream(file, store, *args, **kwargs):
    content = list()
    h = sha256()
    size = 0
    for chunk in chunks(file):
        h.update(chunk)
        size += len(chunk)
        c = save_chunk(chunk, store, *args, **kwargs)
        content.append(c)
    return { 'content': content, 'sha256': h.hexdigest(), 'size': size }

def load_content(item, store):
    for part in item.get('content', ()):
        yield load_chunk(part, store)

def hexify(data):
    h = binascii.hexlify(data)
    return str(h)

def save_chunk(chunk, store, lock='aes256ctr'):
    c = compress(make_blob(chunk))
    k = sha256(c)
    l = encrypt(c, lock=lock, key=k.digest())
    d = store.save(l)
    return dict(
        size=len(chunk),
        sha256=sha256(chunk).hexdigest(),
        lock=lock,
        key=k.hexdigest(),
        data=d)

def make_blob(data):
    return b'blob\n' + data

def compress(data):
    z = b'zlib\n' + zlib.compress(data, 9)
    return z  if len(z) < len(data)  else data

def encrypt(data, lock, key):
    if lock == 'aes256ctr':
        c = Counter.new(nbits=128)
        alg = AES.new(key, mode=AES.MODE_CTR, counter=c)
    else:
        raise TypeError('unknown lock type', lock)
    return make_blob(alg.encrypt(data))

def fromblob(data):
    """unwrap a blob, raise TypeError if it is not a blob."""
    if not data.startswith(b'blob\n'):
        raise TypeError('not a blob')
    return data[5:]

def decompress(data):
    if not data.startswith(b'zlib\n'):
        return data
    return zlib.decompress(data[5:])
    
def decrypt(data, lock, key):
    if lock == 'aes256ctr':
        ctr = Counter.new(nbits=128)
        alg = AES.new(key, mode=AES.MODE_CTR, counter=ctr)
    else:
        raise TypeError('unknown lock type: ', lock)
    l = fromblob(data)
    return alg.decrypt(l)

def load_chunk(part, store):
    d = part['data']
    l = store.load(part['data'])
    k = bytes.fromhex(part['key'])
    c = decrypt(data=l, lock=part['lock'], key=k)
    b = decompress(c)
    chunk = fromblob(b)
    return chunk

def unfold(items, store):
    work = deque(sorted(items, key=itemkey))
    while work:
        item = work.popleft()
        if item['type'] == 'fold':
            for chunk in load_content(item, store):
                work.extend(fromjsons(chunk))
            work = deque(sorted(work, key=itemkey))
        else:
            yield item
    return

def itemname(item):
    for field in 'name', 'name_min':
        if field in item:
            return item.get(field)

    raise TypeError('item has no name')

def itemkey(item):
    return pathkey(itemname(item))

def tochunk(items):
    return ''.join(map(tojsons, items)).encode()

def pathkey(name):
    '''split and clean up a path in an archive, for sorting'''
    p = PurePosixPath(name)

    # only relative
    if p.is_absolute():
        p = p.relative_to(p.root)
    
    # strip out '..' if present
    p = tuple(i for i in p.parts if i != '..')

    return p

def save_fold(items, store):
    items = sorted(items, key=itemkey)
    o = OrderedDict()
    o['type'] = 'fold'
    o['content'] = [ save_chunk(tochunk(items), store) ]
    o['count'] = len(items)
    o['name_min'] = itemname(items[0])
    o['name_max'] = itemname(items[-1])
    return o

def fold(items, store, fold_size=2**22):
    page = list()
    page_size = 0
    for item in items:
        part = tojsons(item).encode()
        if len(part) + page_size > fold_size:
            yield save_fold(page, store)
            page.clear()
            page_size = 0
        page.append(item)
        page_size += len(part)
    if page:
        yield save_fold(page, store)
    return

