import os
import os.path

from collections import OrderedDict
from functools import partial
from hashlib import sha256
from os.path import isdir, isfile, join
from pathlib import Path


def bucket_path(base, name):
    bucket = ''
    path = base
    for i in (1, 3):
        part = name[0:i]
        bucket = join(bucket, part)
        path = join(path, part)
    return path


def file_path(path, name, suffix=''):
    bucket = bucket_path(path, name)
    return join(bucket, name + suffix) 


def save_blob(path, name, blob, suffix='', overwrite=False):
    bp = bucket_path(path, name)
    if not isdir(bp):
        if not isdir(path):
            raise ValueError("path does not exist", path)
        os.makedirs(bp)

    fn = file_path(path, name, suffix)
    create = not isfile(fn)
    if overwrite or create:
        with open(fn + '.part', 'wb') as f:
            f.write(blob)
        os.rename(fn + '.part', fn)


def load_blob(path, name, suffix=''):
    fn = file_path(path, name, suffix)
    with open(fn, 'rb') as f:
        return f.read()

    
def save_chunk(store, chunk, **kwargs):
    name = sha256(chunk).hexdigest()
    store.save_blob(name, chunk, **kwargs)
    return name


def attach(path):
    '''
    Attach to the blob store for the given repository
    '''
    return Store(path)


class FileStore:
    def __init__(self, path):
        self.path = Path(path)
    
    def save_blob(self, name, blob, *args, **kwargs):
        base = join(self.path, 'blobs')
        save_blob(base, name, blob, *args, **kwargs)

    def save(store, blob, **kwargs):
        name = sha256(blob).hexdigest()
        store.save_blob(name, blob, **kwargs)
        return name
        
    def load_blob(self, name, *args, **kwargs):
        base = join(self.path, 'blobs')
        return load_blob(base, name, *args, **kwargs)
    
    def getsize(self, name, *args, **kwargs):
        base = join(self.path, 'blobs')
        fn = file_path(base, name, *args, **kwargs)
        return os.path.getsize(fn)
       
    def load(self, name, *args, **kwargs):
        base = join(self.path, 'blobs')
        return load_blob(base, name, *args, **kwargs)
        
    def __repr__(self):
        return "[Store: %s]"%(self.path)
    
    pass


import requests

class UrlStore:
    def __init__(self, url, auth):
        self.url = url
        self.auth = auth
        
    def load(self, name):
        resp = requests.get(self.url + name, auth=self.auth)
        if resp.status_code != 200:
            raise IOError(resp)

        return resp.content
    
    def save(self, blob):
        name = sha256(blob).hexdigest()
        
        resp = requests.head(self.url + name, auth=self.auth)
        if resp.status_code == 200:
            return name
                
        headers = { 'content-type': 'application/octet-stream' }
        resp = requests.put(self.url + name, data=blob, headers=headers, auth=self.auth)
        if resp.status_code != 200:
            raise IOError(resp)
        
        return name
