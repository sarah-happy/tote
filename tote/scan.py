import hashlib
import os
import os.path
import re
import time

from collections import deque, OrderedDict
from functools import lru_cache
from os import listdir, lstat
from os.path import basename, dirname, isdir, islink, join, ismount
from pathlib import Path
from warnings import warn

from .text import textline, textlines, escape, unescape
from .text import dumps, dump, load_all, load_list

def hash_file(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            h.update(chunk)
    return h.hexdigest()

def strgmtime(secs=None):
    t = time.gmtime(secs)
    return time.strftime('%Y-%m-%dT%H:%M:%SZ', t)

def strmtime(path):
    return strgmtime(os.path.getmtime(path))

def adjust_name(item, have, want):
    name = item['name']
    if name[0:len(have)] == have:
        name = want + name[len(have):]
        item.update(name=name)
    return item

class Rules(list):
    def __call__(self, item):
        for rule in self:
            result = rule(item)
            if result is not None:
                return result
        return None

def translate_match(pattern):
    if pattern[:1] == '/':
        pattern = pattern[1:]
        a = '^'
    else:
        a = '^(.*/)?'
    
    while pattern.endswith('/'):
        pattern = pattern[:-1]
    def rep(match):
        c = match.group(0)
        if c[:1] == '[':
            r = c[1:-1]
            if r[:1] == '!':
                n = '^'
                r = r[1:]
            else:
                n = ''
            return '[' + n + r + ']'
        if c == '*':
            return '[^/]*'
        if c == '?':
            return '[^/]?'
        return re.escape(c)
    p = re.sub(r'(\[\!?\]?[^\]]*\]|\*|\?|.)', rep, pattern)

    return a + p + '$'

def compile_match(pattern):
    return re.compile(translate_match(pattern))

class MatchRule:
    def __init__(self, pattern, path):
        self.pattern = compile_match(pattern)
        self.path = Path(path)
    
    def __call__(self, path):
        path = Path(path)
        # if we have a base, make path relative to it
        path = path.relative_to(self.path)
        if self.pattern.match(str(path)):
            return True
        return None

    def __repr__(self):
        return '[MatchRule: pattern=%s, path=%s]'%(self.pattern, self.path)
    
def make_rule(line, path):
    if not line or line.startswith('#'):
        return None
    
    # can't ignore the directory where the rule applies.
    if line in ('', '/', '.'):
        return None
    
    return MatchRule(line, path)

def load_ignore(path):
    path = Path(path)
    rules = Rules()
    try:
        with Path(path, '.toteignore').open('rt') as f:
            for line in textlines(f):
                rule = make_rule(line, path)
                if rule is not None:
                    rules.append(rule)
    except FileNotFoundError:
        pass
    
    return rules

def torule(line):
    if not line or line.startswith('#'):
        return None
    
    # can't ignore the directory where the rule applies.
    if line in ('', '/', '.'):
        return None
    
    return Rule(line)

class Rule:
    def __init__(self, pattern):
        self.pattern = compile_match(pattern)
    
    def __call__(self, name):
        if self.pattern.match(str(name)):
            return True
        return None

    def __repr__(self):
        return '[Rule: pattern=%s]'%(self.pattern)
    

def load_rules(path):
    rules = Rules()
    try:
        with Path(path, '.toteignore').open('rt') as f:
            for line in textlines(f):
                rule = torule(line)
                if rule is not None:
                    rules.append(rule)
    except FileNotFoundError:
        pass
    
    return rules

class FilterPath:
    """A Path wrapper that filters based on .toteignore"""
    
    def __init__(self, path, ignore=None):
        self.path = Path(path)
        self.ignore = load_ignore(path)
        if ignore is not None:
            self.ignore.append(ignore)
        
    def iterdir(self):
        for path in sorted(self.path.iterdir()):
            if self.ignore(path):
                continue
            if path.is_dir():
                yield FilterPath(path, self.ignore)
            else:
                yield path
    
    def __getattr__(self, attr):
        return getattr(self.path, attr)

    def __str__(self):
        return str(self.path)
    
    def __repr__(self):
        return repr(self.path)
        
    def __fspath__(self):
        return str(self.path)

def toteview(path):
    return PathView(path)

def lazy_property(fn):
    # https://stevenloria.com/lazy-properties/
    '''Decorator that makes a property lazy-evaluated.
    '''
    attr_name = '_lazy_' + fn.__name__

    @property
    def _lazy_property(self):
        if not hasattr(self, attr_name):
            setattr(self, attr_name, fn(self))
        return getattr(self, attr_name)
    return _lazy_property

def ToteIgnore(base_path=None):
    
    @lru_cache()
    def get_rules(path):
        return load_rules(path)

    def in_parent(path, name):
        if name is None:
            return dirname(path), basename(path)
        else:
            return dirname(path), join(basename(path), name)
    
    def check(path, name=None):
        if path == base_path:
            # we are at the base, the search is done
            return None

        parent, name = in_parent(path, name)
        if parent == path:
            # we tried to go up from the root
            return None

        if (parent, name) == (base_path, '.tote'):
            # ignore the base path .tote, but not the children
            # so we can name them explicitly to store.
            return True

        # XXX name could be any type of path, but the rules are based on posix paths
        ignore = get_rules(parent)(name)
        if ignore is not None:
            # we got a rule hit
            return ignore

        return check(parent, name)
    
    return check

def spathkey(name):
    '''split and clean up a native path, for sorting'''
    p = Path(name)

    # only relative
    if p.is_absolute():
        p = p.relative_to(p.root)
    
    # strip out '..' if present
    p = tuple(i for i in p.parts if i != '..')

    return p

def treescan(path, ignore=None, one_filesystem=False):
    '''
    scan from path, filtering through ignore, recursing into dirs that are not links.
    
    if ignore is not provided, create a new ToteIgnore.
    
    yield each found path
    '''
    if ignore is None:
        ignore = ToteIgnore()
    work = deque([path])
    while work:
        name = work.popleft()
        if ignore(name):
            continue
        yield name
        if isdir(name) and not islink(name) and not(one_filesystem and ismount(name)):
            try:
                l = listdir(name)
            except PermissionError as e:
                warn(str(e))
            else:
                for c in l:
                    p = join(name, c)
                    work.append(p)
                work = deque(sorted(work, key=spathkey))
