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
from .save import ts, get_file_info, pathkey


#def lazy_property(fn):
#    # https://stevenloria.com/lazy-properties/
#    '''Decorator that makes a property lazy-evaluated.
#    '''
#    attr_name = '_lazy_' + fn.__name__
#
#    @property
#    def _lazy_property(self):
#        if not hasattr(self, attr_name):
#            setattr(self, attr_name, fn(self))
#        return getattr(self, attr_name)
#    return _lazy_property


def hash_file(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            h.update(chunk)
    return h.hexdigest()


def strmtime(path):
    return strgmtime(os.path.getmtime(path))


def adjust_name(item, have, want):
    name = item['name']
    if name[0:len(have)] == have:
        name = want + name[len(have):]
        item.update(name=name)
    return item

#
# start ignore rules
#

def translate_match(pattern):
    '''
    translate ignore file line to regex
    '''
    if pattern[:1] == '/':
        pattern = pattern[1:]
        a = '^'
    else:
        a = '^(.*/)?'
    
    while pattern.endswith('/'):
        pattern = pattern[:-1]

    def _rep(match):
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
    p = re.sub(r'(\[\!?\]?[^\]]*\]|\*|\?|.)', _rep, pattern)

    return a + p + '$'

def rule_noop(name):
    return None

def torule(line):
    '''
    translate ignore file line into ignore match callable
    '''
    if not line or line.startswith('#'):
        return rule_noop
    
    # can't ignore the directory where the rule applies.
    if line in ('', '/', '.'):
        return rule_noop
    
    pattern = translate_match(line)
    matcher = re.compile(pattern)
    
    class _ignore_rule:
        def __call__(self, name):
            if matcher.match(str(name)):
                return True
            return None

        def __repr__(self):
            return '[rule: line=%s, pattern=%s]'%(line, pattern)

    return _ignore_rule()

def load_rules(path):
    '''
    load the ignore rules defined at path. return a callable that will check the
    ignore rules against a relative name.
    '''
    rules = list()
    
    try:
        with Path(path, '.toteignore').open('rt') as f:
            for line in textlines(f):
                if line.startswith('#'):
                    continue

                # can't ignore the directory where the rule applies.
                if line in ('', '/', '.'):
                    continue
                
                rules.append(torule(line))
    except FileNotFoundError:
        pass

    def _check(name):
        for rule in rules:
            out = rule(name)
            if out is not None:
                return out
        return None
    
    return _check


def make_ignore(base_path=None):
    '''
    return a function to check ignore rules
    '''
    @lru_cache()
    def _get_rules(path):
        return load_rules(path)

    def _in_parent(path, name):
        '''move the path split up one level'''
        parent_path = dirname(path)
        if name is None:
            parent_name = basename(path)
        else:
            parent_name = join(basename(path), name)
        return (parent_path, parent_name)
    
    def _check(path, name=None):
        if path == base_path:
            # we are at the base, the search is done
            return None

        parent, name = _in_parent(path, name)
        
        if parent == path:
            # we tried to go up from the root
            return None

        if (parent, name) == (base_path, '.tote'):
            # ignore the base path .tote, but not the children
            # so we can name them explicitly to store.
            return True

        if (parent, name) == ('', '.tote'):
            # ignore the base path .tote, but not the children
            # so we can name them explicitly to store.
            return True

        rules = _get_rules(parent)
        
        # XXX name could be any type of path, but the rules are based on posix paths
        ignore = rules(name)
        
        if ignore is not None:
            # we got a rule hit
            return ignore

        return _check(parent, name)
    
    def check(path):
        path = os.path.normpath(path)
        return _check(path)
    
    return check

#
# end ignore rules
# 

#def pathkey(name):
#    '''split and clean up a path for sorting'''
#    p = Path(name)
#
#    # only relative
#    if p.is_absolute():
#        p = p.relative_to(p.root)
#    
#    # strip out '..' if present
#    return tuple(i for i in p.parts if i != '..')


def treescan(path, ignore=None, one_filesystem=False):
    '''
    scan from path, filtering through ignore, recursing into dirs that are not links.
    
    if ignore is not provided, create a new ToteIgnore.
    
    yield each found path
    '''
    return list_trees([path], ignore=ignore, one_filesystem=one_filesystem)

#    if ignore is None:
#        ignore = make_ignore(path)
#    work = deque([path])
#    while work:
#        name = work.popleft()
#        if ignore(name):
#            continue
#        yield name
#        if isdir(name) and not islink(name) and not(one_filesystem and ismount(name)):
#            try:
#                l = listdir(name)
#            except PermissionError as e:
#                warn(str(e))
#            else:
#                for c in l:
#                    p = join(name, c)
#                    work.append(p)
#                work = deque(sorted(work, key=pathkey))


def scan_tree_relative(path, ignore=None, one_filesystem=False):
    """
    read the file tree from path, the items generated will not have content filled in,
    just the metadata that can be seen without reading the file.
    
    yields item objects with names relative to path sorted by path parts
    """
    if ignore is None:
        ignore = make_ignore(path)
    return scan_trees(
        ['.'],
        relative_to=path,
        ignore=ignore,
        one_filesystem=one_filesystem
    )

#    def do_item(name):
#        fullname = join(path, name)
#        
#        if ignore(fullname):
#            return
#        
#        info = get_file_info(fullname)
#        info['name'] = name
#        yield info
#        
#        if should_decend(fullname):
#            yield from do_children(name)
#
#    def do_children(name):
#        try:
#            names = listdir(join(path, name))
#        except PermissionError as e:
#            warn(str(e))
#        else:
#            for child in sorted(names):
#                yield from do_item(join(name, child))
#    
#    def should_decend(fullname):
#        return (
#            isdir(fullname) 
#            and not islink(fullname)
#            and not(one_filesystem and ismount(fullname))
#        )
#
#    yield from do_item('.')


from pathlib import PurePosixPath

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


def list_trees(paths, relative_to=None, ignore=None, recursive=True, one_filesystem=False):
    '''
    scan from paths. filter through ignore. recursing into dirs that are not links.
    
    if ignore is not provided, the default file system based ignore is used.
    
    yield each found path
    '''
    
    def sorted_queue(paths):
        return deque(sorted(set(paths), key=pathkey))
    
    def should_decend(fullname):
        return (
            recursive
            and isdir(fullname) 
            and not islink(fullname)
            and not(one_filesystem and ismount(fullname))
        )

    if ignore is None:
        ignore = make_ignore()
    
    work = sorted_queue(paths)
    
    while work:
        name = work.popleft()
        
        if relative_to is None:
            fullname = name
        else:
            fullname = join(relative_to, name)
        
        if ignore(fullname):
            continue
        
        yield name
        
        if should_decend(fullname):
            try:
                l = listdir(fullname)
            except PermissionError as e:
                warn(str(e))
            else:
                work.extend(join(name, c) for c in l)
                work = sorted_queue(work)


def scan_trees(paths, relative_to=None, **kwargs):
    for path in list_trees(paths, relative_to=relative_to, **kwargs):
        if relative_to is None:
            item = get_file_info(path)
        else:
            item = get_file_info(join(relative_to, path))
            item['name'] = path
        yield item
    return


