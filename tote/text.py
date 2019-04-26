import re

from collections import OrderedDict
from itertools import groupby
from functools import partial
import json

def textline(line):
    try:
        line = line.decode()
    except AttributeError:
        pass
    return line.rstrip('\r\n')

def textlines(text):
    try:
        lines = text.splitlines()
    except AttributeError:
        lines = text
    return map(textline, lines)

def escape(text):
    e = { '\n': '\\n', '\t': '\\t', '\r': '\\r', '\\': '\\\\' }
    def escape_one(match):
        return e.get(match.group(0))
    return re.sub(r'[\n\r\t\\]', escape_one, text)
    
def unescape(text):
    u = { '\\n': '\n', '\\t': '\t', '\\r': '\r', '\\\\': '\\' }
    def unescape_one(match):
        return u.get(match.group(1))
    return re.sub(r'\\([nrt\\])', unescape_one, text)

def dumps(obj):
    parts = []
    for k, v in obj.items():
        part = "{}: {}".format(k, v)
        part = escape(part)
        parts += [ part + "\n" ]
    parts += [ "\n" ]
    return "".join(parts)

def dump(item, stream, flush=False):
    stream.write(dumps(item))
    if flush:
        stream.flush()

def load_all(text):
    lines = ( line.split(": ", 1) for line in textlines(text) )
    chunk = OrderedDict()
    for line in lines:
        if line == [""]:
            if chunk:
                yield chunk
                chunk = OrderedDict()
        elif len(line) == 1:
            chunk[str(len(chunk))] = unescape(line[0])
        else:    
            chunk[line[0]] = unescape(line[1])
    if chunk:
        yield chunk

def load_list(name):
    with open(name, 'rt') as f:
        return list(load_all(f))

#
# json stream, but using '---' as the seperator instead of '\x1b'

def fromstream(text):
    """
    text -> texts
    """
    lines = textlines(text)
    
    for k, g in groupby(lines, lambda l: l.startswith('---')):
        if not k:
            yield '\n'.join(g)

def tostream(text):
    return ''.join(['---\n', text, '\n'])

def fromjson(text):
    """
    json -> obj
    """
    return json.loads(text, object_pairs_hook=OrderedDict)

def tojson(obj):
    """
    obj -> json
    """
    return json.dumps(obj, indent=2)

def tojsons(obj):
    """
    obj -> stream
    
    prepends the stream seprator to the json
    """
    return ''.join(['---\n', tojson(obj), '\n'])

def fromjsons(text):
    """
    text -> obj iterable
    """
    return map(fromjson, fromstream(text))

