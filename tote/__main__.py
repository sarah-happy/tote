#!/usr/bin/env python3
import sys
from inspect import getsourcefile
from os.path import dirname, abspath, join

file = getsourcefile(lambda:0)
base = abspath(join(dirname(file), '..'))

sys.path.insert(0, base)
import tote
# sys.path.remove(base)

import tote.main as m
m.main()
