"""Module that adds directories needed by amble to sys.path when imported."""

import sys 
import fnmatch
from os.path import abspath, dirname

AMBLEDIR = dirname(abspath(__file__))

def add_path(path, end=False):
    if not end:
        remove_path(path)
        sys.path.insert(0, path)
    elif not any(fnmatch.fnmatch(p, path) for p in sys.path):
        sys.path.append(path)

def remove_path(path):
    sys.path = [p for p in sys.path if not fnmatch.fnmatch(p, path)]


add_path(dirname(AMBLEDIR))
remove_path(AMBLEDIR)
