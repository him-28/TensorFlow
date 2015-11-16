#encoding=utf-8

import os
import time

def path_chk_or_create(path):
    os.path.exists(path) or os.makedirs(path)

def timer(fn):
    def wrapper():
        start = time.clock()
        fn()
        end = time.clock()
        return start, end
    return wrapper
