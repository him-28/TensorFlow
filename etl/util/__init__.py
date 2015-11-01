#encoding=utf-8

import os

def path_chk_or_create(path):
    os.path.exists(path) or os.makedirs(path)
