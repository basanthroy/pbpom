__author__ = 'cliu'

import hashlib


def compute_hash(method, data):
    if method == 'md5':
        method = hashlib.md5()
    if method == 'sha1':
        method = hashlib.sha1()
    method.update(data)
    return method.hexdigest()