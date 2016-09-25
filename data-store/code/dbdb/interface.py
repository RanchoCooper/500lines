#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @author   : Rancho Cooper
# @date     : 2016-09-23 14:28
# @email    : ranchocooper@gmail.com
# 改成装饰器

from dbdb.binary_tree import BinaryTree
from dbdb.physical import Storage

class DBDB(object):
    """
    @brief      offer key-value operations
    _tree       a bin-tree to manager elements
    _storage    physical access to db-file, only for judging connect alive
    """
    def __init__(self, f):
        self._storage = Storage(f)
        self._tree = BinaryTree(self._storage)

    def _assert_not_closed(self):
        if self._storage.closed:
            raise ValueError('x-> database closed.')

    def close(self):
        self._storage.close()

    def commit(self):
        # actually update to db
        self._assert_not_closed()
        self._tree.commit()

    def __getitem__(self, key):
        # db[key]
        self._assert_not_closed()
        return self._tree.get(key)

    def __setitem__(self, key, value):
        # db[key] = value
        self._assert_not_closed()
        return self._tree.set(key, value)

    def __delitem__(self, key):
        # del db[key]
        self._assert_not_closed()
        return self._tree.pop(key)

    def __contains__(self, key):
        # key in db
        try:
            self[key]
        except KeyError:
            return False
        else:
            return True

    def __len__(self):
        return len(self._tree)
