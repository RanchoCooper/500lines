#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @author   : Rancho Cooper
# @date     : 2016-09-23 15:46
# @email    : ranchocooper@gmail.com

import os
import struct
import portalocker

class Storage(object):
    """provide operation to made db file-like"""
    SUPERBLOCK_SIZE = 4096
    INTEGER_FORMAT = "!Q"
    INTEGER_LENGTH = 8

    def __init__(self, f):
        # preset lock-state and superblock
        # Boolean-lock: assure only one process accessible
        # superblock: preset block for control message
        self._f = f
        self.locked = False
        self._ensure_superblock()

    def _ensure_superblock(self):
        """fill superblock with b'\x00'"""
        self.lock()
        self._seek_end()
        end_address = self._f.tell()       # aka. size of file
        if end_address < self.SUPERBLOCK_SIZE:
            self._f.write(b'\x00' * (self.SUPERBLOCK_SIZE - end_address))
        self.unlock()

    def lock(self):
        """use lock before any write-related operatoin"""
        if not self.locked:
            portalocker.lock(self._f, portalocker.LOCK_EX)
            self.locked = True

    def unlock(self):
        """clear buffer and unlock"""
        if self.locked:
            self._f.flush()
            portalocker.unlock(self._f)
            self.locked = False

    def _seek_end(self):
        """located @end of file"""
        self._f.seek(0, os.SEEK_END)

    def _seek_superblock(self):
        """located @start of file"""
        self._f.seek(0)

    def _bytes_to_integer(self, integer_bytes):
        """Byte-level: decode '!Q' to integer, ignore redundant parts"""
        return struct.unpack(self.INTEGER_FORMAT, integer_bytes)[0]     # calsize(in_bt)

    def _integer_to_bytes(self, integer):
        """Byte-level: encode integer to '!Q' """
        return struct.pack(self.INTEGER_FORMAT, integer)

    def _read_integer(self):
        """file-level: read a integer from file"""
        return self._bytes_to_integer(self._f.read(self.INTEGER_LENGTH))

    def _write_integer(self, integer):
        """file-level: write a integer to file, lock-related"""
        self.lock()
        self._f.write(self._integer_to_bytes(integer))

    def write(self, data):
        self.lock()
        self._seek_end()
        object_address = self._f.tell()
        self._write_integer(len(data))
        self._f.write(data)
        return object_address

    def read(self, address):
        self._f.seek(address)
        length = self._read_integer()
        data = self._f.read(length)
        return data

    def get_root_address(self):
        self._seek_superblock()
        root_address = self._read_integer()
        return root_address

    def commit_root_address(self, root_address):
        self.lock()
        self._f.flush()
        self._seek_superblock()
        self._write_integer(root_address)
        self._write_integer(root_address)
        self._f.flush()
        self.unlock()

    def close(self):
        self.unlock()
        self._f.close()

    @property
    def closed(self):
        return self._f.closed
