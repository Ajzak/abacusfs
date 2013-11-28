#!/usr/bin/env python
#-*- coding: utf-8 -*-

import os, sys, stat, errno, time
import fuse
import redis

# Specify what Fuse API use: 0.2
fuse.fuse_python_api = (0, 2)

import logging
LOG_FILENAME = 'htfs.log'
logging.basicConfig(filename=LOG_FILENAME,level=logging.DEBUG)

class Item(object):
    """
    An Item is an Object on Disk, it can be a Directory, File, Symlink, ...
    """
    def __init__(self, mode, uid, gid):
        # ----------------------------------- Metadata --
        self.atime = time.time()   # time of last acces
        self.mtime = self.atime    # time of last modification
        self.ctime = self.atime    # time of last status change

        self.dev  = 0        # device ID (if special file)
        self.mode = mode     # protection and file-type
        self.uid  = uid      # user ID of owner
        self.gid  = gid      # group ID of owner

        # ------------------------ Extended Attributes --
        self.xattr = {}

        # --------------------------------------- Data --
        if stat.S_ISDIR(mode):
            self.data = set()
        else:
            self.data = ''

    def read(self, offset, length):
        return self.data[offset:offset+length]

    def write(self, offset, data):
        length = len(data)
        self.data = self.data[:offset] + data + self.data[offset+length:]
        return length

    def truncate(self, length):
		if len(self.data) > length:
			self.data = self.data[:length]
		else:
			self.data += (length-len(self.data))*'\x00'

def zstat(stat):
    stat.st_mode  = 0
    stat.st_ino   = 0
    stat.st_dev   = 0
    stat.st_nlink = 2
    stat.st_uid   = 0
    stat.st_gid   = 0
    stat.st_size  = 0
    stat.st_atime = 0
    stat.st_mtime = 0
    stat.st_ctime = 0
    return stat

class HTFS(fuse.Fuse):
    def __init__(self, *args, **kwargs):
        fuse.Fuse.__init__(self, *args, **kwargs)

        self.uid = os.getuid()
        self.gid = os.getgid()

        self._storage = {'/': Item(0755 | stat.S_IFDIR, self.uid, self.gid)}

    # --- Metadata -----------------------------------------------------------
    def getattr(self, path):
		if not path in self._storage:
			return -errno.ENOENT
			
		# Lookup Item and fill the stat struct
		item = self._storage[path]
		st = zstat(fuse.Stat())
		st.st_mode  = item.mode
		st.st_uid   = item.uid
		st.st_gid   = item.gid
		st.st_dev   = item.dev
		st.st_atime = item.atime
		st.st_mtime = item.mtime
		st.st_ctime = item.ctime
		st.st_size  = len(item.data)
		return st

    def chmod(self, path, mode):
        item = self._storage[path]
        item.mode = mode

    def chown(self, path, uid, gid):
        item = self._storage[path]
        item.uid = uid
        item.gid = gid

    def utime(self, path, times):
        item = self._storage[path]
        item.ctime = item.mtime = times[0]

    # --- Namespace ----------------------------------------------------------
    def unlink(self, path):
        self._remove_from_parent_dir(path)
        del self._storage[path]

    def rename(self, oldpath, newpath):
        item = self._storage.pop(oldpath)
        self._storage[newpath] = item

    # --- Links --------------------------------------------------------------
    def symlink(self, path, newpath):
        item = Item(0644 | stat.S_IFLNK, self.uid, self.gid)
        item.data = path
        self._storage[newpath] = item
        self._add_to_parent_dir(newpath)

    def readlink(self, path):
        return self._storage[path].data

    # --- Extra Attributes ---------------------------------------------------
    def setxattr(self, path, name, value, flags):
        self._storage[path].xattr[name] = value

    def getxattr(self, path, name, size):
        value = self._storage[path].xattr.get(name, '')
        if size == 0:   # We are asked for size of the value
            return len(value)
        return value

    def listxattr(self, path, size):
        attrs = self._storage[path].xattr.keys()
        if size == 0:
            return len(attrs) + len(''.join(attrs))
        return attrs

    def removexattr(self, path, name):
        if name in self._storage[path].xattr:
            del self._storage[path].xattr[name]

    # --- Files --------------------------------------------------------------
    def mknod(self, path, mode, dev):
        item = Item(mode, self.uid, self.gid)
        item.dev = dev
        self._storage[path] = item
        self._add_to_parent_dir(path)

    def create(self, path, flags, mode):
        self._storage[path] = Item(mode | stat.S_IFREG, self.uid, self.gid)
        self._add_to_parent_dir(path)

    def truncate(self, path, len):
        self._storage[path].truncate(len)

    def read(self, path, size, offset):					
		return self._storage[path].read(offset, size)

    def write(self, path, buf, offset):
        return self._storage[path].write(offset, buf)

    # --- Directories --------------------------------------------------------
    def mkdir(self, path, mode):				
		self._storage[path] = Item(mode | stat.S_IFDIR, self.uid, self.gid)
		self._add_to_parent_dir(path)

    def rmdir(self, path):
        if self._storage[path].data:
            return -errno.ENOTEMPTY

        self._remove_from_parent_dir(path)
        del self._storage[path]

    def readdir(self, path, offset):
        dir_items = self._storage[path].data
        for item in dir_items:
            yield fuse.Direntry(item)

    def _add_to_parent_dir(self, path):
        parent_path = os.path.dirname(path)
        filename = os.path.basename(path)
        self._storage[parent_path].data.add(filename)

    def _remove_from_parent_dir(self, path):
        parent_path = os.path.dirname(path)
        filename = os.path.basename(path)
        self._storage[parent_path].data.remove(filename)

def main():
    usage="""
HTFS - HashTable File-System

""" + fuse.Fuse.fusage
    server = HTFS(version="%prog " + fuse.__version__,
                     usage=usage,
                     dash_s_do='setsingle')

    server.parse(errex=1)
    server.main()

if __name__ == '__main__':
    main()

