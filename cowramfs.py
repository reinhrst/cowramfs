# copied from http://www.stavros.io/posts/python-fuse-filesystem/

import os
import stat
import sys
import errno
import time
import logging

from passthrough import Passthrough
import fuse

TYPE_DELETED = 1
TYPE_FILE = 2
TYPE_DIRECTORY = 3
TYPE_LINK = 4

# these values are ignored in fuse systems
VALUE_IGNORED = 0

log = None


def getnow():
    return int(time.time())


class CowRamFS(Passthrough):
    def __init__(self, root):
        super(CowRamFS, self).__init__(root)
        # keys are relative pathnames, values either mods or data
        self.fh = 0
        self.entries = {}
        self.fhmap = {}

    def _exists(self, path):
        if path in self.entries:
            if self.entries[path]["type"] == TYPE_DELETED:
                return False
            else:
                return True
        else:
            full_path = self._full_path(path)
            return os.path.exists(full_path)

    def _updatepath(self, path, type=None, stat=None, data=None):
        full_path = self._full_path(path)
        if path not in self.entries:
            self.entries[path] = {}
        entry = self.entries[path]

        if type is None:
            if "type" not in entry:
                assert(os.path.exists(full_path))
                # make same type as original
                if os.path.isdir(full_path):
                    entry["type"] = TYPE_DIRECTORY
                    entry['stat'] = super(CowRamFS, self).getattr(path)
                elif os.path.isfile(full_path):
                    entry["type"] = TYPE_FILE
                    entry['stat'] = super(CowRamFS, self).getattr(path)
                elif os.path.islink():
                    entry["type"] = TYPE_LINK
                    entry['stat'] = super(CowRamFS, self).getattr(path)
                else:
                    assert("unknown file type")
        else:
            entry["type"] = type

        if entry["type"] == TYPE_DELETED:
            return

        if data is None:
            if "data" not in entry:
                if entry["type"] == TYPE_FILE:
                    with open(full_path, "r") as f:
                        entry["data"] = f.read()
                elif entry["type"] == TYPE_LINK:
                    entry["data"] = os.readlink(full_path)
                else:
                    pass
        else:
            entry["data"] = data

        if "stat" not in entry:
            entry["stat"] = stat
        elif stat is not None:
            entry["stat"].update(stat)
        assert(len(entry["stat"]) == 8)

    def _unlink(self, path):
        full_path = self._full_path(path)
        if os.path.exists(full_path):
            self.entries[path] = {
                "type": TYPE_DELETED,
                "data": "",
                "stat": {}
                }
        else:
            del(self.entries[path])

    def _getstatdict_forcreate(self, mode):
        """get the stats struct for a new file"""
        now = getnow()
        stat = {
            'st_mode': mode,
            'st_nlink': 1,
            'st_uid': os.getuid(),
            'st_gid': os.getgid(),
            'st_size': 0,
            'st_atime': now,
            'st_mtime': now,
            'st_ctime': now,
            }
        return stat

    def _nextfh(self):
        self.fh += 1
        return self.fh
    # Filesystem methods
    # ==================

    def access(self, path, mode):
        if path in self.entries:
            if self.entries[path]["type"] == TYPE_DELETED:
                log.debug("access? %s, mode %d -- deleted: no", path, mode)
                raise fuse.FuseOSError(errno.EACCES)
            else:
                # NOTE: Assume that uid is always the same as the user's uid...
                filemode = self.entries[path]["stat"]["st_mode"]
                if mode == os.R_OK:
                    if filemode & stat.S_IRUSR == 0:
                        raise fuse.FuseOSError(errno.EACCES)
                if mode == os.W_OK:
                    if filemode & stat.S_IWUSR == 0:
                        raise fuse.FuseOSError(errno.EACCES)
                if mode == os.X_OK:
                    if filemode & stat.S_IXUSR == 0:
                        raise fuse.FuseOSError(errno.EACCES)
                log.debug("access? %s, mode %d internal: yes", path, mode)
                return
        try:
            ret = super(CowRamFS, self).access(path, mode)
            log.debug("access? %s, mode %d external: yes", path, mode)
            return ret
        except:
            log.debug("access? %s, mode %d external: no", path, mode)
            raise

    def chmod(self, path, mode):
        log.info("chmod %s %o", path, mode)
        self._updatepath(path, stat={"st_mode": mode})

    def chown(self, path, uid, gid):
        log.error("chown %s %d %d -- not implemented", path, uid, gid)
        raise Exception("chown not supported")

    def getattr(self, path, fh=None):
        if path in self.entries:
            if not self._exists(path):
                log.debug("getattr %s -- Deleted", path)
                raise fuse.FuseOSError(fuse.ENOENT)
            else:
                ret = self.entries[path]["stat"]
                log.debug("getattr %s (int) %s", path, repr(ret))
                return ret
        else:
            ret = super(CowRamFS, self).getattr(path)
            log.debug("getattr %s (ext) %s", path, repr(ret))
            return ret

    def readdir(self, path, fh):
        log.info("readdir %s", path)
        print "\n".join(["%s: %s" % e for e in self.entries.iteritems()])
        print "Current size of entries: %d" % len(self.entries)

        originallisting = super(CowRamFS, self).readdir(path, fh)
        for filename in originallisting:
            if os.path.join(path, filename) not in self.entries:
                yield filename

        if path == os.sep:
            pathstart = path
        else:
            pathstart = path + os.sep
        pathstartlength = len(pathstart)
        for pathname in self.entries:
            if pathname.startswith(pathstart):
                filename = pathname[pathstartlength:]
                if filename.find(os.sep) == -1:
                    if self.entries[pathname]["type"] != TYPE_DELETED:
                        yield filename

    def readlink(self, path):
        log.info("readlink %s", path)
        if path in self.entries:
            if self.entries[path]["type"] == TYPE_LINK:
                return self.entries[path]["data"]
            else:
                raise OSError("Not a link")
        else:
            return super(CowRamFS, self).readlink(path)

    def mknod(self, path, mode, dev):
        log.error("mknod %s -- not implemented", path)
        raise Exception("Not supported")

    def rmdir(self, path):
        log.info("rmdir %s", path)
        if not self._exists(path):
            raise OSError("path doesn't exist")
        for filename in self.readdir(path):
            newpath = os.path.join(path, filename)
            if self.isdir(newpath):
                self.rmdir(newpath)
            else:
                self.unlink(newpath)
        self.entries[path]["type"] = TYPE_DELETED

    def mkdir(self, path, mode):
        log.info("mkdir %s, mode %o", path, mode)
        if self._exists(path):
            raise OSError("path exist")
        self._updatepath(path, type=TYPE_DIRECTORY,
                         stat=self._getstatdict_forcreate(mode))

    def statfs(self, path):
        log.warning("statfs -- not implemented, returning host fs info")
        # NOTE: returning for the host filesystem which doesn't make sense....
        return super(CowRamFS, self).statfs(path)

    def unlink(self, path):
        log.info("unlink %s", path)
        if not self._exists(path):
            raise OSError("path doesn't exist")
        self._unlink(path)

    def symlink(self, target, path):
        log.info("symlink %s <-- %s", target, path)
        if self._exists(path):
            raise OSError("path exist")
        self._updatepath(
            path,
            type=TYPE_LINK,
            data=target,
            stat=self._getstatdict_forcreate(
                stat.S_IFLNK |
                stat.S_IXUSR |
                stat.S_IXGRP |
                stat.S_IXOTH
                )
            )

    def rename(self, old, new):
        log.info("rename %s to %s", old, new)
        self._updatepath(old)  # puts the file in the self.entries
        self.entries[new] = self.entries[old]
        self._unlink(old)  # this unlinks the current entry at that spot

    def link(self, target, name):
        log.error("hardlink %s <-- %s not implemented", target, name)
        raise Exception("hardlink not supported")

    def utimens(self, path, times=None):
        log.info("utimens %s %s", path, repr(times))
        if times is None:
            now = getnow()
            times = (now, now)

        self._updatepath(path, stat={"st_atime": times[0],
                                     "st_mtime": times[1]})

    # File methods
    # ============

    def open(self, path, flags):
        log.info("open %s", path)
        if path in self.entries:
            return self._nextfh()
        else:
            fh = super(CowRamFS, self).open(path, flags)
            myfh = self._nextfh()
            self.fhmap[myfh] = fh
            return myfh

    def create(self, path, mode, fi=None):
        log.info("create %s", path)
        if self._exists(path):
            raise OSError("path exist")
        self._updatepath(
            path,
            type=TYPE_FILE,
            data="",
            stat=self._getstatdict_forcreate(mode)
            )
        return self._nextfh()

    def read(self, path, length, offset, fh):
        log.info("read %s from %d to %d", path, offset, offset + length)
        if not self._exists(path):
            raise OSError("path doesn't exist")
        # NOTE: not updating atime to avoid useless copies of files only read
        if path in self.entries:
            if self.entries[path]["type"] != TYPE_FILE:
                raise Exception("reading on not a file")
            return self.entries[path]["data"][offset:length]
        else:
            if fh in self.fhmap:
                theirfh = self.fhmap[fh]
            else:
                theirfh = fh
            ret = super(CowRamFS, self).read(path, length, offset, theirfh)
            return ret

    def write(self, path, buf, offset, fh):
        log.info("write %s from %d for %d bytes ", path, offset, len(buf))
        now = getnow()
        if not self._exists(path):
            raise OSError("path doesn't exist")
        stat = self.getattr(path)
        size = stat["st_size"]
        if offset == 0:
            buf_begin = ""
        else:
            buf_begin = self.read(path, offset, offset=0, fh=fh)
        start_end = offset + len(buf)
        if size > start_end:
            buf_end = self.read(path, length=size - start_end,
                                offset=start_end, fh=fh)
        else:
            buf_end = ""
        buf_all = buf_begin + buf + buf_end
        self._updatepath(
            path,
            data=buf_all,
            stat={
                "st_size": len(buf_all),
                "st_mtime": now,
                "st_atime": now,
                }
            )
        if fh in self.fhmap:
            self.release(path, fh)
        return len(buf)

    def truncate(self, path, length, fh=None):
        log.info("truncate %s to %d bytes", path, length)
        now = getnow()
        stat = self.getattr(path)
        size = stat["st_size"]
        newlen = min(size, length)
        if length == 0:
            data = ""
        else:
            data = self.read(path, newlen, offset=0, fh=fh)
        self._updatepath(
            path,
            data=data,
            stat={
                "st_size": newlen,
                "st_mtime": now,
                "st_atime": now,
                }
            )
        if fh in self.fhmap:
            self.release(path, fh)

    def flush(self, path, fh):
        if fh in self.fhmap:
            log.info("flush %s: pass-on", path)
            super(CowRamFS, self).flush(path, self.fhmap[fh])
        else:
            log.info("flush %s: nop", path)
            pass

    def release(self, path, fh):
        if fh in self.fhmap:
            log.info("release %s: pass-on", path)
            super(CowRamFS, self).release(path, self.fhmap[fh])
            del(self.fhmap[fh])
        else:
            log.info("release %s: nop", path)
            pass
        pass

    def fsync(self, path, fdatasync, fh):
        if fh in self.fhmap:
            log.info("fsync %s: pass-on", path)
            super(CowRamFS, self).fsync(path, fdatasync, self.fhmap[fh])
        else:
            log.info("fsync %s: nop", path)
            pass


def main(mountpoint, root):
    global log
    logging.basicConfig()
    log = logging.getLogger("nl.claude.cowramfs")
    log.setLevel(logging.DEBUG)
    fuse.FUSE(CowRamFS(root), mountpoint, foreground=True)

if __name__ == '__main__':
    main(sys.argv[2], sys.argv[1])
