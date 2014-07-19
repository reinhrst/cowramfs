cowramfs
========

Copy on write RAM-based file system in python

cowramfs is a fuse filesystem written in python.

The mountpoint will show exactly the same files as another directory (rootdir), but all write operations on the mountpoint will only be visible in the mountpoint (and stored in memory).

The system has a quick startup time and low memory footprint, since the root directory is not actually copied to memory; rather only writes that take place are copied to memory.

Use:

python cowramfs.py rootdir mountpoint

