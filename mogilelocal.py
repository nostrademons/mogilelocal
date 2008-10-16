"""
This is a local filesystem implementation of the same API that the Python
MogileFS client supplies.  It allows us to transparently swap the local
implementation and the full MogileFS in when we need it, without paying the
deployment and configuration price of installing the full MogileFS or the
single-server performance cost of running it.

In general, this tries to be faithful to the semantics of the API, but if you
dig deep enough there are bound to be differences.  For example, MogileFS stores
its actual files with a .fid extension, always, while this stores them under
whatever key you supply.  If you depend on the extension of the URL, they
probably won't match up.  

Similarly, exact error messages are unlikely to be consistent, though I've
tried to use the same class names so you can just catch MogileFSError instead
of the specific IOExceptions raised by the filesystem.  And public fields like
``domain`` and ``trackers`` will be accessible but have empty values.
"""

import os, glob, md5, shutil, sys, urlparse
from os import path as osp

class MogileFSError(Exception):
    """
    Exception class for all MogileFS errors.
    """
    pass

class Client:
    """
    The main MogileFS client.  This is the interface to the filestore.

    This implements most of the dictionary interface (contains, getitem,
    setitem, delitem, and iter), and that's the preferred interface if you
    don't need to deal with bigfiles or storage classes.
    """

    def __init__(self, dir, url):
        """
        Creates a new MogileLocal client.  ``dir`` is the filesystem path where
        files will be stored, while ``url`` is a web-accessible URL that points
        to that directory.  No trailing slash on either.

        >>> fsh = _make_test_client()

        Here, _make_test_client is a helper function that just returns 
        Client('/tmp/mogilelocal', 'http://localhost/mogilelocal'), for easy
        doctesting.

        >>> fsh.dir
        '/tmp/mogilelocal'

        >>> fsh.url
        'http://localhost/mogilelocal'

        >>> fsh.domain
        'Local filesystem'

        >>> fsh.trackers[0]
        'http://localhost:6001/'

        >>> fsh.verify_data
        False

        >>> fsh.verify_repcount
        False

        """
        self.dir = dir
        self.url = url

        self.domain = 'Local filesystem'
        self.trackers = ['http://localhost:6001/']
        self.backend = None
        self.admin = Admin(self.url)
        self.root = ''
        self.clas = ''
        self.verify_data = False
        self.verify_repcount = False

    def reload(self):
        """
        Reinitialize the MogileFS client, resetting all variables (except
        internal MogileLocal implementation details) to their defaults.
        """
        return self.__init__(self.dir, self.url)

    def _ensure_dirs_exist(self, key):
        try:
            os.makedirs(osp.join(self.dir, osp.dirname(key)))
        except OSError, e:
            pass
    
    def _copy_file_or_filename(self, fp_or_path, dest_key):
        if hasattr(fp_or_path, 'read'):
            shutil.copyfileobj(fp_or_path, self.new_file(dest_key))
        else:
            shutil.copyfile(fp_or_path, self._real_path(dest_key))

    def _real_path(self, key):
        """
        Converts a Mogile key to a filesystem path we can use.

        This performs basic sanitation so that you can't pass in parent
        directory references in a key and access the whole hard drive.

        >>> fsh = _make_test_client()
        >>> fsh._real_path('..')
        Traceback (most recent call last):
        ValueError: Key ".." contains .. references

        >>> fsh._real_path('foo/..')
        Traceback (most recent call last):
        ValueError: Key "foo/.." contains .. references

        >>> fsh._real_path('foo/..namme')
        '/tmp/mogilelocal/foo/..namme'

        >>> fsh._real_path('../whatever')
        Traceback (most recent call last):
        ValueError: Key "../whatever" contains .. references

        """
        if key.find('../') != -1 or key.endswith('..'):
            raise ValueError('Key "%s" contains .. references' % key)
        return osp.join(self.dir, key)

    def _real_key(self, path):
        return path[len(self.dir) + 1:] 

    def croak(self, msg):
        """
        Raise a MogileFSError with the supplied ``msg``.
        """
        raise MogileFSError('MogileFS: ' + msg)

    def __contains__(self, key):
        """
        Returns true if the key exists in the filesystem.

        >>> fsh = _make_test_client()
        >>> fsh['new_dir/test'] = 'This is a test'
        >>> fsh['new_dir/test']
        'This is a test'

        >>> 'new_dir/test' in fsh
        True

        >>> del fsh['new_dir/test']
        >>> 'new_dir/test' in fsh
        False

        """
        return osp.exists(self._real_path(key))

    def __getitem__(self, key):
        return self.get_file_data(key)

    def __setitem__(self, key, data):
        self.set_file_data(key, data, self.clas)

    def __delitem__(self, key):
        return self.delete(key)

    def __iter__(self):
        # Original has list_keys('/') here, but I don't think that's correct...
        return iter( self.list_keys('')[1] )

    def setdefault(self, k, default=None):
        f = self[k]
        if f:
            return f
        else :
            self[k] = default
            return default

    def get_file_data(self, key):
        """
        Retrieves the file data associated with ``key``.
        """
        if key not in self:
            return None

        try:
            fp = open(self._real_path(key))
            try:
                return fp.read()
            finally:
                fp.close()
        except IOError, e:
            return self.croak('IO error retrieving %s: %s' % (key, str(e)))

    def set_file_data(self, key, data, clas=None):
        """
        Sets the file ``data`` associated with ``key``.

        >>> fsh = _make_test_client()
        >>> fsh.set_file_data('test/subdir/temp.txt', 'Hello, world')
        >>> fsh.get_file_data('test/subdir/temp.txt')
        'Hello, world'

        Repeated calls simply change the contents of the file:

        >>> fsh.set_file_data('test/subdir/temp.txt', 'This is a test')
        >>> fsh.get_file_data('test/subdir/temp.txt')
        'This is a test'

        >>> fsh.delete('test/subdir/temp.txt')
        True
        >>> fsh.get_file_data('test/subdir/temp.txt')

        """
        try:
            fp = self.new_file(key)
            try:
                fp.write(data)
            finally:
                fp.close()
        except IOError, e:
            return self.croak('IO error saving to %s: %s' % (key, str(e)))

    def new_file(self, key, clas=None, bytes=0):
        """
        Creates a new file under the specified ``key`` and returns a File
        object pointing to it.  The other two arguments are unused, for API
        compatibility.

        >>> fsh = _make_test_client()
        >>> fp = fsh.new_file('test/new.txt')
        >>> fp.write('A new file')
        >>> fp.close()

        >>> fsh['test/new.txt']
        'A new file'
        >>> fsh.rename('test/new.txt', 'newer.txt')
        True
        >>> fsh['newer.txt']
        'A new file'
        >>> 'test/new.txt' in fsh
        False

        >>> fsh.delete('newer.txt')
        True

        """
        try:
            self._ensure_dirs_exist(key)
            return open(self._real_path(key), 'w')
        except IOError, e:
            return self.croak('IO error creating file for %s: %s' % (key, str(e)))

    def delete(self, key):
        """
        Deletes the file associated with ``key``.
        """
        try:
            if key in self:
                os.remove(self._real_path(key))
                return True
            else:
                return False
        except (IOError, OSError), e:
            return self.croak('IO error deleting file %s: %s' % (key, str(e)))

    def delete_small(self, key):
        """
        Deletes a single-chunk file.  In MogileLocal, there's no distinction
        between 'small' and 'big' files, so this is exactly the same as
        `delete`.  However, the real MogileFS system has a distinction between
        'small' files (those that fit in a single chunk) and 'big' files
        (those that are split across machines).  Use delete_small, rename_small
        on normal files, and delete_big, rename_big on those created by
        send_bigfile.
        """
        return self.delete(key)

    def delete_big(self, key):
        """
        Deletes a muli-chunk file.
        """
        return self.delete(key)

    def rename(self, fkey, tkey):
        """
        Rename a file from `fkey` to `tkey`.
        """
        try:
            if fkey in self:
                os.rename(self._real_path(fkey), self._real_path(tkey))
                return True
            else:
                return False
        except OSError, e:
            return self.croak('OS error renaming %s to %s: %s' % 
                    (fkey, tkey, str(e)))

    def rename_small(self, fkey, tkey):
        """
        Rename a single-chunk file.
        """
        return self.rename(fkey, tkey)

    def rename_big(self, key, tkey):
        """
        Rename all chunks of a multi-chunk file.
        """
        return self.rename(fkey, tkey)

    def get_paths(self, key, noverify=0, zone=None):
        """
        Returns the URL for a key, or an empty list of it doesn't exist.

        >>> fsh = _make_test_client()
        >>> fsh['new_dir/test'] = 'This is a test'
        >>> fsh.get_paths('new_dir/test')
        ['http://localhost/mogilelocal/new_dir/test']
        >>> fsh.delete_small('new_dir/test')
        True

        """
        if key not in self:
            return []
        return [self.url + '/' + key]

    def list_keys(self, prefix, after=None, limit=None):
        """
        Lists all keys beginning with ``prefix``.  Returns a tuple (after,
        list) where after is the last element of the returned list.

        >>> fsh = _make_test_client()
        >>> for i in xrange(10): fsh['test' + str(i)] = 'Test'

        >>> fsh.list_keys('test')
        ('test9', ['test0', 'test1', 'test2', 'test3', 'test4', 'test5', 'test6', 'test7', 'test8', 'test9'])

        A nonexistent key results in an empty list and a null string for after:

        >>> fsh.list_keys('no matches here')
        ('', [])

        If ``after`` is specified, it starts the list at the key after
        ``after``.  

        >>> fsh.list_keys('test', 'test4')
        ('test9', ['test5', 'test6', 'test7', 'test8', 'test9'])

        >>> fsh.list_keys('test', 'foo')
        ('test9', ['test0', 'test1', 'test2', 'test3', 'test4', 'test5', 'test6', 'test7', 'test8', 'test9'])

        >>> fsh.list_keys('test', 'test9')
        ('', [])

        If ``limit`` is specified, at most that many elements will be returned.  

        >>> fsh.list_keys('test', None, 2)
        ('test1', ['test0', 'test1'])

        >>> fsh.list_keys('test', 'test1', 2)
        ('test3', ['test2', 'test3'])

        >>> fsh.list_keys('test', 'test1', 12)
        ('test9', ['test2', 'test3', 'test4', 'test5', 'test6', 'test7', 'test8', 'test9'])
        >>> for key in fsh: del fsh[key]

        Slashes in key names shouldn't confuse list_keys:

        >>> for i in xrange(3): fsh['test/%d.json' % i] = 'Test'
        >>> fsh.list_keys('')
        ('test/2.json', ['test/0.json', 'test/1.json', 'test/2.json'])
        >>> fsh.list_keys('test/')
        ('test/2.json', ['test/0.json', 'test/1.json', 'test/2.json'])
        >>> fsh.list_keys('test')
        ('test/2.json', ['test/0.json', 'test/1.json', 'test/2.json'])
        >>> fsh.list_keys('test/0')
        ('test/0.json', ['test/0.json'])

        >>> for key in fsh: del fsh[key]

        """
        path_prefix = self._real_path(prefix)
        dir_prefix = osp.dirname(path_prefix)
        raw_list = []
        for dirpath, dirnames, filenames in os.walk(self.dir):
            if not dirpath.startswith(dir_prefix):
                continue
            for file in filenames:
                path = osp.join(dirpath, file)
                if path.startswith(path_prefix):
                    raw_list.append(self._real_key(path))

        start = 0
        raw_list.sort()
        if after is not None:
            for i, path in zip(xrange(len(raw_list)), raw_list):
                if path == after:
                    start = i + 1

        end = len(raw_list)
        if limit:
            end = min(start + limit, end)

        res_list = raw_list[start:end]
        if not res_list:
            return '', []
        return res_list[-1], res_list

    def set_pref_ip(self, pref_ip):
        """
        No-op for API compatibility.
        """
        pass

    def replication_wait(self, key, mindevcount, seconds):
        """
        No-op for API compatibility.
        """
        return False

    def sleep(self, seconds):
        """
        No-op for API compatibility.
        """
        pass

    def cat(self, key, fp=sys.stdout, big=False):
        """
        Writes the file specified by `key` to the file descriptor `fp` (default
        of sys.stdout).  `big` should be set to True for multi-chunk files.
        """
        if big:
            for part in self.get_bigfile_iter(key):
                fp.write(part)
        else:
            fp.write(self[key])

    def send_file(self, key, source, clas=None, blocksize=1024*1024):
        """
        Sends ``source``, a file-like object or filename, to Mogile, setting it
        as ``key``.  Other arguments are unused and are for API compatibility.

        >>> fsh = _make_test_client()
        >>> fsh['copy_from'] = 'Test'
        >>> fsh.send_file('copy_to', '/tmp/mogilelocal/copy_from')
        True
        >>> fsh['copy_to']
        'Test'

        >>> del fsh['copy_to']
        >>> del fsh['copy_from']

        """

        self._copy_file_or_filename(source, key)
        return True

    def send_bigfile(self, key, source, clas=None, 
                    description="", overwrite=True, chunksize=1024*1024*16):
        """
        Sends the file-like object `source` to Mogile, storing it as `key`.
        """
        if not overwrite and key in self:
            self.choke("pre file or info file for %s already exists" % key)

        return self.send_file(key, source, clas, chunksize)

    def get_bigfile_iter(self, key, chunk_size=1024*1024):
        r"""
        Gets an iterator with the contents of the bigfile.  This returns the
        file data in increments of ``chunk_size``.

        >>> fsh = _make_test_client()
        >>> fsh['copy_from'] = 'This is a test.\nOf the emergency b-cast system.'
        >>> fp = open('/tmp/mogilelocal/copy_from')
        >>> fsh.send_bigfile('copy_to', fp)
        True

        >>> fsh.get_bigfile_as_lines('copy_to').next()
        'This is a test.\n'

        >>> i = fsh.get_bigfile_iter('copy_to', 5)
        >>> i.next()
        'This '
        >>> i.next()
        'is a '

        >>> fsh.delete_big('copy_to')
        True
        >>> fsh.delete_small('copy_from')
        True

        """
        fp = self.get_bigfile_as_file(key)
        while 1:
            chunk = fp.read(chunk_size)
            if not chunk:
                fp.close()
                return
            yield chunk

    def get_bigfile_as_lines(self, key):
        """
        Gets a bigfile as a generator of lines.
        """
        fp = self.get_bigfile_as_file(key)
        for line in fp:
            yield line
        fp.close()

    def get_bigfile_as_file(self, key):
        """
        Gets a bigfile as a file-like object.
        """
        return open(self._real_path(key))

class Admin:
    """
    Mock implementation of the Admin interface.  Nearly all of these methods
    do nothing, since MogileLocal doesn't have the concept of devices or
    classes and assumes that you'll use a different directory for each
    separate instantiation of MogileLocal.  It's provided so that client code
    that relies upon the admin class won't break.
    """

    def __init__(self, url):
        self.url = urlparse.urlparse(url)

    def get_hosts(self, hostid=None):
        return ['%s://%s' % (self.url[0], self.url[1])]

    def get_devices(self, devid=None):
        return [self.url[2]]

    def get_domains(self):
        return []

    def create_domain(self, domain):
        return True

    def delete_domain(self, domain):
        return True

    def create_class(self, domain, clas, mindevcount):
        return True

    def update_class(self, domain, clas, mindevcount):
        return True

    def delete_class(self, domain, clas):
        return True

    def change_device_state(self, host, device, state):
        return True

def _make_test_client():
    return Client('/tmp/mogilelocal', 'http://localhost/mogilelocal')


