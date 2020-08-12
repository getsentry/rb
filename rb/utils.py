from __future__ import absolute_import

import sys

PY2 = sys.version_info[0] == 2

if PY2:
    integer_types = (int, long)
    text_type = unicode
    bytes_type = str

    def iteritems(d, **kw):
        return iter(d.iteritems(**kw))

    def itervalues(d, **kw):
        return iter(d.itervalues(**kw))

    from itertools import izip

    from binascii import crc32
else:
    integer_types = (int,)
    text_type = str
    bytes_type = bytes

    izip = zip

    def iteritems(d, **kw):
        return iter(d.items(**kw))

    def itervalues(d, **kw):
        return iter(d.values(**kw))

    from binascii import crc32 as _crc32

    # In python3 crc32 was changed to never return a signed value, which is
    # different from the python2 implementation. As noted in
    # https://docs.python.org/3/library/binascii.html#binascii.crc32
    #
    # Note the documentation suggests the following:
    #
    # > Changed in version 3.0: The result is always unsigned. To generate the
    # > same numeric value across all Python versions and platforms, use
    # > crc32(data) & 0xffffffff.
    #
    # However this will not work when transitioning between versions, as the
    # value MUST match what was generated in python 2.
    #
    # We can sign the return value using the following bit math to ensure we
    # match the python2 output of crc32.
    def crc32(*args):
        rt = _crc32(*args)
        return rt - ((rt & 0x80000000) << 1)
