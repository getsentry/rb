from __future__ import absolute_import

try:
    integer_types = (int, long)
    text_type = unicode
    bytes_type = str

    _iteritems = "iteritems"
    _itervalues = "itervalues"

    from itertools import izip

    from binascii import crc32
except NameError:
    integer_types = (int,)
    text_type = str
    bytes_type = bytes

    izip = zip

    _iteritems = "items"
    _itervalues = "values"

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


def iteritems(d, **kw):
    return iter(getattr(d, _iteritems)(**kw))


def itervalues(d, **kw):
    return iter(getattr(d, _itervalues)(**kw))
