from __future__ import absolute_import

import sys

PY2 = sys.version_info[0] == 2

if PY2:
    integer_types = (int, long)

    text_type = unicode

    bytes_type = str

    _iteritems = "iteritems"
    _itervalues = "itervalues"

    from itertools import izip
else:
    integer_types = (int,)

    text_type = str

    bytes_type = bytes

    izip = zip

    _iteritems = "items"
    _itervalues = "values"


def iteritems(d, **kw):
    return iter(getattr(d, _iteritems)(**kw))


def itervalues(d, **kw):
    return iter(getattr(d, _itervalues)(**kw))
