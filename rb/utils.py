from __future__ import absolute_import

import sys

PY2 = sys.version_info[0] == 2

if PY2:
    integer_types = (int, int)

    text_type = type(u'')

    bytes_type = type('')

    _iteritems = 'iteritems'
    _itervalues = 'itervalues'

    
else:
    integer_types = (int,)

    text_type = type('')

    bytes_type = type(b'')

    izip = zip

    _iteritems = 'items'
    _itervalues = 'values'


def iteritems(d, **kw):
    return iter(getattr(d, _iteritems)(**kw))

def itervalues(d, **kw):
    return iter(getattr(d, _itervalues)(**kw))
