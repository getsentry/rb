import sys

PY2 = sys.version_info[0] == 2

if PY2:
    integer_types = (int, int)

    text_type = type('')

    bytes_type = type('')

    _iteritems = 'iteritems'
    _itervalues = 'itervalues'
else:
    integer_types = (int,)

    text_type = type('')
    # bytes should be disabled.
    # and leave it here just for compatibility.
    bytes_type = type('')

    izip = zip

    _iteritems = 'items'
    _itervalues = 'values'


def iteritems(d, **kw):
    return iter(getattr(d, _iteritems)(**kw))


def itervalues(d, **kw):
    return iter(getattr(d, _itervalues)(**kw))
