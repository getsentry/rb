from functools import partial


class Promise(object):
    """A promise object that attempts to mirror the ES6 APIs for promise
    objects.  Unlike ES6 promises this one however also directly gives
    access to the underlying value and it has some slightly different
    static method names as this promise can be resolved externally.
    """
    __slots__ = ('value', 'reason', '_state', '_callbacks', '_errbacks')

    def __init__(self):
        #: the value that this promise holds if it's resolved.
        self.value = None
        #: the reason for this promise if it's rejected.
        self.reason = None
        self._state = 'pending'
        self._callbacks = []
        self._errbacks = []

    @staticmethod
    def resolved(value):
        """Creates a promise object resolved with a certain value."""
        p = Promise()
        p._state = 'resolved'
        p.value = value
        return p

    @staticmethod
    def rejected(reason):
        """Creates a promise object rejected with a certain value."""
        p = Promise()
        p._state = 'rejected'
        p.reason = reason
        return p

    @staticmethod
    def all(iterable_or_dict):
        """A promise that resolves when all passed promises resolve.  You can
        either pass a list or a dictionary of promises.
        """
        if isinstance(iterable_or_dict, dict):
            return _promise_from_dict(iterable_or_dict)
        return _promise_from_iterable(iterable_or_dict)

    def resolve(self, value):
        """Resolves the promise with the given value."""
        if self is value:
            raise TypeError('Cannot resolve promise with itself.')

        if isinstance(value, Promise):
            value.done(self.resolve, self.reject)
            return

        if self._state != 'pending':
            raise RuntimeError('Promise is no longer pending.')

        self.value = value
        self._state = 'resolved'
        callbacks = self._callbacks
        self._callbacks = None
        for callback in callbacks:
            callback(value)

    def reject(self, reason):
        """Rejects the promise with the given reason."""
        if self._state != 'pending':
            raise RuntimeError('Promise is no longer pending.')

        self.reason = reason
        self._state = 'rejected'
        errbacks = self._errbacks
        self._errbacks = None
        for errback in errbacks:
            errback(reason)

    @property
    def is_pending(self):
        """`True` if the promise is still pending, `False` otherwise."""
        return self._state == 'pending'

    @property
    def is_resolved(self):
        """`True` if the promise was resolved, `False` otherwise."""
        return self._state == 'resolved'

    @property
    def is_rejected(self):
        """`True` if the promise was rejected, `False` otherwise."""
        return self._state == 'rejected'

    def done(self, on_success=None, on_failure=None):
        """Attaches some callbacks to the promise and returns the promise."""
        if on_success is not None:
            if self._state == 'pending':
                self._callbacks.append(on_success)
            elif self._state == 'resolved':
                on_success(self.value)
        if on_failure is not None:
            if self._state == 'pending':
                self._errbacks.append(on_failure)
            elif self._state == 'rejected':
                on_failure(self.reason)
        return self

    def then(self, success=None, failure=None):
        """A utility method to add success and/or failure callback to the
        promise which will also return another promise in the process.
        """
        rv = Promise()

        def on_success(v):
            try:
                rv.resolve(success(v))
            except Exception as e:
                rv.reject(e)

        def on_failure(r):
            try:
                rv.resolve(failure(r))
            except Exception as e:
                rv.reject(e)

        self.done(on_success, on_failure)
        return rv

    def __repr__(self):
        if self._state == 'pending':
            v = '(pending)'
        elif self._state == 'rejected':
            v = repr(self.reason) + ' (rejected)'
        else:
            v = repr(self.value)
        return '<%s %s>' % (
            self.__class__.__name__,
            v,
        )


def _ensure_promise(value):
    return value if isinstance(value, Promise) else Promise.resolved(value)


def _promise_from_iterable(iterable):
    l = [_ensure_promise(x) for x in iterable]
    if not l:
        return Promise.resolved([])

    pending = set(l)
    rv = Promise()

    def on_success(promise, value):
        pending.discard(promise)
        if not pending:
            rv.resolve([p.value for p in l])

    for promise in l:
        promise.done(partial(on_success, promise), rv.reject)

    return rv


def _promise_from_dict(d):
    d = dict((k, _ensure_promise(v)) for k, v in d.iteritems())
    if not d:
        return Promise.resolved({})

    pending = set(d.keys())
    rv = Promise()

    def on_success(key, value):
        pending.discard(key)
        if not pending:
            rv.resolve(dict((k, p.value) for k, p in d.iteritems()))

    for key, promise in d.iteritems():
        promise.done(partial(on_success, key), rv.reject)

    return rv
