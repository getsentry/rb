class Promise(object):
    """A promise object that attempts to mirror the ES6 APIs for promise
    objects.  Unlike ES6 promises this one however also directly gives
    access to the underlying value.
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
        p.resolve(value)
        return p

    @staticmethod
    def rejected(reason):
        """Creates a promise object rejected with a certain value."""
        p = Promise()
        p.reject(reason)
        return p

    @staticmethod
    def all(iterable):
        """A promise that resolves when all passed promises resolve."""
        promises = list(iterable)
        pending = set(promises)

        rv = Promise()

        def handle_success(promise):
            def handler(value):
                pending.discard(promise)
                if not pending:
                    rv.resolve([x.value for x in promises])
            return handler

        for promise in promises:
            promise.add_callback(handle_success(promise))
            promise.add_errback(rv.reject)

        return rv

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

    def add_callback(self, f):
        """Adds a success callback to the promise."""
        if self._state == 'pending':
            self._callbacks.append(f)
        elif self._state == 'resolved':
            f(self.value)
        return f

    def add_errback(self, f):
        """Adds a error callback to the promise."""
        if self._state == 'pending':
            self._errbacks.append(f)
        elif self._state == 'rejected':
            f(self.reason)
        return f

    def then(self, success=None, failure=None):
        """A utility method to add success and/or failure callback to the
        promise which will also return another promise in the process.
        """
        rv = Promise()
        def resolve(v):
            try:
                rv.resolve(success(v))
            except Exception as e:
                rv.reject(e)
        def reject(r):
            try:
                rv.resolve(failure(r))
            except Exception as e:
                rv.reject(e)
        if success is not None:
            self.add_callback(resolve)
        if failure is not None:
            self.add_callback(reject)
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
