class Promise(object):

    def __init__(self):
        self.value = None
        self.reason = None
        self._state = 'pending'
        self._callbacks = []
        self._errbacks = []

    @staticmethod
    def resolved(x):
        p = Promise()
        p.resolve(x)
        return p

    @staticmethod
    def rejected(reason):
        p = Promise()
        p.reject(reason)
        return p

    def resolve(self, value):
        if self is value:
            raise TypeError('Cannot resolve promise with itself.')

        if isinstance(value, Promise):
            value.done(self.resolve, self.reject)
            return

        if self._state != 'pending':
            return

        self.value = value
        self._state = 'resolved'
        callbacks = self._callbacks
        self._callbacks = None
        for callback in callbacks:
            callback(value)

    def reject(self, reason):
        if self._state != 'pending':
            return

        self.reason = reason
        self._state = 'rejected'
        errbacks = self._errbacks
        self._errbacks = None
        for errback in errbacks:
            errback(reason)

    @property
    def is_pending(self):
        return self._state == 'pending'

    @property
    def is_resolved(self):
        return self._state == 'resolved'

    @property
    def is_rejected(self):
        return self._state == 'rejected'

    def add_callback(self, f):
        if self._state == 'pending':
            self._callbacks.append(f)
        elif self._state == 'resolved':
            f(self.value)
        return f

    def add_errback(self, f):
        if self._state == 'pending':
            self._errbacks.append(f)
        elif self._state == 'rejected':
            f(self.reason)
        return f

    def done(self, success=None, failure=None):
        if success is not None:
            self.add_callback(success)
        if failure is not None:
            self.add_errback(failure)

    def then(self, success=None, failure=None):
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
