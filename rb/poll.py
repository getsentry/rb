import select


class BasePoller(object):
    is_availabe = False

    def __init__(self):
        self.objects = {}

    def register(self, key, f):
        self.objects[key] = f

    def unregister(self, key):
        return self.objects.pop(key, None)

    def poll(self, timeout=None):
        raise NotImplementedError()

    def get(self, key):
        return self.objects.get(key)

    def __len__(self):
        return len(self.objects)

    def __iter__(self):
        # Make a copy when iterating so that modifications to this object
        # are possible while we're going over it.
        return iter(self.objects.values())


class SelectPoller(BasePoller):
    is_availabe = hasattr(select, 'select')

    def poll(self, timeout=None):
        return select.select(self.objects.values(), [], [], timeout)[0]


class PollPoller(BasePoller):
    is_availabe = hasattr(select, 'poll')

    def __init__(self):
        BasePoller.__init__(self)
        self.pollobj = select.poll()
        self.objects = {}
        self.fd_to_object = {}

    def register(self, key, f):
        BasePoller.register(self, key, f)
        self.pollobj.register(f.fileno(), select.POLLIN | select.POLLHUP)
        self.fd_to_object[f.fileno()] = f

    def unregister(self, key):
        rv = BasePoller.unregister(self, key)
        if rv is not None:
            self.pollobj.unregister(rv.fileno())
            self.fd_to_object.pop(rv.fileno(), None)
        return rv

    def poll(self, timeout=None):
        return [self.fd_to_object[x[0]] for x in
                self.pollobj.poll(timeout)]


class KQueuePoller(BasePoller):
    is_availabe = hasattr(select, 'kqueue')

    def __init__(self):
        BasePoller.__init__(self)
        self.kqueue = select.kqueue()
        self.events = {}
        self.event_to_object = {}

    def register(self, key, f):
        BasePoller.register(self, key, f)
        event = select.kevent(
            f.fileno(), filter=select.KQ_FILTER_READ,
            flags=select.KQ_EV_ADD | select.KQ_EV_ENABLE)
        self.events[f.fileno()] = event
        self.event_to_object[f.fileno()] = f

    def unregister(self, key):
        rv = BasePoller.unregister(self, key)
        if rv is not None:
            self.events.pop(rv.fileno(), None)
            self.event_to_object.pop(rv.fileno(), None)
        return rv

    def poll(self, timeout=None):
        events = self.kqueue.control(self.events.values(), 128, timeout)
        return [self.event_to_object[ev.ident] for ev in events]


class EpollPoller(BasePoller):
    is_availabe = hasattr(select, 'epoll')

    def __init__(self):
        BasePoller.__init__(self)
        self.epoll = select.epoll()
        self.fd_to_object = {}

    def register(self, key, f):
        BasePoller.register(self, key, f)
        self.epoll.register(f.fileno(), select.EPOLLIN | select.EPOLLHUP)
        self.fd_to_object[f.fileno()] = f

    def unregister(self, key):
        rv = BasePoller.unregister(self, key)
        if rv is not None:
            self.epoll.unregister(rv.fileno())
            self.fd_to_object.pop(rv.fileno(), None)
        return rv

    def poll(self, timeout=None):
        if timeout is None:
            timeout = -1
        return [self.fd_to_object[x] for x, _ in
                self.epoll.poll(timeout)]


available_pollers = [poll for poll in [KQueuePoller, PollPoller,
                                       EpollPoller, SelectPoller]
                     if poll.is_availabe]
poll = available_pollers[0]
