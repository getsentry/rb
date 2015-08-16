import select


class SelectPoller(object):

    def __init__(self):
        self.objects = []

    def register(self, f):
        self.objects.append(f)

    def unregister(self, f):
        try:
            self.objects.remove(f)
        except ValueError:
            return False
        return True

    def poll(self, timeout=None):
        return select.select(self.objects, [], [], timeout)[0]


class PollPoller(object):

    def __init__(self):
        self.pollobj = select.poll()

    def register(self, f):
        self.pollobj.register(f, select.POLLIN | select.POLLHUP)

    def unregister(self, f):
        try:
            self.pollobj.unregister(f)
        except LookupError:
            return False
        return True

    def poll(self, timeout=None):
        return [x[0] for x in self.pollobj.poll(timeout)]


class KQueuePoller(object):

    def __init__(self):
        self.kqueue = select.kqueue()
        self.events = {}
        self.objects = {}

    def register(self, f):
        kevent = select.kevent(f.fileno(),
                               filter=select.KQ_FILTER_READ,
                               flags=select.KQ_EV_ADD | select.KQ_EV_ENABLE)
        self.events[f.fileno()] = kevent
        self.objects[f.fileno()] = f

    def unregister(self, f):
        self.objects.pop(f.fileno(), None)
        return self.events.pop(f.fileno(), None) is not None

    def poll(self, timeout=None):
        events = self.kqueue.control(self.events.values(), 128, timeout)
        return [self.objects[ev.ident] for ev in events]


if hasattr(select, 'kqueue'):
    poll = KQueuePoller
elif hasattr(select, 'poll'):
    poll = PollPoller
else:
    poll = SelectPoller
