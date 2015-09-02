from redis.connection import ConnectionPool, UnixDomainSocketConnection
try:
    from redis.connection import SSLConnection
except ImportError:
    SSLConnection = None

from threading import Lock

from rb.router import PartitionRouter
from rb.clients import RoutingClient, LocalClient


class HostInfo(object):

    def __init__(self, host_id, host, port, unix_socket_path=None, db=0,
                 password=None, ssl=False, ssl_options=None):
        self.host_id = host_id
        self.host = host
        self.unix_socket_path = unix_socket_path
        self.port = port
        self.db = db
        self.password = password
        self.ssl = ssl
        self.ssl_options = ssl_options

    def __eq__(self, other):
        if self.__class__ is not other.__class__:
            return NotImplemented
        return self.host_id == other.host_id

    def __ne__(self, other):
        rv = self.__eq__(other)
        if rv is NotImplemented:
            return NotImplemented
        return not rv

    def __hash__(self):
        return self.host_id

    def __repr__(self):
        return '<%s %s>' % (
            self.__class__.__name__,
            ' '.join('%s=%r' % x for x in sorted(self.__dict__.items())),
        )


def _iter_hosts(iterable):
    if isinstance(iterable, dict):
        iterable = iterable.iteritems()
    for item in iterable:
        if isinstance(item, tuple):
            host_id, cfg = item
            cfg = dict(cfg)
            cfg['host_id'] = host_id
        else:
            cfg = item
        yield cfg


class Cluster(object):
    """The cluster is the core object behind rb.  It holds the connection
    pools to the individual nodes and can be shared for the duration of
    the application in a central location.

    Basic example of a cluster over four redis instances with the default
    router::

        cluster = Cluster(hosts={
            0: {'port': 6379},
            1: {'port': 6380},
            2: {'port': 6381},
            3: {'port': 6382},
        }, host_defaults={
            'host': '127.0.0.1',
        })

    `hosts` is a dictionary of hosts which maps the number host IDs to
    configuration parameters.  The parameters correspond to the signature
    of the :meth:`add_host` function.  The defaults for these parameters
    are pulled from `host_defaults`.  To override the pool class the
    `pool_cls` and `pool_options` parameters can be used.  The same
    applies to `router_cls` and `router_options` for the router.  The pool
    options are useful for setting socket timeouts and similar parameters.
    """

    def __init__(self, hosts, host_defaults=None, pool_cls=None,
                 pool_options=None, router_cls=None, router_options=None):
        if pool_cls is None:
            pool_cls = ConnectionPool
        if router_cls is None:
            router_cls = PartitionRouter
        self._lock = Lock()
        self.pool_cls = pool_cls
        self.pool_options = pool_options
        self.router_cls = router_cls
        self.router_options = router_options
        self._pools = {}
        self._router = None
        self.hosts = {}
        self._hosts_age = 0
        self.host_defaults = host_defaults or {}
        for host_config in _iter_hosts(hosts):
            self.add_host(**host_config)

    def add_host(self, host_id=None, host='localhost', port=6379,
                 unix_socket_path=None, db=0, password=None,
                 ssl=False, ssl_options=None):
        """Adds a new host to the cluster.  This is only really useful for
        unittests as normally hosts are added through the constructor and
        changes after the cluster has been used for the first time are
        unlikely to make sense.
        """
        if host_id is None:
            raise RuntimeError('Host ID is required')
        elif not isinstance(host_id, (int, long)):
            raise ValueError('The host ID has to be an integer')
        host_id = int(host_id)
        with self._lock:
            if host_id in self.hosts:
                raise TypeError('Two hosts share the same host id (%r)' %
                                (host_id,))
            self.hosts[host_id] = HostInfo(host_id=host_id, host=host,
                                           port=port, db=db,
                                           unix_socket_path=unix_socket_path,
                                           password=password, ssl=ssl,
                                           ssl_options=ssl_options)
            self._hosts_age += 1

    def remove_host(self, host_id):
        """Removes a host from the client.  This only really useful for
        unittests.
        """
        with self._lock:
            rv = self._hosts.pop(host_id, None) is not None
            pool = self._pools.pop(host_id, None)
            if pool is not None:
                pool.disconnect()
            self._hosts_age += 1
            return rv

    def disconnect_pools(self):
        """Disconnects all connections from the internal pools."""
        with self._lock:
            for pool in self._pools.itervalues():
                pool.disconnect()
            self._pools.clear()

    def get_router(self):
        """Returns the router for the cluster.  If the cluster reconfigures
        the router will be recreated.  Usually you do not need to interface
        with the router yourself as the cluster's routing client does that
        automatically.

        This returns an instance of :class:`BaseRouter`.
        """
        cached_router = self._router
        ref_age = self._hosts_age

        if cached_router is not None:
            router, router_age = cached_router
            if router_age == ref_age:
                return router

        with self._lock:
            router = self.router_cls(self, **(self.router_options or {}))
            self._router = (router, ref_age)
            return router

    def get_pool_for_host(self, host_id):
        """Returns the connection pool for the given host.

        This connection pool is used by the redis clients to make sure
        that it does not have to reconnect constantly.  If you want to use
        a custom redis client you can pass this in as connection pool
        manually.
        """
        if isinstance(host_id, HostInfo):
            host_info = host_id
            host_id = host_info.host_id
        else:
            host_info = self.hosts.get(host_id)
            if host_info is None:
                raise LookupError('Host %r does not exist' % (host_id,))

        rv = self._pools.get(host_id)
        if rv is not None:
            return rv
        with self._lock:
            rv = self._pools.get(host_id)
            if rv is None:
                opts = dict(self.pool_options or ())
                opts['db'] = host_info.db
                opts['password'] = host_info.password
                if host_info.unix_socket_path is not None:
                    opts['path'] = host_info.unix_socket_path
                    opts['connection_class'] = UnixDomainSocketConnection
                    if host_info.ssl:
                        raise TypeError('SSL is not supported for unix '
                                        'domain sockets.')
                else:
                    opts['host'] = host_info.host
                    opts['port'] = host_info.port
                    if host_info.ssl:
                        if SSLConnection is None:
                            raise TypeError('This version of py-redis does '
                                            'not support SSL connections.')
                        opts['connection_class'] = SSLConnection
                        opts.update(('ssl_' + k, v) for k, v in
                                    (host_info.ssl_options or {}).iteritems())
                rv = self.pool_cls(**opts)
                self._pools[host_id] = rv
            return rv

    def get_local_client(self, host_id):
        """Returns a localized client for a specific host ID.  This client
        works like a regular Python redis client and returns results
        immediately.
        """
        return LocalClient(
            self, connection_pool=self.get_pool_for_host(host_id))

    def get_local_client_for_key(self, key):
        """Similar to :meth:`get_local_client_for_key` but returns the
        client based on what the router says the key destination is.
        """
        return self.get_local_client(self.get_router().get_host_for_key(key))

    def get_routing_client(self, auto_batch=True):
        """Returns a routing client.  This client is able to automatically
        route the requests to the individual hosts.  It's thread safe and
        can be used similar to the host local client but it will refused
        to execute commands that cannot be directly routed to an
        individual node.

        The default behavior for the routing client is to attempt to batch
        eligible commands into batch versions thereof.  For instance multiple
        `GET` commands routed to the same node can end up merged into an
        `MGET` command.  This behavior can be disabled by setting `auto_batch`
        to `False`.  This can be useful for debugging because `MONITOR` will
        more accurately reflect the commands issued in code.

        See :class:`RoutingClient` for more information.
        """
        return RoutingClient(self, auto_batch=auto_batch)

    def map(self, timeout=None, max_concurrency=64, auto_batch=True):
        """Shortcut context manager for getting a routing client, beginning
        a map operation and joining over the result.  `max_concurrency`
        defines how many outstanding parallel queries can exist before an
        implicit join takes place.

        In the context manager the client available is a
        :class:`MappingClient`.  Example usage::

            results = {}
            with cluster.map() as client:
                for key in keys_to_fetch:
                    results[key] = client.get(key)
            for key, promise in results.iteritems():
                print '%s => %s' % (key, promise.value)
        """
        return self.get_routing_client(auto_batch).map(
            timeout=timeout, max_concurrency=max_concurrency)

    def fanout(self, hosts=None, timeout=None, max_concurrency=64,
               auto_batch=True):
        """Shortcut context manager for getting a routing client, beginning
        a fanout operation and joining over the result.

        In the context manager the client available is a
        :class:`FanoutClient`.  Example usage::

            with cluster.fanout(hosts='all') as client:
                client.flushdb()
        """
        return self.get_routing_client(auto_batch).fanout(
            hosts=hosts, timeout=timeout, max_concurrency=max_concurrency)

    def all(self, timeout=None, max_concurrency=64, auto_batch=True):
        """Fanout to all hosts.  Works otherwise exactly like :meth:`fanout`.

        Example::

            with cluster.all() as client:
                client.flushdb()
        """
        return self.fanout('all', timeout=timeout,
                           max_concurrency=max_concurrency,
                           auto_batch=auto_batch)
