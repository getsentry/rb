import time

from redis.connection import ConnectionPool, UnixDomainSocketConnection

from threading import Lock
from contextlib import contextmanager

from abanico.router import PartitionRouter
from abanico.clients import RoutingClient


class HostInfo(object):

    def __init__(self, host_id, host, port, unix_socket_path=None, db=0):
        self.host_id = host_id
        self.host = host
        self.unix_socket_path = unix_socket_path
        self.port = port
        self.db = db

    def __eq__(self, other):
        if self.__class__ is not other.__class__:
            return NotImplemented
        return self.host_id == other.host_id

    def __ne__(self, other):
        rv = self.__eq__(other)
        if rv is NotImplemented:
            return NotImplemented
        return rv

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
                 unix_socket_path=None, db=0):
        """Adds a new host to the cluster."""
        if host_id is None:
            raise RuntimeError('Host ID is required')
        with self._lock:
            if host_id in self.hosts:
                raise TypeError('Two hosts share the same host id (%r)' %
                                (host_id,))
            self.hosts[host_id] = HostInfo(host_id=host_id, host=host,
                                           port=port, db=db,
                                           unix_socket_path=unix_socket_path)
            self._hosts_age += 1

    def remove_host(self, host_id):
        """Removes a host from the client."""
        with self._lock:
            rv = self._hosts.pop(host_id, None) is not None
            pool = self._pools.pop(host_id, None)
            if pool is not None:
                pool.disconnect()
            self._hosts_age += 1
            return rv

    def disconnect_pools(self):
        """Disconnects all internal pools."""
        with self._lock:
            for pool in self._pools.itervalues():
                pool.disconnect()
            self._pools.clear()

    def get_router(self):
        """Returns the router for the cluster.  If the cluster reconfigures
        the router will be recreated.
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
        """Returns the connection pool for the given host."""
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
                if host_info.unix_socket_path is not None:
                    opts['path'] = host_info.unix_socket_path
                    opts['connection_class'] = UnixDomainSocketConnection
                else:
                    opts['host'] = host_info.host
                    opts['port'] = host_info.port
                rv = self.pool_cls(**opts)
                self._pools[host_id] = rv
            return rv

    def get_routing_client(self, max_concurrency=64):
        """Returns a routing client.  This client can operate
        automatically across the routing targets but instead of returning
        results directly it will return result objects that you need to
        await.
        """
        return RoutingClient(self, max_concurrency=max_concurrency)

    @contextmanager
    def map(self, timeout=None):
        """Shortcut context manager for getting a routing client and joining
        for the result.
        """
        now = time.time()
        client = self.get_routing_client()
        try:
            yield client
        finally:
            if timeout is not None:
                timeout = max(1, timeout - (time.time() - now))
            client.wait_for_outstanding_responses(timeout=timeout)
