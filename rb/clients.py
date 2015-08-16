import time

from weakref import ref as weakref

from redis import StrictRedis
from redis.exceptions import ConnectionError, TimeoutError

from rb.promise import Promise
from rb.poll import poll


def assert_open(client):
    if client.closed:
        raise ValueError('I/O operation on closed file')


class CommandBuffer(object):
    """The command buffer is an internal construct """

    def __init__(self, host_id, connection):
        self.host_id = host_id
        self.connection = connection
        self.commands = []
        self.last_command_sent = 0
        self.last_command_received = 0

        # Ensure we're connected.  Without this, we won't have a socket
        # we can select over.
        connection.connect()

    @property
    def closed(self):
        """Indicates if the command buffer is closed."""
        return self.connection is None or self.connection._sock is None

    def fileno(self):
        """Returns the file number of the underlying connection's socket
        to be able to select over it.
        """
        assert_open(self)
        return self.connection._sock.fileno()

    def enqueue_command(self, command_name, args):
        """Enqueue a new command into this pipeline."""
        assert_open(self)
        promise = Promise()
        self.commands.append((command_name, args, promise))
        return promise

    def send_pending_requests(self):
        """Sends all pending requests into the connection."""
        assert_open(self)
        unsent_commands = self.commands[self.last_command_sent:]
        if not unsent_commands:
            return

        all_cmds = self.connection.pack_commands(
            [(x[0],) + tuple(x[1]) for x in unsent_commands])
        self.connection.send_packed_command(all_cmds)
        self.last_command_sent += len(unsent_commands)

    def wait_for_responses(self, client):
        """Waits for all responses to come back and resolves the
        eventual results.
        """
        assert_open(self)
        pending = self.last_command_sent - self.last_command_received
        if pending <= 0:
            return

        for idx in xrange(pending):
            real_idx = self.last_command_received + idx
            command_name, _, promise = self.commands[real_idx]
            value = client.parse_response(
                self.connection, command_name)
            promise.resolve(value)
        self.last_command_received += idx


class RoutingPool(object):
    """The routing pool works together with the routing client to
    internally dispatch through the cluster's router to the correct
    internal connection pool.
    """

    def __init__(self, cluster):
        self.cluster = cluster

    def get_connection(self, command_name, shard_hint=None):
        host_id = shard_hint
        if host_id is None:
            raise RuntimeError('The routing pool requires the host id '
                               'as shard hint')

        real_pool = self.cluster.get_pool_for_host(host_id)
        con = real_pool.get_connection(command_name)
        con.__creating_pool = weakref(real_pool)
        return con

    def release(self, connection):
        # The real pool is referenced by the connection through an
        # internal weakref.  If the weakref is broken it means the
        # pool is already gone and we do not need to release the
        # connection.
        try:
            real_pool = connection.__creating_pool()
        except (AttributeError, TypeError):
            real_pool = None

        if real_pool is not None:
            real_pool.release(connection)

    def disconnect(self):
        self.cluster.disconnect_pools()

    def reset(self):
        pass


class BaseClient(StrictRedis):
    pass


class RoutingBaseClient(BaseClient):

    def pubsub(self, **kwargs):
        raise NotImplementedError('Pubsub is unsupported.')

    def pipeline(self, transaction=True, shard_hint=None):
        raise NotImplementedError('Manual pipelines are unsupported. rb '
                                  'automatically pipelines commands.')

    def lock(self, *args, **kwargs):
        raise NotImplementedError('Locking is not supported.')


class MappingClient(RoutingBaseClient):
    """The routing client uses the cluster's router to target an individual
    node automatically based on the key of the redis command executed.
    """

    def __init__(self, connection_pool, max_concurrency=None):
        RoutingBaseClient.__init__(self, connection_pool=connection_pool)
        # careful.  If you introduce any other variables here, then make
        # sure that FanoutClient.target still works correctly!
        self._max_concurrency = max_concurrency
        self._command_buffer_poll = poll()

    # Standard redis methods

    def execute_command(self, *args):
        router = self.connection_pool.cluster.get_router()
        host_id = router.get_host_for_command(args[0], args[1:])
        buf = self._get_command_buffer(host_id, args[0])
        return buf.enqueue_command(args[0], args[1:])

    # Custom Internal API

    def _get_command_buffer(self, host_id, command_name):
        """Returns the command buffer for the given command and arguments."""
        buf = self._command_buffer_poll.get(host_id)
        if buf is not None:
            return buf

        while len(self._command_buffer_poll) >= self._max_concurrency:
            self._try_to_clear_outstanding_requests()

        connection = self.connection_pool.get_connection(
            command_name, shard_hint=host_id)
        buf = CommandBuffer(host_id, connection)
        self._command_buffer_poll.register(host_id, buf)
        return buf

    def _release_command_buffer(self, command_buffer):
        """This is called by the command buffer when it closes."""
        if command_buffer.closed:
            return

        self._command_buffer_poll.unregister(command_buffer.host_id)
        self.connection_pool.release(command_buffer.connection)
        command_buffer.connection = None

    def _try_to_clear_outstanding_requests(self, timeout=1.0):
        """Tries to clear some outstanding requests in the given timeout
        to reduce the concurrency pressure.
        """
        if not self._command_buffer_poll:
            return

        for command_buffer in self._command_buffer_poll:
            command_buffer.send_pending_requests()

        for command_buffer in self._command_buffer_poll.poll(timeout):
            command_buffer.wait_for_responses(self)
            self._release_command_buffer(command_buffer)

    # Custom Public API

    def join(self, timeout=None):
        """Waits for all outstanding responses to come back or the timeout
        to be hit.
        """
        remaining = timeout

        for command_buffer in self._command_buffer_poll:
            command_buffer.send_pending_requests()

        while self._command_buffer_poll and (remaining is None or
                                             remaining > 0):
            now = time.time()
            rv = self._command_buffer_poll.poll(remaining)
            if remaining is not None:
                remaining -= (time.time() - now)
            for command_buffer in rv:
                command_buffer.wait_for_responses(self)
                self._release_command_buffer(command_buffer)

    def cancel(self):
        """Cancels all outstanding requests."""
        for command_buffer in self._command_buffer_poll:
            self._release_command_buffer(command_buffer)


class FanoutClient(MappingClient):
    """This works similar to the :class:`MappingClient` but instead of
    using the router to target hosts, it sends the commands to all manually
    specified hosts.

    The results are accumulated in a dictionary keyed by the `host_id`.
    """

    def __init__(self, hosts, connection_pool, max_concurrency=None):
        MappingClient.__init__(self, connection_pool, max_concurrency)
        self._target_hosts = hosts
        self.__is_retargeted = False

    def target(self, hosts):
        """Temporarily retarget the client for one call.  This is useful
        when having to deal with a subset of hosts for one call.
        """
        if self.__is_retargeted:
            raise TypeError('Cannot use target more than once.')
        rv = FanoutClient(hosts, connection_pool=self.connection_pool,
                          max_concurrency=self._max_concurrency)
        rv._command_buffer_poll = self._command_buffer_poll
        rv._target_hosts = hosts
        rv.__is_retargeted = True
        return rv

    def execute_command(self, *args):
        promises = {}

        hosts = self._target_hosts
        if hosts == 'all':
            hosts = self.connection_pool.cluster.hosts.keys()
        elif hosts is None:
            raise RuntimeError('Fanout client was not targeted to hosts.')

        for host_id in hosts:
            buf = self._get_command_buffer(host_id, args[0])
            promises[host_id] = buf.enqueue_command(args[0], args[1:])
        return Promise.all(promises)


class RoutingClient(RoutingBaseClient):
    """A client that can route to individual targets."""

    def __init__(self, cluster):
        RoutingBaseClient.__init__(self, connection_pool=RoutingPool(cluster))

    # Standard redis methods

    def execute_command(self, *args, **options):
        pool = self.connection_pool
        command_name = args[0]
        command_args = args[1:]
        router = self.connection_pool.cluster.get_router()
        host_id = router.get_host_for_command(command_name, command_args)
        connection = pool.get_connection(command_name, shard_hint=host_id)
        try:
            connection.send_command(*args)
            return self.parse_response(connection, command_name, **options)
        except (ConnectionError, TimeoutError) as e:
            connection.disconnect()
            if not connection.retry_on_timeout and isinstance(e, TimeoutError):
                raise
            connection.send_command(*args)
            return self.parse_response(connection, command_name, **options)
        finally:
            pool.release(connection)

    # Custom Public API

    def get_mapping_client(self, max_concurrency=64):
        """Returns a thread unsafe mapping client.  This client works
        similar to a redis pipeline and returns eventual result objects.
        It needs to be joined on to work properly.  Instead of using this
        directly you shold use the :meth:`map` context manager which
        automatically joins.

        Returns an instance of :class:`MappingClient`.
        """
        return MappingClient(connection_pool=self.connection_pool,
                             max_concurrency=max_concurrency)

    def get_fanout_client(self, hosts, max_concurrency=64):
        return FanoutClient(hosts, connection_pool=self.connection_pool,
                            max_concurrency=max_concurrency)

    def map(self, timeout=None, max_concurrency=64):
        """Returns a context manager for a map operation.  This runs
        multiple queries in parallel and then joins in the end to collect
        all results.

        In the context manager the client available is a
        :class:`MappingClient`.  Example usage::

            results = {}
            with cluster.map() as client:
                for key in keys_to_fetch:
                    results[key] = client.get(key)
            for key, promise in results.iteritems():
                print '%s => %s' % (key, promise.value)
        """
        return MapManager(self.get_mapping_client(max_concurrency),
                          timeout=timeout)

    def fanout(self, hosts=None, timeout=None, max_concurrency=64):
        """Returns a context manager for a map operation that fans out to
        manually specified hosts instead of using the routing system.  This
        can for instance be used to empty the database on all hosts.  The
        context manager returns a :class:`FanoutClient`.  Example usage::

            with cluster.fanout(hosts='all') as client:
                client.flushdb()

        The promise returned accumulates all results in a dictionary keyed
        by the `host_id`.

        The `hosts` parameter is a list of `host_id`\s or alternatively the
        string ``'all'`` to send the commands to all hosts.

        The fanout APi needs to be used with a lot of care as it can cause
        a lot of damage when keys are written to hosts that do not expect
        them.
        """
        return MapManager(self.get_fanout_client(hosts, max_concurrency),
                          timeout=timeout)


class LocalClient(BaseClient):
    """The local client is just a convenient method to target one specific
    host.
    """

    def __init__(self, cluster, connection_pool=None, **kwargs):
        if connection_pool is None:
            raise TypeError('The local client needs a connection pool')
        BaseClient.__init__(self, cluster, connection_pool=connection_pool,
                            **kwargs)


class MapManager(object):
    """Helps with mapping."""

    def __init__(self, mapping_client, timeout):
        self.mapping_client = mapping_client
        self.timeout = timeout
        self.entered = None

    def __enter__(self):
        self.entered = time.time()
        return self.mapping_client

    def __exit__(self, exc_type, exc_value, tb):
        if exc_type is not None:
            self.mapping_client.cancel()
        else:
            timeout = self.timeout
            if timeout is not None:
                timeout = max(1, timeout - (time.time() - self.started))
            self.mapping_client.join(timeout=timeout)
