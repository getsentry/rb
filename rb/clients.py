import time
import select

from weakref import ref as weakref

from redis import StrictRedis
from redis.exceptions import ConnectionError, TimeoutError


def assert_open(client):
    if client.closed:
        raise ValueError('I/O operation on closed file')


class EventualResult(object):
    """Helper that holds data for an eventually available result value."""

    def __init__(self, command_buffer):
        self._command_buffer = command_buffer
        self.value = None
        self.result_ready = False

    def resolve(self, value):
        if self.result_ready:
            return
        self.value = value
        self.result_ready = True

    def wait_for_result(self):
        if not self.result_ready:
            self._command_buffer.wait_for_specific_result(self)
        return self.value

    def __repr__(self):
        return '<%s %s>' % (
            self.__class__.__name__,
            self.result_ready and repr(self.value) or '(pending)',
        )


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
        er = EventualResult(self)
        self.commands.append((command_name, args, er))
        return er

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
            command_name, _, er = self.commands[real_idx]
            value = client.parse_response(self.connection, command_name)
            er.resolve(value)
        self.last_command_received += idx

    def wait_for_specific_result(self, er):
        """Waits for an eventual result."""
        assert_open(self)
        for idx, (command_name, args, other_er) in enumerate(self.commands):
            if other_er is er:
                break
        else:
            raise LookupError('This result is not guarded by this command '
                              'buffer.')

        if idx < self.last_command_sent:
            self.send_pending_requests()
        if idx < self.last_command_received:
            self.wait_for_outstanding_responses()


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
        self.max_concurrency = max_concurrency
        self.active_command_buffers = {}

    # Standard redis methods

    def execute_command(self, *args):
        buf = self._get_command_buffer(args[0], args[1:])
        return buf.enqueue_command(args[0], args[1:])

    # Custom Internal API

    def _get_command_buffer(self, command_name, command_args):
        """Returns the command buffer for the given command and arguments."""
        router = self.connection_pool.cluster.get_router()
        host_id = router.get_host_for_command(command_name, command_args)
        buf = self.active_command_buffers.get(host_id)
        if buf is not None:
            return buf

        while len(self.active_command_buffers) >= self.max_concurrency:
            self._try_to_clear_outstanding_requests()

        connection = self.connection_pool.get_connection(
            command_name, shard_hint=host_id)
        buf = CommandBuffer(host_id, connection)
        self.active_command_buffers[host_id] = buf
        return buf

    def _release_command_buffer(self, command_buffer):
        """This is called by the command buffer when it closes."""
        if command_buffer.closed:
            return

        self.active_command_buffers.pop(command_buffer.host_id, None)
        self.connection_pool.release(command_buffer.connection)
        command_buffer.connection = None

    def _get_readable_command_buffers(self, timeout=None):
        """Return a list of all command buffers that are readable."""
        # XXX: select is fucking awful.  We should use poll if available
        # but i'm too lazy to do this right now.
        buffers = self.active_command_buffers.values()
        return select.select(buffers, [], [], timeout)[0]

    def _try_to_clear_outstanding_requests(self, timeout=1.0):
        """Tries to clear some outstanding requests in the given timeout
        to reduce the concurrency pressure.
        """
        if not self.active_command_buffers:
            return

        for command_buffer in self.active_command_buffers.values():
            command_buffer.send_pending_requests()

        for command_buffer in self._get_readable_command_buffers(timeout):
            command_buffer.wait_for_responses(self)
            self._release_command_buffer(command_buffer)

    # Custom Public API

    def join(self, timeout=None):
        """Waits for all outstanding responses to come back or the timeout
        to be hit.
        """
        remaining = timeout

        for command_buffer in self.active_command_buffers.values():
            command_buffer.send_pending_requests()

        while self.active_command_buffers and (remaining is None or
                                               remaining > 0):
            now = time.time()
            rv = self._get_readable_command_buffers(remaining)
            if remaining is not None:
                remaining -= (time.time() - now)
            for command_buffer in rv:
                command_buffer.wait_for_responses(self)
                self._release_command_buffer(command_buffer)

    def cancel(self):
        """Cancels all outstanding requests."""
        for command_buffer in self.active_command_buffers.values():
            self._release_command_buffer(command_buffer)


class RoutingClient(RoutingBaseClient):
    """A client that can route to individual targets."""

    def __init__(self, cluster):
        RoutingBaseClient.__init__(self, connection_pool=RoutingPool(cluster))

    # Standard redis methods

    def execute_command(self, *args, **options):
        "Execute a command and return a parsed response"
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
        """
        return MappingClient(connection_pool=self.connection_pool,
                             max_concurrency=max_concurrency)

    def map(self, timeout=None, max_concurrency=64):
        """Returns a context manager for a map operation."""
        return MapManager(self.get_mapping_client(max_concurrency),
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
