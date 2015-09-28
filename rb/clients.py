import time
import errno
import socket

from weakref import ref as weakref
from itertools import izip

from redis import StrictRedis
from redis.client import list_or_args
from redis.exceptions import ConnectionError
try:
    from redis.exceptions import TimeoutError
except ImportError:
    TimeoutError = ConnectionError

from rb.promise import Promise
from rb.poll import poll


AUTO_BATCH_COMMANDS = {
    'GET': ('MGET', True),
    'SET': ('MSET', False),
}


def assert_open(client):
    if client.closed:
        raise ValueError('I/O operation on closed file')


def send_buffer(buf, sock, host_id):
    """Utility function that sends the buffer into the provided socket.
    The buffer itself will slowly clear out and is modified in place.
    """
    try:
        timeout = sock.gettimeout()
        sock.setblocking(False)
        try:
            for idx, item in enumerate(buf):
                sent = 0
                while 1:
                    try:
                        sent = sock.send(item)
                    except IOError as e:
                        if e.errno == errno.EAGAIN:
                            continue
                        elif e.errno == errno.EWOULDBLOCK:
                            break
                        raise
                    break
                if sent < len(item):
                    buf[:idx + 1] = [item[sent:]]
                    break
            else:
                del buf[:]
        finally:
            sock.settimeout(timeout)
    except IOError as e:
        if isinstance(e, socket.timeout):
            raise TimeoutError('Timeout writing to socket (host %s)'
                               % host_id)
        raise ConnectionError('Error while writing to socket (host %s)'
                              % host_id)


def merge_batch(command_name, arg_promise_tuples):
    batch_command, list_response = AUTO_BATCH_COMMANDS[command_name]

    if len(arg_promise_tuples) == 1:
        args, promise = arg_promise_tuples[0]
        return command_name, args, promise

    promise = Promise()

    @promise.done
    def on_success(value):
        if list_response:
            for item, (_, promise) in izip(value, arg_promise_tuples):
                promise.resolve(item)
        else:
            for _, promise in arg_promise_tuples:
                promise.resolve(value)

    args = []
    for individual_args, _ in arg_promise_tuples:
        args.extend(individual_args)

    return batch_command, args, promise


def auto_batch_commands(commands):
    """Given a pipeline of commands this attempts to merge the commands
    into more efficient ones if that is possible.
    """
    pending_batch = None

    for command_name, args, promise in commands:
        # This command cannot be batched, return it as such.
        if command_name not in AUTO_BATCH_COMMANDS:
            if pending_batch:
                yield merge_batch(*pending_batch)
                None
            yield command_name, args, promise
            continue

        if pending_batch and pending_batch[0] == command_name:
            pending_batch[1].append((args, promise))
        else:
            if pending_batch:
                yield merge_batch(*pending_batch)
            pending_batch = (command_name, [(args, promise)])

    if pending_batch:
        yield merge_batch(*pending_batch)


class CommandBuffer(object):
    """The command buffer is an internal construct """

    def __init__(self, host_id, connection, auto_batch=True):
        self.host_id = host_id
        self.connection = connection
        self.commands = []
        self.pending_responses = []
        self.auto_batch = auto_batch
        self._send_buf = []

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

    @property
    def has_pending_requests(self):
        """Indicates if there are outstanding pending requests on this
        buffer.
        """
        return bool(self._send_buf or self.commands)

    def send_pending_requests(self):
        """Sends all pending requests into the connection.  The default is
        to only send pending data that fits into the socket without blocking.
        This returns `True` if all data was sent or `False` if pending data
        is left over.
        """
        assert_open(self)

        unsent_commands = self.commands
        if unsent_commands:
            self.commands = []

            if self.auto_batch:
                unsent_commands = auto_batch_commands(unsent_commands)

            buf = []
            for command_name, args, promise in unsent_commands:
                buf.append((command_name,) + tuple(args))
                self.pending_responses.append((command_name, promise))

            cmds = self.connection.pack_commands(buf)
            self._send_buf.extend(cmds)

        if not self._send_buf:
            return True

        try:
            send_buffer(self._send_buf, self.connection._sock, self.host_id)
        except Exception:
            self.connection.disconnect()
            raise

        return not self._send_buf

    def wait_for_responses(self, client):
        """Waits for all responses to come back and resolves the
        eventual results.
        """
        assert_open(self)

        if self.has_pending_requests:
            raise RuntimeError('Cannot wait for responses if there are '
                               'pending requests outstanding.  You need '
                               'to wait for pending requests to be sent '
                               'first.')

        pending = self.pending_responses
        self.pending_responses = []
        for command_name, promise in pending:
            value = client.parse_response(
                self.connection, command_name)
            promise.resolve(value)


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

    def __init__(self, connection_pool, auto_batch=True):
        BaseClient.__init__(self, connection_pool=connection_pool)
        self.auto_batch = auto_batch

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

    For the parameters see :meth:`Cluster.map`.
    """

    def __init__(self, connection_pool, max_concurrency=None,
                 auto_batch=True):
        RoutingBaseClient.__init__(self, connection_pool=connection_pool,
                                   auto_batch=auto_batch)
        # careful.  If you introduce any other variables here, then make
        # sure that FanoutClient.target still works correctly!
        self._max_concurrency = max_concurrency
        self._cb_poll = poll()

    # For the mapping client we can fix up some redis standard commands
    # as we are promise based and have some flexibility here.

    def mget(self, keys, *args):
        args = list_or_args(keys, args)
        return Promise.all([self.get(arg) for arg in args])

    def mset(self, *args, **kwargs):
        return Promise.all([self.set(k, v) for k, v in dict(*args, **kwargs)
                            .iteritems()]).then(lambda x: None)

    # Standard redis methods

    def execute_command(self, *args):
        router = self.connection_pool.cluster.get_router()
        host_id = router.get_host_for_command(args[0], args[1:])
        buf = self._get_command_buffer(host_id, args[0])
        return buf.enqueue_command(args[0], args[1:])

    # Custom Internal API

    def _get_command_buffer(self, host_id, command_name):
        """Returns the command buffer for the given command and arguments."""
        buf = self._cb_poll.get(host_id)
        if buf is not None:
            return buf

        if self._max_concurrency is not None:
            while len(self._cb_poll) >= self._max_concurrency:
                self.join(timeout=1.0)

        connection = self.connection_pool.get_connection(
            command_name, shard_hint=host_id)
        buf = CommandBuffer(host_id, connection, self.auto_batch)
        self._cb_poll.register(host_id, buf)
        return buf

    def _release_command_buffer(self, command_buffer):
        """This is called by the command buffer when it closes."""
        if command_buffer.closed:
            return

        self._cb_poll.unregister(command_buffer.host_id)
        self.connection_pool.release(command_buffer.connection)
        command_buffer.connection = None

    # Custom Public API

    def join(self, timeout=None):
        """Waits for all outstanding responses to come back or the timeout
        to be hit.
        """
        remaining = timeout

        while self._cb_poll and (remaining is None or remaining > 0):
            now = time.time()
            rv = self._cb_poll.poll(remaining)
            if remaining is not None:
                remaining -= (time.time() - now)

            for command_buffer, event in rv:
                # This command buffer still has pending requests which
                # means we have to send them out first before we can read
                # all the data from it.
                if command_buffer.has_pending_requests:
                    if event == 'write':
                        command_buffer.send_pending_requests()

                # The general assumption is that all response is available
                # or this might block.  On reading we do not use async
                # receiving.  This generally works because latency in the
                # network is low and redis is super quick in sending.  It
                # does not make a lot of sense to complicate things here.
                elif event == 'read':
                    command_buffer.wait_for_responses(self)
                    self._release_command_buffer(command_buffer)

        if self._cb_poll and timeout is not None:
            raise TimeoutError('Did not receive all data in time.')

    def cancel(self):
        """Cancels all outstanding requests."""
        for command_buffer in self._cb_poll:
            self._release_command_buffer(command_buffer)


class FanoutClient(MappingClient):
    """This works similar to the :class:`MappingClient` but instead of
    using the router to target hosts, it sends the commands to all manually
    specified hosts.

    The results are accumulated in a dictionary keyed by the `host_id`.

    For the parameters see :meth:`Cluster.fanout`.
    """

    def __init__(self, hosts, connection_pool, max_concurrency=None,
                 auto_batch=True):
        MappingClient.__init__(self, connection_pool, max_concurrency,
                               auto_batch=auto_batch)
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
        rv._cb_poll = self._cb_poll
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
    """A client that can route to individual targets.

    For the parameters see :meth:`Cluster.get_routing_client`.
    """

    def __init__(self, cluster, auto_batch=True):
        RoutingBaseClient.__init__(self, connection_pool=RoutingPool(cluster),
                                   auto_batch=auto_batch)

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

    def get_mapping_client(self, max_concurrency=64, auto_batch=None):
        """Returns a thread unsafe mapping client.  This client works
        similar to a redis pipeline and returns eventual result objects.
        It needs to be joined on to work properly.  Instead of using this
        directly you shold use the :meth:`map` context manager which
        automatically joins.

        Returns an instance of :class:`MappingClient`.
        """
        if auto_batch is None:
            auto_batch = self.auto_batch
        return MappingClient(connection_pool=self.connection_pool,
                             max_concurrency=max_concurrency,
                             auto_batch=auto_batch)

    def get_fanout_client(self, hosts, max_concurrency=64,
                          auto_batch=None):
        """Returns a thread unsafe fanout client.

        Returns an instance of :class:`FanoutClient`.
        """
        if auto_batch is None:
            auto_batch = self.auto_batch
        return FanoutClient(hosts, connection_pool=self.connection_pool,
                            max_concurrency=max_concurrency,
                            auto_batch=auto_batch)

    def map(self, timeout=None, max_concurrency=64, auto_batch=None):
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
        return MapManager(self.get_mapping_client(max_concurrency, auto_batch),
                          timeout=timeout)

    def fanout(self, hosts=None, timeout=None, max_concurrency=64,
               auto_batch=None):
        """Returns a context manager for a map operation that fans out to
        manually specified hosts instead of using the routing system.  This
        can for instance be used to empty the database on all hosts.  The
        context manager returns a :class:`FanoutClient`.  Example usage::

            with cluster.fanout(hosts=[0, 1, 2, 3]) as client:
                results = client.info()
            for host_id, info in results.value.iteritems():
                print '%s -> %s' % (host_id, info['is'])

        The promise returned accumulates all results in a dictionary keyed
        by the `host_id`.

        The `hosts` parameter is a list of `host_id`\s or alternatively the
        string ``'all'`` to send the commands to all hosts.

        The fanout APi needs to be used with a lot of care as it can cause
        a lot of damage when keys are written to hosts that do not expect
        them.
        """
        return MapManager(self.get_fanout_client(hosts, max_concurrency,
                                                 auto_batch),
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
                timeout = max(1, timeout - (time.time() - self.entered))
            self.mapping_client.join(timeout=timeout)
