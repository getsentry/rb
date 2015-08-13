import time
import select

from weakref import ref as weakref
from threading import local, RLock

from redis import StrictRedis

from redis.exceptions import ConnectionError


_local = local()


def get_current_routing_client():
    try:
        return _local.routing_stack[-1]
    except (AttributeError, IndexError):
        return None


class EventualResult(object):

    def __init__(self, connection, pool, read_response):
        self.connection = connection
        self.pool = pool
        self.read_response = read_response
        self.value = None
        self.result_ready = False

    def fileno(self):
        if self.connection is None or \
           self.connection._sock is None:
            raise ValueError('I/O operation on closed file')
        return self.connection._sock.fileno()

    def cancel(self):
        if self.result_ready or self.connection is None:
            return
        self.connection.disconnect()
        self.pool.release(self.connection)
        self.connection = None

    def wait_for_result(self):
        if not self.result_ready:
            try:
                self.value = self.read_response()
            finally:
                self.result_ready = True
                self.pool.release(self.connection)
        return self.value


class RoutingPool(object):
    """The routing pool works together with the routing client to
    internally dispatch through the cluster's router to the correct
    internal connection pool.
    """

    def __init__(self, cluster):
        self._cluster = weakref(cluster)

    @property
    def cluster(self):
        rv = self._cluster()
        if rv is None:
            raise RuntimeError('Cluster went away')
        return rv

    def get_connection(self, command_name, shard_hint=None,
                       command_args=None):
        if command_args is None:
            raise TypeError('The routing pool requires that the command '
                            'arguments are provided.')

        router = self.cluster.get_router()
        host_id = router.route(command_name, command_args)
        if host_id is None:
            raise RuntimeError('Unable to determine host for command')

        real_pool = self.cluster.get_pool_for_host(host_id)

        con = real_pool.get_connection(command_name, shard_hint)
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


class RoutingClient(StrictRedis):

    def __init__(self, cluster, max_concurrency=None):
        StrictRedis.__init__(self, connection_pool=RoutingPool(cluster))
        self.max_concurrency = max_concurrency
        self.current_requests = []
        self._routing_lock = RLock()

    def pubsub(self, **kwargs):
        raise NotImplementedError('Pubsub is unsupported.')

    def pipeline(self, transaction=True, shard_hint=None):
        raise NotImplementedError('Pipelines are unsupported.')

    def execute_command(self, *args, **options):
        # TODO: if current_requests > max_concurrency consider trying
        # to clear outstanding requests and block.
        pool = self.connection_pool
        command_name = args[0]
        connection = pool.get_connection(command_name,
                                         command_args=args[1:],
                                         **options)
        try:
            connection.send_command(*args)
            return self.eventual_parse_response(pool, connection,
                                                command_name, **options)
        except ConnectionError:
            connection.disconnect()
            connection.send_command(*args)
            return self.eventual_parse_response(pool, connection,
                                                command_name, **options)

    def eventual_parse_response(self, pool, connection, *args, **kwargs):
        def read_response():
            try:
                return self.parse_response(connection, *args, **kwargs)
            finally:
                with self._routing_lock:
                    try:
                        self.current_requests.remove(er)
                    except ValueError:
                        pass
        er = EventualResult(connection, pool, read_response)
        with self._routing_lock:
            if self.max_concurrency is not None:
                while len(self.current_requests) >= self.max_concurrency:
                    self.wait_for_outstanding_response()
            self.current_requests.append(er)
        return er

    def wait_for_outstanding_response(self, timeout=1.0):
        """Waits for at least one outstanding response in the given timeout."""
        for er in select.select(self.current_requests[:], [], [], timeout)[0]:
            er.wait_for_result()

    def wait_for_outstanding_responses(self, timeout=None):
        """Waits for all outstanding responses to come back or the
        timeout to be hit.
        """
        remaining = timeout

        while self.current_requests and (remaining is None or
                                         remaining > 0):
            now = time.time()
            rv = select.select(self.current_requests[:], [], [], remaining)
            if remaining is not None:
                remaining -= (time.time() - now)
            for er in rv[0]:
                er.wait_for_result()
