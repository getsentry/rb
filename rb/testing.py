import os
import time
import uuid
import shutil
import socket
import tempfile

from contextlib import contextmanager
from subprocess import Popen, PIPE

from rb.cluster import Cluster


devnull = open(os.devnull, 'r+')


class Server(object):

    def __init__(self, cl, socket_path):
        self._cl = cl
        self.socket_path = socket_path

    def test_connection(self):
        try:
            s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            s.connect(self.socket_path)
        except IOError:
            return False
        return True

    def signal_stop(self):
        if self._cl is not None:
            self._cl.kill()

    def close(self):
        if self._cl is not None:
            self.signal_stop()
            self._cl.wait()
            self._cl = None
        try:
            os.remove(self.socket_path)
        except OSError:
            pass


class TestSetup(object):
    """The test setup is a convenient way to spawn multiple redis servers
    for testing and to shut them down automatically.  This can be used as
    a context manager to automatically terminate the clients.
    """

    def __init__(self, servers=4, databases_each=8,
                 server_executable='redis-server'):
        self._fd_dir = tempfile.mkdtemp()
        self.databases_each = databases_each
        self.server_executable = server_executable
        self.servers = []

        for server in xrange(servers):
            self.spawn_server()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, tb):
        self.close()

    def make_cluster(self):
        """Creates a correctly configured cluster from the servers
        spawned.  This also automatically waits for the servers to be up.
        """
        self.wait_for_servers()
        hosts = []
        host_id = 0
        for server in self.servers:
            for x in xrange(self.databases_each):
                hosts.append({
                    'host_id': host_id,
                    'unix_socket_path': server.socket_path,
                    'db': x,
                })
                host_id += 1
        return Cluster(hosts)

    def spawn_server(self):
        """Spawns a new server and adds it to the pool."""
        socket_path = os.path.join(self._fd_dir, str(uuid.uuid4()))
        cl = Popen([self.server_executable, '-'], stdin=PIPE,
                   stdout=devnull)
        cl.stdin.write('''
        port 0
        unixsocket %(path)s
        databases %(databases)d
        save ""
        ''' % {
            'path': socket_path,
            'databases': self.databases_each,
        })
        cl.stdin.flush()
        cl.stdin.close()
        self.servers.append(Server(cl, socket_path))

    def wait_for_servers(self, timeout=10):
        """Waits for all servers to to be up and running."""
        unconnected_servers = dict((x.socket_path, x)
                                   for x in self.servers)
        now = time.time()
        while unconnected_servers:
            for server in unconnected_servers.itervalues():
                if server.test_connection():
                    unconnected_servers.pop(server.socket_path, None)
                    break
            if time.time() > now + timeout:
                return False
            if unconnected_servers:
                time.sleep(0.05)

        return True

    def close(self):
        """Closes the test setup which shuts down all redis servers."""
        for server in self.servers:
            server.signal_stop()
        for server in self.servers:
            server.close()
        try:
            shutil.rmtree(self._fd_dir)
        except (OSError, IOError):
            pass

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass


@contextmanager
def make_test_cluster(*args, **kwargs):
    """Convenient shortcut for creating a test setup and then a cluster
    from it.  This must be used as a context manager::

        from rb.testing import make_test_cluster
        with make_test_cluster() as cluster:
            ...
    """
    with TestSetup(*args, **kwargs) as ts:
        cluster = ts.make_cluster()
        try:
            yield cluster
        finally:
            cluster.disconnect_pools()
