rb: the redis blaster
=====================

.. module:: rb

Rb, the redis blaster, is a library that implements non-replicated
sharding for redis.  It implements a custom routing system on top of
python redis that allows you to automatically target different servers
without having to manually route requests to the individual nodes.

It does not implement all functionality of redis and does not attempt to
do so.  You can at any point get a client to a specific host, but for the
most part the assumption is that your operations are limited to basic
key/value operations that can be routed to different nodes automatically.

What you can do:

*   automatically target hosts for single-key operations
*   execute commands against all or a subset of nodes
*   do all of that in parallel

Installation
------------

rb is available on PyPI and can be installed from there::

    $ pip install rb

Configuration
-------------

Getting started with rb is super easy.  If you have been using py-redis
before you will feel right at home.  The main difference is that instead
of connecting to a single host, you configure a cluster to connect to
multiple::

    from rb import Cluster

    cluster = Cluster(hosts={
        0: {'port': 6379},
        1: {'port': 6380},
        2: {'port': 6381},
        3: {'port': 6382},
        4: {'port': 6379},
        5: {'port': 6380},
        6: {'port': 6381},
        7: {'port': 6382},
    }, host_defaults={
        'host': '127.0.0.1',
    })

In this case we set up 8 nodes on four different server processes on the
same host.  The `hosts` parameter is a mapping of hosts to connect to.
The key of the dictionary is the host ID (an integer) and the value is
a dictionary of parameters.  The `host_defaults` is a dictionary of
optional defaults that is filled in for all hosts.  This is useful if you
want to share some common defaults that repeat (in this case all hosts
connect to localhost).

In the default configuration the :class:`PartitionRouter` is used for
routing.

Routing
-------

Now that the cluster is constructed we can use
:meth:`Cluster.get_routing_client` to get a redis client that
automatically routes to the right redis nodes for each command::

    client = cluster.get_routing_client()
    results = {}
    for key in keys_to_look_up:
        results[key] = client.get(key)

The client works pretty much exactly like a standard pyredis
`StrictClient` with the main difference that it can only execute commands
that involve exactly one key.

This basic operation however runs in series.  What makes rb useful is that
it can automatically build redis pipelines and send out queries to many
hosts in parallel.  This however changes the usage slightly as now the
value is not immediately available::

    results = {}
    with cluster.map() as client:
        for key in keys_to_look_up:
            results[key] = client.get(key)

While it looks similar so far, instead of storing the actual values in the
result dictionary, :class:`Promise` objects are stored instead.  When the
map context manager ends they are guaranteed however to have been executed
and you can access the :attr:`Promise.value` attribute to get the value::

    for key, promise in results.iteritems():
        print '%s: %s' % (key, promise.value)

If you want to send a command to all participating hosts (for instance to
delete the database) you can use the :meth:`Cluster.all` method::

    with cluster.all() as client:
        client.flushdb()

If you do that, the promise value is a dictionary with the host IDs as
keys and the results as value.  As an example::

    with cluster.all() as client:
        results = client.info()
    for host_id, info in results.iteritems():
        print 'host %s is running %s' % (host_id, info['os'])

To explicitly target some hosts you can use :meth:`Cluster.fanout` which
accepts a list of host IDs to send the command to.

API
---

This is the entire reference of the public API.  Note that this library
extends the Python redis library so some of these classes have more
functionality for which you will need to consult the py-redis library.

Cluster
```````

.. autoclass:: Cluster
   :members:

Clients
```````

.. autoclass:: RoutingClient
   :members:

.. autoclass:: MappingClient
   :members:

.. autoclass:: FanoutClient
   :members:

Promise
```````

.. autoclass:: Promise
   :members:

Routers
```````

.. autoclass:: BaseRouter
   :members:

.. autoclass:: ConsistentHashingRouter
   :members:

.. autoclass:: PartitionRouter
   :members:

.. autoexception:: UnroutableCommand

Testing
```````

.. autoclass:: rb.testing.TestSetup

.. autofunction:: rb.testing.make_test_cluster
