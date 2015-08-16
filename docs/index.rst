rb: the redis blaster
=====================

.. module:: rb

Rb, the redis blaster, is a library that implements sharding for redis.
It implements a custom routing system on top of python redis that allows
you to automatically target different servers without having to manually
route requests to the individual nodes.

It does not implement all functionality of redis and does not attempt to
do so.  You can at any point get a client to a specific node, but for the
most part the assumption is that your operations are limited to basic
key/value operations that can be routed to different nodes automatically.

Installation
------------

rb is available on pypi and can be installed from there::

    $ pip install rb

Quickstart
----------

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
same host.

Now that the cluster is constructed we can use
:meth:`Cluster.get_routing_client` to get a redis client that
automatically routes to the right redis nodes for each command::

    client = cluster.get_routing_client()
    results = {}
    for key in keys_to_look_up:
        results[key] = client.get(key)

This basic operation however runs in series.  What makes rb useful is that
it can automatically build redis pipelines and send out queries to many
hosts in parallel.  This however changes the usage slightly::

    results = {}
    with cluster.map() as client:
        for key in keys_to_look_up:
            results[key] = client.get(key)

Instead of storing the actual values in the result dictionary,
:class:`Promise` objects are stored instead.  When the map context manager
ends they are guaranteed however to have been executed and you can access
the :attr:`Promise.value` attribute to get the value.

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

Testing
```````

.. autoclass:: rb.testing.TestSetup

.. autofunction:: rb.testing.make_test_cluster
