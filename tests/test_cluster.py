from rb.cluster import Cluster


def test_basic_interface():
    cluster = Cluster({
        0: {'db': 0},
        1: {'db': 2},
        2: {'db': 4, 'host': '127.0.0.1'},
    })

    assert len(cluster.hosts) == 3

    assert cluster.hosts[0].host_id == 0
    assert cluster.hosts[0].db == 0
    assert cluster.hosts[0].host == 'localhost'
    assert cluster.hosts[0].port == 6379

    assert cluster.hosts[1].host_id == 1
    assert cluster.hosts[1].db == 2
    assert cluster.hosts[1].host == 'localhost'
    assert cluster.hosts[1].port == 6379

    assert cluster.hosts[2].host_id == 2
    assert cluster.hosts[2].db == 4
    assert cluster.hosts[2].host == '127.0.0.1'
    assert cluster.hosts[2].port == 6379


def test_router_access():
    cluster = Cluster({
        0: {'db': 0},
    })

    router = cluster.get_router()
    assert router.cluster is cluster
    assert cluster.get_router() is router

    cluster.add_host(1, {'db': 1})
    new_router = cluster.get_router()
    assert new_router is not router


def test_basic_cluster(cluster):
    iterations = 10000

    with cluster.map() as client:
        for x in xrange(iterations):
            client.set('key-%06d' % x, x)
    responses = []
    with cluster.map() as client:
        for x in xrange(iterations):
            responses.append(client.get('key-%06d' % x))
    ref_sum = sum(int(x.value) for x in responses)
    assert ref_sum == sum(xrange(iterations))


def test_simple_api(cluster):
    client = cluster.get_routing_client()
    with client.map() as map_client:
        for x in xrange(10):
            map_client.set('key:%s' % x, x)

    for x in xrange(10):
        assert client.get('key:%d' % x) == str(x)


def test_promise_api(cluster):
    results = []
    with cluster.map() as client:
        for x in xrange(10):
            client.set('key-%d' % x, x)
        for x in xrange(10):
            promise = client.get('key-%d' % x)
            promise.then(lambda x: results.append(int(x)))
    assert sorted(results) == range(10)
