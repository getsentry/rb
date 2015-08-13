from abanico.cluster import Cluster
from abanico.testing import make_test_cluster


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


def test_basic_cluster():
    iterations = 1000

    with make_test_cluster() as cluster:
        with cluster.map() as client:
            for x in xrange(iterations):
                client.set('demo-key-%d' % x, x)
        responses = []
        with cluster.map() as client:
            for x in xrange(iterations):
                responses.append(client.get('demo-key-%d' % x))
        ref_sum = sum(int(x.value) for x in responses)
        assert ref_sum == sum(xrange(iterations))
