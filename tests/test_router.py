import pytest

from abanico.cluster import Cluster
from abanico.router import UnroutableCommand


def test_router_key_routing():
    cluster = Cluster({
        0: {'db': 0},
    })

    router = cluster.get_router()
    assert router.get_key('INCR', ['foo']) == 'foo'
    assert router.get_key('GET', ['bar']) == 'bar'
    assert router.get_key('CLIENT', ['LIST']) is None

    with pytest.raises(UnroutableCommand):
        router.get_key('MGET', ['foo', 'bar', 'baz'])

    with pytest.raises(UnroutableCommand):
        router.get_key('UNKNOWN', [])


def test_router_basics():
    cluster = Cluster({
        0: {'db': 0},
        1: {'db': 1},
        2: {'db': 2},
    })

    router = cluster.get_router()
    assert router.route('INCR', ['foo']) == 1
    assert router.route('INCR', ['bar']) == 2
    assert router.route('INCR', ['baz']) == 0
