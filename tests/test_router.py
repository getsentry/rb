import pytest

from rb.cluster import Cluster
from rb.router import UnroutableCommand, extract_keys


def test_router_key_routing():
    cluster = Cluster({
        0: {'db': 0},
    })

    router = cluster.get_router()
    assert router.get_key('INCR', ['foo']) == 'foo'
    assert router.get_key('GET', ['bar']) == 'bar'

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
    assert router.get_host_for_command('INCR', ['foo']) == 1
    assert router.get_host_for_command('INCR', ['bar']) == 2
    assert router.get_host_for_command('INCR', ['baz']) == 0

    assert router.get_host_for_key('foo') == 1
    assert router.get_host_for_key('bar') == 2
    assert router.get_host_for_key('baz') == 0


def test_key_extraction():
    assert extract_keys(['foo'], (1, 1, 1))
    assert extract_keys(['foo', 'value', 'foo2', 'value2'],
                        (1, -1, 2)) == ['foo', 'foo2']
    assert extract_keys(['extra', 'foo', 'value', 'foo2', 'value2'],
                        (2, -1, 2)) == ['foo', 'foo2']
    assert extract_keys(['foo', 'foo2'], (1, -1, 1)) == ['foo', 'foo2']
