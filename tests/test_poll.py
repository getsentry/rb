import pytest

from rb import clients
from rb.poll import available_pollers
from rb.utils import bytes_type


@pytest.mark.parametrize('poll', available_pollers,
                         ids=[x.__name__ for x in available_pollers])
def test_simple_api(cluster, poll, monkeypatch):
    monkeypatch.setattr(clients, 'poll', poll)

    client = cluster.get_routing_client()
    with client.map() as map_client:
        for x in range(10):
            map_client.set('key:%s' % x, x)

    for x in range(10):
        assert client.get('key:%d' % x) == bytes_type(x)
