# rb [![test](https://github.com/getsentry/rb/actions/workflows/test.yml/badge.svg)](https://github.com/getsentry/rb/actions/workflows/test.yml)

![logo](https://github.com/getsentry/rb/blob/master/docs/_static/rb.png?raw=true)

rb - the redis blaster.

The fastest way to talk to many redis nodes.  Can do routing as well as
blindly blasting commands to many nodes.  How does it work?

For full documentation see [rb.rtfd.org](http://rb.rtfd.org/)

## Quickstart

Set up a cluster:

```python
from rb import Cluster

cluster = Cluster({
    0: {'port': 6379},
    1: {'port': 6380},
    2: {'port': 6381},
    3: {'port': 6382},
}, host_defaults={
    'host': '127.0.0.1',
})
```

Automatic routing:

```python
results = []
with cluster.map() as client:
    for key in range(100):
        client.get(key).then(lambda x: results.append(int(x or 0)))

print('Sum: %s' % sum(results))
```

Fanout:

```python
with cluster.fanout(hosts=[0, 1, 2, 3]) as client:
    infos = client.info()
```

Fanout to all:

```python
with cluster.fanout(hosts='all') as client:
    client.flushdb()
```
