import hashlib
import math

from bisect import bisect


def md5_bytes(key):
    return map(ord, hashlib.md5(key).digest())


class Ketama(object):
    """This class implements the Ketama consistent hashing algorithm.
    """

    def __init__(self, nodes=None, weights=None):
        self._nodes = set(nodes or [])
        self._weights = weights if weights else {}

        self._rebuild_circle()

    def _rebuild_circle(self):
        """Updates the hash ring."""
        self._hashring = {}
        self._sorted_keys = []
        total_weight = 0
        for node in self._nodes:
            total_weight += self._weights.get(node, 1)

        for node in self._nodes:
            weight = self._weights.get(node, 1)

            ks = math.floor((40 * len(self._nodes) * weight) / total_weight)

            for i in xrange(0, int(ks)):
                k = md5_bytes('%s-%s-salt' % (node, i))

                for l in xrange(0, 4):
                    key = ((k[3 + l * 4] << 24) | (k[2 + l * 4] << 16) |
                           (k[1 + l * 4] << 8) | k[l * 4])
                    self._hashring[key] = node
                    self._sorted_keys.append(key)

        self._sorted_keys.sort()

    def _get_node_pos(self, key):
        """Return node position(integer) for a given key or None."""
        if not self._hashring:
            return

        k = md5_bytes(key)
        key = (k[3] << 24) | (k[2] << 16) | (k[1] << 8) | k[0]

        nodes = self._sorted_keys
        pos = bisect(nodes, key)

        if pos == len(nodes):
            return 0
        return pos

    def remove_node(self, node):
        """Removes node from circle and rebuild it."""
        try:
            self._nodes.remove(node)
            del self._weights[node]
        except (KeyError, ValueError):
            pass
        self._rebuild_circle()

    def add_node(self, node, weight=1):
        """Adds node to circle and rebuild it."""
        self._nodes.add(node)
        self._weights[node] = weight
        self._rebuild_circle()

    def get_node(self, key):
        """Return node for a given key. Else return None."""
        pos = self._get_node_pos(key)
        if pos is None:
            return None
        return self._hashring[self._sorted_keys[pos]]
