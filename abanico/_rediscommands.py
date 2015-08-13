COMMANDS = {'APPEND': [0],
 'AUTH': [],
 'BGREWRITEAOF': [],
 'BGSAVE': [],
 'BITCOUNT': [0],
 'BITOP': None,
 'BITPOS': [0],
 'BLPOP': None,
 'BRPOP': None,
 'BRPOPLPUSH': [0, 1],
 'CLIENT GETNAME': [],
 'CLIENT KILL': [],
 'CLIENT LIST': [],
 'CLIENT PAUSE': [],
 'CLIENT SETNAME': [],
 'CLUSTER ADDSLOTS': [],
 'CLUSTER COUNT-FAILURE-REPORTS': [],
 'CLUSTER COUNTKEYSINSLOT': [],
 'CLUSTER DELSLOTS': [],
 'CLUSTER FAILOVER': [],
 'CLUSTER FORGET': [],
 'CLUSTER GETKEYSINSLOT': [],
 'CLUSTER INFO': [],
 'CLUSTER KEYSLOT': [],
 'CLUSTER MEET': [],
 'CLUSTER NODES': [],
 'CLUSTER REPLICATE': [],
 'CLUSTER RESET': [],
 'CLUSTER SAVECONFIG': [],
 'CLUSTER SET-CONFIG-EPOCH': [],
 'CLUSTER SETSLOT': [],
 'CLUSTER SLAVES': [],
 'CLUSTER SLOTS': [],
 'COMMAND': [],
 'COMMAND COUNT': [],
 'COMMAND GETKEYS': [],
 'COMMAND INFO': [],
 'CONFIG GET': [],
 'CONFIG RESETSTAT': [],
 'CONFIG REWRITE': [],
 'CONFIG SET': [],
 'DBSIZE': [],
 'DEBUG OBJECT': [0],
 'DEBUG SEGFAULT': [],
 'DECR': [0],
 'DECRBY': [0],
 'DEL': None,
 'DISCARD': [],
 'DUMP': [0],
 'ECHO': [],
 'EVAL': None,
 'EVALSHA': None,
 'EXEC': [],
 'EXISTS': None,
 'EXPIRE': [0],
 'EXPIREAT': [0],
 'FLUSHALL': [],
 'FLUSHDB': [],
 'GEOADD': [0],
 'GEODIST': [0],
 'GEOHASH': [0],
 'GEOPOS': [0],
 'GEORADIUS': [0],
 'GEORADIUSBYMEMBER': [0],
 'GET': [0],
 'GETBIT': [0],
 'GETRANGE': [0],
 'GETSET': [0],
 'HDEL': [0],
 'HEXISTS': [0],
 'HGET': [0],
 'HGETALL': [0],
 'HINCRBY': [0],
 'HINCRBYFLOAT': [0],
 'HKEYS': [0],
 'HLEN': [0],
 'HMGET': [0],
 'HMSET': [0],
 'HSCAN': [0],
 'HSET': [0],
 'HSETNX': [0],
 'HSTRLEN': [0],
 'HVALS': [0],
 'INCR': [0],
 'INCRBY': [0],
 'INCRBYFLOAT': [0],
 'INFO': [],
 'KEYS': [],
 'LASTSAVE': [],
 'LINDEX': [0],
 'LINSERT': [0],
 'LLEN': [0],
 'LPOP': [0],
 'LPUSH': [0],
 'LPUSHX': [0],
 'LRANGE': [0],
 'LREM': [0],
 'LSET': [0],
 'LTRIM': [0],
 'MGET': None,
 'MIGRATE': [2],
 'MONITOR': [],
 'MOVE': [0],
 'MSET': None,
 'MSETNX': None,
 'MULTI': [],
 'OBJECT': [],
 'PERSIST': [0],
 'PEXPIRE': [0],
 'PEXPIREAT': [0],
 'PFADD': [0],
 'PFCOUNT': None,
 'PFMERGE': None,
 'PING': [],
 'PSETEX': [0],
 'PSUBSCRIBE': [],
 'PTTL': [0],
 'PUBLISH': [],
 'PUBSUB': [],
 'PUNSUBSCRIBE': [],
 'QUIT': [],
 'RANDOMKEY': [],
 'RENAME': [0, 1],
 'RENAMENX': [0, 1],
 'RESTORE': [0],
 'ROLE': [],
 'RPOP': [0],
 'RPOPLPUSH': [0, 1],
 'RPUSH': [0],
 'RPUSHX': [0],
 'SADD': [0],
 'SAVE': [],
 'SCAN': [],
 'SCARD': [0],
 'SCRIPT EXISTS': [],
 'SCRIPT FLUSH': [],
 'SCRIPT KILL': [],
 'SCRIPT LOAD': [],
 'SDIFF': None,
 'SDIFFSTORE': None,
 'SELECT': [],
 'SET': [0],
 'SETBIT': [0],
 'SETEX': [0],
 'SETNX': [0],
 'SETRANGE': [0],
 'SHUTDOWN': [],
 'SINTER': None,
 'SINTERSTORE': None,
 'SISMEMBER': [0],
 'SLAVEOF': [],
 'SLOWLOG': [],
 'SMEMBERS': [0],
 'SMOVE': [0, 1],
 'SORT': [0],
 'SPOP': [0],
 'SRANDMEMBER': [0],
 'SREM': [0],
 'SSCAN': [0],
 'STRLEN': [0],
 'SUBSCRIBE': [],
 'SUNION': None,
 'SUNIONSTORE': None,
 'SYNC': [],
 'TIME': [],
 'TTL': [0],
 'TYPE': [0],
 'UNSUBSCRIBE': [],
 'UNWATCH': [],
 'WAIT': [],
 'WATCH': None,
 'ZADD': [0],
 'ZCARD': [0],
 'ZCOUNT': [0],
 'ZINCRBY': [0],
 'ZINTERSTORE': None,
 'ZLEXCOUNT': [0],
 'ZRANGE': [0],
 'ZRANGEBYLEX': [0],
 'ZRANGEBYSCORE': [0],
 'ZRANK': [0],
 'ZREM': [0],
 'ZREMRANGEBYLEX': [0],
 'ZREMRANGEBYRANK': [0],
 'ZREMRANGEBYSCORE': [0],
 'ZREVRANGE': [0],
 'ZREVRANGEBYLEX': [0],
 'ZREVRANGEBYSCORE': [0],
 'ZREVRANK': [0],
 'ZSCAN': [0],
 'ZSCORE': [0],
 'ZUNIONSTORE': None}


if __name__ == '__main__':
    REF_URL = 'https://raw.githubusercontent.com/antirez/redis-doc/master/commands.json'

    import json
    import pprint
    import urllib
    spec = json.load(urllib.urlopen(REF_URL))
    rv = {}

    for command, defs in spec.iteritems():
        is_permitted = True
        looking_for_key = True
        key_positions = []

        for arg_idx, arg_spec in enumerate(defs.get('arguments') or ()):
            if arg_spec.get('command'):
                looking_for_key = False
            elif isinstance(arg_spec['type'], list):
                for arg in arg_spec['type']:
                    if arg == 'key':
                        is_permitted = False
            elif arg_spec.get('type') == 'key':
                if arg_spec.get('multiple'):
                    is_permitted = False
                elif looking_for_key:
                    key_positions.append(arg_idx)
                else:
                    is_permitted = False

        if is_permitted:
            rv[str(command)] = key_positions
        else:
            rv[str(command)] = None

    tail = []
    with open(__file__.rstrip('co'), 'r+') as f:
        for line in f:
            if line.strip() == "if __name__ == '__main__':":
                tail.append(line)
                tail.extend(f)
                break

        f.seek(0)
        f.truncate(0)
        f.write('COMMANDS = %s\n\n\n%s' % (
            pprint.pformat(rv),
            ''.join(tail)))
