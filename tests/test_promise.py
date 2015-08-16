from rb.promise import Promise


def test_resolved_promise():
    p = Promise.resolved(42)
    assert p.is_resolved
    assert not p.is_pending
    assert not p.is_rejected
    assert p.value == 42


def test_rejected_promise():
    err = RuntimeError('So fail')
    p = Promise.rejected(err)
    assert not p.is_resolved
    assert not p.is_pending
    assert p.is_rejected
    assert p.reason == err


def test_success_callbacks():
    results = []

    p = Promise()
    assert p.is_pending

    @p.add_callback
    def on_success(value):
        results.append(value)

    assert results == []
    p.resolve(42)
    assert results == [42]

    p = Promise.resolved(23)

    @p.add_callback
    def on_success2(value):
        results.append(value)

    assert results == [42, 23]


def test_failure_callbacks():
    results = []

    p = Promise()
    assert p.is_pending

    @p.add_errback
    def on_error(value):
        results.append(value)

    assert results == []
    p.reject(42)
    assert results == [42]

    p = Promise.rejected(23)

    @p.add_errback
    def on_error2(value):
        results.append(value)

    assert results == [42, 23]


def test_promise_then():
    p = Promise.resolved([1, 2, 3])
    def on_success(value):
        return value + [4]
    p2 = p.then(success=on_success)
    assert p2.value == [1, 2, 3, 4]


def test_promise_all():
    p = Promise.all([
        Promise.resolved(1),
        Promise.resolved(2),
        Promise.resolved(3),
    ])

    assert p.is_resolved
    assert p.value == [1, 2, 3]

    p = Promise.all([
        Promise.resolved(1),
        Promise.rejected(2),
        Promise.resolved(3),
    ])
    assert p.is_rejected
    assert p.reason == 2
