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
