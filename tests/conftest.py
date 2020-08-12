import pytest

from rb.testing import make_test_cluster


@pytest.fixture
def cluster(request):
    mgr = make_test_cluster()
    cluster = mgr.__enter__()

    @request.addfinalizer
    def cleanup():
        mgr.__exit__(None, None, None)

    return cluster
