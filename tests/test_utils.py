import pytest

from rb.utils import bytes_type, crc32


def test_crc32():
    """
    Test that we get consistent values from python 2/3
    """
    assert crc32(bytes_type("test")) == -662733300
