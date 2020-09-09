import re
import ast
import os
from setuptools import setup


_version_re = re.compile(r"__version__\s+=\s+(.*)")


with open("rb/__init__.py", "rb") as f:
    version = str(
        ast.literal_eval(_version_re.search(f.read().decode("utf-8")).group(1))
    )

install_requires = ["redis>=2.6,<3.4"]

# override django version in requirements file if DJANGO_VERSION is set
REDIS_VERSION = os.environ.get('REDIS_VERSION')
if REDIS_VERSION:
    install_requires = [
        u'redis{}'.format(REDIS_VERSION)
        if r.startswith('redis>=') else r
        for r in install_requires
    ]


setup(
    name="rb",
    author="Functional Software Inc.",
    author_email="hello@getsentry.com",
    version=version,
    url="http://github.com/getsentry/rb",
    packages=["rb"],
    description="rb, the redis blaster",
    install_requires=install_requires,
    classifiers=[
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python",
    ],
)
