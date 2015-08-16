import re
import ast
from setuptools import setup


_version_re = re.compile(r'__version__\s+=\s+(.*)')


with open('rb/__init__.py', 'rb') as f:
    version = str(ast.literal_eval(_version_re.search(
        f.read().decode('utf-8')).group(1)))


setup(
    name='rb',
    author='Functional Software Inc.',
    author_email='hello@getsentry.com',
    version=version,
    url='http://github.com/getsentry/rb',
    packages=['rb'],
    description='rb, the redis blaster',
    install_requires=[
        'redis>=2.6',
    ],
    classifiers=[
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
    ],
)
