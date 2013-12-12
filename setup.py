from setuptools import find_packages
from setuptools import setup


setup(
    name="vimap",
    version="0.1.4",
    provides=["vimap"],
    author="gatoatigrado",
    author_email="gatoatigrado@gmail.com",
    url="https://github.com/gatoatigrado/vimap",
    description='vimap',
    classifiers=[
        "Programming Language :: Python",
        "Operating System :: OS Independent",
        "License :: OSI Approved :: Apache Software License",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Intended Audience :: Developers",
        "Development Status :: 3 - Alpha",
    ],
    install_requires=['testify'],
    packages=find_packages(exclude=['tests*']),
    long_description="""vimap -- variations on imap, not in C

The vimap package is designed to provide a more flexible alternative for
multiprocessing.imap_unordered.  It aspires to support HTTP-like clients
processing data, though contains nothing client-specific.
"""
)
