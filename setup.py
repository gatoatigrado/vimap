import os.path

from setuptools import find_packages
from setuptools import setup


# Robust-but-boilerplate code to detect the README path
#
# Note: For some reason, `tox` doesn't copy README.md into its build
# directory. So, allow this to not exist.
base_path = os.path.dirname(os.path.abspath(__file__))
readme_path = os.path.join(base_path, 'README.md')


# Default docs, in case the pandoc stuff doesn't work
readme_rst = """vimap -- variations on imap, not in C

The vimap package is designed to provide a more flexible alternative for
multiprocessing.imap_unordered.  It aspires to support HTTP-like clients
processing data, though contains nothing client-specific.
"""


# Mumbo jumbo to convert Markdown --> RST for PyPI
#
# brew install pandoc  # or whatever for your OS
# pip install pyandoc
if os.path.isfile(readme_path):
    try:
        import pandoc
    except ImportError:
        print("WARNING: COULD NOT IMPORT pandoc; YOU WON'T HAVE A NICE README")
    else:
        def get_rst(markdown_file):
            doc = pandoc.Document()
            doc.markdown = open(markdown_file).read()
            return doc.rst

        readme_rst = get_rst(readme_path)


setup(
    name="vimap",
    version="0.1.9-alpha-deadlock-quickfix",
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
    install_requires=[],
    packages=find_packages(exclude=['tests*']),

    # mumbo-jumbo for PyPI :(
    long_description=readme_rst
)
