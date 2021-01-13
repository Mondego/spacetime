"""A setuptools based setup module.
See:
https://packaging.python.org/en/latest/distributing.html
https://github.com/pypa/sampleproject
"""

# Always prefer setuptools over distutils
import os
from setuptools import setup, find_packages, Extension
# To use a consistent encoding
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

# TODO: Connect this to cmake to build the libdataframe_core.a.
# TODO: Make it work with windows.
repository = Extension('spacetime.repository',
                #library_dirs = ['.'],
                include_dirs = ['../core/include', '../core/libs', '../core/libs/asio/include'],
                sources = ['spacetime/py_repository.cpp', 'spacetime/pyobj_guard.cpp'],
                extra_objects=['../build/libdataframe_core.a'],
                extra_compile_args=['--std=c++17', '-O3'],
                language="c++")

setup(
    name='spacetime',

    # Versions should comply with PEP440.  For a discussion on single-sourcing
    # the version across setup.py and the project code, see
    # https://packaging.python.org/en/latest/single_source_version.html
    version='2.2.0',

    description='Spacetime Node Framework',
    long_description='This is the implementation of spacetime and relational types in Python. See https://github.com/Mondego/spacetime',
    long_description_content_type="text/markdown",

    # The project's main homepage.
    url='https://github.com/Mondego/spacetime',

    # Author details
    author='Rohan Achar',
    author_email='ra.rohan@gmail.com',

    # Choose your license
    license='MIT',

    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 3 - Alpha',

        # Indicate who your project is intended for
        'Intended Audience :: Developers',
        'Intended Audience :: Education',
        'Intended Audience :: Science/Research',

        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Application Frameworks',
        'Topic :: Software Development :: Libraries :: Application Frameworks',
        'Topic :: System :: Distributed Computing',

        # Pick your license as you wish (should match "license" above)
        'License :: OSI Approved :: MIT License',

        # Specify the Python versions you support here. In particular, ensure
        # that you indicate whether you support Python 2, Python 3 or both.
        'Programming Language :: Python :: 3',
    ],

    # What does your project relate to?
    keywords='spacetime, distributed frameworks, distributed simulation frameworks',

    # You can just specify the packages manually here if your project is
    # simple. Or you can use find_packages().
    packages=['spacetime', 'rtypes', 'rtypes.types', 'rtypes.utils', 'spacetime.managers', 'spacetime.utils', 'spacetime.managers.connectors'],

    # Alternatively, if you want to distribute just a my_module.py, uncomment
    # this:
    #   py_modules=["my_module"],

    # List run-time dependencies here.  These will be installed by pip when
    # your project is installed. For an analysis of "install_requires" vs pip's
    # requirements files see:
    # https://packaging.python.org/en/latest/requirements.html
    install_requires=['cbor', 'numpy'],

    # List additional groups of dependencies here (e.g. development
    # dependencies). You can install these using the following syntax,
    # for example:
    # $ pip install -e .[dev,test]
    extras_require={
        'dev': [],
        'test': [],
        'flask': ['flask'],
        'crypto' : ['cryptography']
    },
    ext_modules=[repository],

    # If there are data files included in your packages that need to be
    # installed, specify them here.  If using Python 2.6 or less, then these
    # have to be included in MANIFEST.in as well.
    package_data={
    },

    # Although 'package_data' is the preferred approach, in some case you may
    # need to place data files outside of your packages. See:
    # http://docs.python.org/3.4/distutils/setupscript.html#installing-additional-files # noqa
    # In this case, 'data_file' will be installed into '<sys.prefix>/my_data'
    data_files=[],

    # To provide executable scripts, use entry points in preference to the
    # "scripts" keyword. Entry points provide cross-platform support and allow
    # pip to create the appropriate form of executable for the target platform.
    entry_points={
    },
)