from distutils.core import setup, Extension
import os

module1 = Extension('repository',
                    #library_dirs = ['.'],
                    include_dirs = ['./include', './libs', './libs/asio/include'],
                    sources = ['pye.cpp', 'pyobj_guard.cpp'],
                    extra_objects=['./liblibtry.a'],
                    extra_compile_args=['-g', '--std=c++17'],
                    language="c++")

setup (name = 'PackageName',
       version = '1.0',
       description = 'This is a demo package',
       ext_modules = [module1])
