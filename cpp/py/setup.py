from distutils.core import setup, Extension
import os

os.environ["CC"] = "g++-4.7" 
os.environ["CXX"] = "g++-4.7"

module1 = Extension('repository',
                    #library_dirs = ['.'],
                    include_dirs = ['../try', '../try/include', '../try/libs', '../try/libs/asio/include'],
                    sources = ['pye.cpp', 'pyobj_guard.cpp'],
                    extra_objects=['./liblibtry.a'],
                    extra_compile_args=['-g', '--std=c++17', '-w'],
                    language="c++")

setup (name = 'PackageName',
       version = '1.0',
       description = 'This is a demo package',
       ext_modules = [module1])
