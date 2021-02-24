#!/bin/bash

if [ "$1" != "" ]; then
    echo "Clearing build_py/"
    rm -rf build_py/*
else
    echo "not clearing buid_py/"
fi
echo "//" >> python/spacetime/py_repository.cpp
# CXX=/home/lg/2020/research/asio_experiment/emsdktry/emsdk/upstream/bin/clang++ cmake -DCMAKE_BUILD_TYPE=Release -S core/ -B build_py/
# CXX=/home/lg/2020/research/asio_experiment/emsdktry/emsdk/upstream/bin/clang++ cmake --build build_py/
CXX=/usr/bin/clang++ cmake -DCMAKE_BUILD_TYPE=Release -S core/ -B build_py/
CXX=/usr/bin/clang++ cmake --build build_py/
cd python && sudo python3 setup.py install
