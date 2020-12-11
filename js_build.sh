#!/bin/bash
sudo docker run --rm -v $(pwd):/src -u $(id -u):$(id -g) extended_emscripten em++ -s USE_BOOST_HEADERS=1 -I core/libs -I core/include -I core/libs/asio/include -I /usr/include/boost -L /src/build -std=c++17 -s DISABLE_EXCEPTION_CATCHING=0 -o javascript/entry/js_repository.js javascript/js_repository.cpp  /src/build/libdataframe_core.a --bind
