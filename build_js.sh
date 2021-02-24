# rm -rf build_js/*
emcmake cmake -DCMAKE_BUILD_TYPE=Release -D CMAKE_CXX_FLAGS="-g4 -s EXIT_RUNTIME=1 -s USE_BOOST_HEADERS=1 -s DISABLE_EXCEPTION_CATCHING=0" -S core/ -B build_js/ 
cmake --build build_js/
em++ -g4 -s EXIT_RUNTIME=1 -s USE_BOOST_HEADERS=1 -I core/libs -I core/include -I core/libs/asio/include -I /usr/include/boost -L build_js -std=c++17 -s DISABLE_EXCEPTION_CATCHING=0 -o javascript/entry/js_repository.js javascript/js_repository.cpp  build_js/libdataframe_core.a --bind
