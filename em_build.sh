# sudo docker run --rm -v $(pwd):/src -u $(id -u):$(id -g) emscripten/emsdk cmake -DCMAKE_BUILD_TYPE=Release -S core/ -B build/
# sudo docker run --rm -v $(pwd):/src -u $(id -u):$(id -g) emscripten/emsdk cmake --build build/
sudo docker run --rm -v $(pwd):/src -u $(id -u):$(id -g) extended_emscripten emcmake cmake -DCMAKE_BUILD_TYPE=Release -S core/ -B build/ 
sudo docker run --rm -v $(pwd):/src -u $(id -u):$(id -g) extended_emscripten cmake --build build/
# sudo docker run --rm -v $(pwd):/src -u $(id -u):$(id -g) extended_emscripten emcmake -S core/ -B build/
#sudo docker run --rm -v $(pwd):/src -u $(id -u):$(id -g) extended_emscripten cmake --build build/
