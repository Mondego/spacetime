#!/bin/sh
sudo kill -9 $(sudo netstat -nlp | grep 59159 | awk '{print $7}' | sed 's/\/python3//')
sudo kill -9 $(sudo netstat -nlp | grep 59160 | awk '{print $7}' | sed 's/\/python3//')
