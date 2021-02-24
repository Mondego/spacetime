#!/bin/bash

mongo localhost:27017/parse init.js
python3 -m http.server
