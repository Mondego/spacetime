# Extended Spacetime Demo Setup Instructions

Pre-requisite: Download and install [emscripten](https://emscripten.org/docs/getting_started/downloads.html)

1. Build both the Javascript and Python versions of spacetime. To do this, goto the project root and then run:

```
$ bash build_js.sh
$ bash build_py.sh
```

2. Build the Typescript bindings for Spacetime. To do this, goto javascript/entry/support and run:

```
$ bash devserver.sh
```

3. Run the server. To do this, goto demo/PresenceServer, and then run:

```
$ python3 server.py > server.log
```

4. Run Websockify proxy server. The browser client will hit websockify port 59159. The server port for the WASM server is 59160.

```
$ websockify 59159 127.0.0.1:59160
```

5. Serve the WebPresence App UI & the wasm binary. Goto javascript/entry

```
$ python3 -m http.server
```

6. Access the WebPresence app at localhost:8000


# Parse Server Demo Setup Instructions

1. Start mongodb. To do this, goto demo/parse-server and run

```
$ mongodb-runner start
```

2. Start parse-server. In demo/parse-server, run:

```
$ npm start
```

3. Start the UI. In demo/parse-server/web

```
$ bash startserver.sh
```

More details on parse-server operation at [npm](https://www.npmjs.com/package/parse-server#getting-started)