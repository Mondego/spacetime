'''
Created on Aug 26, 2016

@author: Arthur Valadares
'''
from __future__ import absolute_import
from spacetime_local.frame import frame

from requests.exceptions import HTTPError, ConnectionError
from common.instrument import timethis
import common.instrument as inst
import json
from common.converter import create_jsondict, create_complex_obj
from requests.models import Response
import sys
import time

DIFF_PULL = set(["FULL", "DIFF_PUSHPULL"])
DIFF_PUSH = set(["FULL", "DIFF_PUSH", "DIFF_PUSHPULL"])

class BenchmarkFrame(frame):
    def __init__(self, *args, **kwargs):
        self.mode = None
        super(BenchmarkFrame, self).__init__(*args, **kwargs)
        if hasattr(self, "_instruments"):
            inst.INSTRUMENT_HEADERS[BenchmarkFrame.__module__] = list(set(inst.INSTRUMENT_HEADERS[BenchmarkFrame.__module__]).union(set(inst.INSTRUMENT_HEADERS[frame.__module__])))  # @UndefinedVariable
            #self._instrument_headers.append('bytes sent')
            #self._instrument_headers.append('bytes received')

    def set_benchmode(self, mode):
        self.mode = mode

    @timethis
    def _frame__pull(self):
        if not self.mode or self.mode in DIFF_PULL:
            super(BenchmarkFrame, self)._frame__pull()
        else:
            self._instruments['bytes received'] = 0
            if self._frame__disconnected:
                return
            self.object_store.clear_buffer()
            try:
                for host in self._frame__host_typemap:
                    typemap =  self._frame__host_typemap[host]
                    pull_types = typemap["getting"].union(typemap["gettingsetting"].union(typemap["tracking"]))
                    for t in pull_types:
                        if t.__realname__ in self.object_store.object_map:
                            self.object_store.object_map[t.__realname__].clear()
                    resp = self._frame__sessions[host].get("%s/dictupdate" % host, data = {
                        "observed_types": json.dumps([t.__realname__ for t in pull_types])})
                    objs = resp.json()
                    for t in objs:
                        if objs[t]:
                            typeObj = self._frame__name2type[t]
                            real_objs = [create_complex_obj(typeObj, obj, self.object_store.object_map) for obj in objs[t]]
                            self.object_store.extend(typeObj, real_objs)
                    self._instruments['bytes received'] += len(resp.content)
                    pass
            except ConnectionError:
                self.logger.exception("Disconnected from host.")
                self._frame__disconnected = True
                self._stop()

    @timethis
    def _frame__push(self):
        if not self.mode or self.mode in DIFF_PUSH:
            super(BenchmarkFrame, self)._frame__push()
        else:
            self._instruments['bytes sent'] = 0
            if self._frame__disconnected:
                return
            for host in self._frame__host_typemap:
                im = self._frame__host_typemap[host]
                insert_list = {}
                for t in im["producing"].union(im["gettingsetting"]).union(im["setting"]).union(im["deleting"]):
                    insert_list[t.__realname__] = {}
                    for o in self.object_store.get(t):
                        insert_list[t.__realname__][str(o.__primarykey__)] = create_jsondict(o)
                json_msg = json.dumps(insert_list)
                self._instruments['bytes sent'] += sys.getsizeof(json_msg)
                resp = self._frame__sessions[host].put("%s/dictupdate" % host, data = {
                    "insert_list": json_msg})
