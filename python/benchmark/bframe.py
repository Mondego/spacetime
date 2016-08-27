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

class BenchmarkFrame(frame):
    def __init__(self, *args, **kwargs):
        self.mode = None
        super(BenchmarkFrame, self).__init__(*args, **kwargs)
        if hasattr(self, "_instruments"):
            inst.INSTRUMENT_HEADERS[BenchmarkFrame.__module__] = list(set(inst.INSTRUMENT_HEADERS[BenchmarkFrame.__module__]).union(set(inst.INSTRUMENT_HEADERS[frame.__module__])))  # @UndefinedVariable

    def set_benchmode(self, mode):
        self.mode = mode

    @timethis
    def _frame__pull(self):
        if not self.mode:
            super(BenchmarkFrame, self)._frame__pull()
        else:
            if self._frame__disconnected:
                return
            self.object_store.clear_incoming_record()
            try:
                if self.mode == "BASELINE":
                    for host in self._frame__host_typemap:
                        resp = self._frame__sessions[host].get("%s/dictupdate" % host, data = {
                            "observed_types": json.dumps([t.__realname__ for t in self._frame__observed_types])}).json()
                        for t in resp:
                            typeObj = self._frame__name2type[t]
                            self.object_store.frame_insert_all(typeObj, resp[t])

            except ConnectionError:
                self.logger.exception("Disconnected from host.")
                self._frame__disconnected = True
                self._stop()
