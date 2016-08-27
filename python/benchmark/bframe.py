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
from common.converter import create_jsondict

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

    @timethis
    def _frame__push(self):
        if not self.mode:
            super(BenchmarkFrame, self)._frame__push()
        else:
            if self._frame__disconnected:
                return
            if self.mode == "BASELINE":
                for host in self._frame__host_typemap:
                    im = self._frame__host_typemap[host]
                    insert_list = {}
                    for t in im["producing"].union(im["gettingsetting"]).union(im["setting"]).union(im["deleting"]):
                        insert_list[t.__realname__] = {}
                        for o in self.object_store.get(t):
                            insert_list[t.__realname__][str(o.__primarykey__)] = create_jsondict(o)

                    resp = self._frame__sessions[host].put("%s/dictupdate" % host, data = {
                        "insert_list": json.dumps(insert_list)})
            else:
                changes = self.object_store.get_changes()
                for host in self._frame__host_typemap:
                    update_dict = {}
                    for tp in self._frame__host_typemap[host]["producing"]:
                        if tp.Class() in changes["new"]:
                            update_dict.setdefault(tp.__realname__, {"new": {}, "mod": {}, "deleted": []})["new"] = changes["new"][tp.Class()]
                    for tp in self._frame__host_typemap[host]["gettingsetting"]:
                        if tp.Class() in changes["mod"]:
                            update_dict.setdefault(tp.__realname__, {"new": {}, "mod": {}, "deleted": []})["mod"] = changes["mod"][tp.Class()]
                    for tp in self._frame__host_typemap[host]["setting"]:
                        if tp.Class() in changes["mod"]:
                            update_dict.setdefault(tp.__realname__, {"new": {}, "mod": {}, "deleted": []})["mod"] = changes["mod"][tp.Class()]
                    for tp in self._frame__host_typemap[host]["deleting"]:
                        if tp.Class() in changes["deleted"]:
                            update_dict.setdefault(tp.__realname__, {"new": {}, "mod": {}, "deleted": []})["deleted"].extend(changes["deleted"][tp.Class()])

                    if update_dict:
                        try:
                            package = {"update_dict": json.dumps(update_dict)}
                            #resp = requests.post(host + "/updated", json = package)
                            resp = self._frame__sessions[host].post(host + "/updated", json = package)
                        except TypeError:
                            self.logger.exception("error encoding json. Object: %s", update_dict)
                        except HTTPError as exc:
                            self._frame__handle_request_errors(resp, exc)
                        except ConnectionError:
                            self.logger.exception("Disconnected from host.")
                            self._frame__disconnected = True
                            self._stop()