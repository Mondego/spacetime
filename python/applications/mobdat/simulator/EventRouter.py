#!/usr/bin/env python
"""
Copyright (c) 2014, Intel Corporation

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

* Redistributions of source code must retain the above copyright notice,
  this list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright
  notice, this list of conditions and the following disclaimer in the
  documentation and/or other materials provided with the distribution.

* Neither the name of Intel Corporation nor the names of its
  contributors may be used to endorse or promote products derived from
  this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER
OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

@file    EventRouter.py
@author  Mic Bowman
@date    2013-12-03

This module defines the event routing functionality used to bind together
the simulation modules in the mobdat simulation environment.

"""

import os, sys
import logging

from multiprocessing import Queue
from . import EventTypes

## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class EventRouter :
    # -----------------------------------------------------------------
    def __init__(self) :
        self.RouterQueue = Queue()
        self.RouterRegistry = {}
        self.Subscriptions = {}
        self._Logger = logging.getLogger(__name__)

    # -----------------------------------------------------------------
    def RegisterHandler(self, handler, queue) :
        self.RouterRegistry[handler] = queue

    # -----------------------------------------------------------------
    def RouteEvents(self) :
        self.RouteEventsLoop()

    # -----------------------------------------------------------------
    def RouteEventsLoop(self) :
        while True :
            try :
                event = self.RouterQueue.get()
                evtype = event.__class__

                if evtype == EventTypes.SubscribeEvent :
                    self.HandleSubscribeEvent(event)
                    continue

                if evtype == EventTypes.UnsubscribeEvent :
                    self.HandleUnsubscribeEvent(event)
                    continue

                if evtype == EventTypes.ShutdownEvent and event.RouterShutdown :
                    return

                self.RouteEvent(evtype, event)
            except KeyboardInterrupt:
                return
            except :
                exctype, value =  sys.exc_info()[:2]
                self._Logger.warn('failed with exception type %s; %s', exctype, str(value))

    # -----------------------------------------------------------------
    def RouteEvent(self, evtype, event) :
        # print "PublishEvent: " + evtype.__name__ + " for " + str(event)
        if evtype in self.Subscriptions :
            for queue in self.Subscriptions[evtype] :
                queue.put(event)

        bases = evtype.__bases__
        if bases :
            self.RouteEvent(bases[0], event)

    # -----------------------------------------------------------------
    def HandleSubscribeEvent(self, event) :
        # print "SubscribeEvent: " + evtype.__name__
        if event.Handler in self.RouterRegistry :
            queue = self.RouterRegistry[event.Handler]
            if not event.EventType in self.Subscriptions :
                self.Subscriptions[event.EventType] = []

            self.Subscriptions[event.EventType].append(queue)

    # -----------------------------------------------------------------
    def HandleUnsubscribeEvent(self, event) :
        # print "UnsubscribeEvent: " + evtype.__name__
        if event.Handler in self.RouterRegistry :
            queue = self.RouterRegistry[event.Handler]
            if event.EventType in self.Subscriptions :
                self.Subscriptions[event.EventType].remove(queue)

