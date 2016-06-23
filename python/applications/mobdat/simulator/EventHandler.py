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

@file    EventHandler.py
@author  Mic Bowman
@date    2013-12-03

This module defines the event routing functionality used to bind together
the simulation modules in the mobdat simulation environment.

"""

import os, sys, traceback
import logging

from multiprocessing import Queue
from . import EventTypes
import random

## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class EventHandler :
    # -----------------------------------------------------------------
    def __init__(self, router) :
        self.RouterQueue = router.RouterQueue
        self.HandlerQueue = Queue()
        self.HandlerID = 'ID%x' % random.randint(0,1000000)
        self.HandlerRegistry = {}

        self._Logger = logging.getLogger(__name__)
        router.RegisterHandler(self.HandlerID, self.HandlerQueue)

    # -----------------------------------------------------------------
    def SubscribeEvent(self, evtype, handler) :
        if not evtype in self.HandlerRegistry :
            event = EventTypes.SubscribeEvent(self.HandlerID, evtype)
            self.RouterQueue.put(event)
            self.HandlerRegistry[evtype] = []

        self.HandlerRegistry[evtype].append(handler)

    # -----------------------------------------------------------------
    def PublishEvent(self, event) :
        self.RouterQueue.put(event)

    # -----------------------------------------------------------------
    def HandleEvents(self) :
        # subscribe to the shutdown event
        # event = EventTypes.SubscribeEvent(self.HandlerID, EventTypes.ShutdownEvent)
        # self.RouterQueue.put(event)

        # save this so we can add handlers later
        # self.HandlerRegistry[EventTypes.ShutdownEvent] = []

        # now go process events
        self.HandleEventsLoop()

    # -----------------------------------------------------------------
    def HandleEventsLoop(self) :
        while True :
            try :
                event = self.HandlerQueue.get()
                evtype = event.__class__

                self.HandleEvent(evtype, event)

                if evtype == EventTypes.ShutdownEvent :
                    return

            except TypeError as detail :
                self._Logger.warn('handler for event %s failed with type error; %s', evtype.__name__, str(detail))
            except :
                self._Logger.exception('handler for event %s failed with exception\n%s', evtype.__name__, traceback.format_exc(10))
                self.Shutdown()
                return

                # exctype, value, tracebk =  sys.exc_info()
                # frames = traceback.extract_tb(tracebk)[-1]
                # self._Logger.warn('handler failed with exception type %s; %s in %s at line %s',
                #                   exctype, str(value), frames[0], frames[1])
                # self.Shutdown()
                # return

    # -----------------------------------------------------------------
    def HandleEvent(self, evtype, event) :
        if evtype in self.HandlerRegistry :
            for handler in self.HandlerRegistry[evtype] :
                handler(event)

    # -----------------------------------------------------------------
    def Shutdown(self) :
        self._Logger.warn('shutting down handler %s', self.__class__.__name__)
        # self.PublishEvent(EventTypes.ShutdownEvent(False))

