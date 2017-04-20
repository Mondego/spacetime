'''
Created on Apr 19, 2016

@author: Rohan Achar
'''

from abc import ABCMeta, abstractmethod

class IFrame(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def __init__(self, address = "http://localhost:12000/", time_step = 500):
        # Address is the remote store address:port
        # time_step is the time in milliseconds required to wait.
        pass

    @abstractmethod
    def attach_app(self, app):
        # attach an application to the local frame
        pass

    @abstractmethod
    def run_async(self):
        # Spawn parallel process/thread to run this application
        pass

    @abstractmethod
    def run(self):
        # run in separate thread (To ensure consistency)
        # But wait for thread to join before exit
        pass

    @abstractmethod
    def get(self, tp, id = None):
        # args is optional parameter to send id.
        # frame.get(type) -> returns list of objects of type
        # useful for getching all objects of certain type
        # frame.get(type, id) -> returns an object from type with provided id.
        # useful for single object fetch
        pass

    @abstractmethod
    def add(self, object):
        # insert new object to local store (and pushed if app has type in producer)
        pass

    @abstractmethod
    def delete(self, tp, object):
        # delete objects from local store (and pushed if app has type in deleter)
        pass

    @abstractmethod
    def get_new(self, tp):
        # retrieve all objects that are new since last tick
        pass

    @abstractmethod
    def get_mod(self, tp):
        # retrieve all objects that are modified since last tick
        pass

    @abstractmethod
    def get_deleted(self, tp):
        # retrieve all object ids that are deleted since last tick
        pass

    def __pull(self):
        # Internal method to pull updates from remote server
        pass

    def __push(self):
        # Internal method to push updates to remote server
        pass

    def __shutdown(self):
        # shut down procedure
        pass

    def __unregister_app(self):
        # unregister app from server
        pass
