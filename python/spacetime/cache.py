import logging

from pcc.recursive_dictionary import RecursiveDictionary

class Cache(object):
    def __init__(self):
        self.__app_data = RecursiveDictionary()
        self.__app_allowed_types = {}
        self.logger = logging.getLogger(__name__)

    def app_check(self, app):
        if app in self.__app_data:
            return True
        else:
            self.logger.warn("App %s has not been registered to cache", app)
        return False

    def __type_check(self, app, tpname):
        if tpname in self.__app_data[app]:
            return True
        else:
            self.logger.warn("Type %s is not registered to be obtained by App %s.", tpname, app)
        return False

    def app_tp_check(self, app, tpname):
        return self.app_check(app) and self.__type_check(app, tpname)

    def register_app(self, app, types_allowed, types_extra):
        self.__app_data[app] = RecursiveDictionary()
        self.__app_allowed_types[app] = types_allowed
        for tpname in types_allowed.union(types_extra):
            self.reset_cache_for_type(app, tpname)

    def delete_app(self, app):
        if self.app_check(app):
            del self.__app_data[app]
            del self.__app_allowed_types[app]

    def add_new(self, app, tpname, new):
        if self.app_tp_check(app, tpname):
            # New has to be update, because it is replacing the object, not merging it.
            self.__app_data[app][tpname]["new"].update(new)
            pass


    def add_updated(self, app, tpname, updated):
        if self.app_tp_check(app, tpname):
            self.__app_data[app][tpname]["mod"].rec_update(updated)

    def add_deleted(self, app, tpname, deleted):
        if self.app_tp_check(app, tpname):
            self.__app_data[app][tpname]["deleted"].update(deleted)
            for id in deleted:
                self.remove_id(app, tpname, id)

    def add(self, app, tpname, new, updated, deleted):
        self.add_new(app, tpname, new)
        self.add_updated(app, tpname, updated)
        self.add_deleted(app, tpname, deleted)

    def reset_cache_for_type(self, app, tpname):
        if self.app_check(app):
            self.__app_data[app][tpname] = RecursiveDictionary({"new": RecursiveDictionary(), 
                                                                "mod": RecursiveDictionary(), 
                                                                "deleted": set()})

    def reset_tracking_cache_for_type(self, app, tpname):
        if self.app_check(app):
            self.__app_data[app][tpname] = RecursiveDictionary({"new": RecursiveDictionary(), 
                                                                "mod": self.__app_data[app][tpname]["mod"], 
                                                                "deleted": set()})
    def reset_cache_for_all_types(self, app):
        if self.app_check(app):
            for tpname in self.__app_data[app]:
                self.reset_cache_for_type(app, tpname)

    def reset_tracking_cache_for_all_types(self, app):
        if self.app_check(app):
            for tpname in self.__app_data[app]:
                self.reset_tracking_cache_for_type(app, tpname)

    def get_new(self, app, tpname):
        return self.__app_data[app][tpname]["new"] if self.app_tp_check(app, tpname) else {}

    def get_updated(self, app, tpname):
        return self.__app_data[app][tpname]["mod"] if self.app_tp_check(app, tpname) else {}
        

    def get_deleted(self, app, tpname):
        return (list(self.__app_data[app][tpname]["deleted"]) 
                    if self.app_tp_check(app, tpname) else 
               [])

    def get_all_updates(self, app, tpname):
        return (self.get_new(app, tpname),
                self.get_updated(app, tpname),
                self.get_deleted(app, tpname))

    def remove_id(self, app, tpname, id):
        if self.app_tp_check(app, tpname):
            if id in self.__app_data[app][tpname]["new"]:
                del self.__app_data[app][tpname]["new"][id]
            if id in self.__app_data[app][tpname]["mod"]:
                del self.__app_data[app][tpname]["mod"][id]
            