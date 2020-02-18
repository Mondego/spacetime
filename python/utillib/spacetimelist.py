'''SpacetimeList:
A list in which each operation has a record
Hence, every record can be used to create
a diff.
'''

from llist import dllist
import weakref
import binascii
import os
from pprint import pprint

class TwoWayDict(dict):
    def __setitem__(self, key, value):
        # Remove any previous connections with these values
        if key in self:
            del self[key]
        if value in self:
            del self[value]
        dict.__setitem__(self, key, value)
        dict.__setitem__(self, value, key)

    def __delitem__(self, key):
        dict.__delitem__(self, self[key])
        dict.__delitem__(self, key)

    def __len__(self):
        """Returns the number of connections"""
        return dict.__len__(self) // 2

class SpacetimeList:
    ORIG_LST = 0
    ADD_LST = 1
    IDENT_BYTES = 8
    def __init__(self, original_list):
        self.original_list = dllist(original_list)
        self.add_list = dllist()
        self.piece_table = dllist()
        self.orig_offset = 0
        self.add_offset = 0
        self.id_to_pt_item = TwoWayDict()

        self.history_list = dllist()
        self.redo_list = dllist()

        for i in range(len(original_list)):
            the_node = self.original_list.nodeat(i)
            temp = (SpacetimeList.ORIG_LST, the_node)
            the_node = self.piece_table.append(temp)
            hist_obj = self.history_object("i", the_node)
            self.history_list.append(hist_obj)
            # self.history_list.appendleft(temp)
            self.orig_offset += 1

    def history_object(self, action, piece_table_node, ident=None):
        if not ident:
            ident = binascii.hexlify(os.urandom(SpacetimeList.IDENT_BYTES))
        self.id_to_pt_item[ident] = piece_table_node 
        if action == "i":
           return {
               "prev": piece_table_node.prev,
               "next": piece_table_node.next,
               "node": piece_table_node,
               "action": "i",
               "buffer": piece_table_node.value[0],
               "ident": ident
           } 
        elif action == "d":
           return {
               "prev": piece_table_node.prev,
               "next": piece_table_node.next,
               "node": piece_table_node,
               "action": "d",
               "buffer": piece_table_node.value[0],
               "ident": ident
           } 
        
        else:
            return {}

    def insert(self, item, loc=None, ident=None, next_node_id=None):
        add_list_node = self.add_list.append(item)
        self.add_offset += 1
        temp = (SpacetimeList.ADD_LST, add_list_node)
        if not loc and not next_node_id:
            the_node = self.piece_table.append(temp)
            h_obj = self.history_object("i", the_node, ident)
            self.history_list.append(h_obj)
        else:
            if next_node_id:
                before_node = self.id_to_pt_item[next_node_id]
            else:
                before_node = self.piece_table.nodeat(loc)
            the_node = self.piece_table.insert(temp, before_node)
            h_obj = self.history_object("i", the_node, ident)
            self.history_list.append(h_obj)

        return h_obj

    def delete(self, i=None, ident=None):
        if i:
            the_node = self.piece_table.nodeat(i)
            ident = self.id_to_pt_item[the_node]
        else:
            the_node = self.id_to_pt_item[ident]
        h_obj = self.history_object("d", the_node, ident)
        self.history_list.append(h_obj)
        # self.history_list.append((the_node.prev, the_node.next, ))
        self.piece_table.remove(the_node)
        return h_obj


    def undo(self):
        ## TODO
        try:
            last_history_obj = self.history_list.pop()
        except ValueError:
            return False
        # print("***", last_history_obj)
        last_node = last_history_obj["node"]
        if last_history_obj["action"] == "d":
            self.piece_table.insertnode(last_node, last_history_obj["next"])
        elif last_history_obj["action"] == "i":
            self.piece_table.remove(last_node)
        else:
            print("Not implemented")
        
        self.redo_list.append(last_history_obj)
        return True

    def redo(self):
        ## TODO
        try:
            last_history_obj = self.redo_list.pop()
        except ValueError:
            return False
        # print("***", last_history_obj)
        last_node = last_history_obj["node"]
        if last_history_obj["action"] == "i":
            self.piece_table.insertnode(last_node, last_history_obj["next"])
        elif last_history_obj["action"] == "d":
            self.piece_table.remove(last_node)
        else:
            print("Not implemented")
        return True
        

    def export_hist_obj(self, hist_obj):
        # make a copy of it
        hcopy = dict(hist_obj)
        # replace "node link" with "node value"
        hcopy['node'] = hcopy['node'].value[1].value
        if hist_obj['prev'] is not None:
            # replace prev "node link" with "node ID"
            hcopy['prev'] = self.id_to_pt_item[hist_obj['prev']]
        else:
            hcopy['prev'] = None
        if hist_obj['next'] is not None:
            # replace next "node link" with "node ID"
            hcopy['next'] = self.id_to_pt_item[hist_obj['next']]
        else:
            hcopy['next'] = None
        
        return hcopy

    def __merge__(self, diff_list):
        # find the point after which the elements are to be inserted
        prev_id = diff_list[0]['prev']

        while self.history_list[-1]['ident'] != prev_id:
            self.undo()

        for item in diff_list:
            add_list_node = self.add_list.append(item['node'])
            temp = (SpacetimeList.ADD_LST, add_list_node)

            the_node = self.piece_table.append(temp)
            h_obj = self.history_object("i", the_node, item['ident'])
            self.history_list.append(h_obj)

        while self.redo():
            pass


    def __diff__(self, start_id=None):
        ''' What kind of object 'start' can be? It could be a 
        particular history object, it could be a history object
        ID, it could be a location in the history object.'''
        diff_list = []
        if not start_id:
            print('/*/')
            # we start from the first history object
            for hist_obj in self.history_list:
                hcopy = self.export_hist_obj(hist_obj)
                # append the "shaped up" history object
                diff_list.append(hcopy)
        else:
            start_found = False
            # we start from the first history object
            for hist_obj in self.history_list:
                if hist_obj['ident'] == start_id:
                    start_found = True

                if start_found:
                    hcopy = self.export_hist_obj(hist_obj)
                    # append the "shaped up" history object
                    diff_list.append(hcopy)

            
                    
        return diff_list

    def get_sequence(self):
        res = []
        for x in self.piece_table:
            if x[0] == SpacetimeList.ADD_LST:
                # print(x)
                res.append(x[1].value) 
            elif x[0] == SpacetimeList.ORIG_LST:
                res.append(x[1].value) 
        return res

    def get_item(self, i):
        return self.piece_table[i]

    def import_diff(self, history_list):
       for hist_obj in history_list:
            if hist_obj['action'] == 'i':
                if hist_obj['next'] is not None:
                    self.insert(hist_obj['node'], ident=hist_obj['ident'], next_node_id=hist_obj['next'])
                else:
                    self.insert(hist_obj['node'], ident=hist_obj['ident'])
            else:
                self.delete(ident=hist_obj['ident'])
            print(self.get_sequence())

if __name__ == "__main__":
    s = SpacetimeList([1,2,3])

    s.insert(4)
    s.insert(5)
    the_diff = s.__diff__()

    s2 = SpacetimeList([])

    print("# Importing the diff")
    s2.import_diff(the_diff)
    s2.insert('x')
    s2.insert('y')

    diff_point = s.insert(6)
    s.insert(7)
    
    # s.delete(i=3)
    # s.delete(i=3)
    # s.insert('x', 2)
    
    print()
    print()

    print("+-+ Exported data")
    pprint(the_diff)
    print("+-+")
    print()
    print()


    print()
    print("SUMMARY")
    print("Exported sequence:", s.get_sequence())
    print("Imported sequence:", s2.get_sequence())

    difflist = s.__diff__(start_id=diff_point['ident'])
    print(difflist)
    print("before", s2.get_sequence())
    s2.__merge__(difflist)
    print(s2.get_sequence())