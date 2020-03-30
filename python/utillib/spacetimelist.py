'''SpacetimeList:
A list in which each operation has a record
Hence, every record can be used to create
a diff.
'''

import weakref
import binascii
import os, time
from threading import Thread
from pprint import pprint
from utils import TwoWayDict, generate_id
from utillib.dllist4 import dllist
from datamodel import Document
import sys
import pdb
# from llist import dllist

class FakeDocument:
    def __init__(self):
        self.history_list = []

class SpacetimeList(Thread):
    ORIG_LST = 0
    ADD_LST = 1
    IDENT_BYTES = 8
    def __init__(self, df):
        original_list = list()
        self.df = df
        self.original_list = dllist(original_list)
        self.add_list = dllist(original_list)
        self.piece_table = dllist()
        self.id_to_pt_item = TwoWayDict()
        if df:
            while not self.df.read_one(Document, "SINGLETON"):
                self.df.pull_await()
            self.document = self.df.read_one(Document, "SINGLETON")
        else:
            self.document = FakeDocument()

        self.redo_list = dllist()

        for i in range(len(original_list)):
            the_node = self.add_list.nodeat(i)
            temp = (SpacetimeList.ADD_LST, the_node)
            the_node = self.piece_table.append(temp)
            hist_obj = self.history_object("i", the_node)
            self.document.history_list += [hist_obj]
            # self.history_list.appendleft(temp)
        super().__init__(daemon=True)
        self.start()
    
    def run(self):
        while True:
            st = time.perf_counter()
            if self.df:
                self.df.commit()
                self.df.push()
                self.df.pull()
                self.__merge__(list(self.document.history_list_sync))
            et = time.perf_counter()
            if (et - st) < 0.3:
                time.sleep(et - st)


    def history_object(self, action, piece_table_node, action_id=None):
        if not action_id:
            action_id = generate_id()

        self.id_to_pt_item[action_id] = piece_table_node 

        try:
            prev_node_id = self.piece_table.get_id_from_node(piece_table_node.prev)
        except:
            prev_node_id = None
        node_id = self.piece_table.get_id_from_node(piece_table_node)
        print("At i = 0", piece_table_node)

        # next_node_id = self.add_list.get_id_from_node(piece_table_node.next)

        if action == "i":
           return {
               "prev_id": prev_node_id,
               "node_id": node_id,
               "node_value": piece_table_node.value[1].value,
               "action": "i",
               "buffer": piece_table_node.value[0],
               "action_id": action_id
           } 
        elif action == "d":
           return {
               "prev_id": prev_node_id,
               "node_id": node_id,
               "action": "d",
#               "buffer": piece_table_node.value[1].value,
               "action_id": action_id
           } 
        
        else:
            return {}

    def insert(self, item, loc=None, ident=None, next_node_id=None):
        add_list_node = self.add_list.append(item)

        temp = (SpacetimeList.ADD_LST, add_list_node)

        if not loc and not next_node_id:
            the_node = self.piece_table.append(temp)
            h_obj = self.history_object("i", the_node, ident)
            self.document.history_list_sync += [h_obj]
            self.document.history_list += [h_obj]
        else:
            append = False
            if next_node_id:
                before_node = self.id_to_pt_item[next_node_id]
            else:
                try:
                    before_node = self.piece_table.nodeat(loc)
                except Exception:
                    append = True
            if append:
                the_node = self.piece_table.append(temp)
            else:
                the_node = self.piece_table.insert(temp, before=before_node)
            h_obj = self.history_object("i", the_node, ident)
            self.document.history_list_sync += [h_obj]
            self.document.history_list += [h_obj]

        return h_obj

    def delete(self, i=None, ident=None):
        if i is not None:
            the_node = self.piece_table.nodeat(i)
            print("*** the_node at 0", the_node)
            ident = self.piece_table.get_id_from_node(the_node)
        else:
            print("in delete by ident", ident)
            the_node = self.piece_table.get_node_from_id(ident)
            print("the node is", the_node)

        h_obj = self.history_object("d", the_node)

        self.document.history_list_sync += [h_obj]
        self.document.history_list += [h_obj]
        # self.history_list.append((the_node.prev, the_node.next, ))
        self.piece_table.remove(the_node)
        return h_obj


    def undo(self):
        ## TODO
        try:
            last_history_obj = self.document.history_list.pop()
        except ValueError:
            return False
        # print("***", last_history_obj)
        last_node_id = last_history_obj["node_id"]
        last_node = self.piece_table.get_node_from_id(last_node_id)

        prev_node_id = last_history_obj["prev_id"]
        prev_node = self.piece_table.get_node_from_id(prev_node_id)
        print('##', last_node_id)
        print('##', prev_node_id, prev_node)

        # print(last_node_id)
        last_node = self.piece_table.get_node_from_id(last_node_id)
        print("**", last_node)
        if last_history_obj["action"] == "d":
            if prev_node is None:
                self.piece_table.appendleft(last_node)
            else:
                self.piece_table.insert(last_node, after=prev_node)
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


        last_node_id = last_history_obj["node_id"]
        last_node = self.piece_table.get_node_from_id(last_node_id)
        prev_node_id = last_history_obj["prev_id"]
        prev_node = self.piece_table.get_node_from_id(prev_node_id)
        print('##', last_node_id)
        print('##', prev_node_id, prev_node)

        if last_history_obj["action"] == "i":
            self.piece_table.insert(last_node, after=prev_node)
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

        prev_id = None
        try:
            prev_id = self.document.history_list[-1]['action_id']
        except:
            pass

        #if document_last is not None:
        #    prev_id = diff_list[0]['action_id']
        #else:
        #    prev_id = None
        
        try:
            while self.document.history_list[-1]['action_id'] != prev_id:
                self.undo()
        except:
            pass
        
        if prev_id is None:
            common_parent_found = True
        else:
            common_parent_found = False
        
        print("difflist", diff_list)
        print("prev_id =", prev_id)
        print("hlist", self.document.history_list)
        print("ptable", [x for x in self.piece_table])
        print("addlist", [x for x in self.add_list])
        print("ptable2", self.get_sequence())
        print("CPF", common_parent_found)
        
        for item in diff_list:
            # add_list_node = self.add_list.append(item['node_value'])
            #temp = (SpacetimeList.ADD_LST, add_list_node)

            # the_node = self.piece_table.append(temp)
            print("processing item!!!!", "CPF", common_parent_found)
            if common_parent_found is True:
                print("right inside common parent")
                if item['action'] == 'i':
                    print("common parent found, insert")
                    add_list_node = self.add_list.append(item['node_value'])
                    temp = (SpacetimeList.ADD_LST, add_list_node)

                    print("Calling append")
                    the_node = self.piece_table.append(temp, node_id=item['node_id'])
                    print([x for x in self.piece_table])
                    h_obj = self.history_object("i", the_node, item['action_id'])
                    self.document.history_list.append(h_obj)
                elif item['action'] == 'd':
                    print("Deleting", item)
                    # self.delete(ident=item['node_id'])
                    the_node = self.piece_table.get_node_from_id(item['node_id'])
                    print("+the node is", the_node)
                    h_obj = self.history_object("d", the_node, action_id=item['action_id'])

                    self.document.history_list.append(h_obj)
                    # self.history_list.append((the_node.prev, the_node.next, ))
                    self.piece_table.remove(the_node)

            else:
                print("common parent not found yet")
                if item['action_id'] == prev_id:
                    print("common parent found")
                    common_parent_found = True
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
    # s = SpacetimeList([])
    print(s.get_sequence())
    s.insert(4)
    print(s.get_sequence())
    s.insert(5)
    print(s.get_sequence())
    pprint(s.history_list)

    s.insert(6, loc=3)
    print(s.get_sequence())

    s.undo()
    print(s.get_sequence())

    s.undo()
    print(s.get_sequence())

    s.undo()
    print(s.get_sequence())

    s.undo()
    print(s.get_sequence())

    s.undo()
    print(s.get_sequence())

    s.undo()
    print(s.get_sequence())

    s.redo()
    print(s.get_sequence())

    s.redo()
    print(s.get_sequence())

    s.redo()
    print(s.get_sequence())

    s.redo()
    print(s.get_sequence())

    s.redo()
    print(s.get_sequence())

    s.redo()
    print(s.get_sequence())

    s.redo()
    print(s.get_sequence())

    s.delete(i=2)
    print(s.get_sequence())

    s.undo()
    print(s.get_sequence())

    s.delete(i=2)
    s.delete(i=2)
    s.delete(i=2)
    s.delete(i=2)
    print(s.get_sequence())

    s.undo()
    s.undo()
    s.undo()
    s.undo()
    print(s.get_sequence())
#    print(s.get_sequence())
#    s.insert('x', 2)
#    print(s.get_sequence())
#    s.delete(i=3)
#    print(s.get_sequence())
#    s.undo()
#    print(s.get_sequence())
#    s.undo()
#    print(s.get_sequence())
#    s.undo()
#    print(s.get_sequence())
#    s.undo()
#    print(s.get_sequence())
#    s.undo()
#    print(s.get_sequence())
#    s.undo()
#    print(s.get_sequence())
#    s.undo()
#    print(s.get_sequence())


#     the_diff = s.__diff__()
# 
#     s2 = SpacetimeList([])
# 
#     print("# Importing the diff")
#     s2.import_diff(the_diff)
#     s2.insert('x')
#     s2.insert('y')
# 
#     diff_point = s.insert(6)
#     s.insert(7)
#     
#     # s.delete(i=3)
#     # s.delete(i=3)
#     # s.insert('x', 2)
#     
#     print()
#     print()
# 
#     print("+-+ Exported data")
#     pprint(the_diff)
#     print("+-+")
#     print()
#     print()
# 
# 
#     print()
#     print("SUMMARY")
#     print("Exported sequence:", s.get_sequence())
#     print("Imported sequence:", s2.get_sequence())
# 
#     difflist = s.__diff__(start_id=diff_point['ident'])
#     print(difflist)
#     print("before", s2.get_sequence())
#     s2.__merge__(difflist)
#     print(s2.get_sequence())