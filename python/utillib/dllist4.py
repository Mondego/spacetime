# -*- coding: utf-8 -*-

# from compat import 
from utils import TwoWayDict, generate_id

def cmp(a, b):
    return (a > b) - (a < b) 

class dllistnode(object):
    __slots__ = ('__prev', '__next', 'value', '__list', 'ident')

    def __init__(self, value=None, prev=None, next=None, the_list=None, ident=None):
        if isinstance(value, dllistnode):
            ident = the_list.get_id_from_node(value)
            value = value.value

        # self.__prev = the_list._dllist__id_node_2way_map[prev]
        # self.__next = the_list._dllist__id_node_2way_map[next]
        self.__prev = prev
        self.__next = next
        self.value = value
        self.__list = the_list
        if not ident:
            self.ident = generate_id()
        else:
            self.ident = ident

        if prev is not None:
            prev.__next = self
            # prev_node = the_list._dllist__id_node_2way_map[prev]
            # prev_node._dllistnode__next = self.__ident
        if next is not None:
            # the_list._dllist__id_node_2way_map[next]._dllistnode__prev = self.__ident
            next.__prev = self

    @property
    def prev(self):
        return self.__prev

    @property
    def next(self):
        return self.__next

    @property
    def list(self):
        return self.__list

    def _iter(self, direction, to=None):
        if to is not None:
            if not isinstance(to, dllistnode):
                raise TypeError('to argument must be a dllistnode')
            if to.list is not self.__list:
                raise ValueError('to argument belongs to another list')

        current = self
        while current is not None and current != to:
            yield current
            current = direction(current)

    def iternext(self, to=None):
        return self._iter(lambda x: x.__next, to=to)

    def iterprev(self, to=None):
        return self._iter(lambda x: x.__prev, to=to)

    def __call__(self):
        return self.value

    def __str__(self):
        return 'dllistnode(' + str(self.value) + ')'

    def __repr__(self):
        return '<dllistnode(' + repr(self.value) + ')>'


class dllist(object):
    __slots__ = ('__first', '__last', '__size',
                 '__last_access_node', '__last_access_idx', '__id_node_2way_map',
                 'history', 'redo_list')

    def __init__(self, sequence=None):
        self.__first = None
        self.__last = None
        self.__size = 0
        self.__last_access_node = None
        self.__last_access_idx = -1
        self.__id_node_2way_map = TwoWayDict()
        self.__id_node_2way_map[None] = None
        self.history = []
        self.redo_list = []

        if sequence is None:
            return

        for value in sequence:
            node = dllistnode(value, self.__last, None, self)
            self.update_id_node_2way_map("i", node)

            if self.__first is None:
                self.__first = node
            self.__last = node
            self.__size += 1

    def update_id_node_2way_map(self, action, node, update_history=True):
        # TODO: delete from _node_2way, if action = d
        self.__id_node_2way_map[node.ident] = node
        # last_hnode = None
        # try:
            # last_hnode = self.history[-1]
        # except:
            # pass
        hnode = self.history_object(action, node)
        # if last_hnode:
            # last_hnode["next"] = hnode["action_id"]


        if update_history:
            self.history.append(hnode)
    
    def get_id_from_node(self, node):
        try: 
            return self._dllist__id_node_2way_map[node]
        except:
            return -1

    def get_node_from_id(self, ident):
        try:
            return self._dllist__id_node_2way_map[ident]
        except Exception as e:
            return e

    @property
    def first(self):
        return self.__first

    @property
    def last(self):
        return self.__last

    @property
    def size(self):
        return self.__size

    def print_list(self):
        print(', '.join([str(x) for x in self]))

    def history_object(self, action, node, action_id=None):
        if not action_id:
            action_id = generate_id()
        # self.id_to_pt_item[ident] = node 
         
        if action == "i":
           return {
               "prev": self.__id_node_2way_map[node.prev],
               # "next": self.__id_node_2way_map[node.next],
               "value": node.value,
               "action_id": action_id,
               "action": "i",
               "ident": node.ident
           } 
        elif action == "d":
           return {
               "prev": self.__id_node_2way_map[node.prev],
               # "next": self.__id_node_2way_map[node.next],
               "value": node.value,
               "action_id": action_id,
               "action": "d",
               "ident": node.ident
           } 
        
        else:
            return {}

    def undo(self):
        ## TODO
        try:
            last_history_obj = self.history.pop()
        except ValueError:
            return False

        # print("***", last_history_obj)
        last_node_ident_prev = last_history_obj["prev"]
        # TODO: Handle head/tail nodes
        print('###', last_history_obj)
        last_node_prev = self.__id_node_2way_map[last_node_ident_prev]

        last_node_ident = last_history_obj["ident"]

        pprint(last_history_obj)
        if last_history_obj["action"] == "d":
            last_node = dllistnode(value=last_history_obj["value"], the_list=self, ident=last_node_ident)
            print("****", last_node.ident, last_node.list)
            if last_node_prev:
                self.insert(last_node, after=last_node_prev, update_history=False)
            else:
                self.appendleft(last_node, update_history=False)

        elif last_history_obj["action"] == "i":
            # self.piece_table.remove(last_node)
            # last_node = self.__id_node_2way_map[last_node_ident]
            last_node = dllistnode(value=last_history_obj["value"], the_list=self, ident=last_node_ident)
            pprint(last_node)
            pprint(last_node.list)
            self.remove(last_node, update_history=False)
        else:
            print("Not implemented")
        
        self.redo_list.append(last_history_obj)
        return True



    def nodeat(self, index):
        if not isinstance(index, int):
            raise TypeError('invalid index type')

        if index < 0:
            index = self.__size + index

        if index < 0 or index >= self.__size:
            raise IndexError('index out of range')

        middle = index / 2
        if index <= middle:
            node = self.__first
            start_idx = 0
            reverse_dir = False
        else:
            node = self.__last
            start_idx = self.__size - 1
            reverse_dir = True

        if self.__last_access_node is not None and \
                self.__last_access_idx >= 0 and \
                abs(index - self.__last_access_idx) < middle:
            node = self.__last_access_node
            start_idx = self.__last_access_idx
            if index < start_idx:
                reverse_dir = True
            else:
                reverse_dir = False

        if not reverse_dir:
            while start_idx < index:
                node = node.next
                start_idx += 1
        else:
            while start_idx > index:
                node = node.prev
                start_idx -= 1

        self.__last_access_node = node
        self.__last_access_idx = index

        return node

    def appendleft(self, x, update_history=True):
        node = dllistnode(x, None, self.__first, self)
        self.update_id_node_2way_map("i", node, update_history)

        if self.__last is None:
            self.__last = node
        self.__first = node
        self.__size += 1

        if self.__last_access_idx >= 0:
            self.__last_access_idx += 1
        return node

    def appendright(self, item_x, update_history=True):
        node = dllistnode(item_x, prev=self.__last, next=None, the_list=self)
        self.update_id_node_2way_map("i", node, update_history)

        if self.__first is None:
            self.__first = node
        self.__last = node
        self.__size += 1

        return node

    def append(self, item_x):
        return self.appendright(item_x)

    def insert(self, item_x, before=None, after=None, update_history=True):
        if after is not None:
            if before is not None:
                raise ValueError('Only before or after argument can be defined')
            before = after.next

        if before is None:
            return self.appendright(item_x, update_history)

        if not isinstance(before, dllistnode):
            raise TypeError('before/after argument must be a dllistnode')

        if before.list is not self:
            raise ValueError('before/after argument belongs to another list')

        node = dllistnode(item_x, before.prev, before, self)
        self.update_id_node_2way_map("i", node, update_history)

        if before is self.__first:
            self.__first = node
        self.__size += 1

        self.__last_access_node = None
        self.__last_access_idx = -1

        return node

    def popleft(self, update_history=True):
        if self.__first is None:
            raise ValueError('list is empty')

        node = self.__first
        self.update_id_node_2way_map("d", node, update_history)

        self.__first = node.next
        if self.__last is node:
            self.__last = None
        self.__size -= 1

        if node.prev is not None:
            node.prev._dllistnode__next = node.next
        if node.next is not None:
            node.next._dllistnode__prev = node.prev

        node._dllistnode__next = None
        node._dllistnode__list = None

        if self.__last_access_node is not node:
            if self.__last_access_idx >= 0:
                self.__last_access_idx -= 1
        else:
            self.__last_access_node = None
            self.__last_access_idx = -1

        return node.value

    def popright(self, update_history=True):
        if self.__last is None:
            raise ValueError('list is empty')

        node = self.__last
        self.update_id_node_2way_map("d", node, update_history)

        self.__last = node.prev
        if self.__first is node:
            self.__first = None
        self.__size -= 1

        if node.prev is not None:
            node.prev._dllistnode__next = node.next
        if node.next is not None:
            node.next._dllistnode__prev = node.prev

        node._dllistnode__prev = None
        node._dllistnode__list = None

        if self.__last_access_node is node:
            self.__last_access_node = None
            self.__last_access_idx = -1

        return node.value

    def pop(self):
        return self.popright()

    def remove(self, node, update_history=True):
        if not isinstance(node, dllistnode):
            raise TypeError('node argument must be a dllistnode')

        if self.__first is None:
            raise ValueError('list is empty')

        if node.list is not self:
            raise ValueError('node argument belongs to another list')

        self.update_id_node_2way_map("d", node, update_history)

        if self.__first is node:
            self.__first = node.next
        if self.__last is node:
            self.__last = node.prev
        self.__size -= 1

        if node.prev is not None:
            node.prev._dllistnode__next = node.next
        if node.next is not None:
            node.next._dllistnode__prev = node.prev

        node._dllistnode__prev = None
        node._dllistnode__next = None
        node._dllistnode__list = None

        self.__last_access_node = None
        self.__last_access_idx = -1


        return node.value

    def iternodes(self, to=None):
        if self.__first is not None:
            return self.__first.iternext(to=to)
        else:
            return iter([])

    def __len__(self):
        return self.__size

    def __cmp__(self, other):
        for sval, oval in zip(self, other):
            result = cmp(sval, oval)
            if result != 0:
                return result

        result = len(self) - len(other)
        if result < 0:
            return -1
        elif result > 0:
            return 1
        return 0

    def __eq__(self, other):
        for sval, oval in zip(self, other):
            if sval == oval:
                return True
        return len(self) == len(other)

    def __ne__(self, other):
        for sval, oval in zip(self, other):
            if sval != oval:
                return True
        return len(self) != len(other)

    def __lt__(self, other):
        for sval, oval in zip(self, other):
            if sval < oval:
                return True
        return len(self) < len(other)

    def __le__(self, other):
        for sval, oval in zip(self, other):
            if sval <= oval:
                return True
        return len(self) <= len(other)

    def __gt__(self, other):
        for sval, oval in zip(self, other):
            if sval > oval:
                return True
        return len(self) > len(other)

    def __ge__(self, other):
        for sval, oval in zip(self, other):
            if sval >= oval:
                return True
        return len(self) >= len(other)

    def __str__(self):
        if self.__first is not None:
            return 'dllist([' + ', '.join((str(x) for x in self)) + '])'
        else:
            return 'dllist()'

    def __repr__(self):
        if self.__first is not None:
            return 'dllist([' + ', '.join((repr(x) for x in self)) + '])'
        else:
            return 'dllist()'

    def __iter__(self):
        current = self.__first
        while current is not None:
            yield current.value
            current = current.next

    def __reversed__(self):
        current = self.__last
        while current is not None:
            yield current.value
            current = current.prev

    def __getitem__(self, index):
        return self.nodeat(index).value

    def __setitem__(self, index, value):
        self.nodeat(index).value = value

    def __delitem__(self, index):
        node = self.nodeat(index)
        self.remove(node)

        if node.prev is not None and index > 0:
            self.__last_access_node = node.prev
            self.__last_access_idx = index - 1

    def __add__(self, sequence):
        new_list = dllist(self)

        for value in sequence:
            new_list.append(value)

        return new_list

    def __iadd__(self, sequence):
        if sequence is not self:
            for value in sequence:
                self.append(value)
        else:
            # slower path which avoids endless loop
            # when extending list with itself
            node = sequence.__first
            last_node = self.__last
            while node is not None:
                self.append(node.value)
                if node is last_node:
                    break
                node = node.next

        return self

    def __mul__(self, count):
        if not isinstance(count, int):
            raise TypeError('count must be an integer')

        new_list = dllist()
        for i in range(count):
            new_list += self

        return new_list

    def __imul__(self, count):
        if not isinstance(count, int):
            raise TypeError('count must be an integer')

        last_node = self.__last
        for i in range(count - 1):
            node = self.__first
            while node is not None:
                self.appendright(node.value)
                if node is last_node:
                    break
                node = node.next

        return self

    def __hash__(self):
        h = 0

        for value in self:
            h ^= hash(value)

        return h

if __name__ == "__main__":
    from pprint import pprint
    x = dllist([1, 2, 3])
    x.print_list()

    x.append(10)
    x.print_list()

    x.appendleft(12)
    x.print_list()

    x.popright()
    x.print_list()

    x.remove(x.nodeat(3))
    x.print_list()

    pprint(x.history)

    x.undo()
    x.print_list()

    x.undo()
    x.print_list()

    x.undo()
    x.print_list()

    x.undo()
    x.print_list()


    x.undo()
    x.print_list()
    # pprint(x.history)
