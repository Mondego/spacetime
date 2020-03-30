import unittest
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))
from utillib.spacetimelist import SpacetimeList


class SpacetimelistTest(unittest.TestCase):
    def test_merge(self):

        # hlist = [{'prev_id': None, 'node_id': "b'0491b334d7'", 'node_value': 'h', 'action': 'i', 'buffer': 1, 'action_id': "b'a3c8c2c1dd'"}, {'prev_id': "b'0491b334d7'", 'node_id': "b'9a7035107e'", 'node_value': 'e', 'action': 'i', 'buffer': 1, 'action_id': "b'1795fe6874'"}, {'prev_id': "b'9a7035107e'", 'node_id': "b'6d8fdb9697'", 'node_value': 'l', 'action': 'i', 'buffer': 1, 'action_id': "b'5e1dd11c5e'"}, {'prev_id': "b'6d8fdb9697'", 'node_id': "b'abed6f70d7'", 'node_value': 'l', 'action': 'i', 'buffer': 1, 'action_id': "b'c73de56f2b'"}, {'prev_id': "b'abed6f70d7'", 'node_id': "b'c6274e2123'", 'node_value': 'o', 'action': 'i', 'buffer': 1, 'action_id': "b'ef42904737'"}]
        difflist = [{'prev_id': None, 'node_id': "b'801a227566'", 'node_value': 'h', 'action': 'i', 'buffer': 1, 'action_id': "b'c3be22fb8e'"}]
        s = SpacetimeList([])
        s.__merge__(difflist)
        s.__merge__(difflist)
        s.__merge__(difflist)
        op = s.get_sequence()

        self.assertEqual(''.join(op),"h")