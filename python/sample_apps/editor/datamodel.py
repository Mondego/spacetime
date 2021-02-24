import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))

from rtypes import primarykey, dimension, pcc_set

@pcc_set
class Document:
    document_id = primarykey(str)
    history_list_sync = dimension(list)
    history_list = list()

    def __init__(self):
        self.document_id = "SINGLETON"
        self.history_list_sync = list()
        self.history_list = list()
