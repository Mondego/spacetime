#!/usr/bin/env python

import sys
import tkinter as tk
from spacetime import Node
from rtypes import pcc_set
from rtypes import dimension, primarykey


df = None
@pcc_set
class EditorClient(object):
    oid = primarykey(str)
    text_value = dimension(str)

    def __init__(self):
        self.oid = "1"
        self.text_value = "just some stuff"

def update_object(df, T, root):
    my_editor = df.read_one(EditorClient, "1")
    my_editor.text_value = T.get("1.0",'end-1c')
    df.commit()
    df.push()
    print(my_editor.text_value)
    root.after(1000, update_object, df, T, root)

def editor(dataframe):
    root = tk.Tk()
    my_editor = EditorClient()
    T = tk.Text(root, height=10, width=30)
    T.pack()
    quote = "just some stuff"
    T.insert(tk.END, quote)
    
    
    my_editor.text_value = "another new thing"
    dataframe.add_one(EditorClient, my_editor)
    dataframe.commit()
    dataframe.push_await()
    root.after(1000, update_object, dataframe, T, root)

    # my_editor.text_value = "a start"
    # dataframe.commit()
    # dataframe.push()
    # dataframe.sync()

    # print("***", df.client_count)
    # df.add_one(EditorClient, my_editor)
    # df.sync()
    # my_editor.text_value = "a start"
    # df.commit()
    # df.push()
    # df.sync()
    tk.mainloop()


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8000
    Node(editor, dataframe=("0.0.0.0", port), Types=[EditorClient]).start()
