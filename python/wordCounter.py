#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jan 26 10:43:27 2019

@author: prithadawn
"""

from rtypes import pcc_set, primarykey, dimension, merge
from spacetime import Application
import random, re, argparse
import time


@pcc_set
class word_class(object):

    word_desc = primarykey(str)
    word_count = dimension(int)

    def __init__(self, word_desc, word_count):

        self.word_desc = word_desc
        self.word_count = word_count

    def __str__(self):
        return self.word_desc + ' count: ' + str(self.word_count)

    @merge
    def merge_func(original, mine, theirs):

        if original is None:
            theirs.word_count = mine.word_count + theirs.word_count
        else:
            theirs.word_count = mine.word_count + theirs.word_count - original.word_count
        return theirs


def divide_chunks(l, n):
    # looping till length of l
    for i in range(0, len(l), n):
        yield l[i:i + n]


def get_word_desc(word_obj):
    return word_obj.word_desc


def mapper(df, word_list):
    current_word = 0
    current_count = 0
    print(word_list)
    for w in word_list:
        word = df.read_one(word_class, w)
        if word is None:
            df.add_one(word_class, word_class(w, 1))
        else:
            word.word_count += 1
    df.commit()
    df.push()


def reducer(df):
    n = 4
    mapper_app = []
    word_lists = []
    with open('testFile.txt') as f:
        word_list = re.sub(r'[^\w\s]', '', f.read()).split()
    # x = list(divide_chunks(word_list, n))
    word_list1 = word_list[:len(word_list) // 2]
    word_list2 = word_list[len(word_list) // 2:]

    '''mapper_app1 = Application(mapper, Types=[word_class], dataframe=df)
    mapper_app2 = Application(mapper, Types=[word_class], dataframe=df)
    mapper_app1.start_async(word_list1)
    mapper_app2.start_async(word_list2)
    mapper_app1.join()
    mapper_app2.join()'''
    word_lists = list(divide_chunks(word_list, 10))
    print(len(word_lists))
    for i in range(len(word_lists)) :
        mapper_app.append(Application(mapper, Types=[word_class], dataframe=df))
    for i  in range(len(word_lists)):
         mapper_app[i].start_async(word_lists[i])
    for i in range(n):
        mapper_app[i].join()

    df.checkout()
    for word_obj in df.read_all(word_class):
        print(word_obj)


def main():
    # parser = argparse.ArgumentParser()
    # parser.add_argument("filename")
    # args = parser.parse_args()
    app = Application(reducer, Types=[word_class], debug=True)
    app.start()



main()
