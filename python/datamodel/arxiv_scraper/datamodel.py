'''
Created on Oct 29, 2016

@author: Rohan Achar
'''
from __future__ import absolute_import
import logging
from pcc.subset import subset
from pcc.parameter import parameter, ParameterMode
from pcc.set import pcc_set
from pcc.projection import projection
from pcc.attributes import dimension, primarykey
from pcc.impure import impure
import re, uuid

@pcc_set
class TexFiles(object):
    @primarykey(str)
    def ID(self): return self._id

    @ID.setter
    def ID(self, value): self._id = value

    @dimension(str)
    def path(self): return self._path

    @path.setter
    def path(self, value): self._path = value

    @dimension(dict)
    def contentmap(self): return self._c

    @contentmap.setter
    def contentmap(self, value): self._c = value

    @dimension(bool)
    def isprocessed(self): return self._isp

    @isprocessed.setter
    def isprocessed(self, value): self._isp = value

    @dimension(str)
    def intro(self): return self._intro

    @intro.setter
    def intro(self, value): self._intro = value

    @dimension(str)
    def conclusion(self): return self._conc

    @conclusion.setter
    def conclusion(self, value): self._conc = value

    @dimension(int)
    def conclusion_type(self): return self._conc_type

    @conclusion_type.setter
    def conclusion_type(self, value): self._conc_type = value

    @dimension(str)
    def abstract(self): return self._abs

    @abstract.setter
    def abstract(self, value): self._abs = value

    @dimension(bool)
    def fullyprocessed(self): return self._fp

    @fullyprocessed.setter
    def fullyprocessed(self, value): self._fp = value

    def __init__(self, path, contentmap):
        self.ID = str(uuid.uuid4())
        self.path = path
        self.contentmap = contentmap
        self.isprocessed = False
        self.fullyprocessed = False
        self.intro = ""
        self.conclusion = ""
        self.abstract = ""
        self.conclusion_type = -1

@subset(TexFiles)
class UnProcessedTexFiles(object):
    @staticmethod
    def __predicate__(tf):
        return tf.isprocessed == False and tf.fullyprocessed == False

@subset(TexFiles)
class FullyProcessedTexFiles(object):
    @property
    def dict_form(self):
        return {
            "path": self.path,
            "intro": self.intro,
            "abstract": self.abstract,
            "conclusion": self.conclusion,
            "conclusion_type": self.conclusion_type
        }
    @staticmethod
    def __predicate__(tf):
        return tf.fullyprocessed == False

@impure
@subset(UnProcessedTexFiles)
class OneUnProcessedTexFile(object):
    intro_re = re.compile(
        r".*\\section\{Introduction\}(.*?)\\section", 
        re.DOTALL | re.IGNORECASE)
    conclusion_re1 = re.compile(
        r".*\\section\{Conclusion\}(.*?)\\section", 
        re.DOTALL | re.IGNORECASE)
    conclusion_re2 = re.compile(
        r".*\\section\{Conclusion\}(.*?)Acknowledgements", 
        re.DOTALL | re.IGNORECASE)
    conclusion_re3 = re.compile(
        r".*\\section\{Conclusion\}(.*?)biblio", 
        re.DOTALL | re.IGNORECASE)
    conclusion_re4 = re.compile(
        r".*\\section\{Conclusion\}(.*?)", 
        re.DOTALL | re.IGNORECASE)
    abstract_re = re.compile(
        r".*\\begin\{Abstract\}(.*?)\\end{Abstract}", 
        re.DOTALL | re.IGNORECASE)
    title_re = re.compile(
        r".*\\title.*?\{(.*?)\}", 
        re.DOTALL | re.IGNORECASE)

    @property
    def combined_content(self):
        try:
            return self._cc
        except AttributeError:
            
            main_c = ""
            key = None
            for k, content in self.contentmap.items():
                if "\\begin{document}" in content:
                    main_c = content
                    key = k
                    break
            if main_c == "":
                self._cc = ""
                return self._cc

            self._cc = self.replace_inputs(main_c, key, dict(), set())
            return self._cc

    @staticmethod
    def __query__(uptfs):
        for uptf in uptfs:
            if OneUnProcessedTexFile.__predicate__(uptf):
                uptf.isprocessed = True
                return [uptf]
        return []

    @staticmethod
    def __predicate__(uptf):
        return True

    def replace_inputs(self, root, key, already_done, being_done):
        if key in already_done:
            return already_done[key]
        if key in being_done:
            # cyclic reference
            return root

        parts = root.split("\\input{")
        if len(parts) == 1:
            already_done[key] = root
            return root

        remaining = [parts[0]]
        for part in parts[1:]:
            startindex = part.find("}")
            input_name = part[:startindex]
            if input_name in self.contentmap:
                being_done.add(key)
                part_result = self.replace_inputs(self.contentmap[input_name], input_name, already_done, being_done)
                final = part_result + (part[startindex + 1:] if len(part) > startindex + 1 else "")
                remaining.append(final)
            else:
                remaining.append("\\input{" + part)
        full_root = "\n".join(remaining)
        already_done[key] = full_root
        return already_done[key]

    def scrape(self):
        intro_m = self.intro_re.match(self.combined_content)
        if intro_m:
            self.intro = intro_m.groups()[0]

        abstract_m = self.abstract_re.match(self.combined_content)
        if abstract_m:
            self.abstract = abstract_m.groups()[0]

        conc_m = self.conclusion_re1.match(self.combined_content)
        conc_tp = 0
        if not conc_m:
            conc_m = self.conclusion_re2.match(self.combined_content)
            conc_tp = 1
        if not conc_m:
            conc_m = self.conclusion_re3.match(self.combined_content)
            conc_tp = 2
            if conc_m and len(conc_m.groups()[0].split()) > 500:
                conc_m = None
        if conc_m:
            self.conclusion = conc_m.groups()[0]
            self.conclusion_type = conc_tp
        
        self.fullyprocessed = True

