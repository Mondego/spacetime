from pprint import pprint


class ParseManager:
    # sample https://github.com/Mondego/spacetime/blob/SpacetimeEdit/python/sample_apps/editor/datamodel.py
    LANG_FULLNAME = 'python'
    LANG_CODE = 'py'

    def reinit_vars(self):
        self.klassname = None
        self.primary = None
        self.declarations = []
        self.mergefunc = None

    def __init__(self):
        import_list = ["from rtypes import primarykey, dimension, pcc_set"]
        self.reinit_vars()
        self.file_list = []
        self.file_list.extend(import_list)
        self.code = None


    def identifier(self, ast):
        # print("identifier")
        # pprint(ast)
        return ast

    def typedef(self, ast):
        # print("typedef")
        # pprint(ast)
        return ast

    def mergefunc(self, ast):
        # print("++++MF", ast)
        # print("mergefunc")
        # pprint(ast)
        if ast != 'null':
            self.mergefunc = "def %s(self): pass" % str(ast)
            return 'function ' + str(ast) + '() {}'

    def statement(self, ast):
        # print("statement")
        # pprint(ast)
        return ast
        # return str(ast.type) + str(ast.name)

    def normaldefs(self, ast):
        # print("++++ND", ast)
        self.declarations.append("    %s = dimension(%s)" % (ast[0].name, ast[0].type))
        # print("normaldefs")
        # pprint(ast)
        # return str(ast)
        return ast

    def primarydef(self, ast):
        # print("++++PD", ast)
        # print("primarydefs")
        self.primary = "    %s = primarykey(%s)" % (ast.name, ast.type)
        # pprint(ast)
        return ast
        # return str(ast)

    def classbody(self, ast):
        # print("classbody")
        # pprint(ast)
        return ast
        # return str(ast)

    def classname(self, ast):
        # print("+++CD.classname", ast)
        self.klassname = str(ast)
        # pprint(ast)
        return ast
        # return str(ast)

    def classdef(self, ast):
        # print("CLASSDEF")
        content_list = ['@pcc_set', 'class ' + self.klassname + ':', self.primary, '\n'.join(self.declarations), self.mergefunc]
        # print("---", content_list)
        self.file_list.append('\n'.join(list(filter(None, content_list))))
        self.reinit_vars()
        return ast


    def file(self, ast):
        # pprint("FILE")
        # pprint(ast)
        # print(self.file_list)
        # print("#######################")
        self.code = '\n\n'.join(self.file_list)
        print(self.code)
        # print("#######################")

    def _default(self, ast, third_arg=None):
        # print("_default", third_arg)
        # pprint(ast)
        return ast
        # return ast

