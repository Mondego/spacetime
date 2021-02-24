from pprint import pprint


class ParseManager:
    # sample https://github.com/Mondego/spacetime/blob/SpacetimeEdit/python/sample_apps/editor/datamodel.py
    LANG_FULLNAME = 'python'
    LANG_CODE = 'py'

    def reinit_vars(self):
        self.klassname = None
        self.primary = None
        self.declarations = []
        self.mergefunc_str = None

    def __init__(self):
        import_list = ["import { dimension, primarykey, Dimension } from 'rtypes/attributes'"]
        import_list.append("import { Datatype } from 'rtypes/utils/enums'")
        import_list.append("import { pccSet } from 'rtypes/types/pcc_set'")
        # import_list = ["from rtypes import primarykey, dimension, pcc_set"]
        self.reinit_vars()
        self.file_list = []
        self.file_list.extend(import_list)
        self.code = None
        self.type_map = {
            'str': "Datatype.STRING",
            'int': "Datatype.INTEGER",
            'bool': 'Datatype.BOOL'
        }


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
            self.mergefunc_str = "%s(): {}" % str(ast)
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
        content_list = ['@pcc_set', 'export class ' + self.klassname + ' {\n']
        # print("primary", self.primary.split())
        primary_list = self.primary.split()
        content_list.append("    _" + self.primary.split()[0] + ": Dimension;"); 
        # print("declarations", self.declarations, type(self.declarations))
        for d in self.declarations:
            # print("d = ", d)
            content_list.append("    _" + d.split()[0] + ": Dimension;");
        content_list.append("\n")
        content_list.append("    " + primary_list[0] + ": any;")
        for d in self.declarations:
            # print("d = ", d)
            content_list.append("    " + d.split()[0] + ": any;");
        content_list.append("\n")
        content_list.append("    " + "/∗###  Don't remove these ###∗/");
        content_list.append("    " + "static dtype: Map<string, any> = new Map();")
        content_list.append("    " + "dtype(){ return " + self.klassname + ".dtype; }");
        content_list.append("    " + "transformProps: any;")
        content_list.append("    " + "/* ### */")
        content_list.append("\n")


        content_list.append("    " + "constructor() {")
        temp = primary_list[2].replace("(", ")")
        temp = temp.split(")")
        content_list.append("        " + "this._" + primary_list[0] + " = primarykey(" + self.type_map[temp[1]] + ")")
        for d in self.declarations:
            # print("d = ", d)
            temp = d.split()[2].replace("(", ")")
            temp = temp.split(")")
            content_list.append("        " + "this._" + d.split()[0] + " = dimension(" + self.type_map[temp[1]] + ")");
        
        content_list.append("        " + "this.name = 'datamodel." + self.klassname + "'")
        content_list.append("        " + "/* ### don't remove the following ### */")
        content_list.append("        " + "this.transformProps()")
        content_list.append("    " + "}")
        content_list.append("\n")
        content_list.append("    " + self.mergefunc_str)
        content_list.append("}")
        content_list.append("\n")

        # print("mergefunc_str", self.mergefunc_str)
        # content_list += [self.primary, '\n'.join(self.declarations), self.mergefunc_str]
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

