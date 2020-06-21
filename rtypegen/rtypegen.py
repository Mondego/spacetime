from rtypeparser import rtypeParser
import semantics_python
from grako.util import asjson
from pprint import pprint
import json
import argparse
import os

def setup_argparser():
    parser = argparse.ArgumentParser(
        description="""Convert PCC set specification into python, javascript (TO\
                       DO) and Java (TODO) classes"""
    )
    parser.add_argument(
        "-i",
        "--input",
        required=True,
        dest="input_file",
        help="""Specify path to the input file containing one or more PCC specifications"""
    )
    parser.add_argument(
        "-t",
        "--target",
        default='py',
        choices=['py', 'js', 'java'],
        nargs="+",
        dest="target_lang",
        help="""Specify the output language after code generation from the PCC s\
                pecifications"""
    )
    parser.add_argument(
        "-o",
        "--output",
        dest="output_file",
        help="""Specify the base name for the output file(s) (extensions will de\
                pend on languages specified through -t). Default value will be the \
                base of input file. Warning: overwrites existing files."""
    )
    return parser

def generate_code(ip_file, tgt_langs=['py'], op_file_base=None):
    # TODO: Deal with multiple language input/output
    parser = rtypeParser(parseinfo=False)
    parse_manager_python = semantics_python.ParseManager()
    with open(ip_file, "r") as ip_handler:
        ast = parser.parse(ip_handler.read(), rule_name='start', semantics=parse_manager_python)

    if not op_file_base:
        op_file_base = os.path.splitext(ip_file)[0]

    with open(op_file_base + '.py', "w") as f:
        f.write(parse_manager_python.code)

    # print('==========')
    # print(ast)
    # print(json.dumps(ast, indent=2))

    # print('==========')
    # print("***")
    # print(json.dumps(asjson(ast), indent=2))


    # ...

if __name__ == "__main__":

    parser = setup_argparser()
    args = parser.parse_args()

    generate_code(args.input_file, args.target_lang, args.output_file)

