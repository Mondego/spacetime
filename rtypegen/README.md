# Spacetime::rtypegen

`rtypegen` converts PCC set specification files into python, javascript (TODO) and Java (TODO)
classes


## Quick example

`sampleinput.pcc` contains an example file containing two pcc set definitions.

Following command generates the required python classes and writes them into `output.py`:

`$ python3 rtypegen.py -i sampleinput.pcc -t py -o output`

## `rtypegen.py` Usage

```buildoutcfg
usage: rtypegen.py [-h] -i INPUT_FILE [-t {py,js,java} [{py,js,java} ...]]
                   [-o OUTPUT_FILE]

Convert PCC set specification into python, javascript (TODO) and Java (TODO)
classes

optional arguments:
  -h, --help            show this help message and exit
  -i INPUT_FILE, --input INPUT_FILE
                        Specify path to the input file containing one or more
                        PCC specifications
  -t {py,js,java} [{py,js,java} ...], --target {py,js,java} [{py,js,java} ...]
                        Specify the output language after code generation from
                        the PCC s pecifications
  -o OUTPUT_FILE, --output OUTPUT_FILE
                        Specify the base name for the output file(s)
                        (extensions will de pend on languages specified
                        through -t). Default value will be the base of input
                        file. Warning: overwrites existing files.
```

## Development 

### Changing the pcc set specification language

1. Make changes to `RTYPE_GRAMMAR`
2. Run `python3 parsergen.py`
3. Use `rtypegen.py` as explained in the Example Usage section

### Adding a target language

TODO: Add Javascript, Java




## PCC file format

See `RTYPE_GRAMMAR` for latest definition. Following gives a quick (possibly stale) overview:

```
@@grammar::rtype

start = file:file $ ;

file = { pccset:classdef }+ ;

classdef = classname:classname ':' classbody:classbody ;
classname = 'class' @:identifier ;

classbody = [primarydef:primarydef] declarations:normaldefs mergefunc:mergefunc;

primarydef = 'primary' @:statement ;

normaldefs = { statement }*;
statement = type:typedef name:identifier ;

mergefunc = ['merge' 'func' @:identifier ];

typedef = | 'int' | 'bool' | 'float' ;

identifier = /[_a-zA-Z][_a-zA-Z0-9]*/ ;
```

