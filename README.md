# Spacetime Framework
===========
A framework for developing time-stepped, multi-worker applications based on the tuplespace model. Workers compute within spacetimed frames -- a fixed portion of the shared data during a fixed period of time. The locally modified data may be pushed back to the shared store at the end of each step. Pulling and pushing data from/to the store is done declaratively using two small DSLs: (1) a DSL for
predicate collection classes ([PCC](https://github.com/Mondego/pcc)) used for specifying algebraic operations on data sets,
and (2) a DSL for controlling data flow in terms of direction (to/from store) and, eventually, permissions.

The first implementation of spacetime is in Python. Follow the link to it for specific instructions on how to
use the Python implementation.
