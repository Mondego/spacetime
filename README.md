# Spacetime Framework
===========
A framework for developing time-stepped, multi-worker applications based on the tuplespace model. Workers compute within spacetimed frames -- 
a fixed portion of the shared data during a fixed period of time. The locally modified data may be pushed back to the shared store
at the end of each step.

The first implementation of spacetime is in Python.