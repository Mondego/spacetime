from spacetime.node import Node
try:
    from spacetime.dataframe_cpp import DataframeCPP as Dataframe
except ImportError:
    from spacetime.dataframe_pure import DataframePure as Dataframe