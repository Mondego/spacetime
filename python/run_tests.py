import os
try:
    filename = next(
        p.startswith("repository.cpython") for p in os.listdir("spacetime"))
    os.system(f"rm -rf spacetime/{filename}")
except StopIteration:
    pass
if not os.path.exists("../build/") and not os.path.exists("../build/MakeFile"):
    os.system("cmake -S ../core/ -B ../build/")
os.system("cmake --build ../build")
os.system("touch spacetime/py_repository.cpp")
os.system("python3 setup.py build_ext --inplace")
from tests.test_cpp import *
# from tests.test_pure_py import *
# from tests.test_linked_list import *
# from tests.test_version_manager import *


if __name__ == "__main__":
    unittest.main()