from datamodel import Counter
from spacetime.managers.managed_heap import ManagedHeap

local_heap = ManagedHeap([Counter])
c = local_heap.read_one(Counter, "countDocument")
print(c)
