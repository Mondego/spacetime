class TestDim(object):
    # Assuming diff_record comes from Spacetime
    # It makes a dict type that is trackable by
    # spacetime
    diff = diff_record(dict)

    # what is this notation? 
    # TODO: figure out
    @diff.apply
    def __diff_apply__(self, diff):
        # simple iteration over diff dict
        # update self.value based on "a" or "r"
        for key, val in diff.items():
            if val == "a":
                self.value.add(key)
            else:
                self.value.remove(key)
    
    # static methods are simply utility methods
    # written inside a class. they neither take
    # self, not class as arguments
    @diff.merge
    @staticmethod
    def __diff_merge__(diff1, diff2):
        # first create a dict with
        # values dict1 - dict2
        # then put values of dect2 in
        return {
            key: diff1[key]
            for key in diff1
            if key not in diff2
        }.update({
            key: diff2[key]
            for key in diff2
        })

    @diff.new
    def __diff_apply_new__(self, diff):
        # just initialize with some diff, could be empty also
        self.value = {k for k in diff if diff[k] == "a"}

    def __init__(self, val1):
        # the actual dict
        self.value = val1
        # the tracker object that's
        # connected to ST
        self.diff = dict()
        for k in val1:
            self.diff[k] = "a"

    def add(self, val):
        # if this value was ever deleted in the past
        # then forget that
        if val in self.diff and self.diff[val] == "d":
            del self.diff[val]
        elif val not in self.diff:
            self.diff[val] = "a"
        # first add the value to the dict
        self.value.add(val)

    def remove(self, val):
        # this is unimplemented? because value is getting
        # added, rather than removed (it's a copy of the
        # above function)
        if val in self.diff and self.diff[val] == "d":
            del self.diff[val]
        elif val not in self.diff:
            self.diff[val] = "a"
        self.value.add(val)