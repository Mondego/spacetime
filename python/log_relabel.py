import re

vidmap = dict()
name = (i for i in range(10000000))
def replacement(match):
    vid = match.group()
    if vid not in vidmap:
        vidmap[vid] = str(next(name))
    return vidmap[vid]

open("Logs/spacetime.mod.log", "w").write(
    re.sub(r"([a-z0-9]{8}\-[a-z0-9]{4}\-[a-z0-9]{4}\-[a-z0-9]{4}\-[a-z0-9]{12})",
    replacement, open("Logs/spacetime.log").read()))
