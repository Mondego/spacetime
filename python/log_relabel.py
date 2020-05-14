import re

vidmap = dict()
name = (i for i in range(10000000))
def replacement(match):
    vid = match.group()
    if vid not in vidmap:
        vidmap[vid] = str(next(name))
    return vidmap[vid]

uuidmap = dict()
uuid_repl = (i for i in range(10000000))
def replace_uuid(match):
    uuid = match.group()
    if uuid not in uuidmap:
        uuidmap[uuid] = str(next(uuid_repl))
    return uuidmap[uuid]


open("Logs/spacetime.mod.log", "w").write(
    re.sub(r"([a-z0-9]{8}\-[a-z0-9]{4}\-[a-z0-9]{4}\-[a-z0-9]{4}\-[a-z0-9]{12})",
        replace_uuid,
        re.sub(r"([0-9]{20}[0-9]+)", replacement, open("Logs/spacetime.log").read())))
