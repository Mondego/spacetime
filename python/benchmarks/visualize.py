import matplotlib.pyplot as plt
import os, json

def main():
    groups = dict()
    for jfile in os.listdir("benchmarks/results"):
        settp, testtp, app, dfmode = jfile.split(".")[:-1]
        groups.setdefault(".".join([settp, testtp, app]), list()).append(dfmode)
    for group in groups:
        fname = "benchmarks/graphs/results.{0}.png".format(group)
        fig = plt.figure()
        for mode in groups[group]:
            jfile = "benchmarks/results/" + group + "." + mode + ".json"
            data = json.load(open(jfile, "r"))
            x, y = zip(*enumerate(data["timings"]))
            plt.plot(x, y, label=mode)
        plt.legend()
        fig.savefig(fname, bbox_inches="tight")
        plt.close()

main()
