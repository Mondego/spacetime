import matplotlib.pyplot as plt
import os, json

def main():
    groups = dict()
    for jfile in os.listdir("benchmarks/results"):
        if not jfile.endswith(".tsv"):
            continue
        settp, testtp, app, dfmode = jfile.split(".")[:-1]
        groups.setdefault(".".join([settp, testtp, app]), list()).append(dfmode)
    for group in groups:
        for mode in groups[group]:
            jfile = "benchmarks/results/" + group + "." + mode + ".tsv"
            fname = "benchmarks/graphs/" + group + "." + mode + ".png"
            data = dict()
            for name, ts, delta in (
                    line.strip().split()
                    for line in open(jfile, "r")):
                data.setdefault(name, list()).append((float(ts), float(delta)))
            count = 0
            mints = min(ts for name in data for ts, delta in data[name])
            fig, axes = plt.subplots(1, len(data), sharex=True)
            for name, values in data.items():
                nvalues = [((ts - mints), delta) for ts, delta in values]
                values.sort(key=lambda x: x[0])
                x, y = zip(*nvalues)
                print (len(axes), count, len(data))
                axes[count].plot(x, y, label=name)
                axes[count].legend()
                axes[count].set_ylim((0, 500))

                count += 1
            plt.legend()
            fig.set_size_inches(25, 10)
            fig.savefig(fname, bbox_inches="tight", dpi=100)
            plt.close()

main()
