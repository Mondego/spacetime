import matplotlib.pyplot as plt
import os, json

def main():
    colors = ["y", "m", "c", "g", "r", "b"]
    fig = plt.figure()
    filename = "benchmarks/results.png"
    for jfile in os.listdir("benchmarks/results"):
        data = json.load(open(os.path.join("benchmarks/results/", jfile), "r"))
        x, y = zip(*enumerate(data["timings"]))
        plt.plot(x, y, label=jfile)
    plt.legend()
    fig.savefig(filename, bbox_inches="tight")
    plt.close()

main()
