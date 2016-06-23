#python
import platform
import sys
import subprocess
sys.path.append(r'C:\Users\Arthur Valadares\Dropbox\Projects\UrbanSim\Resources\sumo-0.23.0\tools')
#from tools import traci
import traci
from sumolib import checkBinary

if __name__ == "__main__":
    if platform.system() == 'Windows' or platform.system().startswith("CYGWIN"):
	sumoBinary = checkBinary('sumo.exe')
    else:
	sumoBinary = checkBinary('sumo')

    sumoCommandLine = [sumoBinary, "-c", "networks/fullnet/fullnet.sumocfg", "-l", "sumo.log"]
    SumoProcess = subprocess.Popen(sumoCommandLine, stdout=sys.stdout, stderr=sys.stderr)
    traci.init(8813)

    SimulationBoundary = traci.simulation.getNetBoundary()
    print "Simulation boundary is ", SimulationBoundary
