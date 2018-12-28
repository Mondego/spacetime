import gym
import time
import sys
from sample_apps.gym_mp_lunarlander.envs.multiplayer_lunar_lander import Lander
from spacetime import app

WAIT_FOR_START = 10.0

@app(Types=[Lander])
def lander_server(dataframe):
    env = gym.make('MultiplayerLunarLander-v0')
    players = list()
    start = time.time()
    while (time.time() - start) < WAIT_FOR_START:
        print ("\rWaiting for %d " % (int(WAIT_FOR_START - (time.time() - start)),), "Seconds for clients to connect.")
        time.sleep(1)
    
    dataframe.fork()
    players = dataframe.read_all(Lander)
    if not players:
        print ("No players connected, the game cannot continue. Exiting")
        return
    for pid, player in enumerate(players):
        player.player_id = pid
    env.build(players)
    for player in players:
        player.ready = True
    dataframe.join()

    while dataframe.sync() and not env.game_over:
        env.render()
        env.step()
    print ("game ended, waiting 5 secs for clients to disconnect")
    time.sleep(5)

def main(port):
    server = lander_server(server_port=port)
    server.start()

if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8000
    main(port)
