import sys
from sample_apps.gym_mp_lunarlander.envs.multiplayer_lunar_lander import Lander
from spacetime import Application

def lander(dataframe):
    my_lander = Lander()
    dataframe.add_one(Lander, my_lander)
    done = False
    while dataframe.sync() and not done:
        if not my_lander.ready:
            continue
        print (my_lander.player_id, my_lander.state, my_lander.reward, my_lander.done)
        my_lander.do_sample_action()
        done = my_lander.done


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8000
    Application(lander, dataframe=("0.0.0.0", port), Producer=[Lander]).start()

