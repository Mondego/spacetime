from gym.envs.registration import register

register(
    id='MultiplayerLunarLander-v0',
    entry_point='sample_apps.gym_mp_lunarlander.envs:MultiplayerLunarLander',
)
