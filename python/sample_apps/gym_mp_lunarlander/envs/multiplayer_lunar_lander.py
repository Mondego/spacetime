import sys, math, uuid
import numpy as np

import Box2D
from Box2D.b2 import (edgeShape, circleShape, fixtureDef, polygonShape, revoluteJointDef, contactListener)

import gym
from gym import spaces
from gym.utils import seeding, EzPickle

from rtypes import pcc_set
from rtypes import dimension, primarykey

FPS    = 50
SCALE  = 30.0   # affects how fast-paced the game is, forces should be adjusted as well

MAIN_ENGINE_POWER  = 13.0
SIDE_ENGINE_POWER  =  0.6

INITIAL_RANDOM = 1000.0   # Set 1500 to make game harder

LANDER_POLY =[
    (-14,+17), (-17,0), (-17,-10),
    (+17,-10), (+17,0), (+14,+17)
    ]
LEG_AWAY = 20
LEG_DOWN = 18
LEG_W, LEG_H = 2, 8
LEG_SPRING_TORQUE = 40

SIDE_ENGINE_HEIGHT = 14.0
SIDE_ENGINE_AWAY   = 12.0

VIEWPORT_W = 600
VIEWPORT_H = 400

class ContactDetector(contactListener):
    def __init__(self, env):
        contactListener.__init__(self)
        self.env = env
    def BeginContact(self, contact):
        landers_contact = [l for l in self.env.landers if l.body==contact.fixtureA.body or l.body==contact.fixtureB.body]
        legs_contact = [
            l for l in self.env.landers
            if any(leg in [contact.fixtureA.body, contact.fixtureB.body]
                   for leg in l.legs)]
        if (len(landers_contact) + len(legs_contact)) == 2:
            return
        if landers_contact:
            winner = landers_contact[0]
            winner.winner = True
            self.env.game_over = True
        if legs_contact:
            lander = legs_contact[0]
            for i in range(2):
                if lander.legs[i] in [contact.fixtureA.body, contact.fixtureB.body]:
                    lander.legs[i].ground_contact = True

    def EndContact(self, contact):
        for lander in self.env.landers:
            for i in range(2):
                if lander.legs[i] in [contact.fixtureA.body, contact.fixtureB.body]:
                    lander.legs[i].ground_contact = False
        

@pcc_set
class Lander(object):
    oid = primarykey(str)
    player_id = dimension(int)
    ready = dimension(bool)
    pos_x = dimension(int)
    pos_y = dimension(int)
    vel_x = dimension(float)
    vel_y = dimension(float)
    x_distance_to_landing = dimension(float)
    y_distance_to_landing = dimension(float)
    angle = dimension(float)
    angular_velocity = dimension(float)
    right_leg_in_contact = dimension(bool)
    left_leg_in_contact = dimension(bool)
    winner = dimension(bool)
    done = dimension(bool)
    reward = dimension(int)

    main_thrust = dimension(float)
    side_thrust = dimension(float)

    def do_sample_action(self):
        self.main_thrust, self.side_thrust = map(float, spaces.Box(-1, +1, (2,), dtype=np.float32).sample())

    @property
    def state(self):
        return np.array([
            self.x_distance_to_landing,
            self.y_distance_to_landing,
            self.vel_x, self.vel_y,
            self.angle,
            self.right_leg_in_contact,
            self.left_leg_in_contact], dtype=np.float32)

    def get_state(self, helipad_y):
        return [
            (self.pos_x - VIEWPORT_W/SCALE/2) / (VIEWPORT_W/SCALE/2),
            (self.pos_y - (helipad_y+LEG_DOWN/SCALE)) / (VIEWPORT_H/SCALE/2),
            self.vel.x*(VIEWPORT_W/SCALE/2)/FPS,
            self.vel.y*(VIEWPORT_H/SCALE/2)/FPS,
            self.body.angle,
            20.0*self.body.angularVelocity/FPS,
            self.legs[0].ground_contact,
            self.legs[1].ground_contact
        ]

    def __init__(self):
        self.oid = str(uuid.uuid4())
        self.body = None
        self.legs = list()
        self.prev_shaping = None
        self.vel = None
        self.s_power = None
        self.m_power = None
        self.main_thrust = 0.0
        self.side_thrust = 0.0
        self.ready = False
        self.done = False

    def set_up_local_objects(self):
        self.body = None
        self.legs = list()
        self.prev_shaping = None
        self.vel = None
        self.s_power = None
        self.m_power = None

    def set_state(self, helipad_y):
        pos = self.body.position
        self.vel = self.body.linearVelocity
        if self.pos_x != pos.x:
            self.pos_x = pos.x
        if self.pos_y != pos.y:
            self.pos_y = pos.y
        state = self.get_state(helipad_y)
        x, y, vel_x, vel_y, angle, angularvel, l_leg, r_leg = state
        if self.x_distance_to_landing != x:
            self.x_distance_to_landing = x
        if self.y_distance_to_landing != y:
            self.y_distance_to_landing = y
        if self.vel_x != vel_x:
            self.vel_x = vel_x
        if self.vel_y != vel_y:
            self.vel_y = vel_y
        if self.angle != angle:
            self.angle = angle
        if self.angular_velocity != angularvel:
            self.angular_velocity = angularvel
        if self.left_leg_in_contact != l_leg:
            self.left_leg_in_contact = l_leg
        if self.right_leg_in_contact != r_leg:
            self.right_leg_in_contact = r_leg
        reward = 0
        shaping = \
            - 100*np.sqrt(state[0]*state[0] + state[1]*state[1]) \
            - 100*np.sqrt(state[2]*state[2] + state[3]*state[3]) \
            - 100*abs(state[4]) + 10*state[6] + 10*state[7]   # And ten points for legs contact, the idea is if you
                                                              # lose contact again after landing, you get negative reward
        if self.prev_shaping is not None:
            reward = shaping - self.prev_shaping
        self.prev_shaping = shaping

        reward -= self.m_power*0.30  # less fuel spent is better, about -30 for heurisic landing
        reward -= self.s_power*0.03
        if self.reward != reward:
            self.reward = reward


class MultiplayerLunarLander(gym.Env, EzPickle):
    metadata = {
        'render.modes': ['human', 'rgb_array'],
        'video.frames_per_second' : FPS
    }

    continuous = False

    def __init__(self):
        EzPickle.__init__(self)
        self.seed()
        self.viewer = None

        self.world = Box2D.b2World()
        self.moon = None
        self.landers = None
        self.particles = []

        # self.prev_reward = None

        # useful range is -1 .. +1, but spikes can be higher
        #self.observation_space = spaces.Box(-np.inf, np.inf, shape=(8,), dtype=np.float32)

        # Nop, fire left engine, main engine, right engine
        # self.action_space = spaces.Discrete(4)

    def build(self, landers):
        self.landers = landers
        for lander in self.landers:
            lander.set_up_local_objects()
        self.reset()

    def seed(self, seed=None):
        self.np_random, seed = seeding.np_random(seed)
        return [seed]

    def _destroy(self):
        if not self.moon: return
        self.world.contactListener = None
        self._clean_particles(True)
        self.world.DestroyBody(self.moon)
        self.moon = None
        for lander in self.landers:
            self.world.DestroyBody(lander)
            lander.body = None
            for leg in lander.legs:
                self.world.DestroyBody(leg)
            lander.legs = list()
    

    def _create_lander(self, lander, number_of_landers):
        initial_y = VIEWPORT_H/SCALE
        body_xpos = (lander.player_id + 1) * VIEWPORT_W/SCALE / (number_of_landers + 1)
        body_ypos = initial_y
        body = self.world.CreateDynamicBody(
            position = (body_xpos, body_ypos),
            angle=0.0,
            fixtures = fixtureDef(
                shape=polygonShape(vertices=[ (x/SCALE,y/SCALE) for x,y in LANDER_POLY ]),
                density=5.0,
                friction=0.1,
                categoryBits=0x0010,
                maskBits=0x001,  # collide only with ground
                restitution=0.0) # 0.99 bouncy
                )
        body.color1 = (0.5,0.4,0.9)
        body.color2 = (0.3,0.3,0.5)
        body.ApplyForceToCenter( (
            self.np_random.uniform(-INITIAL_RANDOM, INITIAL_RANDOM),
            self.np_random.uniform(-INITIAL_RANDOM, INITIAL_RANDOM)
            ), True)

        legs = []
        for i in [-1,+1]:
            leg = self.world.CreateDynamicBody(
                position = (body_xpos - i*LEG_AWAY/SCALE, body_ypos),
                angle = (i*0.05),
                fixtures = fixtureDef(
                    shape=polygonShape(box=(LEG_W/SCALE, LEG_H/SCALE)),
                    density=1.0,
                    restitution=0.0,
                    categoryBits=0x0020,
                    maskBits=0x001)
                )
            leg.ground_contact = False
            leg.color1 = (0.5,0.4,0.9)
            leg.color2 = (0.3,0.3,0.5)
            rjd = revoluteJointDef(
                bodyA=body,
                bodyB=leg,
                localAnchorA=(0, 0),
                localAnchorB=(i*LEG_AWAY/SCALE, LEG_DOWN/SCALE),
                enableMotor=True,
                enableLimit=True,
                maxMotorTorque=LEG_SPRING_TORQUE,
                motorSpeed=+0.3*i  # low enough not to jump back into the sky
                )
            if i==-1:
                rjd.lowerAngle = +0.9 - 0.5  # Yes, the most esoteric numbers here, angles legs have freedom to travel within
                rjd.upperAngle = +0.9
            else:
                rjd.lowerAngle = -0.9
                rjd.upperAngle = -0.9 + 0.5
            leg.joint = self.world.CreateJoint(rjd)
            legs.append(leg)
        lander.body, lander.legs = body, legs

    def reset(self):
        self._destroy()
        self.world.contactListener_keepref = ContactDetector(self)
        self.world.contactListener = self.world.contactListener_keepref
        self.game_over = False

        W = VIEWPORT_W/SCALE
        H = VIEWPORT_H/SCALE

        # terrain
        CHUNKS = 11
        height = self.np_random.uniform(0, H/2, size=(CHUNKS+1,) )
        chunk_x  = [W/(CHUNKS-1)*i for i in range(CHUNKS)]
        self.helipad_x1 = chunk_x[CHUNKS//2-1]
        self.helipad_x2 = chunk_x[CHUNKS//2+1]
        self.helipad_y  = H/4
        height[CHUNKS//2-2] = self.helipad_y
        height[CHUNKS//2-1] = self.helipad_y
        height[CHUNKS//2+0] = self.helipad_y
        height[CHUNKS//2+1] = self.helipad_y
        height[CHUNKS//2+2] = self.helipad_y
        smooth_y = [0.33*(height[i-1] + height[i+0] + height[i+1]) for i in range(CHUNKS)]

        self.moon = self.world.CreateStaticBody( shapes=edgeShape(vertices=[(0, 0), (W, 0)]) )
        self.sky_polys = []
        for i in range(CHUNKS-1):
            p1 = (chunk_x[i],   smooth_y[i])
            p2 = (chunk_x[i+1], smooth_y[i+1])
            self.moon.CreateEdgeFixture(
                vertices=[p1,p2],
                density=0,
                friction=0.1)
            self.sky_polys.append( [p1, p2, (p2[0],H), (p1[0],H)] )

        self.moon.color1 = (0.0,0.0,0.0)
        self.moon.color2 = (0.0,0.0,0.0)

        for lander in self.landers:
            self._create_lander(lander, len(self.landers))

        self.drawlist = (
            [lander.body for lander in self.landers]
            + [leg for lander in self.landers for leg in lander.legs])

        return self.step()

    def _create_particle(self, mass, x, y, ttl):
        p = self.world.CreateDynamicBody(
            position = (x,y),
            angle=0.0,
            fixtures = fixtureDef(
                shape=circleShape(radius=2/SCALE, pos=(0,0)),
                density=mass,
                friction=0.1,
                categoryBits=0x0100,
                maskBits=0x001,  # collide only with ground
                restitution=0.3)
                )
        p.ttl = ttl
        self.particles.append(p)
        self._clean_particles(False)
        return p

    def _clean_particles(self, all):
        while self.particles and (all or self.particles[0].ttl<0):
            self.world.DestroyBody(self.particles.pop(0))

    def _apply_action(self, lander, action):
        tip  = (math.sin(lander.body.angle), math.cos(lander.body.angle))
        side = (-tip[1], tip[0])
        dispersion = [self.np_random.uniform(-1.0, +1.0) / SCALE for _ in range(2)]

        m_power = 0.0
        if action[0] > 0.0:
            # Main engine
            m_power = (np.clip(action[0], 0.0,1.0) + 1.0)*0.5   # 0.5..1.0
            assert m_power>=0.5 and m_power <= 1.0
            ox =  tip[0]*(4/SCALE + 2*dispersion[0]) + side[0]*dispersion[1]   # 4 is move a bit downwards, +-2 for randomness
            oy = -tip[1]*(4/SCALE + 2*dispersion[0]) - side[1]*dispersion[1]
            impulse_pos = (lander.body.position[0] + ox, lander.body.position[1] + oy)
            p = self._create_particle(3.5, impulse_pos[0], impulse_pos[1], m_power)    # particles are just a decoration, 3.5 is here to make particle speed adequate
            p.ApplyLinearImpulse(           ( ox*MAIN_ENGINE_POWER*m_power,  oy*MAIN_ENGINE_POWER*m_power), impulse_pos, True)
            lander.body.ApplyLinearImpulse( (-ox*MAIN_ENGINE_POWER*m_power, -oy*MAIN_ENGINE_POWER*m_power), impulse_pos, True)
        lander.m_power = m_power

        s_power = 0.0
        if np.abs(action[1]) > 0.5:
            # Orientation engines
            direction = np.sign(action[1])
            s_power = np.clip(np.abs(action[1]), 0.5,1.0)
            assert s_power>=0.5 and s_power <= 1.0
            ox =  tip[0]*dispersion[0] + side[0]*(3*dispersion[1]+direction*SIDE_ENGINE_AWAY/SCALE)
            oy = -tip[1]*dispersion[0] - side[1]*(3*dispersion[1]+direction*SIDE_ENGINE_AWAY/SCALE)
            impulse_pos = (lander.body.position[0] + ox - tip[0]*17/SCALE, lander.body.position[1] + oy + tip[1]*SIDE_ENGINE_HEIGHT/SCALE)
            p = self._create_particle(0.7, impulse_pos[0], impulse_pos[1], s_power)
            p.ApplyLinearImpulse(           ( ox*SIDE_ENGINE_POWER*s_power,  oy*SIDE_ENGINE_POWER*s_power), impulse_pos, True)
            lander.body.ApplyLinearImpulse( (-ox*SIDE_ENGINE_POWER*s_power, -oy*SIDE_ENGINE_POWER*s_power), impulse_pos, True)
        lander.s_power = s_power


    def step(self):
        for lander in self.landers:
            action = np.clip(
                [lander.main_thrust, lander.side_thrust], -1, +1).astype(np.float32) 
            # Engines
            self._apply_action(lander, action)
        
        self.world.Step(1.0/FPS, 6*30, 2*30)

        for lander in self.landers:
            lander.set_state(self.helipad_y)

        done = False
        if (self.game_over 
                or all(abs(lander.x_distance_to_landing)
                       for lander in self.landers) >= 1.0):
            for lander in self.landers:
                lander.reward = -100
        # if not self.lander.awake:
        #     done   = True
        #     reward = +100
        

    def render(self, mode='human'):
        from gym.envs.classic_control import rendering
        if self.viewer is None:
            self.viewer = rendering.Viewer(VIEWPORT_W, VIEWPORT_H)
            self.viewer.set_bounds(0, VIEWPORT_W/SCALE, 0, VIEWPORT_H/SCALE)

        for obj in self.particles:
            obj.ttl -= 0.15
            obj.color1 = (max(0.2,0.2+obj.ttl), max(0.2,0.5*obj.ttl), max(0.2,0.5*obj.ttl))
            obj.color2 = (max(0.2,0.2+obj.ttl), max(0.2,0.5*obj.ttl), max(0.2,0.5*obj.ttl))

        self._clean_particles(False)

        for p in self.sky_polys:
            self.viewer.draw_polygon(p, color=(0,0,0))

        for obj in self.particles + self.drawlist:
            for f in obj.fixtures:
                trans = f.body.transform
                if type(f.shape) is circleShape:
                    t = rendering.Transform(translation=trans*f.shape.pos)
                    self.viewer.draw_circle(f.shape.radius, 20, color=obj.color1).add_attr(t)
                    self.viewer.draw_circle(f.shape.radius, 20, color=obj.color2, filled=False, linewidth=2).add_attr(t)
                else:
                    path = [trans*v for v in f.shape.vertices]
                    self.viewer.draw_polygon(path, color=obj.color1)
                    path.append(path[0])
                    self.viewer.draw_polyline(path, color=obj.color2, linewidth=2)

        for x in [self.helipad_x1, self.helipad_x2]:
            flagy1 = self.helipad_y
            flagy2 = flagy1 + 50/SCALE
            self.viewer.draw_polyline( [(x, flagy1), (x, flagy2)], color=(1,1,1) )
            self.viewer.draw_polygon( [(x, flagy2), (x, flagy2-10/SCALE), (x+25/SCALE, flagy2-5/SCALE)], color=(0.8,0.8,0) )

        return self.viewer.render(return_rgb_array = mode=='rgb_array')

    def close(self):
        if self.viewer is not None:
            self.viewer.close()
            self.viewer = None

def heuristic(env, s):
    # Heuristic for:
    # 1. Testing. 
    # 2. Demonstration rollout.
    angle_targ = s[0]*0.5 + s[2]*1.0         # angle should point towards center (s[0] is horizontal coordinate, s[2] hor speed)
    if angle_targ >  0.4: angle_targ =  0.4  # more than 0.4 radians (22 degrees) is bad
    if angle_targ < -0.4: angle_targ = -0.4
    hover_targ = 0.55*np.abs(s[0])           # target y should be proporional to horizontal offset

    # PID controller: s[4] angle, s[5] angularSpeed
    angle_todo = (angle_targ - s[4])*0.5 - (s[5])*1.0
    #print("angle_targ=%0.2f, angle_todo=%0.2f" % (angle_targ, angle_todo))

    # PID controller: s[1] vertical coordinate s[3] vertical speed
    hover_todo = (hover_targ - s[1])*0.5 - (s[3])*0.5
    #print("hover_targ=%0.2f, hover_todo=%0.2f" % (hover_targ, hover_todo))

    if s[6] or s[7]: # legs have contact
        angle_todo = 0
        hover_todo = -(s[3])*0.5  # override to reduce fall speed, that's all we need after contact

    if env.continuous:
        a = np.array( [hover_todo*20 - 1, -angle_todo*20] )
        a = np.clip(a, -1, +1)
    else:
        a = 0
        if hover_todo > np.abs(angle_todo) and hover_todo > 0.05: a = 2
        elif angle_todo < -0.05: a = 3
        elif angle_todo > +0.05: a = 1
    return a

def demo_heuristic_lander(env, seed=None, render=False):
    env.seed(seed)
    total_reward = 0
    steps = 0
    s = env.reset()
    while True:
        a = heuristic(env, s)
        s, r, done, info = env.step(a)
        total_reward += r

        if render:
            still_open = env.render()
            if still_open == False: break

        if steps % 20 == 0 or done:
            print("observations:", " ".join(["{:+0.2f}".format(x) for x in s]))
            print("step {} total_reward {:+0.2f}".format(steps, total_reward))
        steps += 1
        if done: break
    return total_reward


if __name__ == '__main__':
    demo_heuristic_lander(LunarLander(), render=True)
    
    
