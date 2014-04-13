#!/usr/bin/env python

import cmath
import numpy as np

from numpy.fft.fftpack import fft
from random import randint


def magnitude(c):
    return sqrt(c*c.conjugate())

def get_random_signal(T):
    k = randint(0,T)
    x = [1 if i%T == k else 0 for i in range(N)]
    while not np.any(x):
        k = randint(0,T)
        x = [1 if i%T == k else 0 for i in range(N)]
    return k, x

# Original data
exp_data = [0.0, 65.60500001907349, 1825.5309998989105, 1861.1949999332428, 3616.5469999313354, 3652.177999973297, 5413.882999897003, 5449.318000078201, 7207.401999950409, 7243.092000007629, 8328.25200009346, 9006.778000116348, 9042.30999994278, 10797.611999988556, 10863.18799996376, 12623.076999902725, 12658.436000108719, 14415.46900010109, 14450.895999908447, 16213.163000106812, 16248.641000032425, 18005.18600010872, 18070.802999973297, 18522.302000045776, 19803.196000099182, 19868.661999940872, 20802.37299990654, 21594.209000110626, 21659.77800011635, 23422.048000097275, 23457.487999916077, 25214.668999910355, 25250.15300011635, 27011.65199995041, 27047.0569999218, 28803.25200009346, 28868.885999917984, 30222.260999917984, 30599.648000001907, 30665.289000034332, 32419.674999952316, 32455.194000005722, 34214.93099999428, 34250.35400009155, 36008.079999923706, 36043.68400001526, 37804.78500008583, 37870.19900012016, 38442.32400012016, 39594.3789999485, 39665.11899995804, 41424.59200000763, 41460.00900006294, 43217.22499990463, 43252.643000125885, 45012.87599992752, 45048.49799990654, 46848.959000110626, 46914.168999910355, 47809.790999889374, 47874.954999923706, 48406.518000125885, 48471.700000047684]


N = 50000

# Test 1

T = N / 80      # T = 625

k = randint(0, T)

x = [1 if i%T == k else 0 for i in range(N)]

y = fft(x)

y_real = [yi.real for yi in y]
y_imag = [yi.imag for yi in y]

y_phase = [cmath.phase(yi) for yi in y]
y_magnitude = [magnitude(yi) for yi in y]


# Test 2

T1 = N / 80     # T1 = 625
T2 = N / 200    # T2 = 250 

k1 = randint(0, T)
k2 = randint(0, T)

x1 = [1 if i%T1 == k1 else 0 for i in range(N)]
x2 = [1 if i%T2 == k2 else 0 for i in range(N)]

x = np.sum([x1, x2], axis=0)

y = fft(x)

y_real = [yi.real for yi in y]
y_imag = [yi.imag for yi in y]

y_phase = [cmath.phase(yi) for yi in y]
y_magnitude = [magnitude(yi) for yi in y]

spikes = [ymi.real for ymi in y_magnitude]
spikes = [i.round() for i in spikes]
spike_idxs = [i if s > 0 else -1 for (s,i) in zip(spikes, range(len(spikes)))]
nz_spike_idxs = filter(lambda x: x > -1, spike_idxs)

# Test 3

x_e = [round(i) for i in exp_data]
s = [0.]*N
for i in x_e:
    s[int(i)] = 1.
t = fft(s)
t_mag = [magnitude(i) for i in t]

spikes = [tmi.real for tmi in t_mag]
spikes = [i.round() for i in spikes]
spike_idxs = [i if s > 0 else -1 for (s,i) in zip(spikes, range(len(spikes)))]
nz_spike_idxs = filter(lambda x: x > -1, spike_idxs)

cutoff = np.percentile(spikes, 99.9)

for (s,i) in w:                            
    if s > cutoff:
        spike_idxs[i] = i
    else:
        spike_idxs[i] = -1.

nz_spike_idxs = filter(lambda x: x > -1, spike_idxs)

spike_test = []

for i in nz_spike_idxs:
    mod = float(i) % 28.
    if mod == 27 or mod == 26 or mod == 0 or mod == 1 or mod == 2:
        continue
    spike_test.append(i)
