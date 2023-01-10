from random import uniform
import simpy
import numpy as np
import pandas as pd
from scipy.stats import uniform
from scipy.stats import expon


# Parameters
serviceRep_count = 1
pCheck_count = 1

pArrival_time  = .2 
repService_time = .75 

Sim_Time  = 3

in_system = []

def p_arrival(env, serviceRep_count, pCheck_count):
    # Assign counts to pasengers
    p_counter = 0
    while True:
       # Exponential generator for arrivals
       next_p = expon.rvs(scale = pArrival_time, size = 1)
       # Wait for next person to arrive
       yield env.timeout(next_p)

       arrival_time = env.now

       p_counter += 1
       print('P {} arrives at t = {}'.format(p_counter, env.now))

       env.process(pServiceQueue(env, serviceRep_count, p_counter))
       env.process(pScanningQueue(env, pCheck_count, p_counter, arrival_time))

def pServiceQueue(env, serviceRep_count, p_number):
    with service_line.request() as req:
        yield req
        # Exponential distribution for the service process
        serviced = expon.rvs(scale = repService_time, size = 1)
        yield env.timeout(serviced)

        time_service = serviced
        print('P {} time in ServiceQueue = {}'.format(p_number, time_service))
        #print('P {} leaves the service queue at {}'.format(p_number, env.now))


def pScanningQueue(env, pCheck_count, p_number, arrival_time):
    with scan_line.request() as req:
        yield req
        # Uniform distribution for the service process
        scanned = uniform.rvs(.5, 1, size = 1)
        yield env.timeout(scanned)

        time_scan = scanned
        print('P {} time in ScanQueue = {}'.format(p_number, time_scan))

        departure_time = env.now
        print('P {} leaves at {}'.format(p_number, departure_time))

        system_time = departure_time[0] - arrival_time[0]
        #print('P {} system time = {}'.format(p_number, system_time))
        in_system.append(system_time)


# Start environment
env = simpy.Environment()

# define resources
service_line = simpy.Resource(env, capacity = serviceRep_count)
scan_line = simpy.Resource(env, capacity = pCheck_count)

## defining the FULL arrival process
env.process(p_arrival(env, service_line, scan_line))

## run the simultion
env.run(until = Sim_Time)

#Wait time
avg_delay = np.mean(in_system)
print('The average delay in system is {}'.format(avg_delay))
