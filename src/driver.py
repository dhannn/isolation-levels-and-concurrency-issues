import os
import random
import time

import pandas as pd
# from concurrency import *
from concurrency_scenarios import *
from experiment import Experiment
import transactions
from user_transaction import UserTransaction
from dotenv import load_dotenv

load_dotenv()

conn = {
    'host': os.environ['DB_HOST'],
    'user': os.environ['DB_USER'],
    'password': os.environ['DB_PASSWORD'],
    'database': 'isolation_levels_exercise'
}

# print(random.seed(1))

# Experiment A: Simulating Dirty Reads
# ex = Experiment('dirty_reads', conn, simulate_dirty_reads, 10)
# ex.start()

# Experiment B: Simulating Non-Repeatable Read
# ex = Experiment('nonrepeatable_reads', conn, simulate_nonrepeatable_reads, 3)
# ex.start()

# # Experiment C: Simulating Phantom Reads
# ex = Experiment('phantom_reads', conn, simulate_phantom_reads, 100)
# ex.start()

def run_simulation(sample_size, simulate_concurrency_issue):
    isolation_levels = ['READ UNCOMMITTED', 'READ COMMITTED', 'REPEATABLE READ', 'SERIALIZABLE']
    data = { level: { 'pass_count': 0, 'time_elapsed': 0 } for level in isolation_levels }

    for i in range(sample_size):

        isolation_levels = ['READ UNCOMMITTED', 'READ COMMITTED', 'REPEATABLE READ', 'SERIALIZABLE']
        
        for _ in range(len(isolation_levels)):

            initialize_database(conn)
            
            level = random.choice(isolation_levels)
            isolation_levels.remove(level)

            print(f'------------------- {level.center(16)} -------------------')
            
            before = time.time()
            time_elapsed, pass_fail = simulate_concurrency_issue(conn, level)
            after = time.time()

            time_elapsed = after - before
            print(f'Pass or Fail: {"Pass" if pass_fail else "Fail"}')
            print(f'Time Elapsed: {time_elapsed}')

            if pass_fail:
                data[level]['pass_count'] += 1
            
            data[level]['time_elapsed'] += time_elapsed

    isolation_levels = ['READ UNCOMMITTED', 'READ COMMITTED', 'REPEATABLE READ', 'SERIALIZABLE']

    for level in isolation_levels:
        data[level]['time_elapsed'] /= sample_size

    return data


# isolation_levels = ['READ UNCOMMITTED', 'READ COMMITTED', 'REPEATABLE READ', 'SERIALIZABLE']
# sample_size = 100

# data = { level: { 'pass_count': 0, 'time_elapsed': 0 } for level in isolation_levels }

# for i in range(sample_size):

#     isolation_levels = ['READ UNCOMMITTED', 'READ COMMITTED', 'REPEATABLE READ', 'SERIALIZABLE']
    
#     for _ in range(len(isolation_levels)):

#         initialize_database(conn)
        
#         level = random.choice(isolation_levels)
#         isolation_levels.remove(level)

#         print(f'------------------- {level.center(16)} -------------------')
        
#         before = time.time()
#         time_elapsed, pass_fail = simulate_dirty_reads(conn, level)
#         after = time.time()

#         time_elapsed = after - before
#         print(f'Pass or Fail: {"Pass" if pass_fail else "Fail"}')
#         print(f'Time Elapsed: {time_elapsed}')

#         if pass_fail:
#             data[level]['pass_count'] += 1
        
#         data[level]['time_elapsed'] += time_elapsed

# isolation_levels = ['READ UNCOMMITTED', 'READ COMMITTED', 'REPEATABLE READ', 'SERIALIZABLE']

# for level in isolation_levels:
#     data[level]['time_elapsed'] /= sample_size

concurrency_scenarios = [
    simulate_dirty_reads,
    simulate_non_repeatable_reads,
    simulate_phantom_reads
]

SAMPLE_SIZE = 500

for scenario in concurrency_scenarios:
    initialize_database(conn)
    data = run_simulation(SAMPLE_SIZE, scenario)
    name = '_'.join(scenario.__name__.split('_')[1:])
    pd.DataFrame(data).T.to_csv(f'{name}.csv')
    
# simulate_non_repeatable_reads(conn, 'READ COMMITTED')

print(pd.DataFrame(data).T)
