# Simulation A: Dirty Reads

from transactions import *
from user_transaction import UserTransaction
import pandas as pd

def dirty_read_consistency(connection_info):
    connection = initialize_connection(connection_info)
    cursor = connection.cursor()
    cursor.execute('SELECT sum(amount) FROM Accounts')
    return cursor.fetchone()[0]

def simulate_dirty_reads(connection_info, isolation_level):
    # If sequential:
    # Account 1 transfers 50 to Account 2
    # A1 = 100; A2 = 200 -> A1 = 50; A2 = 250
    userA = UserTransaction()
    userA.set_transaction(connection_info, transfer_funds, dirty_read_consistency, source_account=1, destination_account=2, amount=50, isolation_level=isolation_level)

    # Account 2 transfers 100 to Account 3
    # A2 = 250; A3 = 300 -> A2 = 150; A3 = 400
    userB = UserTransaction()
    userB.set_transaction(connection_info, transfer_funds, dirty_read_consistency, source_account=2, destination_account=3, amount=100, isolation_level=isolation_level)

    # Account 3 transfers 200 to Account 1
    # A3 = 400; A1 = 50 -> A3 = 200; A1 = 200
    userC = UserTransaction()
    userC.set_transaction(connection_info, transfer_funds, dirty_read_consistency, source_account=4, destination_account=1, amount=200, isolation_level=isolation_level)

    users = [userA, userB, userC]
    done = set()

    for _ in range(3):
        user = random.choice(users)
        
        while user in done:
            user = random.choice(users)

        user.start()
        done.add(user)
    
    df = pd.DataFrame(columns=['is_consistent', 'runtime'])

    for i, user in enumerate(users):
        user.join()
        df.loc[i + 1] = [ user.is_consistent, user.transaction_runtime ]

    return df

# Simulation B: Non-Repeatable Reads
def simulate_nonrepeatable_reads(connection_info, isolation_level):
    amounts = {
        1: 100,
        2: 40,
        3: 80
    }

    userA = UserTransaction()
    userA.set_transaction(connection_info, transfer_funds_to_multiple, dirty_read_consistency, source_account=4, amounts=amounts, isolation_level=isolation_level)

    userB = UserTransaction()
    userB.set_transaction(connection_info, transfer_funds, dirty_read_consistency, source_account=2, destination_account=4, amount=120, isolation_level=isolation_level)

    userC = UserTransaction()
    userC.set_transaction(connection_info, transfer_funds, dirty_read_consistency, source_account=3, destination_account=4, amount=130, isolation_level=isolation_level)

    users = [userB, userC]
    done = set()

    userA.start()
    time.sleep(0.5)
    userB.start()
    time.sleep(1)
    userC.start()
    
    df = pd.DataFrame(columns=['is_consistent', 'runtime'])

    users.insert(0, userA)
    for user in users:
        user.join()
    
    for i, user in enumerate(users):
        df.loc[i + 1] = [ user.is_consistent, user.transaction_runtime ]

    return df


def check_total_accounts(connection_info):
    db = initialize_connection(connection_info)
    cursor = db.cursor()
    
    cursor.execute('SELECT COUNT(id) FROM Accounts')
    return cursor.fetchone()[0]
    
    

# Simulation C: Phantom Reads
def simulate_phantom_reads(connection_info, isolation_level):
    newUsers = [
        100,
        200,
        50,
        50
    ]

    users = []

    for userInfo in newUsers:
        user = UserTransaction()
        user.set_transaction(connection_info, create_account, check_total_accounts, starting_balance=userInfo)
        users.append(user)
    
    done = set()

    for _ in range(len(newUsers)):
        user = random.choice(users)
        
        user: UserTransaction
        while user in done:
            user = random.choice(users)

        user.start()
        done.add(user)

    df = pd.DataFrame(columns=['is_consistent', 'runtime'])
    for i, user in enumerate(users):
        user.join()
        df.loc[i + 1] = [ user.is_consistent, user.transaction_runtime ]

    return df
