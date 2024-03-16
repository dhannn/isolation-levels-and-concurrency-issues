import random
import time
import mysql.connector as mysql

def initialize_connection(connection_info: str) -> mysql.MySQLConnection:

    connection = mysql.connect(
        host=connection_info['host'],
        user=connection_info['user'],
        password=connection_info['password'],
        database=connection_info['database']
    )

    return connection


def sleep():
    delay = random.random()
    time.sleep(delay)

# Transfer funds
def transfer_funds(
        connection: mysql.MySQLConnection,
        source_account: int,
        destination_account: int,
        amount: int, 
        isolation_level: str = 'READ UNCOMMITTED'):
    connection.start_transaction(isolation_level=isolation_level)
    cursor = connection.cursor()

    try:
        cursor.execute('SELECT amount FROM Accounts WHERE id = %s', (source_account,))
        source_amount = cursor.fetchone()[0]

        if source_amount - amount < 0:
            print('Not enough cash')
            connection.rollback()
            return

        cursor.execute('SELECT amount FROM Accounts WHERE id = %s ', (destination_account,))
        destination_amount = cursor.fetchone()[0]
        

        source_amount -= amount
        destination_amount += amount

        cursor.execute('UPDATE Accounts SET amount = %s WHERE id = %s', (source_amount, source_account))
        cursor.execute('UPDATE Accounts SET amount = %s WHERE id = %s', (destination_amount, destination_account))

        connection.commit()

        return []

    except Exception as e:
        print(e)
        connection.rollback()

        return []

# Transfer funds to multiple accounts
def transfer_funds_to_multiple(
        connection: mysql.MySQLConnection,
        source_account: int,
        amounts: dict[int, int], 
        isolation_level: str = 'READ UNCOMMITTED'):
    
    connection.start_transaction(isolation_level=isolation_level)
    cursor = connection.cursor()

    try:
        destination_amounts = {}

        ret = []
        
        cursor.execute('SELECT amount FROM Accounts WHERE id = %s', (source_account,))
        source_amount_before = cursor.fetchone()[0]

        for account in amounts:
            cursor.execute('DO SLEEP(1)')
            cursor.execute('SELECT amount FROM Accounts WHERE id = %s', (source_account,))
            source_amount = cursor.fetchone()[0]
            ret.append(source_amount_before == source_amount)
            print(str(ret) + ': ' + str(source_amount_before) + '\t' + str(source_amount))
            
            source_amount_before = source_amount


            if source_amount - amounts[account] < 0:
                print('Not enough cash')
                connection.rollback()
                return

            cursor.execute('UPDATE Accounts SET amount = %s WHERE id = %s', (source_amount - amounts[account], source_account))


            source_amount_before -= amounts[account]


        for account in amounts:
            cursor.execute('SELECT amount FROM Accounts WHERE id = %s', (account, ))
            destination_amount = cursor.fetchone()[0]
            destination_amounts[account] = destination_amount + amounts[account]

        for account in amounts:
            cursor.execute('UPDATE Accounts SET amount = %s WHERE id = %s', (destination_amounts[account], account))

        connection.commit()

        return ret

    except Exception as e:
        print(e)
        connection.rollback()


# Create an account
def create_account(
        connection: mysql.MySQLConnection,
        starting_balance: int, 
        isolation_level: str = 'READ UNCOMMITTED'):
    
    connection.start_transaction(isolation_level=isolation_level)
    cursor = connection.cursor()

    try:
        if starting_balance < 0:
            connection.rollback()
        
        cursor.execute('INSERT INTO Accounts VALUES (%s)', (starting_balance,))
        connection.commit()
    except Exception as e:
        print(e)
        connection.rollback()


# Get the total bank funds 
def get_total_bank_funds(
        connection: mysql.MySQLConnection,
        isolation_level: str = 'READ UNCOMMITTED'):
    
    connection.start_transaction(isolation_level=isolation_level)
    cursor = connection.cursor()

    try:
        cursor.execute('SELECT SUM(amount) FROM Accounts')
        connection.commit()
    except Exception as e:
        print(e)
        connection.rollback()
