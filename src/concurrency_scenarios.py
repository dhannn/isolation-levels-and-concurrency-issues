import random
import threading
import time
import mysql.connector

from transactions import initialize_connection

def simulate_dirty_reads(connection_info, isolation_level):

    # The concurrent transaction starts with User A having a balance of 100
    # and User B having a balance of 200

    # Transaction 1: User A transfers 50 to User B

    # Transaction 2: User A withdraws 20 from their account

    # Dirty read occurs when T1 has a delay between updating User A's balance
    # and committing. In case of failures between that delay, T2 might be dealing
    # with inconsistent data 

    delay = 1.0
    
    def T1__fund_transfer(src_acc, dst_acc, amount, fail: bool=True):
        db = initialize_connection(connection_info)
        db.start_transaction(isolation_level=isolation_level)
        cursor = db.cursor()

        try:
            cursor.execute('SELECT amount FROM Accounts WHERE id = %s', (src_acc,))
            source_amount = cursor.fetchone()[0]

            nonlocal T1_state
            T1_state = source_amount

            source_amount -= amount
            cursor.execute('UPDATE Accounts SET amount = %s WHERE id = %s', (source_amount, src_acc))

            cursor.execute(f'DO SLEEP({delay})')

            cursor.execute('SELECT amount FROM Accounts where id = %s', (dst_acc,))
            destination_amount = cursor.fetchone()[0]

            destination_amount += amount
            cursor.execute('UPDATE Accounts SET amount = %s WHERE id = %s', (destination_amount, dst_acc))
            
            if fail:
                db.rollback()
            else:
                db.commit()
        except Exception as e:
            print(e)
            db.rollback()
        finally:
            cursor.close()
            db.close()
    
    def T2__withdraw(acc, amount):
        db = initialize_connection(connection_info)
        db.start_transaction(isolation_level=isolation_level)

        cursor = db.cursor()

        try:
            cursor.execute(f'DO SLEEP({delay})')
            cursor.execute('SELECT amount FROM Accounts WHERE id = %s', (acc,))
            curr_amount = cursor.fetchone()[0]

            nonlocal T2_state
            T2_state = curr_amount

            curr_amount -= amount
            cursor.execute('UPDATE Accounts SET amount = %s WHERE id = %s', (curr_amount, acc))

            db.commit()
        except Exception as e:
            print(e)
            db.rollback()
        finally:
            cursor.close()
            db.close()

    T1 = threading.Thread(target=T1__fund_transfer, name='Transaction 1', args=(1, 2, 50))
    T2 = threading.Thread(target=T2__withdraw, name='Transaction 2', args=(1, 20))
    
    T1_state = None
    T2_state = None
    
    before = time.time()
    T1.start()
    T2.start()

    T1.join()
    T2.join()
    after = time.time()
    
    has_dirty_read = T1_state == T2_state
    return after - before, has_dirty_read

def simulate_non_repeatable_reads(connection_info, isolation_level):

    # The concurrent transaction starts with User D having a balance of 400, 
    # User B having a balance of 200 and User C having a balance of 300

    # Transaction 1: User D transfers to B and C an amount of 100, 50 
    # respectively.

    # Transaction 2: User A transfers to D an amount of 30

    # Transaction 3: User D deposits 50

    # Non-repeatable reads occur when reading User D's balance between 
    # the two multi-account transfers and the two other transactions


    pass_fail = True
    delay = 2.0

    def T1__fund_transfer_multiple(src_acc, amounts):
        db = initialize_connection(connection_info)
        db.start_transaction(isolation_level=isolation_level)
        cursor = db.cursor()

        cursor.execute('SELECT amount FROM Accounts WHERE id = %s', (src_acc,))
        prev_src_amt = cursor.fetchone()[0]
        
        nonlocal pass_fail
        try:
            for account in amounts:

                cursor.execute(f'DO SLEEP({delay * 0.55})')

                cursor.execute('SELECT amount FROM Accounts WHERE id = %s', (src_acc,))
                src_amt = cursor.fetchone()[0]
                
                db.commit()

                pass_fail = pass_fail and src_amt == prev_src_amt

                amount = amounts[account]
                src_amt -= amount
                prev_src_amt = src_amt

                cursor.execute('UPDATE Accounts SET amount = %s WHERE id = %s', (src_amt, src_acc))

                cursor.execute('SELECT amount FROM Accounts WHERE id = %s', (account,))
                dst_amt = cursor.fetchone()[0]

                dst_amt += amount
                cursor.execute('UPDATE Accounts SET amount = %s WHERE id = %s', (dst_amt, account))
            db.commit()
                

        except Exception as e:
            print(e)
            db.rollback()
        finally:
            cursor.close()
            db.close()

    def T2__fund_transfer(src_acc, dst_acc, amount):
        db = initialize_connection(connection_info)
        db.start_transaction(isolation_level=isolation_level)
        cursor = db.cursor()

        try:
            cursor.execute(f'DO SLEEP({delay * 0.5})')

            cursor.execute('SELECT amount FROM Accounts WHERE id = %s', (src_acc,))
            source_amount = cursor.fetchone()[0]

            source_amount -= amount
            cursor.execute('UPDATE Accounts SET amount = %s WHERE id = %s', (source_amount, src_acc))


            cursor.execute('SELECT amount FROM Accounts where id = %s', (dst_acc,))
            destination_amount = cursor.fetchone()[0]

            destination_amount += amount
            cursor.execute('UPDATE Accounts SET amount = %s WHERE id = %s', (destination_amount, dst_acc))

            db.commit()
        except:
            db.rollback()
        finally:
            cursor.close()
            db.close()
    
    def T3__deposit(acc, amount):
        db = initialize_connection(connection_info)
        db.start_transaction(isolation_level=isolation_level)

        cursor = db.cursor()

        try:
            cursor.execute(f'DO SLEEP({delay * 0.6})')
            cursor.execute('SELECT amount FROM Accounts WHERE id = %s', (acc,))
            curr_amount = cursor.fetchone()[0]

            curr_amount += amount
            cursor.execute('UPDATE Accounts SET amount = %s WHERE id = %s', (curr_amount, acc))

            db.commit()
        except Exception as e:
            print(e)
            db.rollback()
        finally:
            cursor.close()
            db.close()
    

    T1 = threading.Thread(target=T1__fund_transfer_multiple, name='Transaction 1', args=(4, {2: 100, 3: 50}))
    T2 = threading.Thread(target=T2__fund_transfer, name='Transaction 2', args=(1, 4, 30))
    T3 = threading.Thread(target=T3__deposit, name='Transaction 3', args=(4, 50))
    
    before = time.time()
    T1.start()
    T2.start()
    T3.start()

    T1.join()
    T2.join()
    T3.join()
    after = time.time()
    
    return after - before, pass_fail


def initialize_database(connection_info):
    
    db = initialize_connection(connection_info)
    cursor = db.cursor()
    
    with open('./sql/init.sql', 'r') as file:
        sqls = (' '.join(file.read().split('\n'))).split(';')
        
    for sql in sqls:
        cursor.execute(sql.strip())

    db.commit()
    db.close()

def simulate_phantom_reads(connection_info, isolation_level):

    delay = 0.5
    
    # The concurrent transaction starts with User D having a balance of 400, 
    # User B having a balance of 200 and User C having a balance of 300

    # Transaction 1: Application gets the average balance in the bank
    def T1__get_average_balance():
        db = initialize_connection(connection_info)
        cursor = db.cursor()

        db.start_transaction(isolation_level=isolation_level)

        try:
            cursor.execute('SELECT COUNT(amount) FROM Accounts')
            num = cursor.fetchone()[0]

            total = 0
            i = 1
            cursor.execute('SELECT amount FROM Accounts WHERE id = %s', (i,))
            res = cursor.fetchone()

            total += res[0]

            while res is not None:
                
                i += 1
                cursor.execute(f'DO SLEEP({delay * 0.1})')
                cursor.execute('SELECT amount FROM Accounts WHERE id = %s', (i,))
                
                res = cursor.fetchone()

                if res is None:
                    break

                total += res[0]

            calculated_average = total / num

            db.commit()

            cursor.execute('SELECT AVG(amount) FROM Accounts')
            expected_average = cursor.fetchone()[0]
        
        except Exception as e:
            print(e)
            db.rollback()
        finally:
            cursor.close()
            db.close()



        nonlocal pass_fail
        pass_fail = calculated_average == expected_average


    # Transaction 2 - 20: Application adds users
    def Tn__add_user(index, starting_balance):
        db = initialize_connection(connection_info)
        cursor = db.cursor()

        db.start_transaction(isolation_level=isolation_level)

        try:
            cursor.execute('DO SLEEP(%s)', (delay, ))
            cursor.execute('INSERT INTO Accounts (amount) VALUES (%s)', (starting_balance,))
            db.commit()
        except Exception as e:
            print(e)
            db.rollback()
        finally:
            cursor.close()
            db.close()
    
    # Transaction 21 - 40: Users withdraw
    def Tm__withdraw(account, amount):
        db = initialize_connection(connection_info)
        cursor = db.cursor()

        db.start_transaction(isolation_level=isolation_level)

        try:
            cursor.execute('DO SLEEP(%s)', (delay * 0.01, ))
            cursor.execute('SELECT amount FROM Accounts WHERE id = %s', (account, ))
            amt = cursor.fetchone()
            if amt is None:
                db.rollback()
            # else:
            cursor.execute('UPDATE Accounts SET amount = %s WHERE id = %s', (amt[0] + amount, account))
            db.commit()
        except Exception as e:
            print(e)
            db.rollback()
        finally:
            cursor.close()
            db.close()

    # A phantom read occurs when, during a transaction that aggregates 
    # information (like T1), another concurrent transaction inserts or deletes
    # rows, potentially producing inaccurate results.

    pass_fail = None
    T1 = threading.Thread(target=T1__get_average_balance, name='Transaction 1')
    Tn = []
    Tm = []
    
    for i in range(1, 20):
        # balance = random.randint(0, 19)
        balance = i * 100
        Tn.append(threading.Thread(target=Tn__add_user, name=f'Transaction {i + 1}', args=(i, balance)))
    
    for i in range(21, 40):
        amt = random.randint(0, 19)
        amt *= 100

        acc = random.randint(1, 3)
        Tm.append(threading.Thread(target=Tm__withdraw, name=f'Transaction {i + 1}', args=(acc, amt)))
    
    before = time.time()
    
    T1.start()

    transaction: threading.Thread
    for transaction in Tn:
        transaction.start()
    
    
    for transaction in Tm:
        transaction.start()

    T1.join()
    transaction: threading.Thread
    for transaction in Tn:
        transaction.join()
    for transaction in Tm:
        transaction.join()
    after = time.time()
    
    return after - before, pass_fail
