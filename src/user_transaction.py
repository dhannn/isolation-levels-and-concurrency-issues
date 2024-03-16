from threading import Thread
import time
import mysql.connector as mysql

class UserTransaction(Thread):

    transaction_runtime = None
    is_consistent = None
    
    def set_transaction(self, connection_info, transaction, check_consistency, **kwargs) -> None:
        self.connection = mysql.connect(
            host=connection_info['host'],
            user=connection_info['user'],
            password=connection_info['password'],
            database=connection_info['database']
        )

        self.connection_info = connection_info
        self.transaction = transaction
        self.check_consistency = check_consistency
        self.kwargs = kwargs
    
    def run(self):
        before_status = self.check_consistency(self.connection_info)
        before_time = time.time()
        
        res = self.transaction(self.connection, **self.kwargs)
        
        print(self.transaction.__name__ + ': ' + str(sum(res)))

        after_status = self.check_consistency(self.connection_info)
        after_time = time.time()

        self.connection.close()

        self.is_consistent = before_status == after_status
        self.transaction_runtime = after_time - before_time
