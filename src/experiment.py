import pandas as pd

from transactions import initialize_connection


class Experiment:

    isolation_levels = [
        'READ UNCOMMITTED',
        'READ COMMITTED',
        'REPEATABLE READ',
        'SERIALIZABLE'
    ]

    concurrency_scenario = None
    sample_size: int = 0

    def __init__(self, scenario, conn_info, concurrency_scenario, sample_size: int) -> None:
        self.scenario = scenario
        self.conn_info = conn_info
        self.concurrency_scenario = concurrency_scenario
        self.sample_size = sample_size

    def start(self):

        samples_df = pd.DataFrame(columns=['isolation_level', 'total_consistent', 'total_runtime'])

        for j, level in enumerate(self.isolation_levels):
            print('-------------- ' + level.center(20) + ' --------------')
            for i in range(self.sample_size):
                self.__reset_database()
                
                result_df: pd.DataFrame
                result_df = self.concurrency_scenario(self.conn_info, level)
                
                # print(result_df)
                
                sample_sum = result_df.agg('sum')
                
                samples_df.index.name = 'sample_num'
                samples_df.loc[i + j * self.sample_size] = {
                    'isolation_level': level,
                    'total_consistent': sample_sum['is_consistent'],
                    'total_runtime': sample_sum['runtime']
                }
        
        samples_df.to_csv('samples.csv')

        agg_df = samples_df.groupby('isolation_level').agg('mean')
        agg_df.to_csv(f'{ self.scenario }.csv')
    

    def __reset_database(self):
        db = initialize_connection(self.conn_info)
        cursor = db.cursor()
        
        with open('./sql/init.sql', 'r') as file:
            sqls = (' '.join(file.read().split('\n'))).split(';')
            
        for sql in sqls:
            cursor.execute(sql.strip())

        db.commit()
        db.close()

