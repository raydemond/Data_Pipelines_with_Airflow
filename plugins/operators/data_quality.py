from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'


    @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',
                 dq_checks = '',
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks

    def execute(self, context):
        '''
        - connects to Redshift
        - checks whether a table is loaded with more than zero records
        - checks whether certain column contains NULL values by counting all the rows that have NULL in the column
        '''
        
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        tables = ['songplays', 'songs', 'artists', 'users', 'time']
        
        # check whether a table has more than zero records 
        for table in tables:
            self.table = table
            
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {self.table}")
            
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {self.table} returned no results")
            num_records = records[0][0]
            
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {self.table} contained 0 rows")
            
            self.log.info(f"Data quality on table {self.table} check passed with {records[0][0]} records")
        
        # check whether a column contains null values
        for dq_check in self.dq_checks:
            records = redshift_hook.get_records(dq_check['check_sql'])
            num_null_values = records[0][0]
            
            if dq_check['comparison'] == '=':
                if num_null_values != 0: 
                    raise ValueError("Data quality check failed. A column contains null values")
        
        self.log.info("Data quality check passed with zero null values")
            

  