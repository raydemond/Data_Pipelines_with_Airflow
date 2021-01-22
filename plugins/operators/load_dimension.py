from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',
                 table = '',
                 query = '',
                 truncate_insert = False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.query = query
        self.truncate_insert = truncate_insert

    def execute(self, context):
        '''
        - connects to Redshift
        - if truncate-insert pattern is True, empties the target table before the load
        - loads data into dimensional tables
        '''
        
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        if self.truncate_insert == True:
            self.log.info("Clearing data from target table")
            redshift.run("DELETE FROM {}".format(self.table))
            
        self.log.info('Loading data into dimensional tables')
        sql_query = f'''
        INSERT INTO {self.table}
        {self.query}
        '''
        redshift.run(sql_query)
