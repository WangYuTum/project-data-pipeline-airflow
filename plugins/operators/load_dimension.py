from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class LoadDimensionOperator(BaseOperator):
    '''
    Operator to load dim tables.
    '''

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 target_table='',
                 target_columns='',
                 redshift_conn_id='',
                 sql_transform_command='',
                 truncate=True,
                 *args, **kwargs):
        '''
        Arg(s):
            target_table: which table to load to
            target_columns: which columns to load to, a tuple containing column names as strings
                            example: ('col1', 'col2')
            redshift_conn_id: redshift connection id (string)
            sql_transform_command: SQL command to transform the original data
            truncate: truncate table before loading or not
        '''

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)

        self.target_table = target_table
        self.target_columns = target_columns
        self.redshift_conn_id = redshift_conn_id
        self.sql_transform_command = sql_transform_command
        self.truncate = truncate

    def execute(self, context):
        
        # get redshift connectoin
        redshift_conn = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # truncate table
        if self.truncate:
            logging.info(f'Truncating table {self.target_table} ...')
            redshift_conn.run('''
                BEGIN;

                TRUNCATE TABLE {};

                COMMIT;
            '''.format(self.target_table))
            logging.info(f'Truncate table {self.target_table} succeed.')

        # command to load dim table
        sql_command = ('''
            BEGIN;

            INSERT INTO {} {}
            {};

            COMMIT;
            ''').format(self.target_table, self.target_columns, self.sql_transform_command)

        # issue command 
        logging.info('Loading dim table: {} ...'.format(self.target_table))
        redshift_conn.run(sql_command)
        logging.info('Load succeed.')

