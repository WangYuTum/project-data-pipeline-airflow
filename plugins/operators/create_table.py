from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class CreateTableOperator(BaseOperator):
    '''
    Operator to create tables.
    '''

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table_name='',
                 sql_command='',
                 *args, **kwargs):
        '''
        Arg(s):
            redshift_conn_id: redshift connection id (string)
            table_name: name of the table to be created
            sql_command: SQL command for creating the table
        '''

        super(CreateTableOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.sql_command = sql_command

    def execute(self, context):
        
        # get redshift connectoin
        redshift_conn = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # sql create table command
        create_tbl_command = ('''
            BEGIN;

            {};

            COMMIT;
            ''').format(self.sql_command)

        # execute sql
        logging.info(f'creating table {self.table_name} ...')
        redshift_conn.run(create_tbl_command)
        logging.info(f'create table {self.table_name} succeed.')


        
