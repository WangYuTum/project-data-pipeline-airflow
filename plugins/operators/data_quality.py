from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class DataQualityOperator(BaseOperator):
    '''
    Operator to perform data quality checks.
    '''

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 metric_dict={},
                 *args, **kwargs):
        '''
        Arg(s):
            redshift_conn_id (string): redshift connection id
            metric_dict (dict): defines what data need to be checked and what are the metrics
        '''

        super(DataQualityOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.metric_dict = metric_dict

    def execute(self, context):

        # get redshift connectoin
        redshift_conn = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Example metric_dict:
        '''
        {'not_null': [{'table': 'songplays', 'columns': ['playid', 'start_time', 'userid']}, 
                      {'table': 'artists', 'columns': ['artistid']},
                      {'table': 'songs', 'columns': ['songid']},
                      {'table': 'time', 'columns': ['start_time']},
                      {'table': 'users', 'columns': ['userid']}],

         'row_count': [{'table': 'songplays', 'min': 1, 'max': None},
                       {'table': 'artists', 'min': 1, 'max': None},
                       {'table': 'songs', 'min': 1, 'max': None},
                       {'table': 'time', 'min': 1, 'max': None},
                       {'table': 'users', 'min': 1, 'max': None}]
        }
        '''

        logging.info('Performing data quality ...')

        # check for row counts
        metric_list = self.metric_dict.get('row_count', None)
        if metric_list is None:
            logging.info('Skip row_count check')
        for item in metric_list:
            table_name = item.get('table', None)
            min_count = item.get('min', None)
            max_count = item.get('max', None)
            if table_name is None:
                continue
            # count rows for the table
            records = redshift_conn.get_records(f'SELECT COUNT(*) FROM {table_name}')
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f'Data quality check failed for row_count for table {table_name}: Returned no results.')
            cnt_rows = records[0][0]
            if min_count is not None and cnt_rows < min_count:
                raise ValueError(f'Data quality check failed for row_count for table {table_name}: get {cnt_rows} rows less than expected {min_count}.')
            if max_count is not None and cnt_rows > max_count:
                raise ValueError(f'Data quality check failed for row_count for table {table_name}: get {cnt_rows} rows greater than expected {max_count}.')
            logging.info(f'Data quality check passed for row_count for table {table_name}: get {cnt_rows} rows.')


        # check for NOT NULL fields
        metric_list = self.metric_dict.get('not_null', None)
        if metric_list is None:
            logging.info('Skip not_null check')
        for item in metric_list:
            table_name = item.get('table', None)
            columns = item.get('columns', [])
            if table_name is None or len(columns) == 0: # nothing to check
                continue

            # prepare sql command
            sql_command = f'SELECT COUNT(*) FROM {table_name} WHERE'
            logical_stmt = ''
            for col in columns:
                logical_stmt += f'{col} is NULL OR '
            logical_stmt = logical_stmt[:-4] # trim the last 'OR' statement
            sql_command = sql_command + ' ' + logical_stmt

            # execute sql
            records = redshift_conn.get_records(sql_command)
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f'Data quality check failed for not_null for table {table_name}: Returned no results.')
            cnt_rows = records[0][0] # should be expecting a value of 0
            if cnt_rows != 0:
                raise ValueError(f'''Data quality check failed for not_null for table {table_name}: get {cnt_rows} rows 
                    containing NULL value in any columns {columns}.''')
            logging.info(f'''Data quality check passed for not_null for table {table_name}: no NULL value in any columns {columns}.''')

        logging.info('Passed all data quality checks.')