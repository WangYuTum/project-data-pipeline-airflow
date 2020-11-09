from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class StageToRedshiftOperator(BaseOperator):
    '''
    Operator for staging data from S3 to Redshift
    '''

    ui_color = '#358140'
    template_fields=('s3_key',)

    @apply_defaults
    def __init__(self,
                 s3_buckt='',
                 s3_key='',
                 s3_format='json',
                 s3_format_mode='auto',
                 region='us-west-2',
                 target_table='',
                 aws_credentials_id='',
                 redshift_conn_id='',
                 render_s3_key=False,
                 truncate=True,
                 *args, **kwargs):
        '''
        Arg(s):
            s3_buckt (string): S3 bucket name
            s3_key (string): key name of S3 bucket, could be templated string
            s3_format (string): format of source data files, could be 'json' or 'csv', ...
            s3_format_mode (string): the loading mode of source data files, check Redshift COPY command reference for details
            region (string): AWS region
            target_table: which table to stage to
            aws_credentials_id (string): aws credentials id
            redshift_conn_id (string): redshift connection id
            render_s3_key (True or False): render the string or not
            truncate (True or False): truncate the staging table before COPY or not
        '''

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

        self.s3_buckt = s3_buckt
        self.s3_key = s3_key
        self.s3_format = s3_format
        self.s3_format_mode = s3_format_mode
        self.region = region
        self.target_table = target_table
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.render_s3_key = render_s3_key
        self.truncate = truncate

    def execute(self, context):

        # get aws credentials
        aws_conn = AwsHook(aws_conn_id=self.aws_credentials_id) # get a wrapper around the boto3 python library
        aws_credential_obj = aws_conn.get_credentials() # get the underlying botocore.Credentials object

        # get redshift connection
        redshift_conn = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # get rendered key and S3 full path
        s3_rendered_key = self.s3_key
        if self.render_s3_key:
            s3_rendered_key = s3_rendered_key.format(**context)
        s3_path = 's3://{}/{}'.format(self.s3_buckt, s3_rendered_key)


        # truncate table
        if self.truncate:
            logging.info(f'Truncating table {self.target_table} ...')
            redshift_conn.run('''
                BEGIN;

                TRUNCATE TABLE {};

                COMMIT;
            '''.format(self.target_table))
            logging.info(f'Truncate table {self.target_table} succeed.')

        # COPY command to transfer data
        sql_command = ('''
            BEGIN;

            COPY {}
            FROM '{}'
            access_key_id '{}'
            secret_access_key '{}'
            region '{}'
            {} '{}'
            BLANKSASNULL
            EMPTYASNULL
            TRUNCATECOLUMNS;

            COMMIT;
            ''').format(
                self.target_table, 
                s3_path, 
                aws_credential_obj.access_key, 
                aws_credential_obj.secret_key,
                self.region,
                self.s3_format,
                self.s3_format_mode
            )

        # issue command
        logging.info('Copying data from S3 to redshift ...')
        redshift_conn.run(sql_command)
        logging.info('Copy succeed.')

        # check number of records copied
        records = redshift_conn.get_records(f'''SELECT COUNT(*) FROM {self.target_table}''')
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f'Staging table failed: Return no results from {self.target_table}.')
        cnt_rows = records[0][0]
        if cnt_rows < 1:
            raise ValueError(f'Staging table failed: Number of records in {self.target_table} is {cnt_rows}.')
        logging.info(f'Staging table {self.target_table} succeed: {cnt_rows} rows inserted.')






