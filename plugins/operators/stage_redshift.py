from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#8EB6D4'

    @apply_defaults
    def __init__(self,
                 s3_bucket,
                 s3_prefix,
                 table,
                 redshift_conn_id='redshift',
                 aws_conn_id='aws_credentials',
                 extra_params='',
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.extra_params = extra_params

    def execute(self, context):
        aws_hook = AwsHook("aws_credentials")
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook("redshift")

        self.log.info(f'Staging data from {self.s3_bucket}/{self.s3_prefix} to {self.table} table...')

        copy_query = """
                    COPY {table}
                    FROM 's3://{s3_bucket}/{s3_prefix}'
                    with credentials
                    'aws_access_key_id={access_key};aws_secret_access_key={secret_key}'
                    {extra_params};
                """.format(table=self.table,
                           s3_bucket=self.s3_bucket,
                           s3_prefix=self.s3_prefix,
                           access_key=credentials.access_key,
                           secret_key=credentials.secret_key,
                           extra_params=self.extra_params)

        self.log.info('Processing data...')
        self.log.info(copy_query)
        redshift_hook.run(copy_query)
        self.log.info("Data processing complete.")
