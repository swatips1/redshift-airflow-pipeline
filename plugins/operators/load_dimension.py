from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

# Operator to load dimentions 
# Parameter details:
## table: The name of table being loaded
## redshift_conn_id: Name of the redshift conneciton created using Airflow Admin
## select_sql: Select query for the data load
## mode: Append data to the table


class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 table,
                 redshift_conn_id='redshift',
                 select_sql='',
                 mode='append',
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.select_sql = select_sql
        self.mode = mode

    def execute(self, context):
        redshift_hook = PostgresHook("redshift")

        if self.mode == 'truncate':
            self.log.info(f'Purging old data from {self.table} dimension table...')
            redshift_hook.run(f'DELETE FROM {self.table};')
            self.log.info("Deletion complete.")

        sql = """
            INSERT INTO {table}
            {select_sql};
        """.format(table=self.table, select_sql=self.select_sql)

        self.log.info(f'Refreshing {self.table} dimension table...')
        redshift_hook.run(sql)
        self.log.info("Data refresh complete.")
