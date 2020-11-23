from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

# Operator to load fact tables 
# Parameter details:
## table: The name of table being loaded
## redshift_conn_id: Name of the redshift conneciton created using Airflow Admin
## select_sql: Select query for the data load
## mode: Append data to the table

class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 table,
                 redshift_conn_id='redshift',
                 select_sql='',
                 *args, **kwargs):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.select_sql = select_sql

    def execute(self, context):
        redshift_hook = PostgresHook("redshift")
        self.log.info(f'Refreshing {self.table} fact table...')

        sql = """
            INSERT INTO {table}
            {select_sql};
        """.format(table=self.table, select_sql=self.select_sql)

        redshift_hook.run(sql)
        self.log.info("Data refresh complete.")
