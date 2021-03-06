from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

# Operator to check the quality of the data at its ultimate destination
# Parameter details:
## redshift_conn_id: Name of the redshift conneciton created using Airflow Admin
## check_stmts: The queries to be executed to check data quality.

class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift',
                 check_stmts=[],
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.check_stmts = check_stmts

    def execute(self, context):
        redshift_hook = PostgresHook("redshift")

        for stmt in self.check_stmts:
            result = int(redshift_hook.get_first(sql=stmt['sql'])[0])

            # check if equal
            if stmt['op'] == 'eq':
                if result != stmt['val']:
                    raise AssertionError(f"Validation failed: {result} {stmt['op']} {stmt['val']}")
            # check if not equal
            elif stmt['op'] == 'ne':
                if result == stmt['val']:
                    raise AssertionError(f"Validation failed: {result} {stmt['op']} {stmt['val']}")
            # check if greater than
            elif stmt['op'] == 'gt':
                if result <= stmt['val']:
                    raise AssertionError(f"Validation failed: {result} {stmt['op']} {stmt['val']}")

            self.log.info(f"Passed validation: {result} {stmt['op']} {stmt['val']}")
