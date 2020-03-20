from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table_rowcounts_tests={},
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.table_rowcounts_tests = table_rowcounts_tests
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        self.log.info('Execution of DataQualityOperator started.')
        redshift = PostgresHook(self.redshift_conn_id)
        
        for table, exp_row_count in self.table_rowcounts_tests.items():
            self.log.info(f'Retrieving records from table {table}.')
            records = redshift.get_records(f'SELECT COUNT(*) FROM {table}')
            actual_row_count = records[0][0]
            if actual_row_count != exp_row_count:
                raise ValueError(f'Data quality check failed. {table} returned {actual_row_count} records instead of {exp_row_count} records.')
            self.log.info(f'Data quality on table {table} check passed with {actual_row_count} records.')   
            
        self.log.info('Data quality checks completed.')        