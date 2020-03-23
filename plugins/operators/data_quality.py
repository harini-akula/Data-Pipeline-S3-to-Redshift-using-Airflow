from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    Description: This class represents a data quality operator used to check
    quality of data loaded into a table.

    Attributes:
        ui_color: color of task created by instantiating this class on 
            Airflow UI.
        redshift_conn_id: reference to a specific AWS redshift instance.
        table_rowcounts_tests: dictionary object with table name as key
            and expected count of records in that table as value.       
        
    Methods:
        execute(self, context): validates if the actual count of records in
        the table match the expected count of records. 
    """
    ui_color = '#89DA59'

    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table_rowcounts_tests={},
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_rowcounts_tests = table_rowcounts_tests
        

    def execute(self, context):
        self.log.info('Execution of DataQualityOperator started.')
        # Creating a postgres hook object
        redshift = PostgresHook(self.redshift_conn_id) 
        
        # Comparing actual record count to expected record count for each table in the dictionary object.
        for table, exp_row_count in self.table_rowcounts_tests.items():
            self.log.info(f'Retrieving records from table {table}.')
            
            # Retrieving records from the given table
            records = redshift.get_records(f'SELECT COUNT(*) FROM {table}')
            
            # Retrieving record count from the given table
            actual_row_count = records[0][0]
            
            # Raising ValueError exception when actual record count does not match expected record count for a given table
            if actual_row_count != exp_row_count:
                raise ValueError(f'Data quality check failed. {table} returned {actual_row_count} records instead of {exp_row_count} records.')
                
            self.log.info(f'Data quality on table {table} check passed with {actual_row_count} records.')               
        self.log.info('Data quality checks completed.')        