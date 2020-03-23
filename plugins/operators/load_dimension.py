from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    Description: This class represents an operator used to load data into
    dimension table in either append-only mode or delete-load mode.

    Attributes:
        ui_color: color of task created by instantiating this class on 
            Airflow UI.
        insert_query: SQL query to insert records into dimension table.
        delete_query: SQL query to delete records from dimension table.
        redshift_conn_id: reference to a specific AWS redshift instance.  
        select_dim_query: SQL query to select records to be inserted 
            into the dimension table.
        dim_table_name: dimension table name
        append_only_flag: indicator if records in dimension table should 
            be deleted before inserting selected records into the table.
        
    Methods:
        execute(self, context): inserts records into the given dimension
            table on the given Redshift instance either in append-only 
            or delete-load mode. 
    """
    ui_color = '#80BD9E'
    insert_query = """
    INSERT INTO {}
    {}
    """
    delete_query = """
        DELETE FROM {}
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',  
                 select_dim_query='',
                 dim_table_name='',
                 append_only_flag=True,
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id  
        self.select_dim_query=select_dim_query
        self.dim_table_name=dim_table_name
        self.append_only_flag=append_only_flag
        
    def execute(self, context):
        self.log.info('LoadDimensionOperator for {} table started.'.format(self.dim_table_name))
        # Creating a postgres hook object
        redshift = PostgresHook(self.redshift_conn_id)
        
        # Deleting existing records from dimension table when in delete-load mode
        if not self.append_only_flag:    
            # Formatting delete query
            formatted_delete_query = self.delete_query.format(self.dim_table_name)
            
            self.log.info('Deletion of records from {} table started.'.format(self.dim_table_name)) 
            # Running query to delete records from dimension table on Redshift
            redshift.run(formatted_delete_query)
            self.log.info('Existing records from {} table are deleted.'.format(self.dim_table_name))          
        
        # Formatting insert query
        formatted_insert_query = self.insert_query.format(self.dim_table_name, self.select_dim_query)
        
        self.log.info('Insertion of records into {} table started.'.format(self.dim_table_name)) 
        # Running query to insert records into dimension table on Redshift
        redshift.run(formatted_insert_query)
        self.log.info('Records inserted into {} table.'.format(self.dim_table_name))
        
