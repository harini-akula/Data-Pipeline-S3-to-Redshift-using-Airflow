from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

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
        redshift = PostgresHook(self.redshift_conn_id)
        
        if not self.append_only_flag:            
            formatted_delete_query = self.delete_query.format(self.dim_table_name)
            self.log.info('Deletion of records from {} table started.'.format(self.dim_table_name)) 
            redshift.run(formatted_delete_query)
            self.log.info('Existing records from {} table are deleted.'.format(self.dim_table_name))            
            
        formatted_insert_query = self.insert_query.format(self.dim_table_name, self.select_dim_query)
        self.log.info('Insertion of records into {} table started.'.format(self.dim_table_name)) 
        redshift.run(formatted_insert_query)
        self.log.info('Records inserted into {} table.'.format(self.dim_table_name))
        
