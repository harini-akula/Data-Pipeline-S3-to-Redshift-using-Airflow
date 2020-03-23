from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    Description: This class represents an operator used to load data into
    fact table.

    Attributes:
        ui_color: color of task created by instantiating this class on 
            Airflow UI.
        insert_query: SQL query to insert records into fact table.
        redshift_conn_id: reference to a specific AWS redshift instance.  
        insert_facts_query: SQL query to select records to be inserted 
            into the fact table.
        facts_table_name: fact table name
        
    Methods:
        execute(self, context): inserts records into the given fact table
            on the given Redshift instance. 
    """
    ui_color = '#F98866'
    insert_query = """
    INSERT INTO {}
    {}
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",  
                 insert_facts_query="",
                 facts_table_name="",
                 *args, **kwargs):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.insert_facts_query=insert_facts_query
        self.facts_table_name=facts_table_name

    def execute(self, context):
        self.log.info('LoadFactOperator started.')
        # Creating a postgres hook object
        redshift = PostgresHook(self.redshift_conn_id)
        
        # Formatting insert query
        formatted_insert_query=self.insert_query.format(self.facts_table_name, self.insert_facts_query)
        
        self.log.info('Loading fact table started.')
        # Running query to insert records into fact table on Redshift
        redshift.run(formatted_insert_query)        
        self.log.info('Records inserted into {} table.'.format(self.facts_table_name))
