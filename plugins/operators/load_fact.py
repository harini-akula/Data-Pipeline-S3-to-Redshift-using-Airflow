from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

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
        redshift = PostgresHook(self.redshift_conn_id)
        formatted_insert_query=self.insert_query.format(self.facts_table_name, self.insert_facts_query)
        self.log.info('Loading fact table started.')
        redshift.run(formatted_insert_query)        
        self.log.info('Records inserted into {} table.'.format(self.facts_table_name))
