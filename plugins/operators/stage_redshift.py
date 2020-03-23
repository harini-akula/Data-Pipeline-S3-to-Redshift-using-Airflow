from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    """
    Description: This class represents an operator used to copy data from
    the S3 path to staging table on AWS Redshift.

    Attributes:
        ui_color: color of task created by instantiating this class on 
            Airflow UI.
        template_fields: a tuple with templated fields as elements.
        copy_query: Redshift copy query string to copy data from S3 to staging
            table.
        aws_conn_id: reference to a specific AWS instance.  
        redshift_conn_id: reference to a specific AWS Redshift instance.  
        table: staging table name.
        s3_bucket: name of S3 bucket containing data.
        s3_key: S3 key containing data.
        region: region of the S3 bucket containing data.
        json_format: format of the data in json file in given S3 path.       
        
    Methods:
        execute(self, context): copies data from json file in the given S3 
            path to the staging table on the given Redshift instance. 
    """
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_query = """
                    COPY {}
                    FROM '{}'
                    ACCESS_KEY_ID '{}'
                    SECRET_ACCESS_KEY '{}'
                    REGION '{}'
                    JSON '{}'
                 """
    
    @apply_defaults
    def __init__(self,
                 aws_conn_id="",
                 redshift_conn_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 region="",
                 json_format="auto",
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_conn_id=aws_conn_id
        self.redshift_conn_id=redshift_conn_id
        self.table=table
        self.s3_bucket=s3_bucket
        self.s3_key=s3_key
        self.region=region
        self.json_format=json_format

    def execute(self, context):
        self.log.info(f'Creating AWS connection for loading {self.table} table.')
        # Creating a AWS hook object
        aws_conn = AwsHook(self.aws_conn_id)
        self.log.info(f'Created AWS connection for loading {self.table} table.')
        
        # Retrieve aws credentials
        credentials = aws_conn.get_credentials()
        
        self.log.info(f'Creating Redshift connection for loading {self.table} table.')
        # Creating a postgres hook object
        redshift = PostgresHook(self.redshift_conn_id)
        self.log.info(f'Created Redshift connection for loading {self.table} table.')
        
        # Formatting S3 key using context variable
        rendered_key = self.s3_key.format(**context)
        
        # Formatting S3 path 
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        
        # Formatting copy query
        formatted_query = self.copy_query.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.json_format
        )
        
        self.log.info(f'Started loading data into {self.table} table from s3 path: {s3_path}.')
        # Running formatted copy query to load data from S3 to staging table
        redshift.run(formatted_query)
        self.log.info(f'Loaded data into {self.table} table from s3 path: {s3_path}.')
        