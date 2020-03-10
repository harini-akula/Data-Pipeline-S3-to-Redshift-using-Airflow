from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
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
        aws_conn = AwsHook(self.aws_conn_id)
        credentials = aws_conn.get_credentials()
        
        redshift = PostgresHook(self.redshift_conn_id)
        
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_query = self.copy_query.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.json_format
        )
        redshift.run(formatted_query)
        