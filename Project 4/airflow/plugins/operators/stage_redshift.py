from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class StageToRedshiftOperator(BaseOperator):
    template_fields=("aws_s3_key",)
    ui_color = '#358140'
    copy_sql = """COPY {} FROM '{}' ACCESS_KEY_ID '{}' SECRET_ACCESS_KEY '{}' IGNOREHEADER {} json '{}';"""

    @apply_defaults
    def __init__(self,
                 redshift_id="",
                 aws_cred="",
                 table_name="",
                 aws_s3_bucket="",
                 aws_s3_key="",
                 json="auto",
                 autocommit=False,
                 ignore_header=1,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table_name = table_name
        self.redshift_id = redshift_id
        self.aws_s3_bucket = aws_s3_bucket
        self.aws_s3_key = aws_s3_key
        self.json = json
        self.autocommit = autocommit
        self.ignore_header = ignore_header
        self.aws_cred = aws_cred

    def execute(self, context):
        aws_hook = AwsHook(self.aws_cred)
        credentials = aws_hook.get_credentials()
        aws_access_key = credentials.access_key
        aws_secret_key = credentials.secret_key
        
        postgrs_hook = PostgresHook(postgres_conn_id=self.redshift_id)
        postgrs_hook.run("DROP TABLE {} IF EXISTS".format(self.table_name), self.autocommit)
        
        aws_s3_path = "s3://{}/{}".format(self.aws_s3_bucket, self.aws_s3_key.format(**context))
        
        formatted_sql = StageToRedshiftOperator.copy_sql.format(self.table_name,
                                                                aws_s3_path,
                                                                aws_access_key,
                                                                aws_secret_key,
                                                                self.ignore_header,
                                                                self.json)
        
        postgrs_hook.run(formatted_sql, self.autocommit)




