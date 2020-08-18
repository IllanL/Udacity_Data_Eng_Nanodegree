from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_id="",
                 table_name="",
                 sql_query="",
                 autocommit = False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table_name=table_name
        self.redshift_id = redshift_id
        self.autocommit = autocommit
        self.sql_query=sql_query
        
    def execute(self, context):
    
        postgrs_hook = PostgresHook(postgres_conn_id=self.redshift_id)

        postgrs_hook.run("DROP TABLE {} IF EXISTS".format(self.table_name), self.autocommit)
        postgrs_hook.run("INSERT INTO {} {}".format(self.table_name, self.sql_query), self.autocommit)
