from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_id="",
                 sql_queries=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.sql_queries = sql_queries
        self.redshift_id = redshift_id

    def execute(self, context):
        postgrs_hook = PostgresHook("redshift")
        
        for query in self.sql_queries:
            query_records = postgrs_hook.get_records(query)
            self.log.info(query_records)