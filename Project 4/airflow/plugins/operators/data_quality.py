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
            check_sql = query['check_sql']
            expected_result = query['expected_result']
            query_records = postgrs_hook.get_records(check_sql)
            
            if query_records[0][0] != expected_result:
                raise ValueError("""Data quality check failed.\n
                                    Query run: '{}'\n
                                    Expected result: {}\n 
                                    Obtained result: {}""".format(check_sql, expected_result, query_records[0][0]))
            
            else:
                 self.log.info(query_records)