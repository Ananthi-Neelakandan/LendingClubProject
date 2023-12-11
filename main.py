import sys
from lib import dataManipulation, dataReader, utils
from pyspark.sql.functions import *

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Please specify the environment")
        sys.exit(-1)
job_run_env = sys.argv[1]
print("Creating Spark Session")
spark = utils.get_spark_session(job_run_env)
print("Created Spark Session")
customer_df = dataReader.read_customers(spark, job_run_env)
loans_df = dataReader.read_loans_schema(spark, job_run_env)
loans_repayment_df = dataReader.read_loan_repay_schema(spark, job_run_env)
loan_defaulters_df = dataReader.read_loan_defaulters_schema(spark, job_run_env)
loan_public_records_df = dataReader.read_public_records_schema(spark, job_run_env)
first_case = dataManipulation.first_score(loans_df, loans_repayment_df)
print(first_case)