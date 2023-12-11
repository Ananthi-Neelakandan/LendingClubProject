from lib import configReader
#defining customers schema
def get_customers_schema():
    schema = "member_id string, emp_title string, emp_length int,\
    home_ownership string, annual_income float, address_state string,\
    address_zipcode string,grade string, sub_grade string,\
    verification_status string, total_high_credit_limit float,\
    application_type string, joint_status_verification string,\
    annual_income_joint string, ingest_date timestamp"
    return schema

# creating customers dataframe
def read_customers(spark,env):
    conf = configReader.get_app_config(env)
    customers_file_path = conf["customers.file.path"]
    return spark.read \
    .format("csv") \
    .option("header", "true") \
    .schema(get_customers_schema()) \
    .load(customers_file_path)

def get_loan_defaulters_schema():
    schema = "member_id string, delinq_2yrs int, delinq_amnt float,\
    mths_since_last_delinq float"
    return schema

#creating orders dataframe
def read_loan_defaulters_schema(spark,env):
    conf = configReader.get_app_config(env)
    orders_file_path = conf["loan_defaulters.file.path"]
    return spark.read \
    .format("csv") \
    .option("header", "true") \
    .schema(get_loan_defaulters_schema()) \
    .load(orders_file_path)

def get_loans_repay_schema():
    schema = "loan_id string, total_received_principle float, total_received_interset float,\
    total_received_late_fee float, total_payment float, last_payment_amount float,\
    last_payment_date string, next_payment_date string,ingest_date timestamp"
    return schema

#creating orders dataframe
def read_loan_repay_schema(spark,env):
    conf = configReader.get_app_config(env)
    orders_file_path = conf["loan_repayments.file.path"]
    return spark.read \
    .format("csv") \
    .option("header", "true") \
    .schema(get_loans_repay_schema()) \
    .load(orders_file_path)

def get_loans_schema():
    schema =  "loan_id string, member_id string, loan_amount float,\
    funded_amount float, long_term_year int, interest_rate float,\
    installement float, loan_issue_date string, loan_status string,\
    purpose string, loan_title string, ingest_date timestamp"
    return schema


#creating orders dataframe
def read_loans_schema(spark,env):
    conf = configReader.get_app_config(env)
    orders_file_path = conf["loan_data.file.path"]
    return spark.read \
    .format("csv") \
    .option("header", "true") \
    .schema(get_loans_schema()) \
    .load(orders_file_path)

def get_public_records_schema():
    schema = "member_id string,pub_rec int,pub_rec_bankruptcies int,inq_last_6mths int"
    return schema


#creating orders dataframe
def read_public_records_schema(spark,env):
    conf = configReader.get_app_config(env)
    orders_file_path = conf["loan_public_record.file.path"]
    return spark.read \
    .format("csv") \
    .option("header", "true") \
    .schema(get_public_records_schema()) \
    .load(orders_file_path)