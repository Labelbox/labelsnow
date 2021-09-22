#!/usr/bin/env python
import snowflake.connector
import pandas as pd
import labelbox
import labelsnow

#for debugging
# pd.set_option('display.max_columns', None)

#Authenticate into Labelbox and Snowflake
import credentials
# Enter your Labelbox API key here or store them in a separate file ignored by git
LB_API_KEY = credentials.LB_API_KEY
lb = labelbox.Client(api_key=LB_API_KEY)
#dataset = lb.create_dataset(name="SF Test")

#Enter your Snowflake credentials here or store them in a separate file ignored by git
ctx = snowflake.connector.connect(
    user=credentials.user,
    password=credentials.password,
    account=credentials.account
    )
cs = ctx.cursor()

#Upload Sample Unstructured Data Files
# try:
#     cs.execute("USE DATABASE SAMPLE_NL")
#     cs.execute("create or replace stage my_stage directory = (enable = true) encryption = (type = 'SNOWFLAKE_SSE')")
#     cs.execute("put file:////tmp/SFImages/*.jpeg @my_stage AUTO_COMPRESS=False") #add all images in my local directory
#     cs.execute("alter stage my_stage refresh")
#     cs.execute("select * from directory(@my_stage)")
#     cs.execute("select relative_path as external_id, get_presigned_url(@my_stage, relative_path, 604800) as row_data from directory(@my_stage)")
#     df = cs.fetch_pandas_all()
# finally:
#     cs.close()
# ctx.close()

# Create dataset in Labelbox
#my_demo_dataset = labelsnow.create_dataset(labelbox_client=lb, snowflake_pandas_dataframe=df, dataset_name="SF Test")

#Get annotations dataframe from Labelbox for a demo project (returns Pandas dataframes)
bronze_df = labelsnow.get_annotations(lb, "cktrls5t7379d0y9i9pv8cicu")
flattened_table = labelsnow.flatten_bronze_table((bronze_df))
silver_table =labelsnow.silver_table(bronze_df)

from snowflake.connector.pandas_tools import write_pandas
import warnings
warnings.simplefilter(action='ignore', category=UserWarning) #I do this to suppress Pandas' outdated warnings
def put_tables_in_snowflake(your_tables):
    ctx = snowflake.connector.connect(
        user=credentials.user,
        password=credentials.password,
        account=credentials.account,
        warehouse="COMPUTE_WH",
        database="SAMPLE_NL",
        schema="PUBLIC"
    )
    cs = ctx.cursor()

    for table_name in your_tables.keys():
        try:
            sql_command = pd.io.sql.get_schema(your_tables[table_name], table_name)
            insertion_index = sql_command.find("TABLE")
            sql_command = sql_command[:insertion_index] + "OR REPLACE " + sql_command[insertion_index:]
            cs.execute(sql_command)
            success, nchunks, nrows, _ = write_pandas(ctx, your_tables[table_name], table_name)
        finally:
            #check for correctness
            cs.execute("SELECT * FROM " + table_name)
            df2 = cs.fetch_pandas_all()
            print(df2)
    cs.close()
    ctx.close()

my_table_payload = {"BRONZE_TABLE": bronze_df,
                    "FLATTENED_BRONZE_TABLE": flattened_table,
                    "SILVER_TABLE": silver_table}

put_tables_in_snowflake(my_table_payload)

def cleanup():
    print("Cleanup method here")
    #delete dataset
    #delete project
