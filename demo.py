#!/usr/bin/env python
import snowflake.connector
import pandas as pd
import labelbox
import labelsnow
import logging

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

logging.getLogger().setLevel(logging.INFO) #uncomment to suppress logs from printing

#Upload Sample Unstructured Data Files
try:
    cs.execute("USE DATABASE SAMPLE_NL")
    cs.execute("create or replace stage my_stage directory = (enable = true) encryption = (type = 'SNOWFLAKE_SSE')")
    logging.info("Begin uploading files to Snowflake stage.")
    cs.execute("put file:////tmp/SFImages/*.jpeg @my_stage AUTO_COMPRESS=False") #add all images in my local directory
    cs.execute("alter stage my_stage refresh")
    cs.execute("select * from directory(@my_stage)")
    df = labelsnow.get_snowflake_datarows(cs, "my_stage", 604800)
    # cs.execute("select relative_path as external_id, get_presigned_url(@my_stage, relative_path, 604800) as row_data from directory(@my_stage)")
    # df = cs.fetch_pandas_all()
finally:
    cs.close()
ctx.close()

#Create dataset in Labelbox

my_demo_dataset = labelsnow.create_dataset(labelbox_client=lb, snowflake_pandas_dataframe=df, dataset_name="SF Test")

#Get annotations dataframe from Labelbox for a demo project (returns Pandas dataframes)
bronze_df = labelsnow.get_annotations(lb, "ckolzeshr7zsy0736w0usbxdj") #sample completed project
flattened_table = labelsnow.flatten_bronze_table((bronze_df))
silver_table =labelsnow.silver_table(bronze_df)

from snowflake.connector.pandas_tools import write_pandas
import warnings
warnings.simplefilter(action='ignore', category=UserWarning) #I do this to suppress Pandas' outdated warnings
def put_tables_in_snowflake(snowflake_connector, your_tables):
    """Takes in your SF Connector, and a dictionary of tables (key = table name) to deposit into Snowflake."""
    cs = snowflake_connector.cursor()

    for table_name in your_tables.keys():
        try:
            sql_command = pd.io.sql.get_schema(your_tables[table_name], table_name)
            insertion_index = sql_command.find("TABLE")
            sql_command = sql_command[:insertion_index] + "OR REPLACE " + sql_command[insertion_index:]
            cs.execute(sql_command)
            success, nchunks, nrows, _ = write_pandas(ctx, your_tables[table_name], table_name)
        finally:
            cs.execute("SELECT * FROM " + table_name)
            logging.info("Finished writing tables to Snowflake, confirmed Select * works on table.")
            # cs.execute("SELECT * FROM " + table_name)
            # df2 = cs.fetch_pandas_all()
    cs.close()
    snowflake_connector.close()

my_table_payload = {"BRONZE_TABLE": bronze_df,
                    "FLATTENED_BRONZE_TABLE": flattened_table,
                    "SILVER_TABLE": silver_table}

ctx = snowflake.connector.connect(
        user=credentials.user,
        password=credentials.password,
        account=credentials.account,
        warehouse="COMPUTE_WH",
        database="SAMPLE_NL",
        schema="PUBLIC"
    )

put_tables_in_snowflake(ctx, my_table_payload)

def cleanup():
    print("Cleanup method here")
    #delete dataset
    #delete project
