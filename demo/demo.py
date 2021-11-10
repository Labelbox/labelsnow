#!/usr/bin/env python
import snowflake.connector
import pandas as pd
import labelbox
import labelsnow
import logging

#Authenticate into Labelbox and Snowflake
import credentials

#create a credentials.py file with these variables for this demo to run:
"""
user = "your_snowflake_username_here"
password = "your_snowflake_password_here"
account = "your_snowflake_account_URL_prefix_here" 
LB_API_KEY = "your_labelbox_API_key_here" 
"""

LB_API_KEY = credentials.LB_API_KEY
lb_client = labelbox.Client(api_key=LB_API_KEY)

#Enter your Snowflake credentials here or store them in a separate credentials file ignored by git
ctx = snowflake.connector.connect(
    user=credentials.user,
    password=credentials.password,
    account=credentials.account
    )
cs = ctx.cursor()

logging.getLogger().setLevel(logging.INFO) #comment to hide logs from your display

#Upload Sample Unstructured Data Files
#Important: This code refers to a folder in your local directory /tmp/SFImages/
try:
    cs.execute("USE DATABASE SAMPLE_NL")
    cs.execute("create or replace stage my_stage directory = (enable = true) encryption = (type = 'SNOWFLAKE_SSE')")
    logging.info("Begin uploading files to Snowflake stage.")
    cs.execute("put file:////Users/nicklee/SFImages/*.jpeg @my_stage AUTO_COMPRESS=False") #add all images to a top-level folder SFImages
    cs.execute("alter stage my_stage refresh")
    cs.execute("select * from directory(@my_stage)")
    df = labelsnow.get_snowflake_datarows(cs, "my_stage", 604800)
finally:
    cs.close()
ctx.close()

#Create dataset in Labelbox
#my_demo_dataset = labelsnow.create_dataset(labelbox_client=lb_client, snowflake_pandas_dataframe=df, dataset_name="SF Test")

#Get annotations dataframe from Labelbox for a demo project (returns Pandas dataframes)
#insert your own project ID from Labelbox in the get_annotations() method
bronze_df = labelsnow.get_annotations(lb_client, "ckut1646f0ry30zaoh49abz5p") #sample completed project
flattened_table = labelsnow.flatten_bronze_table(bronze_df)
silver_table =labelsnow.silver_table(bronze_df)

#from snowflake.connector.pandas_tools import write_pandas
import warnings
warnings.simplefilter(action='ignore', category=UserWarning) #This suppresses some outdated warnings from Pandas
# def put_tables_in_snowflake(snowflake_connector, your_tables):
#     """Takes in your SF Connector, and a dictionary of tables (key = table name) to deposit into Snowflake."""
#     cs = snowflake_connector.cursor()
#
#     for table_name in your_tables.keys():
#         try:
#             sql_command = pd.io.sql.get_schema(your_tables[table_name], table_name)
#             insertion_index = sql_command.find("TABLE")
#             sql_command = sql_command[:insertion_index] + "OR REPLACE " + sql_command[insertion_index:]
#             cs.execute(sql_command)
#             success, nchunks, nrows, _ = write_pandas(ctx, your_tables[table_name], table_name)
#         finally:
#             cs.execute("SELECT * FROM " + table_name)
#             logging.info("Finished writing tables to Snowflake, confirmed Select * works on table.")
#     cs.close()
#     snowflake_connector.close()

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

labelsnow.put_tables_into_snowflake(ctx, my_table_payload)

#videos demo for a project with multiple videos
video_bronze = labelsnow.get_annotations(lb_client, "ckurowgdr161e0zeh07x6hb6d") #sample completed video project
video_dataframe_framesets = labelsnow.get_videoframe_annotations(video_bronze, LB_API_KEY)
silver_video_dataframes = {}

video_count = 1
for frameset in video_dataframe_framesets:
    silver_table = labelsnow.silver_table(frameset)
    silver_table_with_datarowid = pd.merge(silver_table, video_bronze, how = 'inner', on=["DataRow ID"])
    video_name = "VIDEO_DEMO_{}".format(video_count)
    silver_video_dataframes[video_name] = silver_table_with_datarowid
    video_count += 1

ctx = snowflake.connector.connect(
        user=credentials.user,
        password=credentials.password,
        account=credentials.account,
        warehouse="COMPUTE_WH",
        database="SAMPLE_NL",
        schema="PUBLIC"
    )

labelsnow.put_tables_into_snowflake(ctx, silver_video_dataframes)

