#!/usr/bin/env python
import snowflake.connector
import pandas as pd

import labelbox
import credentials
import labelsnow

#for debugging
pd.set_option('display.max_columns', None)

# Enter your Labelbox API key here
LB_API_KEY = credentials.LB_API_KEY
lb = labelbox.Client(api_key=LB_API_KEY)
#dataset = lb.create_dataset(name="SF Test")

# Gets the version

# ctx = snowflake.connector.connect(
#     user=credentials.user,
#     password=credentials.password,
#     account=credentials.account
#     )
# cs = ctx.cursor()
# try:
#     cs.execute("USE DATABASE SAMPLE_NL")
#     cs.execute("create or replace stage my_stage directory = (enable = true) encryption = (type = 'SNOWFLAKE_SSE')")
#     cs.execute("put file:////tmp/SFImages/bigbird.jpeg @my_stage AUTO_COMPRESS=False")
#     cs.execute("alter stage my_stage refresh")
#     cs.execute("select * from directory(@my_stage)")
#     cs.execute("select relative_path as external_id, get_presigned_url(@my_stage, relative_path, 604800) as row_data from directory(@my_stage)")
#     df = cs.fetch_pandas_all()
# finally:
#     cs.close()
# ctx.close()

#CREATE DATASET
# my_demo_dataset = labelsnow.create_dataset(labelbox_client=lb, snowflake_pandas_dataframe=df, dataset_name="SF Test")

#PUT ANNOTATIONS INTO SNOWFLAKE
bronze_df = labelsnow.get_annotations(lb, "cktrls5t7379d0y9i9pv8cicu")
print(bronze_df)
flattened_table = labelsnow.bronze_flattener((bronze_df))
flattened_table.to_csv("checkit.csv")
for col in flattened_table.columns:
    print(col)

from snowflake.connector.pandas_tools import write_pandas

#labelsnow.table_definition_sql("example_sql_definition")

ctx = snowflake.connector.connect(
    user=credentials.user,
    password=credentials.password,
    account=credentials.account,
    warehouse = "COMPUTE_WH",
    database = "SAMPLE_NL",
    schema = "PUBLIC"
    )
cs = ctx.cursor()
try:
    cs.execute("SELECT * FROM NLDEMO2")
    df2 = cs.fetch_pandas_all()
    # cs.execute("USE DATABASE SAMPLE_NL")
    # cs.execute("USE SCHEMA PUBLIC")
    #cs.execute("CREATE OR REPLACE TABLE HELLOWORLD")
    #success, nchunks, nrows, _ = write_pandas(ctx, bronze_df, 'NLDEMO2')
finally:
    print("foo")
    #cs.close()
ctx.close()


def cleanup():
    print("foo")
    #delete dataset
    #delete project
# External ID is recommended to identify your data_row via unique reference throughout Labelbox workflow.
# my_data_rows = []
# for index, row in df.iterrows():
#     my_data_rows.append(
#       {
#         "row_data": row["ROW_DATA"],
#         "external_id": row["EXTERNAL_ID"]})
#
# task = dataset.create_data_rows(my_data_rows)

# task.wait_till_done()
# print(task.status)

# create stage my_stage directory = (enable = true);
# put file:///<path> @my_stage;
#
# alter stage my_stage refresh;
# select * from directory(@stage);
#
# select get_presigned_url (@stage, relative_path) from directory(@stage);