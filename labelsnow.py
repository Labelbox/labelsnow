import pandas as pd
import urllib

def create_dataset(labelbox_client, snowflake_pandas_dataframe, dataset_name):
    """Takes in a dataframe with the following column names: external_id, row_data
    # external_id is the asset name ex: "photo.jpg"
    # row_data is the signed URL to the asset
    returns: Labelbox client dataset object
    """
    dataSet_new = labelbox_client.create_dataset(name=dataset_name)
    snowflake_pandas_dataframe.columns = snowflake_pandas_dataframe.columns.str.lower()
    data_row_urls = [{
        "external_id": row['external_id'],
        "row_data": row['row_data']
    } for index, row in snowflake_pandas_dataframe.iterrows()]
    upload_task = dataSet_new.create_data_rows(data_row_urls)
    upload_task.wait_till_done()
    print("{}: Dataset creation. Dataset ID: {}".format(upload_task.status, dataSet_new.uid))

    return dataSet_new

LABELBOX_DEFAULT_TYPE_DICTIONARY = {
    'ID': 'string',
    'DataRow ID': 'string',
    'Labeled Data': 'string',
    'Created By': 'string',
    'Project Name': 'string',
    'Seconds to Label': 'float64',
    'External ID': 'string',
    'Agreement': 'int64',
    'Benchmark Agreement': 'int64',
    'Benchmark ID': 'string',
    'Dataset Name': 'string',
    'Reviews': 'object',
    'View Label': 'string',
    'Has Open Issues': 'int64',
    'Skipped': 'bool'
}

def flatten_bronze_table(df):

    df = df.reset_index()

    s = (df.applymap(type) == dict).all()
    dict_columns = s[s].index.tolist()

    while len(dict_columns) > 0:
        new_columns = []

        for col in dict_columns:
            # explode dictionaries horizontally, adding new columns
            horiz_exploded = pd.json_normalize(df[col]).add_prefix(f'{col}_')
            horiz_exploded.index = df.index
            df = pd.concat([df, horiz_exploded], axis=1).drop(columns=[col])
            new_columns.extend(horiz_exploded.columns) # inplace

        # check if there are still dict fields to flatten
        s = (df[new_columns].applymap(type) == dict).all()
        dict_columns = s[s].index.tolist()

    return df

import json
def get_annotations(labelbox_client, project_id):
    """Request annotations for a specific project_id and produce a Snowflake-ready Pandas Dataframe"""
    project = labelbox_client.get_project(project_id)
    with urllib.request.urlopen(project.export_labels()) as url:
        api_response_string = url.read().decode()  # this is a string of JSONs

    data = json.loads(api_response_string)
    df = pd.DataFrame.from_dict(data).astype(LABELBOX_DEFAULT_TYPE_DICTIONARY)

    #For some reason dtype dict does not convert timestamp reliably, so I must include these manual conversions
    df["Created At"] = pd.to_datetime(df["Created At"])
    df["Updated At"] = pd.to_datetime(df["Updated At"])

    return df

def bronze_to_silver(annotations_dataframe):
    """This method refines your annotations_dataframe into a more queryable state.
    annotations_dataframe is the output from get_annotations(labelbox_client, project_id)."""
    print("foo")

#SQL command to produce table for Labelbox annotations --write to a sql file and run in Snowflake
def table_definition_sql(table_name):
    print("""create or replace table {} (ID string,
 "DataRow ID" string,
 "Labeled Data" string, 
 "Label" string, 
 "Created By" string, 
 "Project Name" string,
 "Seconds to Label" float,
"External ID" string, 
"Agreement" integer,
"Benchmark ID" string,
"Benchmark Agreement" integer,
"Dataset Name" string,
"Reviews" string,
"View Label" string, 
"Has Open Issues" integer,
"Skipped" boolean,
"Created At" datetime,
"Updated At" datetime);""".format(table_name))