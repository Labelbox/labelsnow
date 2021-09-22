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

    return df.set_index("index")

def silver_table(df):
    flattened_bronze = flatten_bronze_table(df)

    # search for columns to explode/flatten
    s = (flattened_bronze.applymap(type) == list).all()
    list_columns = s[s].index.tolist() #generally yields ['Label_objects', 'Label_classifications', 'Label_relationships']

    video = False #this will be used for future video frame handling
    if "Label_frameNumber" in list_columns:
        video = True

    new_json = []
    for index, row in flattened_bronze.iterrows():
        my_dictionary = {}

        # classifications
        try:  # this won't work if there are no classifications
            for index, classification_json in enumerate(row["Label_classifications"]):
                title = classification_json["title"]
                if "answer" in classification_json:
                    if classification_json["answer"] is not None:  # if answer is null, that means it exists in secondary "answers" column
                        answer = classification_json["answer"]["title"]
                    else:
                        answer = classification_json["answers"] # TO-DO: FIX HANDLING OF CHECKLIST AND DROPDOWN
                else:
                    print("This line may be unnecessary")#answer = row["Label.classifications.answer.title"][index]
                my_dictionary = add_json_answers_to_dictionary(
                    title, answer, my_dictionary)
        except Exception as e:
            print("No classifications found.")

        # object counting
        try:  # this field won't work if the Label does not have objects in it
            for object in row.get("Label_objects", []):
                object_name = object["title"] + "_count"
                if object_name not in my_dictionary:
                    my_dictionary[object_name] = 1  # initialize with 1
                else:
                    my_dictionary[object_name] += 1  # add 1 to counter
        except Exception as e:
            print("No objects found.")

        my_dictionary["DataRow ID"] = row["DataRow ID"] # close it out
        if video:
            my_dictionary[
                "frameNumber"] = row["Label_frameNumber"]# need to store the unique framenumber identifier for video
        new_json.append(my_dictionary)

    parsed_classifications = pd.DataFrame(new_json)

    if video:
        print("To-Do")
        # need to inner-join with frameNumber to avoid creating N-squared datarows, since each frame has same DataRowID
        # joined_df = parsed_classifications.join(flattened_bronze,
        #                                         ["DataRow ID", "Label_frameNumber"],
        #                                         "inner")
    else:
        joined_df = pd.merge(parsed_classifications, flattened_bronze, how = 'inner', on="DataRow ID")
        # joined_df = parsed_classifications.join(flattened_bronze, ["DataRow ID"],
        #                                         "inner")

    return joined_df


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
def table_definitions_sql(table_name):

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

#helper methods

def is_json(myjson):
    try:
        json_object = json.loads(myjson)
    except Exception as e:
        return False
    return True

import ast
def add_json_answers_to_dictionary(title, answer, my_dictionary):
    try:  # see if I can read the answer string as a literal --it might be an array of JSONs
        convert_from_literal_string = ast.literal_eval(answer)
        if isinstance(convert_from_literal_string, list):
            for item in convert_from_literal_string:  # should handle multiple items
                my_dictionary = add_json_answers_to_dictionary(
                    title, item, my_dictionary)
            # recursive call to get into the array of arrays
    except Exception as e:
        pass

    if is_json(
            answer
    ):  # sometimes the answer is a JSON string; this happens when you have nested classifications
        parsed_answer = json.loads(answer)
        try:
            answer = parsed_answer["title"]
        except Exception as e:
            pass

    # this runs if the literal stuff didn't run and it's not json
    list_of_answers = []
    if isinstance(answer, list):
        for item in answer:
            list_of_answers.append(item["title"])
        answer = ",".join(list_of_answers)
    elif isinstance(answer, dict):
        answer = answer["title"]  # the value is in the title

    # Perform check to make sure we do not overwrite a column
    if title not in my_dictionary:
        my_dictionary[title] = answer

    return my_dictionary

