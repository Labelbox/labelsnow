import requests
import json
import pandas as pd
from .constants import LABELBOX_DEFAULT_TYPE_DICTIONARY


def get_videoframe_annotations(bronze_video_labels, api_key):
    # This method takes in the bronze table from get_annotations and produces
    # an array of bronze dataframes containing frame labels for each project
    # bronze_video_labels = bronze_video_labels.withColumnRenamed(
    #     "DataRow ID", "DataRowID")

    # We manually build a string of frame responses to leverage our existing jsonToDataFrame code, which takes in JSON
    headers = {'Authorization': f"Bearer {api_key}"}
    master_array_of_json_arrays = []
    for index, row in bronze_video_labels.iterrows():
        response = requests.get(row.Label["frames"],
                                headers=headers,
                                stream=False)
        data = []
        for line in response.iter_lines():
            data.append({
                "DataRow ID": row["DataRow ID"],
                "Label": json.loads(line.decode('utf-8'))
            })
        massive_string_of_responses = json.dumps(data)
        master_array_of_json_arrays.append(massive_string_of_responses)

    array_of_bronze_dataframes = []
    for frameset in master_array_of_json_arrays:
        data = json.loads(frameset)  #parse the JSON into a dict
        # df = pd.json_normalize(data) #had to use this b/c the data is a list of json objects
        # df = df.astype(LABELBOX_DEFAULT_TYPE_DICTIONARY)
        df = pd.DataFrame.from_dict(data).astype(
            {'DataRow ID': 'string'})  #create pandas DF with proper col type
        array_of_bronze_dataframes.append(df)  #create array of bronze tables

    return array_of_bronze_dataframes
