import json
import urllib
import pandas as pd
import logging
from labelsnow.constants import LABELBOX_DEFAULT_TYPE_DICTIONARY

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

    logging.info("Returning annotations DataFrame from Labelbox")
    return df
