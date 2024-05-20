import pandas as pd
import logging
from labelsnow.constants import LABELBOX_DEFAULT_TYPE_DICTIONARY


def get_annotations(labelbox_client, project_id):
    """Request annotations for a specific project_id and produce a Snowflake-ready Pandas Dataframe"""
    project = labelbox_client.get_project(project_id)

    task = project.export_v2()
    task.wait_till_done()
    if task.errors:
        logging.warn("Error while exporting: ", task.errors)
    data = task.result
    df = pd.DataFrame.from_dict(data).astype(LABELBOX_DEFAULT_TYPE_DICTIONARY)

    logging.info("Returning annotations DataFrame from Labelbox")
    return df
