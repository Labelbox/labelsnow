import logging

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
    logging.info("{}: Dataset creation. Dataset ID: {}".format(upload_task.status, dataSet_new.uid))

    return dataSet_new