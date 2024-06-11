import json
import pandas as pd

def get_videoframe_annotations(bronze_video_labels, project_id):
    """
    Retrieves frame-level annotations for a specific project from the bronze video labels.

    Args:
        bronze_video_labels (pandas.DataFrame): The bronze video labels dataframe.
        project_id (str): The ID of the project to retrieve annotations for.

    Returns:
        list: A list of bronze dataframes containing frame labels for the specified project.
    """
    # This method takes in the bronze table from get_annotations and produces
    # an array of bronze dataframes containing frame labels for each project
    # bronze_video_labels = bronze_video_labels.withColumnRenamed(
    #     "DataRow ID", "DataRowID")
    
    master_array_of_json_arrays = []
    for _, row in bronze_video_labels.iterrows():
        data = []
        project_labels = row["projects"][project_id]["labels"]
        for annot in project_labels:
            key_frame_feature_map = annot["annotations"]["key_frame_feature_map"]
            frames = annot["annotations"]["frames"]
            #each feature id has frame_values of the frames the feature id appears in
            for feature_id, frame_values in key_frame_feature_map.items():
                for frame in frame_values:
                    frame_objects = frames[str(frame)]["objects"]
                    # only append data if feature is a segmentation mask
                    if feature_id in frame_objects:
                        annotation_kind = frames[str(frame)]["objects"][feature_id]["annotation_kind"]
                        if annotation_kind == "VideoSegmentationMask":
                            data.append({
                                "DataRow ID": row["data_row"]["id"],
                                "Label": frames[str(frame)]["objects"][feature_id]
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
