import pandas as pd
import labelsnow
import logging
from labelsnow.flatten_bronze_table import flatten_bronze_table
from labelsnow.add_json_answers_to_dictionary import add_json_answers_to_dictionary

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

    logging.info("silver_table: Returning final table.")
    return joined_df