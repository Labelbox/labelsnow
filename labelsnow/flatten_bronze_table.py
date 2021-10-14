import pandas as pd
import logging

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

    logging.info("flatten_bronze_table: Returning flattened table.")
    return df.set_index("index")