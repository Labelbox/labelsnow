# Labelbox Connector for Snowflake

Access the Labelbox Connector for Snowflake to connect an unstructured dataset to Labelbox, programmatically set up an ontology for labeling, and load the labeled dataset into your Snowflake environment. 

Labelbox is the enterprise-grade training data solution with fast AI enabled labeling tools, labeling automation, human workforce, data management, a powerful API for integration & SDK for extensibility. Visit [Labelbox](http://labelbox.com/) for more information.

This library is currently in beta. It may contain errors or inaccuracies and may not function as well as commercially released software. Please report any issues/bugs via [Github Issues](https://github.com/Labelbox/labelsnow/issues).


## Table of Contents

* [Requirements](#requirements)
* [Installation](#installation)
* [Documentation](#documentation)
* [Authentication](#authentication)
* [Contribution](#contribution)

## Requirements

* [Snowflake account with credentials](https://signup.snowflake.com/)
* [Snowflake SDK](https://pypi.org/project/snowflake-connector-python/)
* [Labelbox account](http://app.labelbox.com/)
* [Generate a Labelbox API key](https://labelbox.com/docs/api/getting-started#create_api_key)

## Installation

Install LabelSnow to your Python environment. The installation will also add the Labelbox SDK, a requirement for LabelSnow to function. LabelSnow is available via pypi: 

```
pip install labelsnow
```

## Documentation

LabelSnow includes several methods to help facilitate your workflow between Snowflake and Labelbox. 

1. Create your dataset in Labelbox from your Unstructured Data stage in Snowflake: 

```
sf_dataframe = labelsnow.get_snowflake_datarows(snowflake_cursor, "name_of_snowflake_stage", 604800) #604800 is signed_URL expiration time in Snowflake

my_demo_dataset = labelsnow.create_dataset(labelbox_client=lb_client, snowflake_pandas_dataframe=sf_dataframe, dataset_name="SF Test")
```
Where "sf_dataframe" is a pandas dataframe of unstructured data with asset names and asset URLs in two columns, named "external_id" and "row_data" respectively. my_demo_dataset labelsnow.create_dataset() returns a Labelbox Dataset python object. 

| external_id | row_data                             |
|-------------|--------------------------------------|
| image1.jpg  | https://url_to_your_asset/image1.jpg |
| image2.jpg  | https://url_to_your_asset/image2.jpg |
| image3.jpg  | https://url_to_your_asset/image3.jpg |

2. Get your annotations from Labelbox as a Pandas DataFrame. 
```
bronze_df = labelsnow.get_annotations(lb_client, "insert_project_id_here")
```

3. You can use the our flattener to flatten the "Label" JSON column into component columns, or use the silver table method to produce a more queryable table of your labeled assets. Both of these methods take in the bronze table of annotations from above: 

```
flattened_table = labelsnow.flatten_bronze_table(bronze_df)
queryable_silver_DF =labelsnow.silver_table(bronze_df)
```
### Depositing your tables into Snowflake

We also include a helper function `put_tables_into_snowflake` that can help you quickly load Pandas tables into Snowflake. It takes in a dictionary of Pandas tables, creates tables, and loads the data.

```
my_table_payload = {"BRONZE_TABLE": bronze_df,
                    "FLATTENED_BRONZE_TABLE": flattened_table,
                    "SILVER_TABLE": silver_table}
                    
ctx = snowflake.connector.connect(
        user=credentials.user,
        password=credentials.password,
        account=credentials.account,
        warehouse="name_of_warehouse",
        database="SAMPLE_DB",
        schema="PUBLIC"
    )

labelsnow.put_tables_into_snowflake(ctx, my_table_payload)
```

### How To Get Video Project Annotations

Because Labelbox Video projects can contain multiple videos, you must use the `get_videoframe_annotations` method to return an array of Pandas DataFrames for each video in your project. Each DataFrame contains frame-by-frame annotation for a video in the project: 

```
video_bronze = labelsnow.get_annotations(lb_client, "insert_video_project_id_here") #sample completed video project
video_dataframe_framesets = labelsnow.get_videoframe_annotations(video_bronze, LB_API_KEY)
```

You may use standard Python code to  iteratively to create your flattened bronze tables and silver tables: 
```
silver_video_dataframes = {} 

video_count = 1
for frameset in video_dataframe_framesets:
    silver_table = labelsnow.silver_table(frameset)
    silver_table_with_datarowid = pd.merge(silver_table, video_bronze, how = 'inner', on=["DataRow ID"])
    video_name = "VIDEO_DEMO_{}".format(video_count)
    silver_video_dataframes[video_name] = silver_table_with_datarowid
    video_count += 1
```
Then deposit these Pandas dataframes into Snowflake with `put_tables_into_snowflake`


While using LabelSnow, you will likely also use the Labelbox SDK (e.g. for programmatic ontology creation). These resources will help familiarize you with the Labelbox Python SDK: 
* [Visit our docs](https://labelbox.com/docs/python-api) to learn how the SDK works
* View our [LabelSnow demo code](https://github.com/Labelbox/labelsnow/tree/main/demo) for inspiration.
* view our [API reference](https://labelbox.com/docs/python-api/api-reference).

## Authentication

Labelbox uses API keys to validate requests. You can create and manage API keys on [Labelbox](https://app.labelbox.com/account/api-keys). 

## Contribution
Please consult `CONTRIB.md`


