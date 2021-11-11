from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
import logging


def put_tables_into_snowflake(snowflake_connector, your_tables):
    """Takes in your SF Connector, and a dictionary of tables (key = table name) to deposit into Snowflake."""
    cs = snowflake_connector.cursor()

    for table_name in your_tables.keys():
        try:
            sql_command = pd.io.sql.get_schema(your_tables[table_name],
                                               table_name)
            insertion_index = sql_command.find("TABLE")
            sql_command = sql_command[:
                                      insertion_index] + "OR REPLACE " + sql_command[
                                          insertion_index:]
            cs.execute(sql_command)
            success, nchunks, nrows, _ = write_pandas(snowflake_connector,
                                                      your_tables[table_name],
                                                      table_name)
        finally:
            cs.execute("SELECT * FROM " + table_name)
            logging.info(
                "Finished writing tables to Snowflake, confirmed Select * works on table."
            )
    cs.close()
    snowflake_connector.close()
