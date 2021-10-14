import logging

def get_snowflake_datarows(snowflake_cursor, stage_name, signed_url_expiration = 604800):
    sql_string = ("select relative_path as external_id, "
                             "get_presigned_url(@{s_name}, relative_path, {s_expiration}) as row_data "
                             "from directory(@{s_name})".format(s_name = stage_name, s_expiration=signed_url_expiration))
    snowflake_cursor.execute(sql_string)
    logging.info("Executing query on Snowflake: " + sql_string)
    return snowflake_cursor.fetch_pandas_all()