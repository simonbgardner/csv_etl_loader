
"""
Simple ETL loader that takes all .csv files from a directory and
loads it to a SQL db.

To ensure that a .csv file is not loaded twice, there will be an
imports table in the db to avoid duplication
"""

import etl_functions as etl

conn = etl.create_connection('db_name.db')


etl.query(conn,'query.sql')
#etl.query_insert(conn)
#etl.query_select(conn)

#cur.close()
conn.close()