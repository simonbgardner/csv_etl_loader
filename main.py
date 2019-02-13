
"""
Simple ETL loader that takes all .csv files from a directory and
loads it to a SQL db.

To ensure that a .csv file is not loaded twice, there will be an
imports table in the db to avoid duplication
"""

import etl_functions as etl

conn = etl.create_connection('database.db')


#etl.query(conn,'query.sql')
#etl.query_insert(conn)
etl.query_select(conn)
import pandas as pd

csv_path = 'Sacramentorealestatetransactions.csv'

#df = pd.read_csv(csv_path)

#print(df.head())

#df.to_sql("daily_flights", conn, if_exists="replace")
#cur.close()
conn.close()