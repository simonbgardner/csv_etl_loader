
"""
Simple ETL loader that takes all .csv files from a directory and
loads it to a SQL db.

To ensure that a .csv file is not loaded twice, there will be an
imports table in the db to avoid duplication
"""

import etl_functions as etl


     db_file = 'database.db'
to_load_path = 'files_to_load/To Load/'
 loaded_path = 'files_to_load/Loaded/'
  table_name = 'house_sales_revisions'
import_table = 'file_imports'

# creates a connection to the db
conn = etl.create_connection(db_file)

# reads db to read a list of loaded files
imported_files = etl.imports_query(conn)

# creates a list of files to be loaded
loading_files = etl.list_and_compare_files(to_load_path,imported_files)

# Loops through files and writes them to db
for file in loading_files:
    etl.load_to_df(file)
    etl.df_to_db(table_name)
    df = df['import_time'] + df['source_file']
    etl.df_to_db(import_table)

# Moves loaded files to the Loaded folder
etl.move_to_loaded(loading_files)

# Runs sql query to make production table
etl.query_executor('queries/house_sales.sql')



