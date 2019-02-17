

#This file contains functions to be called in main.py

import os
import sqlite3
from sqlite3 import Error
import pandas as pd
import datetime

def list_and_compare_files(to_load_path,loaded_path):
    """
    Function to compare .csvs in loading directory and
    db imports table
    """

    # lists files to be loaded
    to_load_files = os.listdir(to_load_path)

    # lists loaded files
    loaded_files = os.listdir(loaded_path)

    # Checks for files that have already been loaded in to_load_path
    # This is in case a duplicate file is dropped, and returns list
    return [i for i in to_load_files if i not in loaded_files]


def move_to_loaded(loading_files):
    """
    Function moves loaded files to the
    files_to_load/Loaded directory
    """
    for file in loading_files:
        os.rename("files_to_load/To Load/" + 'file', "files_to_load/Loaded/" + 'file')



def create_connection(db_file):
    """
    This creates a connection to the SQLite database
    """
    try:
        conn = sqlite3.connect(db_file,isolation_level=None)
        print('Connection Success')
        return conn
    except Error as e:
        print(e)
    return None


def query(conn,sql_path):
    """

    :param conn:
    :param sql_path:
    :return:
    """

    cur = conn.cursor()
    fd = open(sql_path, 'r')
    sqlFile = fd.read()
    fd.close()
    cur.execute(sqlFile)
    #sql_commands = sqlFile.splt(';')
    rows = cur.fetchall()
    return rows
    cur.close()


def load_to_df(csv_path):

    # load raw csv to a dataframe
    df = pd.read_csv(csv_path)

    # creates a _key
    df['key'] =  df["street"].map(str) + df["sq__ft"].map(str)  + df["sale_date"].map(str) + df["price"].map(str)

    # sets the _key as the index
    df.set_index('key', inplace=True, verify_integrity= False)

    # deduplicates data based on _key
    df = df.groupby('key').first().reset_index()

    # Adds the _key to the columns for importing to db
    df['_key'] = df['key']

    # Adds source file
    df['source_file'] = csv_path

    # Adds import time
    df['import_time'] = datetime.datetime.now()

    return df


def df_to_db(table_name,conn):

    df.to_sql(table_name, conn, if_exists="replace")