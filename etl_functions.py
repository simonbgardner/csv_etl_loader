

#This file contains functions to be called in main.py

import os
import sqlite3
from sqlite3 import Error

def list_and_compare_files(to_load_path,loaded_path):
    """
    Function to compare .csvs in loading directory and
    db imports table
    """

    # list to be returned of files to load
    loading_files = []

    # lists files to be loaded
    to_load_files = os.listdir(to_load_path)

    # lists loaded files
    loaded_files = os.listdir(loaded_path)

    # Checks for files that have already been loaded in to_load_path
    # This is in case a duplicate file is dropped
    for file in to_load_files:
        if file not in loaded_files:
            loading_files.append(file)

    return loading_files

def move_to_loaded(loading_files):
    """
    Function moves loaded files to the
    files_to_load/Loaded directory
    """

    for file in loading_files:
        os.rename('file')



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
    cur = conn.cursor()
    fd = open(sql_path, 'r')
    sqlFile = fd.read()
    fd.close()
    cur.execute(sqlFile)
    #sql_commands = sqlFile.splt(';')
    rows = cur.fetchall()
    for row in rows:
        print(row)
    cur.close()


def query_select(conn):
    """
    Query all rows in the tasks table
    :param conn: the Connection object
    :return:
    """
    cur = conn.cursor()

    cur.execute("""SELECT * FROM daily_flights;""")
    rows = cur.fetchall()
    print(rows)
    for row in rows:
        print(row)
    cur.close()



def query_insert(conn):
    """
    Query all rows in the tasks table
    :param conn: the Connection object
    :return:
    """
    cur = conn.cursor()
    cur.execute("""INSERT
        INTO
        guru99(Id, Name) VALUES(1233, 'Simfon');""")
    cur.close()

def close_connection(db_file):

    return None