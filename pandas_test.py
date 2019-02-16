import pandas as pd
import datetime

csv_path = 'Sacramentorealestatetransactions.csv'


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

# Next is to load df to db


sdf = load_to_df(csv_path)

print(sdf.head())


#df.to_sql("daily_flights", conn, if_exists="replace")