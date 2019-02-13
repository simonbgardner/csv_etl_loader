import pandas as pd

csv_path = 'Sacramentorealestatetransactions.csv'

df = pd.read_csv(csv_path)

#print(df.head())

df.to_sql("daily_flights", conn, if_exists="replace")


# create key
# deduplicate
# insert into a table