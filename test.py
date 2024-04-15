import pandas as pd
import sqlite3

# Create a DataFrame with a MultiIndex
index = pd.MultiIndex.from_tuples([('A', 1), ('A', 2), ('B', 1)], names=['group', 'value'])
data = {'foo': [10, 20, 30], 'bar': [100, 200, 300]}
df = pd.DataFrame(data, index=index)

# Write the DataFrame to a SQLite database
conn = sqlite3.connect('example.db')
df.to_sql('my_table', conn, if_exists='replace', index=True)

# Read the table back into a DataFrame
df_from_sql = pd.read_sql('SELECT * FROM my_table', conn)

print(df_from_sql)