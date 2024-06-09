import sqlite3
import pandas as pd

def query_level1_table(db_filename):
    """Query the level1 table and display its contents using pandas."""
    conn = sqlite3.connect(db_filename)
    
    # Use pandas to read the level1 table into a DataFrame
    df = pd.read_sql_query('SELECT * FROM level1', conn)
    
    # Close the connection
    conn.close()
    
    # Display the DataFrame
    print("Contents of the level1 table:")
    print(df)
    
    return df

# Example usage
db_filename = 'wikilinksdata.db'
df_level1 = query_level1_table(db_filename)

# Optionally, display the DataFrame using a Jupyter Notebook if available
# display(df_level1)
