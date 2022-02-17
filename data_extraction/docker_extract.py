import docker
from psycopg2 import connect
import pandas as pd

table_name = "player_v"

# declare connection instance
conn = connect(
    dbname = "tcb",
    user="postgres",
    host="localhost",
    port=5432,
    password="postgres"
    
)
cursor = conn.cursor()
# df = pd.read_sql_query("SELECT date FROM tcb.match WHERE date between '2015-01-01' and '2021-12-31' LIMIT 20",con=conn)
cursor.execute("CREATE TABLE new_table AS (SELECT MA.date,match_id,winner_id,loser_id,score FROM tcb.match MA WHERE MA.date between '2015-01-01' and '2021-12-31');")
df = pd.read_sql_query("SELECT NT.date,NT.winner_id,NT.match_id,MT.surface,NT.score FROM new_table NT,tcb.match as MT WHERE NT.match_id=MT.match_id",con=conn)

print(df)

conn.close()