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
cursor.execute("CREATE TABLE new_table AS (SELECT MA.date,match_id,winner_id,loser_id FROM tcb.tournament_event TE,tcb.match MA where TE.tournament_id=MA.tournament_event_id and TE.date between '2015/01/01' and '2021/12/31');")
df = pd.read_sql_query("SELECT NT.date,NT.winner_id,NT.match_id,MT.surface FROM new_table NT,tcb.match as MT WHERE NT.match_id=MT.match_id",con=conn)
print(df.drop_duplicates())
# declare a cursor object from the connection


# execute an SQL statement using the psycopg2 cursor object
# cursor.execute("SELECT goat_rank, name,twitter, goat_points FROM player_v ORDER BY goat_points DESC NULLS LAST LIMIT 20;")
# cursor.execute("SELECT DISTINCT name,tournament_id FROM tcb.tournament_event WHERE date between '2015/01/01' and '2021/12/31';")
# cursor.execute("CREATE TABLE new_table AS (SELECT match_id,winner_id,loser_id FROM tcb.tournament_event TE,tcb.match MA where TE.tournament_id=MA.tournament_event_id and TE.date between '2015/01/01' and '2021/12/31');")
# cursor.execute("SELECT NT.winner_id,NT.match_id,MT.surface FROM new_table NT,tcb.match as MT WHERE NT.match_id=MT.match_id")

# cursor.execute("SELECT match_id,winner_id,loser_id FROM tcb.tournament_event TE,tcb.match MA where TE.tournament_id=MA.tournament_event_id and TE.date between '2015/01/01' and '2021/12/31';")

# tables=[]
# # enumerate() over the PostgreSQL records
# for i, record in enumerate(cursor):
#     print(record)

# # close the cursor object to avoid memory leaks
# cursor.close()

# close the connection as well
conn.close()