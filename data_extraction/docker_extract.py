import docker
from psycopg2 import connect

table_name = "player_v"

# declare connection instance
conn = connect(
    dbname = "tcb",
    user = "postgres",
    host = "localhost",
    port=5432,
    password = "postgres"
)

# declare a cursor object from the connection
cursor = conn.cursor()

# execute an SQL statement using the psycopg2 cursor object
cursor.execute("SELECT goat_rank, name,twitter, goat_points FROM tcb.player_v ORDER BY goat_points DESC NULLS LAST LIMIT 20;")

# enumerate() over the PostgreSQL records
for i, record in enumerate(cursor):
    print ("\n", type(record))
    print ( record )

# close the cursor object to avoid memory leaks
cursor.close()

# close the connection as well
conn.close()