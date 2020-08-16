import sys, getopt
import psycopg2
import psycopg2.extras

class PGDBInterface:
    
    def __init__(self, database, user, password, host, port, sslmode=None):


        # Connect & ensures commits ASAP, not waiting for transactions
        self.conn            = None
        self.conn.autocommit = True

        # Attempt connection to database
        self.create_db_connection(database, user, password, host, port, sslmode)
        

    def create_db_connection(self, database, user, password, host, port, sslmode=None):

        """
        Make a connection to the (postgresql) database.        
        """

        self.database = database
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.sslmode = sslmode
        
        try:
            if sslmode:
                self.conn = psycopg2.connect(database=database, user=user,
                                             password=password, host=host,
                                             port=port, sslmode=sslmode)
            else:
                self.conn = psycopg2.connect(database=database, user=user,
                                             password=password, host=host,
                                             port=port)
        except Exception as err:
            print("I am unable to connect to the database.")
            print(err)
            exit()

        print("Connected to database!")

        
class PGSchemaReader(PGDBInterface):
            
    def get_tables(self):

        """
        PostgreSQL reader, prints & returns the tables
        """
        
        cursor = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cursor.execute("""
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema != 'pg_catalog'
        AND table_schema != 'information_schema'
        AND table_type='BASE TABLE'
        ORDER BY table_schema, table_name
        """)

        tables = cursor.fetchall()

        cursor.close()
        
        for row in tables:
            
            print("{}.{}".format(row["table_schema"], row["table_name"]))

        return tables


def main(argv):

    params = {
        "database":None,
        "user":None,
        "password":None,
        "host":None,
        "port":5432,
        "sslmode":None
    }
    
    for i in range(1, len(argv)):
        params[list(params.keys())[i-1]] = argv[i]
    
    pgreader = PGSchemaReader(
        database=params["database"],
        user=params["user"],
        password=params["password"],
        host=params["host"],
        port=params["port"],
        sslmode=params["sslmode"]
    )
    
    tables = pgreader.get_tables()

    pgreader.conn.close()


if __name__ == '__main__':
    main(sys.argv)
