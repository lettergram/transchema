import sys, getopt
import psycopg2
import psycopg2.extras

class PGDBInterface:
    
    def __init__(self, database, user, password, host, port, sslmode=None):

        # Connect 
        self.conn = None

        # Attempt connection to database
        self.create_db_connection(database, user, password, host, port, sslmode)

        # Ensures commits ASAP, not waiting for transactions
        self.conn.autocommit = True

        # Creates tables, from which the DB will be read/created
        self.tables = {}
        

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
            
    def get_database_schema(self):

        """
        PostgreSQL reader, prints & returns the tables
        """
        
        cursor = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cursor.execute("""
        SELECT table_schema, table_name, table_type
        FROM information_schema.tables
        WHERE table_schema != 'pg_catalog'
        AND table_schema != 'information_schema'
        AND table_type='BASE TABLE'
        ORDER BY table_schema, table_name
        """)

        tables = cursor.fetchall()

        # Obtain columns for each table
        for table in tables:
            
            where_dict = {
                "table_schema": table['table_schema'],
                "table_name": table['table_name']
            }
            
            cursor.execute("""
            SELECT column_name, ordinal_position, 
            is_nullable, data_type, character_maximum_length
            FROM information_schema.columns
            WHERE table_schema = %(table_schema)s
            AND table_name   = %(table_name)s
            ORDER BY ordinal_position
            """, where_dict)
            
            table['columns'] = cursor.fetchall()
            
        cursor.close()

        self.tables = tables        
        
        return self.tables


    def print_database(self):
        for table in self.tables:
            max_col_widths = {
                "column_name":0, "data_type":0,
                "is_nullable":0, "character_maximum_length":0
        
            }
            for column in table["columns"]:
                for key in max_col_widths.keys():
                    if not column[key]:
                        column[key] = ""
                        
                    if len(column[key]) > max_col_widths[key]:
                        max_col_widths[key] = len(column[key])

            max_width = 0
            for key in max_col_widths.keys():
                max_width += max_col_widths[key]

            print("\n{} | {} | {}".format(
                table["table_schema"], table["table_name"], table["table_type"]
            ))
            print("-"*(max_width+6))
            for column in table["columns"]:
                s  = f'{column["column_name"]: <{max_col_widths["column_name"]}} |'
                s += f'{column["data_type"]: <{max_col_widths["data_type"]}} |'
                s += f'{column["is_nullable"]: <{max_col_widths["is_nullable"]}} |'
                s += f'{column["character_maximum_length"]: <{max_col_widths["character_maximum_length"]}}'
                print(s)
    

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
    
    pgreader.get_database_schema()

    pgreader.print_database()

    pgreader.conn.close()


if __name__ == '__main__':
    main(sys.argv)
