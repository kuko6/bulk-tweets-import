import psycopg
import os
from dotenv import load_dotenv

load_dotenv()

def connect() -> psycopg.Connection:
    """ Connects to the Postgres database """
    
    conn = None
    try:
        # print('Connecting to the PostgreSQL database...')
        conn = psycopg.connect(
            host=os.getenv('DBHOST'),
            dbname=os.getenv('DBNAME'),
            user=os.getenv('DBUSER'),
            password=os.getenv('DBPSSWD')
        )

    except (Exception, psycopg.DatabaseError) as error:
        print(error)
    
    return conn


if __name__ == '__main__':
    connect()

