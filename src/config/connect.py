import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def connect():
    """ Connects to the Postgres database """
    
    conn = None
    try:
        conn = psycopg2.connect(
            host=os.getenv('DBHOST'),
            dbname=os.getenv('DBNAME'),
            user=os.getenv('DBUSER'),
            password=os.getenv('DBPSSWD')
        )

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    
    return conn

