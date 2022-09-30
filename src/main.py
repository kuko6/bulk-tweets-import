import os
import json
import gzip
from config.connect import connect

def main():
    conn = connect()
    if conn == None:
        print('Connection to database failed :(')
        return

    # # create a cursor
    cur = conn.cursor()
    
    # execute a statement
    print('PostgreSQL database version:')
    cur.execute('SELECT version()')

    # display the PostgreSQL database server version
    db_version = cur.fetchone()
    print(db_version)
    
    # close the communication with the PostgreSQL
    conn.close()

    # with gzip.open('./tweets/conversations.jsonl.gz') as file:
    #     for line in file:
    #         #print(line)
    #         new_json = json.loads(line)
    #         print(new_json)
    #         break


if __name__ == '__main__':
    print(os.getcwd())
    main()
