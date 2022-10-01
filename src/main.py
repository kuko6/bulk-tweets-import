import os
import json
import gzip
import time
from config.connect import connect

def main():
    conn = connect()
    if conn == None:
        print('Connection to the database failed :(')
        return

    # conn.execute(f"""INSERT INTO authors (id, name, username, description, followers_count, following_count, tweet_count, listed_count) 
    # VALUES {(100, 'Kuko', 'kuko6', 'Im the best', 10000, 20, 10, 1)}, {(12, 'Kuko', 'kuko6', 'Im the best', 10000, 20, 10, 1)};""")
    
    # this probably isnt that good since you cant copy multiple at once
    # cur = conn.cursor()
    # with cur.copy("COPY authors (id, name, username, description, followers_count, following_count, tweet_count, listed_count) FROM STDIN with delimiter ','") as copy:
    #     copy.write("22,'Kuko','kuko6','Im the best',10000,20,10,1\n2,'Kuko','kuko6','Im the best',10000,20,10,1")

    # with cur.copy("COPY authors (id, name, username, description, followers_count, following_count, tweet_count, listed_count) FROM STDIN") as copy:
    #     copy.write_row((2135163, 'Kuko', 'kuko6', 'Im the best', 10000, 20, 10, 1))

    cur = conn.execute("SELECT id FROM authors;")
    start_time = time.time()
    inserted_ids = set()
    for id in cur.fetchall():
        inserted_ids.add(str(id[0]))
    print(f'Table already contains: {len(inserted_ids)} rows')
    print(f'Time wasted with getting this information: {round(time.time() - start_time, 3)}s')
        
    cur = conn.cursor()
    inserted_rows = 0
    duplicate_rows = 0
    with gzip.open('./tweets/authors.jsonl.gz') as file:
        with cur.copy("COPY authors (id, name, username, description, followers_count, following_count, tweet_count, listed_count) FROM stdin;") as copy:
            for line in file:
                author = json.loads(line)
                if author['id'] in inserted_ids:
                    duplicate_rows += 1
                    continue

                print(inserted_rows, duplicate_rows)
                copy.write_row((author['id'], author.get('name').replace("\x00", ""), author.get('username').replace("\x00", ""),
                    author.get('description').replace("\x00", ""), author.get('public_metrics').get('followers_count'), 
                    author.get('public_metrics').get('following_count'), author.get('public_metrics').get('tweet_count'), 
                    author.get('public_metrics').get('listed_count')))
                # print((author['id'], author.get('name'), author.get('username'), author.get('description'), author.get('public_metrics').get('followers_count'), author.get('public_metrics').get('following_count'), author.get('public_metrics').get('tweet_count'), author.get('public_metrics').get('listed_count')))
                
                inserted_ids.add(author['id'])
                inserted_rows += 1

                if inserted_rows%100000 == 0: 
                    print(f'Execution after {inserted_rows} rows: {round(time.time() - start_time, 3)}s')

        conn.commit()

    print(f'Total execution time: {round(time.time() - start_time, 3)}s')
    print(f'Total number of inserted rows: {inserted_rows}')
    print(f'Total number of duplicate rows: {duplicate_rows}')

    # close the connection
    conn.close()


if __name__ == '__main__':
    print(os.getcwd())
    main()
