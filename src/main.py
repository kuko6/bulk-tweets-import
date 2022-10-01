import os
import json
import gzip
import time
from typing import Set
from config.connect import connect
from psycopg import Connection


def import_authors(conn: Connection) -> Set:
    """ Imports Twitter accounts from `authors.jsonl.gz` into `authors` table """

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
        with cur.copy(
            "COPY authors (id, name, username, description, followers_count, following_count, tweet_count, listed_count) FROM stdin;"
        ) as copy:
            for line in file:
                author = json.loads(line)

                if author['id'] in inserted_ids:
                    duplicate_rows += 1
                    continue

                copy.write_row((author['id'], author.get('name').replace("\x00", ""), author.get('username').replace("\x00", ""),
                    author.get('description').replace("\x00", ""), author.get('public_metrics').get('followers_count'), 
                    author.get('public_metrics').get('following_count'), author.get('public_metrics').get('tweet_count'), 
                    author.get('public_metrics').get('listed_count')))

                inserted_ids.add(author['id'])
                inserted_rows += 1

                if inserted_rows%100000 == 0: 
                    print(f'Execution after {inserted_rows} rows: {round(time.time() - start_time, 3)}s')

        conn.commit()

    print(f'Total execution time: {round(time.time() - start_time, 3)}s')
    print(f'Total number of inserted rows: {inserted_rows}')
    print(f'Total number of duplicate rows: {duplicate_rows}')

    return inserted_ids


def import_conversations(conn: Connection, authors_ids: Set) -> None:
    """ Imports Tweets from `auconversationsthors.jsonl.gz` into `conversations` table """

    start_time = time.time()
    cur = conn.cursor()
    inserted_rows = 0
    duplicate_rows = 0
    missing_authors = set()
    duplicate_convos = set()
    with gzip.open('./tweets/conversations.jsonl.gz') as file:
        with cur.copy(
            "COPY conversations (id, author_id, content, possibly_sensitive, language, source, retweet_count, reply_count, like_count, quote_count, created_at) FROM stdin;"
        ) as copy:
            for line in file:
                conversation = json.loads(line)

                if conversation['id'] in duplicate_convos:
                    duplicate_rows += 1
                    continue

                if conversation['author_id'] not in authors_ids:
                    missing_authors.add(conversation['author_id'])

                copy.write_row((conversation['id'], conversation['author_id'], conversation['text'],
                    conversation['possibly_sensitive'], conversation['lang'], conversation['source'], 
                    conversation['public_metrics']['retweet_count'], conversation['public_metrics']['reply_count'],
                    conversation['public_metrics']['like_count'], conversation['public_metrics']['quote_count'],
                    conversation['created_at'])) 

                inserted_rows += 1
                duplicate_convos.add(conversation['id'])

                if inserted_rows%100000 == 0: 
                    print(f'Execution after {inserted_rows} rows: {round(time.time() - start_time, 3)}s')
                    #break
                
        for author in missing_authors:
            cur.execute('INSERT INTO authors VALUES (%s) ON CONFLICT DO NOTHING', (author,))
        print(f'Total number of missing authors: {len(missing_authors)}')
        cur.execute('ALTER TABLE conversations ADD CONSTRAINT fk_authors FOREIGN KEY (author_id) REFERENCES authors(id);')
        conn.commit()

    print(f'Total execution time: {round(time.time() - start_time, 3)}s')
    print(f'Total number of inserted rows: {inserted_rows}')
    print(f'Total number of duplicate rows: {duplicate_rows}')


def main():
    conn = connect()
    if conn == None:
        print('Connection to the database failed :(')
        return

    #authors_ids = import_authors(conn)

    cur = conn.execute("SELECT id FROM authors;")
    authors_ids = set()
    for id in cur.fetchall():
        authors_ids.add(str(id[0]))
    print('tu')
    import_conversations(conn, authors_ids)

    # close the connection
    conn.close()


if __name__ == '__main__':
    print(os.getcwd())
    main()
