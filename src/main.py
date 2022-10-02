import os
import json
import gzip
import time
from config.connect import connect
import psycopg
import csv

# different way of copying authors
# doesnt work because of the None spaces, which doesnt get separated :(
def import_authors2(conn: psycopg.Connection) -> set:
    """ Imports Twitter accounts from `authors.jsonl.gz` into `authors` table """

    start_time = time.time()
    inserted_authors = set()
    cur = conn.cursor()
    duplicate_rows = 0
    with gzip.open('./tweets/authors.jsonl.gz') as file:
        with cur.copy(
            "COPY authors (id, name, username, description, followers_count, following_count, tweet_count, listed_count) FROM stdin (DELIMITER ',');"
        ) as copy:
            query = ''
            for line in file:
                author = json.loads(line)

                if author['id'] in inserted_authors:
                    duplicate_rows += 1
                    continue
                
                row = (author['id'], author.get('name').replace("\x00", ""), author.get('username').replace("\x00", ""),
                    author.get('description').replace("\x00", ""), author.get('public_metrics').get('followers_count'), 
                    author.get('public_metrics').get('following_count'), author.get('public_metrics').get('tweet_count'), 
                    author.get('public_metrics').get('listed_count'))
                query += ','.join(str(val) for val in row) + '\n'

                inserted_authors.add(author['id'])

                if len(inserted_authors)%100000 == 0: 
                    copy.write(query)
                    query = ''
                    print(f'Execution after {len(inserted_authors)} rows: {round(time.time() - start_time, 3)}s')

        conn.commit()
        cur.close()

    print(f'Total execution time: {round(time.time() - start_time, 3)}s')
    print(f'Total number of inserted rows: {len(inserted_authors)}')
    print(f'Total number of duplicate rows: {duplicate_rows}')

    return inserted_authors


def import_authors(conn: psycopg.Connection) -> set:
    """ Imports Twitter accounts from `authors.jsonl.gz` into `authors` table """

    start_time = time.time()
    inserted_authors = set()
    # cur = conn.execute("SELECT id FROM authors;")
    # for id in cur.fetchall():
    #     inserted_ids.add(str(id[0]))
    # print(f'Table already contains: {len(inserted_ids)} rows')
    # print(f'Time wasted with getting this information: {round(time.time() - start_time, 3)}s')
        
    cur = conn.cursor()
    #inserted_rows = 0
    duplicate_rows = 0
    with gzip.open('./tweets/authors.jsonl.gz') as file:
        with cur.copy(
            "COPY authors (id, name, username, description, followers_count, following_count, tweet_count, listed_count) FROM stdin;"
        ) as copy:
            for line in file:
                author = json.loads(line)

                if author['id'] in inserted_authors:
                    duplicate_rows += 1
                    continue

                copy.write_row((author['id'], author.get('name').replace("\x00", ""), author.get('username').replace("\x00", ""),
                    author.get('description').replace("\x00", ""), author.get('public_metrics').get('followers_count'), 
                    author.get('public_metrics').get('following_count'), author.get('public_metrics').get('tweet_count'), 
                    author.get('public_metrics').get('listed_count')))

                inserted_authors.add(author['id'])
                #inserted_rows += 1

                if len(inserted_authors)%100000 == 0: 
                    print(f'Execution after {len(inserted_authors)} rows: {round(time.time() - start_time, 3)}s')

        conn.commit()
        cur.close()

    print(f'Total execution time: {round(time.time() - start_time, 3)}s')
    print(f'Total number of inserted rows: {len(inserted_authors)}')
    print(f'Total number of duplicate rows: {duplicate_rows}')

    return inserted_authors


def import_links(conn: psycopg.Connection, convo_id: int, links: list) -> None:
    cur = conn.cursor()
    with cur.copy("COPY links (conversation_id, url, title, description) FROM stdin;") as copy:
        for link in links:
            if len(link['expanded_url']) > 2048: continue
            copy.write_row((convo_id, link['expanded_url'], link.get('title'), link.get('description')))


def import_conversations(conn: psycopg.Connection, authors_ids: set) -> None:
    """ Imports Tweets from `auconversationsthors.jsonl.gz` into `conversations` table """

    start_time = time.time()
    cur = conn.cursor()
    #inserted_rows = 0
    duplicate_rows = 0
    # missing_authors = set()
    missing_authors_num = 0
    inserted_convos = set()
    with gzip.open('./tweets/conversations.jsonl.gz') as file:
        with cur.copy(
            "COPY conversations (id, author_id, content, possibly_sensitive, language, source, retweet_count, reply_count, like_count, quote_count, created_at) FROM stdin;"
        ) as copy:
            for line in file:
                conversation = json.loads(line)
                if conversation['id'] in inserted_convos:
                    duplicate_rows += 1
                    continue

                if conversation['author_id'] not in authors_ids:
                    # missing_authors.add(conversation['author_id'])
                    missing_authors_num += 1
                    cur.execute("INSERT INTO authors VALUES (%s) ON CONFLICT DO NOTHING", (conversation['author_id'],))

                copy.write_row((conversation['id'], conversation['author_id'], conversation['text'],
                    conversation['possibly_sensitive'], conversation['lang'], conversation['source'], 
                    conversation['public_metrics']['retweet_count'], conversation['public_metrics']['reply_count'],
                    conversation['public_metrics']['like_count'], conversation['public_metrics']['quote_count'],
                    conversation['created_at'])) 

                #inserted_rows += 1
                inserted_convos.add(conversation['id'])

                if conversation.get('entities') != None:
                    if conversation.get('entities').get('urls') != None:
                        import_links(conn, conversation['id'], conversation['entities']['urls'])

                if len(inserted_convos)%10000 == 0: 
                    print(f'Execution after {len(inserted_convos)} rows: {round(time.time() - start_time, 3)}s')
                    break

        # for author in missing_authors:
        #     cur.execute('INSERT INTO authors VALUES (%s) ON CONFLICT DO NOTHING', (author,))
        # print(f'Total number of missing authors: {len(missing_authors)}')
        print(f'Total number of missing authors: {missing_authors_num}')
        cur.execute('ALTER TABLE conversations ADD CONSTRAINT fk_authors FOREIGN KEY (author_id) REFERENCES authors(id);')
        cur.execute('ALTER TABLE links ADD CONSTRAINT fk_conversations FOREIGN KEY (conversation_id) REFERENCES conversations(id);')
        conn.commit()
        cur.close()

    print(f'Total execution time: {round(time.time() - start_time, 3)}s')
    print(f'Total number of inserted rows: {len(inserted_convos)}')
    print(f'Total number of duplicate rows: {duplicate_rows}')


def main():
    conn = connect()
    if conn == None:
        print('Connection to the database failed :(')
        return

    # authors_ids = import_authors(conn)
    # cur = conn.execute("SELECT id FROM authors;")
    # authors_ids = set()
    # for id in cur.fetchall():
    #     authors_ids.add(str(id[0]))
    # print('tu')
    import_conversations(conn, None)

    # close the connection
    conn.close()


def test():
    with gzip.open('./tweets/conversations.jsonl.gz') as file:
        a = 0
        for line in file:
            conversation = json.loads(line)
            #print(conversation['entities'].get('urls'))
            if conversation.get('entities') == None:
                continue
                
            if conversation.get('entities').get('urls') == None:
                continue
            
            #print(len(conversation['entities'].get('urls')))
            if len(conversation['entities'].get('urls')) > 1:
                print((conversation['entities'].get('urls')))
        print(a)


if __name__ == '__main__':
    # a = (100, 'Kuko', 'kuko6', 'Im the best', 10000, 20, 10, 1)
    # print(','.join(str(x) for x in a))
    #test()

    #print(os.getcwd())
    main()
