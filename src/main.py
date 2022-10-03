import os
import json
import gzip
import time
from config.connect import connect
import psycopg2
from psycopg2.extras import execute_values


def import_authors(conn) -> set:
    """ Imports Twitter accounts from `authors.jsonl.gz` into `authors` table """

    print('|Inserting authors|')
    start_time = time.time()
    inserted_authors = set()
        
    cur = conn.cursor()
    duplicate_rows = 0
    buff = []
    with gzip.open('./tweets/authors.jsonl.gz') as file:
        for line in file:
            author = json.loads(line)

            if author['id'] in inserted_authors:
                duplicate_rows += 1
                continue

            buff.append((author['id'], author.get('name').replace("\x00", ""), author.get('username').replace("\x00", ""),
                author.get('description').replace("\x00", ""), author.get('public_metrics').get('followers_count'), 
                author.get('public_metrics').get('following_count'), author.get('public_metrics').get('tweet_count'), 
                author.get('public_metrics').get('listed_count')))

            inserted_authors.add(author['id'])
            #inserted_rows += 1

            if len(inserted_authors)%100000 == 0: 
                print(f'Execution after {len(inserted_authors)} rows: {round(time.time() - start_time, 3)}s')
                execute_values(cur, "INSERT INTO authors (id, name, username, description, followers_count, following_count, tweet_count, listed_count) VALUES %s ON CONFLICT DO NOTHING;;", buff, page_size=100000)
                buff = []
        
        if len(buff) > 0:
            execute_values(cur, "INSERT INTO authors (id, name, username, description, followers_count, following_count, tweet_count, listed_count) VALUES %s ON CONFLICT DO NOTHING;;", buff, page_size=100000)

        conn.commit()
        # cur.close()

    print(f'Total execution time: {round(time.time() - start_time, 3)}s')
    print(f'Total number of inserted rows: {len(inserted_authors)}\n')
    print(f'Total number of duplicate rows: {duplicate_rows}')

    return inserted_authors


def import_links(cur, convo_id: int, links: list) -> None:
    """ Imports links from the `urls` array in the `entities` object """

    for link in links:
        if len(link['expanded_url']) > 2048: continue
        cur.execute("INSERT INTO links (conversation_id, url, title, description) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING;",
            (convo_id, link['expanded_url'], link.get('title'), link.get('description')))


def import_conversations(conn, authors_ids: set) -> None:
    """ Imports Tweets from `auconversationsthors.jsonl.gz` into `conversations` table """

    print('|Inserting conversations|')
    start_time = time.time()
    cur = conn.cursor()
    # inserted_rows = 0
    duplicate_rows = 0
    # missing_authors = []
    missing_authors_num = 0
    inserted_convos = set()
    buff = []
    with gzip.open('./tweets/conversations.jsonl.gz') as file:    
        for line in file:
            conversation = json.loads(line)
            if conversation['id'] in inserted_convos:
                duplicate_rows += 1
                continue

            if conversation['author_id'] not in authors_ids:
                # missing_authors.append((conversation['author_id'], ))
                # cur.execute("INSERT INTO authors VALUES (%s) ON CONFLICT DO NOTHING;", (conversation['author_id'],))
                missing_authors_num += 1
                authors_ids.add(conversation['author_id'])

            buff.append((
                conversation['id'], conversation['author_id'], conversation['text'],
                conversation['possibly_sensitive'], conversation['lang'], conversation['source'], 
                conversation['public_metrics']['retweet_count'], conversation['public_metrics']['reply_count'],
                conversation['public_metrics']['like_count'], conversation['public_metrics']['quote_count'],
                conversation['created_at'])
            )

            inserted_convos.add(conversation['id'])

            if conversation.get('entities') != None:
                if conversation.get('entities').get('urls') != None:
                    import_links(cur, conversation['id'], conversation['entities']['urls'])

            if len(inserted_convos)%100000 == 0: 
                print(f'Execution after {len(inserted_convos)} rows: {round(time.time() - start_time, 3)}s')
                execute_values(cur, "INSERT INTO conversations (id, author_id, content, possibly_sensitive, language, source, retweet_count, reply_count, like_count, quote_count, created_at) VALUES %s ON CONFLICT DO NOTHING;", buff, page_size=100000)
                buff = []
                break

        if len(buff) > 0:
            execute_values(cur, "INSERT INTO conversations (id, author_id, content, possibly_sensitive, language, source, retweet_count, reply_count, like_count, quote_count, created_at) VALUES %s ON CONFLICT DO NOTHING;", buff, page_size=100000)

        # adding the missing authors at the end seem slower for 100k records, idk why 
        # execute_values(cur, "INSERT INTO authors (id) VALUES %s ON CONFLICT DO NOTHING;", missing_authors, page_size=100000)
        # print(f'Total number of missing authors: {len(missing_authors)}')

        print(f'Total number of missing authors: {missing_authors_num}')
        cur.execute('ALTER TABLE conversations ADD CONSTRAINT fk_authors FOREIGN KEY (author_id) REFERENCES authors(id);')
        cur.execute('ALTER TABLE links ADD CONSTRAINT fk_conversations FOREIGN KEY (conversation_id) REFERENCES conversations(id);')
        conn.commit()
        # cur.close()

    print(f'Total execution time: {round(time.time() - start_time, 3)}s')
    print(f'Total number of inserted rows: {len(inserted_convos)}\n')
    print(f'Total number of duplicate rows: {duplicate_rows}')


def main():
    conn = connect()
    if conn == None:
        print('Connection to the database failed :(')
        return

    # cur = conn.cursor()
    # cur.execute("INSERT INTO links (conversation_id, url, title, description) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING;", ((11, 'aaa', None, None)))

    authors_ids = import_authors(conn)
    import_conversations(conn, authors_ids)

    # q = "INSERT INTO links (conversation_id, url) VALUES ({})".format(','.join(['%s'] * 2))
    # cur = conn.cursor()
    # cur.executemany(q, [(1, 'aaa'), (2, 'bbb')])  
    # conn.commit()

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

    # data = [(1,'x'), (2,'y')]
    # records_list_template = ','.join(['%s'] * len(data))
    # print(records_list_template)
