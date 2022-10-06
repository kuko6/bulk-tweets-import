from cmath import log
import os
import json
import gzip
import time
from datetime import datetime, timezone
from tracemalloc import start
from config.connect import connect
import psycopg2
from psycopg2.extras import execute_values
import gc
import csv

f = open('./output.csv', 'w', encoding='UTF8', newline='')
writer = csv.writer(f, delimiter=';')
writer.writerow(['time', 'entire_import', 'current_block'])
start_time = datetime.now(timezone.utc)


def log_time(block_time: datetime) -> datetime:
    time_now = datetime.now(timezone.utc)
    block_min, block_s = divmod((time_now - block_time).seconds, 60)
    total_min, total_s = divmod((time_now - start_time).seconds, 60)
    writer.writerow([start_time.strftime("%Y-%m-%dT%H:%MZ"), f'{total_min}:{total_s}', f'{block_min}:{block_s}'])
    return time_now


def import_authors(conn) -> set:
    """ Imports Twitter accounts from `authors.jsonl.gz` into `authors` table """

    print('|Inserting authors|')
    #start_time = time.time()
    block_time = datetime.now(timezone.utc)
    cur = conn.cursor()
    #duplicate_rows = 0
    inserted_authors = set()
    authors = list()
    with gzip.open('./tweets/authors.jsonl.gz') as file:
        for line in file:
            author = json.loads(line)

            if author['id'] in inserted_authors:
                #duplicate_rows += 1
                continue

            authors.append((author['id'], author.get('name').replace("\x00", ""), author.get('username').replace("\x00", ""),
                author.get('description').replace("\x00", ""), author.get('public_metrics').get('followers_count'), 
                author.get('public_metrics').get('following_count'), author.get('public_metrics').get('tweet_count'), 
                author.get('public_metrics').get('listed_count')))

            inserted_authors.add(author['id'])

            if len(inserted_authors)%100000 == 0: 
                print(f'Execution after {len(inserted_authors)} rows: {(datetime.now(timezone.utc) - start_time).seconds}s, {(datetime.now(timezone.utc) - start_time).microseconds}ms')

            if len(inserted_authors)%10000 == 0: 
                execute_values(cur, "INSERT INTO authors (id, name, username, description, followers_count, following_count, tweet_count, listed_count) VALUES %s ON CONFLICT DO NOTHING;", authors, page_size=10000)
                authors = []
                block_time = log_time(block_time)

        if len(authors) > 0:
            execute_values(cur, "INSERT INTO authors (id, name, username, description, followers_count, following_count, tweet_count, listed_count) VALUES %s ON CONFLICT DO NOTHING;", authors, page_size=10000)

    conn.commit()
    cur.close()

    print(f'Total execution time: {(datetime.now(timezone.utc) - start_time).seconds}s, {(datetime.now(timezone.utc) - start_time).microseconds}ms')
    print(f'Total number of rows: {len(inserted_authors)}\n')
    #print(f'Total number of duplicate rows: {duplicate_rows}')

    return inserted_authors


def import_links(cur, convo_id: str, links: list) -> None:
    """ Imports links from the `entities.urls` array """

    #new_links = list()
    for link in links:
        if len(link['expanded_url']) > 2048: continue
        #new_links.append((convo_id, link['expanded_url'], link.get('title'), link.get('description')))
        cur.execute("INSERT INTO links (conversation_id, url, title, description) VALUES (%s, %s, %s, %s);",
            (convo_id, link['expanded_url'], link.get('title'), link.get('description')))

    #execute_values(cur, "INSERT INTO links (conversation_id, url, title, description) VALUES %s;", new_links)


def import_annotations(cur, convo_id: str, annotations: list) -> None:
    """ Imports annotations from the `entities.annotations` array """

    new_annotations = list()
    for annotation in annotations:
        new_annotations.append((convo_id, annotation['normalized_text'], annotation['type'], annotation['probability']))
        # cur.execute("INSERT INTO annotations (conversation_id, value, type, probability) VALUES (%s, %s, %s, %s);",
        #     (convo_id, annotation['normalized_text'], annotation['type'], annotation['probability']))
    
    execute_values(cur, "INSERT INTO annotations (conversation_id, value, type, probability) VALUES %s;", new_annotations)


def import_hashtags(cur, convo_id: str, hashtags: list, inserted_hashtags: dict) -> None:
    """ Imports hashtags from the `entities.hashtags` array """

    new_convo_hashtags = list()
    for hashtag in hashtags:
        tag = hashtag['tag']
        if inserted_hashtags.get(tag) != None: 
            new_convo_hashtags.append((convo_id, inserted_hashtags[tag]))
            # cur.execute("INSERT INTO conversation_hashtags (conversation_id, hashtag_id) VALUES (%s, %s);",
            #     (convo_id, inserted_hashtags[tag]))
        else:
            cur.execute("INSERT INTO hashtags (tag) VALUES (%s) RETURNING id;", (tag, ))
            new_id = cur.fetchone()[0] # or cur.fetchone()['id']
            inserted_hashtags[tag] = new_id
            new_convo_hashtags.append((convo_id, new_id))
            # cur.execute("INSERT INTO conversation_hashtags (conversation_id, hashtag_id) VALUES (%s, %s);",
            #     (convo_id, new_id))
    
    #execute_values(cur, "INSERT INTO hashtags (conversation_id, hashtag_id) VALUES %s;", new_convo_hashtags, page_size=10000)
    execute_values(cur, "INSERT INTO conversation_hashtags (conversation_id, hashtag_id) VALUES %s;", new_convo_hashtags)


def import_context(cur, convo_id: str, context_annotations: list, inserted_context_entities: set, inserted_context_domains: set) -> None:
    """ Imports `entities` and `domains` from the `context_annotations` array """
    
    contexts = list()
    context_entities = list()
    context_domains = list()

    for context in context_annotations:
        if context['entity']['id'] not in inserted_context_entities:
            context_entities.append((context['entity']['id'], context['entity']['name'], context['entity'].get('description')))
            inserted_context_entities.add(context['entity']['id'])
            # cur.execute("INSERT INTO context_entities VALUES (%s, %s, %s);", 
            #     (context['entity']['id'], context['entity']['name'], context['entity'].get('description')))
        if context['domain']['id'] not in inserted_context_domains:
            context_domains.append((context['domain']['id'], context['domain']['name'], context['domain'].get('description')))
            inserted_context_domains.add(context['domain']['id'])
            # cur.execute("INSERT INTO context_domains VALUES (%s, %s, %s);", 
            #     (context['domain']['id'], context['domain']['name'], context['domain'].get('description')))

        contexts.append((convo_id, context['domain']['id'], context['entity']['id']))

    # either like this or with individual inserts
    if len(context_domains) > 0:
        execute_values(cur, "INSERT INTO context_domains (id, name, description) VALUES %s;", context_domains)
    if len(context_entities) > 0:
        execute_values(cur, "INSERT INTO context_entities (id, name, description) VALUES %s;", context_entities)
    execute_values(cur, "INSERT INTO context_annotations (conversation_id, context_domain_id, context_entity_id) VALUES %s;", contexts)


# TODO: - finish
# either in the same cycle as convos or separate
# the problem might be that there are references to tweets that are not in the database
# there can be multiple referenced tweets (probably not more than 2 tho)
def import_reference(cur, convo_id: str, references: list, inserted_convos: set, missing_convos: set) -> None:
    """ Imports referenced Tweets from the `referenced_tweets` array """

    for reference in references:
        if reference['id'] not in inserted_convos:
            missing_convos.add(reference['id'])
        cur.execute("INSERT INTO conversation_references (conversation_id, parent_id, type) VALUES (%s, %s, %s);",
            (convo_id, reference['id'], reference['type']))


def import_conversations(conn, authors_ids: set) -> set:
    """ Imports Tweets from `auconversationsthors.jsonl.gz` into `conversations` table """

    print('|Inserting conversations|')
    #start_time = time.time()
    block_time = datetime.now(timezone.utc)
    cur = conn.cursor()

    #duplicate_rows = 0
    missing_authors = []
    inserted_convos = set()
    inserted_context_entities = set()
    inserted_context_domains = set()
    #missing_convos = set()
    inserted_hashtags = dict()
    convos = list()
    with gzip.open('./tweets/conversations.jsonl.gz') as file:    
        for line in file:
            conversation = json.loads(line)
            if conversation['id'] in inserted_convos:
                #duplicate_rows += 1
                continue

            # if conversation['id'] in missing_convos:
            #     missing_convos.remove(conversation['id'])

            if conversation['author_id'] not in authors_ids:
                missing_authors.append((conversation['author_id'], ))
                #cur.execute("INSERT INTO authors VALUES (%s) ON CONFLICT DO NOTHING;", (conversation['author_id'],))
                #missing_authors_num += 1
                authors_ids.add(conversation['author_id'])

            convos.append((
                conversation['id'], conversation['author_id'], conversation['text'],
                conversation['possibly_sensitive'], conversation['lang'], conversation['source'], 
                conversation['public_metrics']['retweet_count'], conversation['public_metrics']['reply_count'],
                conversation['public_metrics']['like_count'], conversation['public_metrics']['quote_count'],
                conversation['created_at'])
            )

            inserted_convos.add(conversation['id'])

            # if conversation.get('entities') != None:
            #     if conversation.get('entities').get('urls') != None:
            #         import_links(cur, conversation['id'], conversation['entities']['urls'])
            #     if conversation.get('entities').get('annotations') != None:
            #         import_annotations(cur, conversation['id'], conversation['entities']['annotations'])
            #     if conversation.get('entities').get('hashtags') != None:
            #         import_hashtags(cur, conversation['id'], conversation['entities']['hashtags'], inserted_hashtags)

            # if conversation.get('context_annotations') != None:
            #     import_context(cur, conversation['id'], conversation['context_annotations'], inserted_context_entities, inserted_context_domains)

            # if conversation.get('referenced_tweets') != None:
            #     import_reference(cur, conversation['id'], conversation['referenced_tweets'], inserted_convos, missing_convos)

            if len(inserted_convos)%100000 == 0: 
                print(f'Execution after {len(inserted_convos)} rows: {(datetime.now(timezone.utc) - start_time).seconds}s, {(datetime.now(timezone.utc) - start_time).microseconds}ms')

            if len(inserted_convos)%10000 == 0: 
                execute_values(cur, "INSERT INTO conversations (id, author_id, content, possibly_sensitive, language, source, retweet_count, reply_count, like_count, quote_count, created_at) VALUES %s ON CONFLICT DO NOTHING;", convos, page_size=10000)
                convos = []
                block_time = log_time(block_time)


        if len(convos) > 0:
            execute_values(cur, "INSERT INTO conversations (id, author_id, content, possibly_sensitive, language, source, retweet_count, reply_count, like_count, quote_count, created_at) VALUES %s ON CONFLICT DO NOTHING;", convos, page_size=10000)

        execute_values(cur, "INSERT INTO authors (id) VALUES %s ON CONFLICT DO NOTHING;", missing_authors, page_size=10000)
        print(f'Total number of missing authors: {len(missing_authors)}')
        #print(f'Total number of missing authors: {missing_authors_num}')

        # for missing_convo in missing_convos:
        #     cur.execute("DELETE FROM conversation_references WHERE parent_id = (%s);", (missing_convo, ))
        
        # constraints for `conversations`
        # cur.execute('ALTER TABLE conversations ADD CONSTRAINT fk_authors FOREIGN KEY (author_id) REFERENCES authors(id);')
        # # constraints for `links`
        # cur.execute('ALTER TABLE links ADD CONSTRAINT fk_conversations FOREIGN KEY (conversation_id) REFERENCES conversations(id);')
        # # constraints for `annotations`
        # cur.execute('ALTER TABLE annotations ADD CONSTRAINT fk_conversations FOREIGN KEY (conversation_id) REFERENCES conversations(id);')
        # # constraints for `conversation_hashtags`
        # cur.execute('ALTER TABLE conversation_hashtags ADD CONSTRAINT fk_conversations FOREIGN KEY (conversation_id) REFERENCES conversations(id);')
        # cur.execute('ALTER TABLE conversation_hashtags ADD CONSTRAINT fk_hashtags FOREIGN KEY (hashtag_id) REFERENCES hashtags(id);')
        # # constraints for `context_annotations`
        # cur.execute('ALTER TABLE context_annotations ADD CONSTRAINT fk_conversations FOREIGN KEY (conversation_id) REFERENCES conversations(id);')
        # cur.execute('ALTER TABLE context_annotations ADD CONSTRAINT fk_domains FOREIGN KEY (context_domain_id) REFERENCES context_domains(id);')
        # cur.execute('ALTER TABLE context_annotations ADD CONSTRAINT fk_entities FOREIGN KEY (context_entity_id) REFERENCES context_entities(id);')
        #constraints for `conversations_references`
        # cur.execute('ALTER TABLE conversation_references ADD CONSTRAINT fk_conversations FOREIGN KEY (conversation_id) REFERENCES conversations(id);')
        # cur.execute('ALTER TABLE conversation_references ADD CONSTRAINT fk_parent FOREIGN KEY (parent_id) REFERENCES conversations(id);')
    conn.commit()
    cur.close()

    print(f'Total execution time: {(datetime.now(timezone.utc) - start_time).seconds}s, {(datetime.now(timezone.utc) - start_time).microseconds}ms')
    print(f'Total number of rows: {len(inserted_convos)}\n')
    #print(f'Total number of duplicate rows: {duplicate_rows}')

    del missing_authors
    del inserted_context_domains
    del inserted_context_entities
    del missing_convos
    del inserted_hashtags
    del convos
    del authors_ids
    gc.collect()

    return inserted_convos


def import_references(conn, inserted_convos: set) -> None:
    
    print('|Inserting references|')
    #start_time = time.time()
    block_time = datetime.now(timezone.utc)
    cur = conn.cursor()

    processed_convos = set()
    references = list()
    i = 0
    with gzip.open('./tweets/conversations.jsonl.gz') as file:    
        for line in file:
            conversation = json.loads(line)
            if conversation['id'] in processed_convos or conversation.get('referenced_tweets') == None:
                continue

            for reference in conversation['referenced_tweets']:
                if reference['id'] not in inserted_convos: continue
                references.append((conversation['id'], reference['id'], reference['type']))

            processed_convos.add(conversation['id'])

            if len(processed_convos)%100000 == 0: 
                print(f'Execution after {len(processed_convos)} rows: {(datetime.now(timezone.utc) - start_time).seconds}s, {(datetime.now(timezone.utc) - start_time).microseconds}ms')

            if len(processed_convos)%10000 == 0: 
                execute_values(cur, "INSERT INTO conversation_references (conversation_id, parent_id, type) VALUES %s;", references, page_size=10000)
                references = []
                block_time = log_time(block_time)

        if len(references) > 0:
            execute_values(cur, "INSERT INTO conversation_references (id, conversation_id, parent_id, type) VALUES %s;", references, page_size=10000)
            
    #constraints for `conversations_references`
    cur.execute('ALTER TABLE conversation_references ADD CONSTRAINT fk_conversations FOREIGN KEY (conversation_id) REFERENCES conversations(id);')
    cur.execute('ALTER TABLE conversation_references ADD CONSTRAINT fk_parent FOREIGN KEY (parent_id) REFERENCES conversations(id);')
    conn.commit()
    cur.close()

    print(f'Total execution time: {(datetime.now(timezone.utc) - start_time).seconds}s, {(datetime.now(timezone.utc) - start_time).microseconds}ms')
    print(f'Total number of rows: {processed_convos}\n')


def create_tables(conn):
    cur = conn.cursor()
    cur.execute(open('./src/schema.sql', 'r').read())
    conn.commit()
    cur.close()


def main():
    conn = connect()
    if conn == None:
        print('Connection to the database failed :(')
        return

    create_tables(conn)
    # cur.execute("INSERT INTO links (conversation_id, url, title, description) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING;", ((11, 'aaa', None, None)))

    # cur = conn.cursor()
    # cur.execute("DELETE FROM conversation_references WHERE parent_id = (%s)", ('200', ))
    # conn.commit()
    # cur.execute("SELECT id FROM authors;")
    # authors_ids = set()
    # for id in cur.fetchall():
    #     authors_ids.add(str(id[0]))
    
    authors_ids = import_authors(conn)
    # inserted_convos = import_conversations(conn, authors_ids)
    # import_references(conn, inserted_convos)

    # q = "INSERT INTO links (conversation_id, url) VALUES ({})".format(','.join(['%s'] * 2))
    # cur = conn.cursor()
    # cur.executemany(q, [(1, 'aaa'), (2, 'bbb')])  
    # conn.commit()

    # close the connection
    conn.close()


def test():
    inserted_convos = set()
    references = 0
    i = 0
    block_time = datetime.now(timezone.utc)
    test_time = time.time()

    with gzip.open('./tweets/conversations.jsonl.gz') as file:
        for line in file:
            conversation = json.loads(line)
            i += 1

            if i % 100000 == 0:
                block_time = log_time(block_time)

                print(f'Execution after {i} rows: {(datetime.now(timezone.utc) - start_time).seconds}s, {(datetime.now(timezone.utc) - start_time).microseconds}ms')
                print(f'Execution after {i} rows: {round(time.time() - test_time, 3)}s')
                
            if i % 500000 == 0:
                break





if __name__ == '__main__':
    # a = (100, 'Kuko', 'kuko6', 'Im the best', 10000, 20, 10, 1)
    # print(','.join(str(x) for x in a))
    
    #test()
    #print(os.getcwd())
    main()

    f.close()
    # data = [(1,'x'), (2,'y')]
    # records_list_template = ','.join(['%s'] * len(data))
    # print(records_list_template)
