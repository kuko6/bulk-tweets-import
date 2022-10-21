-- Dopyty pouzite v druhom zadani

-- 1. Vyhľadajte v authors username s presnou hodnotou ‘mfa_russia’ a analyzujte daný select. Akú metódu vám vybral plánovač a prečo - odôvodnite prečo sa rozhodol tak ako sa rozhodol?

SELECT * FROM authors WHERE username = 'mfa_russia';
EXPLAIN SELECT * FROM authors WHERE username = 'mfa_russia';
EXPLAIN ANALYSE SELECT * FROM authors WHERE username = 'mfa_russia';

-- 2. Koľko workerov pracovalo na danom selecte a na čo slúžia? Zdvihnite počet workerov a povedzte ako to ovplyvňuje čas. Je tam nejaký strop? Ak áno, prečo? Od čoho to závisí (napíšte a popíšte všetky parametre)?

SHOW max_parallel_workers_per_gather; -- 4
SET max_parallel_workers_per_gather TO 4;

-- 3. Vytvorte btree index nad username a pozrite ako sa zmenil čas a porovnajte výstup oproti požiadavke bez indexu. Potrebuje plánovač v tejto požiadavke viac workerov? Čo ovplyvnilo zásadnú zmenu času?

CREATE INDEX authors_username ON authors (username);

-- 4. Vyberte používateľov, ktorý majú followers_count väčší, rovný ako 100 a zároveň menší, rovný 200. Potom zmeňte rozsah na väčší, rovný ako 100 a zároveň menší, rovný 120. Je tam rozdiel, ak áno prečo?

SELECT * FROM authors WHERE followers_count BETWEEN 100 AND 200; -- 760 088 rows 
EXPLAIN ANALYZE SELECT * FROM authors WHERE followers_count >= 100 AND followers_count <= 200; 
SELECT * FROM authors WHERE followers_count >= 100 AND followers_count <= 120; -- 199 937 rows 
EXPLAIN ANALYZE SELECT * FROM authors WHERE followers_count >= 100 AND followers_count <= 120;

-- 5. Vytvorte index nad 4 úlohou a v oboch podmienkach popíšte prácu s indexom. Čo je to Bitmap Index Scan a prečo je tam Bitmap Heap Scan? Prečo je tam recheck condition? Použil sa vždy index?

CREATE INDEX authors_followers_count ON authors(followers_count);	

-- 6. Vytvorte ďalšie 3 btree indexy na name, followers_count, a description a insertnite si svojho používateľa (to je jedno aké dáta) do authors. Koľko to trvalo? Dropnite indexy a spravte to ešte raz. Prečo je tu rozdiel?

CREATE INDEX authors_name ON authors(name);		
CREATE INDEX authors_followers_count ON authors(followers_count);
CREATE INDEX authors_description ON authors(description);

INSERT INTO authors VALUES 
	(1, 'Jakub', 'jakub22', 'toto je description', 1000, 2, 3000, 400);

EXPLAIN ANALYSE INSERT INTO authors VALUES  
	(1, 'Jakub', 'jakub22', 'toto je description', 1000, 2, 3000, 400);

DELETE FROM authors WHERE id = 1;

DROP INDEX authors_name;		
DROP INDEX authors_followers_count;
DROP INDEX authors_description;

SELECT * FROM authors ORDER BY id DESC LIMIT 1;

-- 7. Vytvorte btree index nad conversations pre retweet_count a pre content. Porovnajte ich dĺžku vytvárania. Prečo je tu taký rozdiel? Čím je ovplyvnená dĺžka vytvárania indexu a prečo?

CREATE INDEX conversations_retweet_count ON conversations(retweet_count);		
CREATE INDEX conversations_content ON conversations(content);

-- 8. Porovnajte indexy pre retweet_count, content, followers_count, name,... v čom sa líšia pre nasledovné parametre: počet root nódov, level stromu, a priemerná veľkosť itemu. Vysvetlite.
CREATE EXTENSION pgstattuple;
CREATE EXTENSION pageinspect;

SELECT 'conversations_content' AS index_name, tree_level, root_block_no, index_size, internal_pages, leaf_pages, avg_item_size 
	FROM pgstatindex('conversations_content'), bt_page_stats('conversations_content', 1)
UNION
SELECT 'conversations_retweet_count', tree_level, root_block_no, index_size, internal_pages, leaf_pages, avg_item_size
	FROM pgstatindex('conversations_retweet_count'), bt_page_stats('conversations_retweet_count', 1)
UNION
SELECT 'authors_username', tree_level, root_block_no, index_size, internal_pages, leaf_pages, avg_item_size
	FROM pgstatindex('authors_username'), bt_page_stats('authors_username', 1)
UNION
SELECT 'authors_name', tree_level, root_block_no, index_size, internal_pages, leaf_pages, avg_item_size 
	FROM pgstatindex('authors_name'), bt_page_stats('authors_name', 1)
UNION
SELECT 'authors_description', tree_level, root_block_no, index_size, internal_pages, leaf_pages, avg_item_size
	FROM pgstatindex('authors_description'), bt_page_stats('authors_description', 1)
UNION
SELECT 'authors_followers_count', tree_level, root_block_no, index_size, internal_pages, leaf_pages, avg_item_size
	FROM pgstatindex('authors_followers_count'), bt_page_stats('authors_followers_count', 1);

-- SELECT tree_level, root_block_no, avg_item_size FROM pgstatindex('conversations_content'), bt_page_stats('conversations_content', 1);

-- 9. Vyhľadajte v conversations content meno „Gates“ na ľubovoľnom mieste a porovnajte výsledok po tom, ako content naindexujete pomocou btree. V čom je rozdiel a prečo?

SELECT * FROM conversations WHERE content LIKE '%Gates%';
EXPLAIN ANALYSE SELECT * FROM conversations WHERE content LIKE '%Gates%';

-- 10. Vyhľadajte tweet, ktorý začína “There are no excuses” a zároveň je obsah potenciálne senzitívny (possibly_sensitive). Použil sa index? Prečo? Ako query zefektívniť?

SELECT * FROM conversations WHERE content LIKE 'There are no excuses%' AND possibly_sensitive;
EXPLAIN ANALYSE SELECT * FROM conversations WHERE content LIKE 'There are no excuses%' AND possibly_sensitive;

-- 11. Vytvorte nový btree index, tak aby ste pomocou neho vedeli vyhľadať tweet, ktorý končí reťazcom „https://t.co/pkFwLXZlEm“ kde nezáleží na tom ako to napíšete. Popíšte čo jednotlivé funkcie robia.

-- SELECT UPPER(RIGHT('https://t.co/pkFwLXZlEm', LENGTH('https://t.co/pkFwLXZlEm'))) = UPPER('https://t.co/pkFwLXZlEm');
CREATE INDEX conversations_content_ending_url ON conversations (UPPER(
	RIGHT(conversations.content, LENGTH('https://t.co/pkFwLXZlEm'))));		

-- EXPLAIN ANALYSE SELECT * FROM conversations WHERE content ILIKE '%https://t.co/pkFwLXZlEm';
EXPLAIN ANALYSE SELECT * FROM conversations conv WHERE UPPER(RIGHT(conv.content, LENGTH('https://t.co/pkFwLXZlEm'))) = UPPER('https://t.co/pkFwLXZlEm');
SELECT * FROM conversations conv WHERE UPPER(RIGHT(conv.content, LENGTH('https://t.co/pkFwLXZlEm'))) = UPPER('https://t.co/pkFwLXZlEm');

-- 12. Nájdite conversations, ktoré majú reply_count väčší ako 150, retweet_count väčší rovný ako 5000 a výsledok zoraďte podľa quote_count. Následne spravte jednoduché indexy a popíšte ktoré má a ktoré nemá zmysel robiť a prečo. Popíšte a vysvetlite query plan, ktorý sa aplikuje v prípade použitia jednoduchých indexov.

SELECT * FROM conversations WHERE reply_count > 150 AND retweet_count >= 5000 ORDER BY quote_count;
EXPLAIN ANALYSE SELECT * FROM conversations WHERE reply_count > 150 AND retweet_count >= 5000 ORDER BY quote_count;

CREATE INDEX conversations_reply_count ON conversations (reply_count);
CREATE INDEX conversations_retweet_count ON conversations (retweet_count);
CREATE INDEX conversations_quote_count ON conversations (quote_count);

-- 13. Na predošlú query spravte zložený index a porovnajte výsledok s tým, keďy je sú indexy separátne. Výsledok zdôvodnite. Popíšte použitý query plan. Aký je v nich rozdiel?

CREATE INDEX conversations_multicol ON conversations(reply_count, retweet_count, quote_count);

EXPLAIN ANALYSE SELECT * FROM conversations WHERE reply_count > 150 AND retweet_count >= 5000 ORDER BY quote_count;

-- 14. Napíšte dotaz tak, aby sa v obsahu konverzácie našlo slovo „Putin“ a zároveň spojenie „New World Order“, kde slová idú po sebe a zároveň obsah je senzitívny. Vyhľadávanie má byť indexe. Popíšte použitý query plan pre GiST aj pre GIN. Ktorý je efektívnejší?

-- SELECT * FROM conversations WHERE content LIKE '%Putin%' and content LIKE '%New World Order%' AND possibly_sensitive; 
EXPLAIN ANALYSE SELECT * FROM conversations WHERE content LIKE '%Putin%New World Order%' AND possibly_sensitive; 
SELECT * FROM conversations WHERE content LIKE '%Putin%New World Order%' AND possibly_sensitive; 

CREATE EXTENSION pg_trgm;

CREATE INDEX conversations_gist_trgm_content ON conversations USING gist(content gist_trgm_ops);
SELECT pg_size_pretty(pg_relation_size('conversations_gist_trgm_content')) gist;

CREATE INDEX conversations_gin_trgm_content ON conversations USING gin(content gin_trgm_ops);
SELECT pg_size_pretty(pg_relation_size('conversations_gin_trgm_content')) gin;

-- 15. Vytvorte vhodný index pre vyhľadávanie v links.url tak aby ste našli kampane z ‘darujme.sk’. Ukážte dotaz a použitý query plan. Vysvetlite prečo sa použil tento index.

SELECT * FROM links WHERE url LIKE '%darujme.sk%';
EXPLAIN ANALYSE SELECT * FROM links WHERE url LIKE '%darujme.sk%';

CREATE INDEX links_gin_trgm_url ON links USING gin(url gin_trgm_ops);
SELECT pg_size_pretty(pg_relation_size('links_gin_trgm_url')) gin;

-- 16. Vytvorte query pre slová "Володимир" a "Президент" pomocou FTS (tsvector a tsquery) v angličtine v stĺpcoch conversations.content, authors.decription a authors.username, kde slová sa môžu nachádzať̌ v prvom, druhom ALEBO treťom stĺpci. Teda vyhovujúci záznam je ak aspoň jeden stĺpec má „match“. Výsledky zoradíte podľa retweet_count zostupne. Pre túto query vytvorte vhodné indexy tak, aby sa nepoužil ani raz sekvenčný scan (správna query dobehne rádovo v milisekundách, max sekundách na super starých PC). Zdôvodnite čo je problém s OR podmienkou a prečo AND je v poriadku pri joine.

-- 824
EXPLAIN ANALYSE SELECT auth.id as author, convs.id as conversation, auth.description, auth.username, convs.content, convs.retweet_count 
FROM authors auth
JOIN conversations convs ON auth.id = convs.author_id
WHERE to_tsvector('english', coalesce(auth.description, '') || ' ' || coalesce(auth.username, '') || ' ' || convs.content) @@ to_tsquery('english', 'Володимир & Президент')
ORDER BY convs.retweet_count DESC;

SELECT auth.id as author, convs.id as conversation, auth.description, auth.username, convs.content, convs.retweet_count 
FROM authors auth
JOIN conversations convs ON auth.id = convs.author_id
WHERE to_tsvector('english', coalesce(auth.description, '') || ' ' || coalesce(auth.username, '') || ' ' || convs.content) @@ to_tsquery('english', 'Володимир & Президент')
ORDER BY convs.retweet_count DESC;

CREATE INDEX authors_gin_descr_username ON authors USING gin(to_tsvector('english', coalesce(description, '') || ' ' || coalesce(username, '')));
CREATE INDEX authors_gin_content ON conversations USING gin(to_tsvector('english', content));

-- Experimenty 
-- 746
SELECT auth.id, convs.id, auth.description, auth.username, convs.content, convs.retweet_count FROM authors auth
JOIN conversations convs ON auth.id = convs.author_id
WHERE to_tsvector('english', COALESCE(auth.description, '') || COALESCE(auth.username, '') || COALESCE(convs.content, '')) @@ to_tsquery('english', 'Володимир & Президент')
ORDER BY convs.retweet_count DESC;

-- 742
SELECT auth.id, convs.id, auth.description, auth.username, convs.content, convs.retweet_count FROM authors auth
JOIN conversations convs ON auth.id = convs.author_id
WHERE to_tsvector('english', auth.description || auth.username || convs.content) @@ to_tsquery('english', 'Володимир & Президент')
ORDER BY convs.retweet_count DESC;

-- 824
SELECT auth.id AS author, convs.id AS convo, auth.description, auth.username, convs.content, convs.retweet_count 
FROM authors auth
JOIN conversations convs ON auth.id = convs.author_id
WHERE to_tsvector('english', COALESCE(auth.description, '')) @@ to_tsquery('english', 'Володимир & Президент') 
	UNION
SELECT auth.id, convs.id, auth.description, auth.username, convs.content, convs.retweet_count 
FROM authors auth
JOIN conversations convs ON auth.id = convs.author_id
WHERE to_tsvector('english', auth.username) @@ to_tsquery('english', 'Володимир & Президент') 
	UNION
SELECT auth.id, convs.id, auth.description, auth.username, convs.content, convs.retweet_count 
FROM authors auth
JOIN conversations convs ON auth.id = convs.author_id
WHERE to_tsvector('english', convs.content) @@ to_tsquery('english', 'Володимир & Президент') 
ORDER BY retweet_count DESC;