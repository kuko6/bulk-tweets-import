CREATE TABLE IF NOT EXISTS authors (
	id int8 PRIMARY KEY,
	name varchar(255),
	username varchar(255),
	description text,
    followers_count int4,
    following_count int4,
    tweet_count int4,
    listed_count int4
);

CREATE TABLE IF NOT EXISTS conversations (
	id int8 PRIMARY KEY,
	author_id int8 NOT NULL,
	content text NOT NULL,
	possibly_sensitive bool NOT NULL,
    language varchar(3) NOT NULL,
    source text NOT NULL,
    retweet_count int4,
    reply_count int4,
    like_count int4,
    quote_count int4,
    created_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS links (
	id bigserial PRIMARY KEY,
	conversation_id int8 NOT NULL,
	url varchar(2048) NOT NULL,
	title text,
    description text
);

CREATE TABLE IF NOT EXISTS annotations (
	id bigserial PRIMARY KEY,
	conversation_id int8 NOT NULL,
	value text NOT NULL,
	type text NOT NULL,
    probability numeric(4, 3) NOT NULL
);

CREATE TABLE IF NOT EXISTS conversation_hashtags (
	id bigserial PRIMARY KEY,
    conversation_id int8 NOT NULL,
    hashtag_id int8 NOT NULL
);

CREATE TABLE IF NOT EXISTS hashtags (
	id bigserial PRIMARY KEY,
	tag text UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS context_annotations (
	id bigserial PRIMARY KEY,
    conversation_id int8 NOT NULL,
    context_domain_id int8 NOT NULL,
	context_entity_id int8 NOT NULL
);

CREATE TABLE IF NOT EXISTS context_domains (
	id int8 PRIMARY KEY,
	name varchar(255) NOT NULL,
	description text
);

CREATE TABLE IF NOT EXISTS context_entities (
	id int8 PRIMARY KEY,
	name varchar(255) NOT NULL,
	description text
);

CREATE TABLE IF NOT EXISTS conversation_references (
	id bigserial PRIMARY KEY,
	conversation_id int8 NOT NULL,
	parent_id int8 NOT NULL,
	type varchar(20) NOT NULL
);