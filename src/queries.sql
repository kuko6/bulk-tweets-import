CREATE TABLE IF NOT EXISTS authors (
	id int8 primary key,
	name varchar(255),
	username varchar(255),
	description text,
    followers_count int4,
    following_count int4,
    tweet_count int4,
    listed_count int4
);

CREATE TABLE IF NOT EXISTS conversations (
	id int8 primary key,
	author_id int8 not null,
	content text not null,
	possibly_sensitive bool not null,
    language varchar(3) not null,
    source text not null,
    retweet_count int4,
    reply_count int4,
    like_count int4,
    quote_count int4,
    created_at timestamptz not null
);

-- Create `conversations` table with constrains
CREATE TABLE IF NOT EXISTS conversations (
	id int8 primary key,
	author_id int8 references authors(id) not null,
	content text not null,
	possibly_sensitive bool not null,
    language varchar(3) not null,
    source text not null,
    retweet_count int4,
    reply_count int4,
    like_count int4,
    quote_count int4,
    created_at timestamptz not null
);

CREATE TABLE IF NOT EXISTS links (
	id bigserial primary key,
	conversation_id int8 not null,
	url varchar(2048) not null,
	title text,
    description text
);

CREATE TABLE IF NOT EXISTS annotations (
	id bigserial primary key,
	conversation_id int8 not null,
	value text not null,
	type text not null,
    probability numeric(4, 3) not null
);

CREATE TABLE IF NOT EXISTS conversation_hashtags (
	id bigserial primary key,
    conversation_id int8 not null,
    hashtag_id int8 not null
);

CREATE TABLE IF NOT EXISTS hashtags (
	id bigserial primary key,
	tag text UNIQUE not null
);

CREATE TABLE IF NOT EXISTS context_annotations (
	id bigserial primary key,
    conversation_id int8 not null,
    context_domain_id int8 not null,
	context_entity_id int8 not null
);

CREATE TABLE IF NOT EXISTS context_domains (
	id int8 primary key,
	name varchar(255) not null,
	description text
);

CREATE TABLE IF NOT EXISTS context_entities (
	id int8 primary key,
	name varchar(255) not null,
	description text
);

CREATE TABLE IF NOT EXISTS conversation_references (
	id int8 primary key,
	conversation_id int8 not null,
	parent_id int8 not null,
	type varchar(20) not null
);