CREATE TABLE IF NOT EXISTS authors (
	id int8 PRIMARY KEY,
	name VARCHAR (255),
	username VARCHAR (255),
	description text,
    followers_count int4,
    following_count int4,
    tweet_count int4,
    listed_count int4
);