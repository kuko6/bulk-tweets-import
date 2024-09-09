# Bulk tweets import
This repository includes: 
- python scripts and sql queries used for importing a large amount of tweets into a postgres database
- advanced sql queries for the tweets database

## Usage

Install the requirements:
```
pip install -r requirements.txt
```

Configure the database connection in the `config/connect.py` file and run the main script:
```
python3 ./src/main.py
```
