import sqlalchemy as sa
import os
from pathlib import Path
from datetime import datetime


db_address = os.getenv("POSTGRES_ADRESS")

engine = sa.create_engine(db_address)
conn = engine.connect()

metadata = sa.MetaData()

# Create Raw table
table = sa.Table(
    "raw_news",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True, autoincrement=True, unique=True),
    sa.Column("source_name", sa.String, nullable=False),
    sa.Column("source_country", sa.Text),
    sa.Column("category", sa.String, nullable=False),
    sa.Column("author", sa.String),
    sa.Column("author_email", sa.String),
    sa.Column("author_page_url", sa.String),
    sa.Column("title", sa.String, nullable=False),
    sa.Column("description", sa.String),
    sa.Column("url", sa.String),
    sa.Column("publishedat", sa.DateTime, nullable=False),
    sa.Column("article_content", sa.Text),
    sa.Column("bias", sa.String),
    sa.Column("language", sa.String),
    sa.Column("ingest_ts", sa.TIMESTAMP)
)


table = sa.Table(
    "sentiment_raw",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True, autoincrement=True, unique=True),
    sa.Column("article_id", sa.Text, nullable=False),
    sa.Column("sentiment_mark", sa.Float, nullable=True),
    sa.Column("sentiment_poilievre", sa.Float, nullable=True),
    sa.Column("model", sa.String, nullable=True),
    sa.Column("ingest_ts", sa.TIMESTAMP),
    extend_existing=True
)

try:
    metadata.create_all(engine)
except Exception as e:
    print(e)
finally:
    conn.close()