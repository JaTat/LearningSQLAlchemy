from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey
from sqlalchemy import create_engine
import sqlalchemy

# checking version
sqlalchemy.__version__

# Tutorial uses a SQLite, I use PostgreSQL+
# psycopg2 is the default DBAPI, i state it explicitely
# URL is dbuser:dbpassword@host:port/database
engine = create_engine(
    'postgresql+psycopg2://postgres:postgres@localhost:5432/postgres', echo=True)

# To parse passowrds with special characters:
# import urllib.parse
# urllib.parse.quote_plus("kx%jj5/g")
# 'kx%25jj5%2Fg'

# Create Table Metadata that is later transformed in literal SQL expressions

metadata = MetaData()

users = Table('users', metadata,
              Column('id', Integer, primary_key=True),
              Column('name', String),
              Column('fullname', String),
              )

addresses = Table('addresses', metadata,
                  Column('id', Integer, primary_key=True),
                  Column('user_id', None, ForeignKey('users.id')),
                  Column('email_address', String, nullable=False)
                  )

metadata.create_all(engine)
