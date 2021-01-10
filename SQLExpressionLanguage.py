import pandas as pd
from sqlalchemy import inspect
from sqlalchemy.sql import select
from sqlalchemy.dialects import postgresql
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

# INSERT

ins = users.insert()
str(ins)
# limit to certain columns
ins = users.insert().values(name='jack', fullname='Jack Jones')
# print out a string of the SQL statement
# Note values are ound too named parameters :name
# values are pulled during compilation

str(ins)
ins.compile().params
# even now the DDL is generic for a specific DB dialect to be printed out
# we need something like statetd here: https://nicolascadou.com/blog/2014/01/printing-actual-sqlalchemy-queries/
# i.e. specifically name the dialect in the compile statement
print(str(ins.compile(dialect=postgresql.dialect())))
# dialect + values:
print(str(ins.compile(dialect=postgresql.dialect(),
                      compile_kwargs={"literal_binds": True})))

# Executing
# connect
conn = engine.connect()
conn
result = conn.execute(ins)

# making str aware of the dialect:
ins.bind = engine
str(ins)

result.inserted_primary_key

# generic insert statement without specifying values
ins = users.insert()
conn.execute(ins, id=2,  name='wendy', fullname='Wendy Williams')

s = select([users])
result = conn.execute(s)

for row in result:
    print(row)

# Insert many statements at once

conn.execute(addresses.insert(), [
    {'user_id': 1, 'email_address': 'jack@yahoo.com'},
    {'user_id': 1, 'email_address': 'jack@msn.com'},
    {'user_id': 2, 'email_address': 'www@www.org'},
    {'user_id': 2, 'email_address': 'wendy@aol.com'},
])
#  note:each dictionary must have the same set of keys; i.e. you cant have fewer keys in some dictionaries than others.
#  This is because the Insert statement is compiled against the first dictionary in the list, and it’s assumed that all subsequent argument dictionaries are compatible with that statement.

# SELECT

s = select([users])
result = conn.execute(s)
# retrieve selected data
# Method 1 iterate over result object
for row in result:
    print(row)

# Method two
row = result.fetchone()
print("name:", row['name'], "; fullname:", row['fullname'])

row = result.fetchone()
print("id:", row[0], "name:", row[1], "; fullname:", row[2])

# reference directly the columns
for row in conn.execute(s):
    print("name:", row[users.c.name], "; fullname:", row[users.c.fullname])

# autoclose when all pending result rows have been fetched else:
result.close()

# SQLalchemy uses pythons built in __eq__() to
# evaluate built in fuction such as ==

users.c.id == addresses.c.user_id
# produces SQL
str(users.c.id == addresses.c.user_id)

# its smart, it takes variable types into consideration:
print("Integer adds:", users.c.id + addresses.c.id)

print("String concatenates:", users.c.name + users.c.fullname)

# Load info on all existing tables, i.e. reflect all tables
meta = MetaData()
meta.reflect(bind=engine)
for tables in meta.tables:
    print(tables)


# Own attempt using string concatenation

# Expected SQL string tested to create tables:
expected = f"CREATE TABLE test(" \
    f"column1 SERIAL PRIMARY KEY NOT NULL,"\
    f"column2 INTEGER NOT NULL,"\
    f"column3 VARCHAR"\
    f" )"

# test if it creates a table
# from sqlalchemy.sql import text
# statement = text(expected)

conn.execute(expected)


inspector = inspect(engine)
inspector.get_columns('test')
inspector.get_pk_constraint('test')
inspector.get_foreign_keys('test')
Return dependency-sorted table and foreign key constraint names in referred to within a particular schema.

# This will yield 2-tuples of (tablename, [(tname, fkname), (tname, fkname), ...])
# consisting of table names in CREATE order grouped with the foreign key constraint
#  names that are not detected as belonging to a cycle. The final element will be
#   (None, [(tname, fkname), (tname, fkname), ..]) which will consist of remaining
#   foreign key constraint names that would require a separate CREATE step after-the-fact,
#    based on dependencies between tables.
inspector.get_sorted_table_and_fkc_names('public')

# Test converted to lower case and stripped of spaces, to strip of spaces .replace(" ","")


def test_pass(input, expected):
    if input.lower() == expected.lower():
        print("passed")
    else:
        print("failed")
    return None


test_pass(input, expected)


df = pd.read_csv("test.csv", sep=";", header=0)


def ddl_generator(df):
    """takes a csv and generates DDL from it. For testing the SQL dialect is Postgresql. 
    expects column1 to contain the table names, column2 the column names, column3 an inidcator
    whether columns is a key, column 4 the type and column 5 information on nullability"""

    tables = df.iloc[:, 0].unique()

    for table in tables:

        create_sql = f"CREATE TABLE {table}("
        alter_sql = ""

        df_sub = df[df.iloc[:, 0] == table]

        # iterate along the rows of the subset dataframe
        for i in range(0, df_sub.shape[0]):

            create_sql = create_sql +\
                f"{df_sub.iloc[i,1]} "\
                f"{df_sub.iloc[i,3]} "\
                f"{'PRIMARY KEY ' if df_sub.iloc[i,2]  == True else ''}"\
                f"{'NOT NULL' if df_sub.iloc[i,4]  == True else ''}"
            if i < (df_sub.shape[0] - 1):
                create_sql = create_sql + ","

        create_sql = create_sql + ")"

    return(create_sql)


input = ddl_generator(df)

test_pass(input, expected)
