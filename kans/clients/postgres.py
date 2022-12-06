import psycopg

conn = psycopg.connect(
    host="localhost",
    dbname="dvdrental",
    user="postgres",
    password="aakashojha873Qa")
cursor = conn.cursor()
cursor.execute("select * from actor")



conn.close()
##################

conn = psycopg.connect(
    host="localhost",
    dbname="dvdrental",
    user="postgres",
    password="aakashojha873Qa")
cursor = conn.cursor();
cursor.execute("CREATE SCHEMA test1 AUTHORIZATION postgres")
conn.close()

# Connect to an existing database
conn = psycopg.connect(
    host="localhost",
    dbname="dvdrental",
    user="postgres",
    password="aakashojha873Qa")

print('PostgreSQL database version:')
conn.execute('SELECT version()')

