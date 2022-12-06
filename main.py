#get imports
import pandas as pd
import pypika
from psycopg import Cursor
from pypika import PostgreSQLQuery, Schema, Column




#put parametrers and constants
DVD = ... #this is the path to config file
'''
# This section contains some script parameters
DATABASE_CONFIG = '.config\.postgres'
SECTION = 'postgresql'
The following is the list of the connection parameters:

   database: the name of the database that you want to connect.

   user: the username used to authenticate.
   
   password: password used to authenticate.

   host: database server address e.g., localhost or an IP address.

   port: the port number that defaults to 5432 if it is not provided.
'''



#put etl function
def create_cursor():
    cursor = ...
    return cursor

#reading from the public schema
def read_table():
    data = ...
    return data 

def transform_dates():
    date = ...
    return date 
  






#create tasks with etl function
import tasks





#put task in dag(graph.py)
#network x 
#graph.py





#sort dag in topological order
#network x




#put sorted task in a queue
#queue.py




#executor pull tasks off queue and runs them
#combine with scheduler or seprate




#scheduler determines when to run the above
#crontab
#apscheduler
#schedule
#https://apscheduler.readthedocs.io/en/3.x/

if __name__ == '__main__':
    # this part is needed to execute the program
    main()







 
