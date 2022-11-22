#get imports




#put parametrers and constants
DVD = ... #this is the path to config file
'''
# This section contains some script parameters
DATABASE_CONFIG = '.config\.postgres'
SECTION = 'postgresql'
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
    main()







 
