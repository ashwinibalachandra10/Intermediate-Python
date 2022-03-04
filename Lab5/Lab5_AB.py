#!/usr/bin/env python
# coding: utf-8




#Lab5 Ashwini Balachandra
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
import sqlite3
from sqlite3 import Error
people_db_file = "sqlite.db"# The name of the database file to use
max_people = 500 # Number  of records to create





#Part1
def generate_people(count):
    # with open('LastNames.txt', 'r') as filehandle:
    #    last_names = [line.rstrip()  for line in filehandle.readlines()]
    # with open('FirstNames.txt', 'r') as filehandle1:
    #    first_names = [line.rstrip() for line in filehandle1.readlines()]
    last_names = []
    first_names =[]
    with ThreadPoolExecutor(max_workers=2) as executor:
         future = (executor.submit(txt_list,'LastNames.txt'))
         future1 = (executor.submit(txt_list,'FirstNames.txt'))
         last_names = future.result()
         first_names = future1.result()
              
    my_tuple = ()
    names = [(counter,first_names[random.randint(0,len(first_names)-1)],last_names[random.randint(0,len(last_names)-1)]) for counter in range(count)]
    return names
                   
def txt_list(file_name):
    with open(file_name, 'r') as filehandle:
         lt_names = [line.rstrip()  for line in filehandle.readlines()]  
    return lt_names
                   
                   



#Part2
def create_people_database(db_file, count):
    conn = sqlite3.connect(db_file)
    with conn:
        
        sql_create_people_table = """ CREATE TABLE IF NOT EXISTS people (
        id integer PRIMARY KEY,
        first_name text NOT NULL,last_name text NOT NULL); """
        cursor = conn.cursor()
        cursor.execute(sql_create_people_table)
        sql_truncate_people = "DELETE FROM people;"
        cursor.execute(sql_truncate_people)
        people = generate_people(count)
        sql_insert_person = "INSERT INTO people(id,first_name,last_name) VALUES(?,?,?);"
        for person in people:
            print(person)# uncomment if you want to see the person object
            cursor.execute(sql_insert_person, person)
            print(cursor.lastrowid)# uncomment if you want to see the row id 
        cursor.close()




#Part 3
class PersonDB():
    def __init__(self, db_file=''):
        ''' Store db_file parameter value '''
        self.db_file = db_file

    def __enter__(self):
        ''' Initiate connection to database '''
        self.conn = sqlite3.connect(self.db_file,check_same_thread=False)
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        ''' Close database connection '''
        self.conn.close()

    def load_person(self, id):
        sql = "SELECT * FROM people WHERE id=?"

        cursor = self.conn.cursor()
        cursor.execute(sql, (id,))
        records = cursor.fetchall()
        result = (-1,'','') # id = -1, first_name = '', last_name = ''

        if records is not None and len(records) > 0:
                result = records[0]
        cursor.close()
        return result





def test_PersonDB():
    with PersonDB(people_db_file) as db:
        print(db.load_person(10000)) # Should print the default
        print(db.load_person(122))
        print(db.load_person(300))
        
print(test_PersonDB())





#PART 4

def load_person(id, db_file):
    with PersonDB(db_file) as db:
        return db.load_person(id)
    
    
    
def load_all_people():
    worker_thread = 10
    result_list = []
    with ThreadPoolExecutor(worker_thread ) as executor:
        futures = [executor.submit(load_person, x,people_db_file) for x in range(max_people -1)]
        for future in as_completed(futures):
            result_list.append(future.result())
            #print(future.result())
        print(result_list)
        result_list.sort(key=lambda x:(x[2],x[1]))
        print(result_list)

if __name__ == "__main__":
    people = generate_people(300)
    print(people)
    create_people_database(people_db_file, max_people-1)
    load_all_people()
    













