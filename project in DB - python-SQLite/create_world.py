import sqlite3
import sys
import os
import sqlite3
import imp
import atexit
import inspect

nextTaskId = 1

def main(args):



    def create_tables():
        global nextTaskId
        # the database doesn't exist, create a database
        _conn = sqlite3.connect('world.db')
        # create tables for the database
        _conn.executescript("""
            CREATE TABLE tasks (
                id              INTEGER  PRIMARY KEY,
                task_name       TEXT     NOT NULL,
                worker_id       INTEGER  REFERENCES workers(id),
                time_to_make    INTEGER  NOT NULL,
                resource_name   TEXT     NOT NULL,
                resource_amount INTEGER  NOT NULL
            );

            CREATE TABLE workers (
                id      INTEGER   PRIMARY KEY,
                name    TEXT      NOT NULL,
                status  TEXT      NOT NULL
            );

            CREATE TABLE resources (
                name    TEXT        PRIMARY KEY,
                amount  INTEGER     NOT NULL
            );
            """)
        
        with open(args[1], 'r') as my_file:
            content = my_file.readlines()
        content = [x.strip() for x in content]

        cur=_conn.cursor()

        for line in content:
            
            data = line.split(',')  # split string into a list
            
            if len(data) == 5:  # task
                id = nextTaskId
                nextTaskId=nextTaskId+1
                task_name = data[0]
                worker_id = int(data[1])
                time_to_make = int(data[4])
                resource_name = data[2]
                resource_amount = int(data[3])
                cur.execute("""
                     INSERT INTO tasks (id, task_name, worker_id, time_to_make, resource_name, resource_amount)\
                      VALUES (?,?,?,?,?,?)
                """, (id, task_name, worker_id, time_to_make, resource_name, resource_amount))
                
            elif len(data) == 3:  # worker
                id = int(data[1])
                name = data[2]
                status = data[0]
                cur.execute("""
                     INSERT INTO workers (id, name, status) VALUES (?,?,?)    
                """, (id, name, status))
                
            elif len(data) == 2:  # resourse
                name = data[0]
                amount = int(data[1])
                cur.execute("""
                     INSERT INTO resources (name, amount) VALUES (?,?)    
                """, (name, amount))

        # connections must be closed when you done with them
        _conn.commit()  # commit any changes not yet written to the database
        _conn.close()  # close the connection



    if os.path.isfile('world.db'):
        # the database is already exists, exist the module
        exit()
    else:
        create_tables()



if __name__ == '__main__':
    main(sys.argv)