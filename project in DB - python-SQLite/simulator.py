import sqlite3
import sys
import os
import sqlite3
import imp
import atexit
import inspect

timeToWork = {}
workingTasks = {}


def main():
    while os.path.isfile('world.db'):

        _conn = sqlite3.connect('world.db')
        if tasksTableIsEmpty(_conn.cursor()):
            #print("no more tasks")
            exit()
        else:

            finishedWorkers = []
            for key, value in timeToWork.items():
                if value == 0:
                    finishedWorkers.append(key)
                    workerName = getWorker(_conn.cursor(), key).name
                    print(workerName + " says: All Done!")
                    removeTask(_conn.cursor(), workingTasks[key])
                    _conn.commit()

                else:
                    timeToWork[key] = timeToWork[key] - 1

            for id in finishedWorkers:
                del timeToWork[id]
                del workingTasks[id]
            finishedWorkers=[]


            checkedIfBusy=[]
            for task in (getTasks(_conn.cursor())):
                worker = getWorker(_conn.cursor(), task.worker_id)
                resourceName = task.resource_name
                resourceAmount = task.resource_amount
                timeToMake = task.time_to_make
                currResourceAmount = currentAmountOfTheSpecificResource(_conn.cursor(), resourceName)
                if worker.id in timeToWork.keys():
                    if  worker.id not in checkedIfBusy:
                        workerTaskName = getTaskName(_conn.cursor(), workingTasks[worker.id])
                        print(worker.name + " is busy " + workerTaskName + "...")
                        checkedIfBusy.append(worker.id)
                elif resourceAmount <=  currResourceAmount:
                    timeToWork[worker.id] = timeToMake
                    workingTasks[worker.id] = task.id
                    updateResourceAmount(_conn.cursor(), resourceName, currResourceAmount - resourceAmount)
                    print(worker.name + " says: work work")
                    checkedIfBusy.append(worker.id)
            checkedIfBusy=[]
            
    if os.path.isfile('world.db'):        
    # connections must be closed when you done with them
        _conn.commit()  # commit any changes not yet written to the database
        _conn.close()  # close the connection

# task DTO
class tasks(object):
    def __init__(self, id, task_name, worker_id, time_to_make, resource_name, resource_amount):
        self.id = id
        self.task_name = task_name
        self.worker_id = worker_id
        self.time_to_make = time_to_make
        self.resource_name = resource_name
        self.resource_amount = resource_amount


# workers DTO
class workers(object):
    def __init__(self, id, name, status):
        self.id = id
        self.name = name
        self.status = status


# resources DTO
class resources(object):
    def __init__(self, name, amount):
        self.name = name
        self.amount = amount


def tasksTableIsEmpty(cur):
    cur.execute("""
      SELECT * FROM tasks
    """)
    return len(cur.fetchall()) == 0


def getWorker(cur, workerId):
    cur.execute("""
      SELECT * FROM workers 
      WHERE id = ? 
    """, (workerId,))
    return workers(*cur.fetchone())


def getTasks(cur):
    cur.execute("""
          SELECT * FROM tasks
    """)
    List = []
    for row in cur.fetchall():
        List.append(tasks(*row))
    return List

def getTaskName(cur, taskId):
    cur.execute("""
      SELECT task_name FROM tasks 
      WHERE id = ? 
    """, (taskId,) )
    return cur.fetchone()[0]


def removeTask(cur, taskId):
    cur.execute("""
      DELETE FROM tasks 
      WHERE id = ? 
    """, (taskId,) )



def currentAmountOfTheSpecificResource(cur, resourceName):
    cur.execute("""
        SELECT amount FROM resources WHERE name = ?
    """, (resourceName,) )
    return cur.fetchone()[0]


def updateResourceAmount(cur, resourceName, newAmount):
    cur.execute("""
      UPDATE resources 
      SET amount = ?
      WHERE name = ? 
    """, (newAmount, resourceName))


if __name__ == '__main__':
    main()
