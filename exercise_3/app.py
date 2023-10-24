import datetime
from DbConnector import DbConnector
from pprint import pprint 
import statistics




class App:

    # initialize the database connection and cursor
    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db


    def removeTime(self, time):
        return (datetime.datetime.isoformat(time))
    def task_1(self):
        print("Task 1")

        print("user")
        user = self.db["users"]
        user_count = user.count_documents({})
        print("user after")
        activities = self.db.activities
        activity_col_count = activities.count_documents({})

        trackpoints = self.db.trackpoints
        trackpoint_col_count = trackpoints.count_documents({})

        print("There are " + str(user_count) + " users in the database")
        print("There are " + str(activity_col_count) + " activities in the database")
        print("There are " + str(trackpoint_col_count) + " trackpoints in the database")

        print("-----------------------")
    
    def task_2(self):
        print("Task 2")

        activities = self.db["activities"]
        result = list(activities.aggregate([
              { "$group": {
        "_id": "$user_id",
        "count": { "$sum": 1 }
              }},
              {"$group": {
                "_id": None,
               "avarage_activity": {"$avg": "$count"}}
              }
        ]))

        print(result[0]["avarage_activity"])
        print("-----------------------")

    def task_3(self):
        print("Task 3")
        print("-----------------------")
        
        activities = self.db["activities"]

        result = list(activities.aggregate([
              { "$group": {
        "_id": "$user_id",
        "count": { "$sum": 1 }
              }},
              {"$sort": { "count": -1}
              },
              {"$limit": 20}

        ]))

        pprint(result)
    def task_4(self):
        print("Task 4")
        print("-----------------------")
        activities = self.db["activities"]
        result = list(activities.aggregate([
              { "$match": {
                  "transportation_mode": "taxi"
              }},
              {"$group": {
                  "_id": "$user_id",
                  "transportation_mode": {"$first": "$transportation_mode"},
              }}
              ]))

        pprint(result)

    def task_5(self):
        print("Task 5")
        activities = self.db["activities"]

        result2 = list(activities.aggregate([
                { "$match": {
                    "transportation_mode": {"$ne": None}
                }},
                {"$group": {
                  "_id": "$transportation_mode",
              }}
        ]))        

        result1 = list(activities.aggregate([
                { "$match": {
                    "transportation_mode": {"$ne": None}
                }},
                {"$group": {
                  "_id": "$user_id",
                  "count": { "$sum": 1 }
              }},
              {"$group": {
                "_id": None,
               "total_activities": {"$sum": "$count"}}
              }
        ]))
        
        pprint(result2)
        pprint(result1)

        
        print("-----------------------")

    def task_6(self):
        print("Task 6")


        activities = self.db["activities"]
        result = list(activities.aggregate([
            {"$group": {
                "_id": {"$substr": ["$start_date_time", 0, 4]},
                "count": {"$sum": 1}
            }},
        ]))

        pprint(result)
        print("-----------------------")

    def task_7(self):
        print("Task 7")

        print("-----------------------")
    
    def task_8(self):
        print("Task 8")
        print("-----------------------")
    
    def task_9(self):
        print("Task 9")
        print("-----------------------")
    
    def task_10(self):
        print("Task 10")
    
        print("-----------------------")

    def task_11(self):
        print("Task 11")
        activities = self.db["activities"]

        result = list(activities.aggregate([
             { "$match": {
                    "transportation_mode": {"$ne": None}
                }},
        {
            "$group": {
            "_id": {
                "transport_mode": "$transport_mode",
                "user_id": "$user_id"
            },
            "count": {
                "$sum": 1
            },
               "transport_mode": "$transport_mode",
            }
        },
        {
            "$sort": {
            "count": -1
            }
        },
        ]))

        pprint(result)

        print("-----------------------")
    
    




def main():
    program = None
    try:
        program = App()
        program.task_1()
        program.task_2()
        program.task_3()
        program.task_4()
        program.task_5()
        program.task_6()
        program.task_7()
        program.task_8()
        program.task_9()
        program.task_10()
        program.task_11()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
