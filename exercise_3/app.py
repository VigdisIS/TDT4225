import datetime
from DbConnector import DbConnector
from pprint import pprint 
from haversine import haversine

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

        activities = self.db["activities"]
        result = list(activities.aggregate([
            # Fetch walk activities and corresponding trackpoints for user 112 
            # performed in year 2008
            {"$match": {
                "user_id": 112,
                "transportation_mode": "walk",
                'start_date_time': {'$regex':'^2008'}
            }},
            {"$lookup": {
                "from": "trackpoints",
                "localField": "_id",
                "foreignField": "activity_id",
                "as": "trackpoints"
            }}
        ]))

        # Calculate total distance for each trackpoint in each activity
        total_distance = 0
        for activity in result:
            trackpoints = activity['trackpoints']
            for i in range(len(trackpoints) - 1):
                # Use haversine formula to calculate distance between the 
                # current trackpoint and the next one, and add this to the 
                # total distance
                total_distance += haversine((float(trackpoints[i]['lat']), float(trackpoints[i]['lon'])), (float(trackpoints[i+1]['lat']), float(trackpoints[i+1]['lon'])))

        print(f'Total distance: {total_distance} km')
        print("-----------------------")
    
    def task_8(self):
        print("Task 8")

        # Get all valid trackpoints sorted by activity_id and timestamp
        trackpoints = list(self.db['trackpoints'].find({"altitude": {"$ne": '-777'}}).sort([('activity_id', 1), ('timestamp', 1)]))

        # Initialize variables for calculating altitude gains per activity
        prev_trackpoint = None
        altitude_gains = {}

        # Calculate altitude gains per activity
        for trackpoint in trackpoints:
            # Check if the current trackpoint is not the first one recorded for
            # and activity and that it is part of the same activity as the 
            # previous trackpoint, i.e. it has the same activity_id
            if prev_trackpoint is not None and trackpoint['activity_id'] == prev_trackpoint['activity_id']:
                # Calculate altitude difference between the current trackpoint
                # and the previous one
                altitude_diff = float(trackpoint['altitude']) - float(prev_trackpoint['altitude'])
                # Check if the altitude difference is positive, i.e. the current
                # trackpoint is at a higher altitude than the previous one
                if altitude_diff > 0:
                    activity_id = trackpoint['activity_id']
                    # Check if the current activity has already been added to 
                    # the altitude_gains dictionary, if not add it
                    if activity_id not in altitude_gains:
                        altitude_gains[activity_id] = 0
                    # Add the altitude difference to the total altitude gain
                    # for the current activity 
                    altitude_gains[activity_id] += altitude_diff
            # Update the previous trackpoint to be the current one, to check
            # for altitude differences in the next iteration
            prev_trackpoint = trackpoint

        # Convert altitude_gains dictionary to list of documents to be inserted
        # as a temporary collection to our database
        docs = [{'activity_id': k, 'altitude_gain': v} for k, v in altitude_gains.items()]

        # Insert the documents into the new altitude_gains collection
        self.db['altitude_gains'].insert_many(docs)

        alt_gain_per_activity = self.db['altitude_gains']
        result = list(alt_gain_per_activity.aggregate([
            # Join activities and altitude_gains collections on activity_id
            # to extract user_id for each activity, since we want to sum the 
            # total altitude gains across all activities for each user
            {"$lookup": {
                "from": "activities",
                "localField": "activity_id",
                "foreignField": "_id",
                "as": "activity"
            }},
            {"$group": {
                "_id": "$activity.user_id",
                "total_meters_gained_per_user": {
                    "$sum": {
                        "$multiply": ["$altitude_gain", 0.3048]  # Convert feet to meters
                    }
                }
            }},
            {"$sort": {"total_meters_gained_per_user": -1}},
            {"$limit": 20}
        ]))

        pprint(result)

        # Drop the temporary altitude_gains collection
        self.db['altitude_gains'].drop()

        print("-----------------------")
    
    def task_9(self):
        print("Task 9")
        # TODO: Implement this
        print("-----------------------")
    
    def task_10(self):
        print("Task 10")

        trackpoints = self.db["trackpoints"]
        result = list(trackpoints.aggregate([
            # Fetch all trackpoints that start with latitude 39.916 and 
            # longitude 116.397
            {"$match": {
                'lat': {'$regex':'^39.916'},
                'lon': {'$regex':'^116.397'}
            }},
            # Join trackpoints and activities collections on activity_id to 
            # extract the user_id for each activity, since we want to count
            # the number of unique users that have performed activities in
            # the Forbidden City of Beijing
            {"$lookup": {
                "from": "activities",
                "localField": "activity_id",
                "foreignField": "_id",
                "as": "activity_info"
            }},
            # Unwind the activity_info array to get a document for each 
            # activity
            {"$unwind": "$activity_info"},
            # Group by user_id to get all unique users that have performed
            # activities in this area
            {"$group": {
                "_id": "$activity_info.user_id"
            }}
        ]))

        pprint(result)
    
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
        # program.task_9()
        program.task_10()
        program.task_11()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
