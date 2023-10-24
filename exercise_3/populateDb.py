import json
from DbConnector import DbConnector
import os
from datetime import datetime, date, time
import uuid
from tabulate import tabulate

base_path = "./data/dataset/Data/"


class User:
    def  __init__(self, id):
        self.id = id
        self.hasLabel = False
    
    def setHasLabel(self, hasLabel):
        self.hasLabel = hasLabel
    
    def toJson(self):
        return {"_id": self.id, "hasLabel": self.hasLabel}
  
class Activity:
    def __init__(self, id, user_id):
        # we create the id manually because it is needed to create the trackpoint table
        self.id = str(uuid.uuid4())
        # secondary_id references the filename of the activity
        self.secondary_id = id
        # user_id is a foreign key to the user table
        self.user_id = user_id
        # transportation mode is defined in the labels.txt file
        self.transportation_mode = None
        # start_date_time and end_date_time are the start and end time of the activity
        self.start_date_time = None
        self.end_date_time = None
    

    # setters for the activity class as these attributes are initalized as None
    def set_transportation_mode(self, transport_mode):
        self.transportation_mode = transport_mode
    
    def set_start_date_time(self, start_date_time):
        self.start_date_time = start_date_time

    def set_end_date_time(self, end_date_time):
        self.end_date_time = end_date_time
    
    def toJson(self):
        return {"_id": self.id, "secondary_id": self.secondary_id, "user_id": self.user_id, "transportation_mode": self.transportation_mode, "start_date_time": self.start_date_time, "end_date_time": self.end_date_time}
class TrackPoint:
    def __init__(self, activity_id, lat, lon, altitude, date_days, date_time):
        # the trackpoint object does not have an id as it is created automatically by the MySQL database
        self.id =  str(uuid.uuid4())
        # activity_id is a foreign key to the activity table
        self.activity_id = activity_id
        # lat, lon and altitude are the coordinates of the trackpoint
        self.lat = lat
        self.lon = lon
        self.altitude = altitude
        # date_days and date_time are the date and time of the trackpoint
        self.date_days = date_days
        self.date_time = date_time
    
    def toJson(self):
        return {"_id": self.id, "activity_id": self.activity_id, "lat": self.lat, "lon": self.lon, "altitude": self.altitude, "date_days": self.date_days, "date_time": self.date_time}

# the label class is created to make it easier to read the labels.txt file and added the information to the activities. 
class Label:
    def __init__(self, start_time, end_time, mode_of_transportation):
        self.start_time = start_time
        self.end_time = end_time
        self.mode_of_transportation = mode_of_transportation

# this clases is separated from the app.py file for two reasons
# 1. to make the code more readable
# 2. to make it easier to reset the database and populate it again
class PopulateDB:

    # initialize the database connection and cursor
    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db
    
    # present all the tables in the database for the raport
    def present(self):
        # TODO
        print("present")
        return
  

    # walk through all the files in the dataset and add them to the database
    # this function is separated from the create_tables and reset functions because 
    # we want the creation and reset of the database to be separate from the population of the database
    # to make it easier to debug 
    def walkFiles(self):
        # walk through all 181 folders with user date
        # Hardcoding the range to 182 would not be a good practice in any other 
        # scenario but given that we know that there are 182 folders with user data we can do it in this case
        for i in range(0, 182):
            # for each folder create a new user as each folder represent the user data
            print("User: " + str(i))

            new_user = User(i)
            # add the correct start to the path of the folder based on the user id
            if i < 10:
                path = base_path + "00" + str(i)
            elif i < 100:
                path = base_path + "0" + str(i)
            else:
                path = base_path + str(i)

            # initialize empty list for track_points, activities and labels
            track_points = []
            activities = []

            # the reason for having a labels list is that we only want to go trough the labels.txt file once
            # and then iterate through the list when we are adding the labels to the activities
            labels = []
            
            # check if the user folder contains a lables.txt file
            # there is also a labeled_ids.txt file but it seemed to not be needed as checking which users are in that file and 
            # adding them to a list would be the same as checking if the folder contains a labels.txt file
            if os.listdir(path).__contains__("labels.txt"):
                # if the folder contains a labels.txt file we set the user hasLabel to true
                # this is to make sure that we only search for labels to activities when the user data is labeled
                new_user.setHasLabel(True)
                # we set the path to the labels.txt file
                label_path = path + "/labels.txt"

                # open the labels.txt file and read through every line except the first line as it is not relevant
                f = open(label_path, "r")
                label_path_lines = f.readlines()[1:]

                for line in label_path_lines:
                    # we replace the "\n" and "\t" with " " and then split the line by " " to get the different attributes
                    split_line = line.replace("\n", "").replace("\t", " ").split(" ")
                    # we split the date and time attributes to combine them into a datetime object
                    start_datesplit = split_line[0].split("/")
                    start_timesplit = split_line[1].split(":")
                    # we create a new datetime object with the date and time attributes
                    d_start = date(int(start_datesplit[0]), int(start_datesplit[1]), int(start_datesplit[2]))
                    t_start = time(int(start_timesplit[0]), int(start_timesplit[1]), int(start_timesplit[2]))
                    # we combine the date and time attributes into a datetime object
                    start_datetime = datetime.combine(d_start, t_start)
                    start_datetime_str = start_datetime.strftime("%Y-%m-%d %H:%M:%S") 

                    # we do the same for the end date and time attributes        
                    end_datesplit = split_line[2].split("/")
                    end_timesplit = split_line[3].split(":")

                    d_end = date(int(end_datesplit[0]), int(end_datesplit[1]), int(end_datesplit[2]))
                    t_end = time(int(end_timesplit[0]), int(end_timesplit[1]), int(end_timesplit[2]))

                    end_datetime = datetime.combine(d_end, t_end)
                    end_datetime_string = end_datetime.strftime("%Y-%m-%d %H:%M:%S") 

                    # we create a new label object and append it to the labels list
                    mode_of_transportation = split_line[4]

                    new_label: Label = Label(start_datetime_str, end_datetime_string, mode_of_transportation)

                    labels.append(new_label)


            # go through each individual file in the Trajectory folder
            for filename in os.listdir(path + "/Trajectory"):
                # if the filename is labels.txt skip that file
                # we do this because we only want to add the labels to the activities and not the labels.txt file
                # this is not really necessary because the labels.txt file is outside 
                # of the Trajectory folder but is done just in case
                if(filename == "labels.txt"):
                        continue
                
                # set the id of the activity to the filename
                new_id = filename.split(".")[0]

                # initialize a new activity object with the id and user_id
                # the activity object is initialized with id = uuid() and the new_id in this case 
                # refers to the secondary_id of the activity
                new_activity = Activity(new_id, i)

                # set the path of the file that we are currently looking at
                new_path = path + "/Trajectory/" + filename

                # open the file and read through it
                f = open(new_path, "r")

                # we read all the lines in the file except the first 7 because they are not relevant
                file_text_lines = f.readlines()[6:]

                # if the file has more than 2500 lines (more than 2500 trackpoints) we skip that file to make sure that we do not proccess to much data
                if (len(file_text_lines) >= 2500):
                    continue
                
                # when iterating through all trackpoints we keep track of the first and last trackpoint
                # because their datetime represent the start and end of the activity
                start_trackpoint: TrackPoint = None
                end_trackpoint: TrackPoint = None

                # for each line in the file we split the line by "," to get the different attributes
                for line in file_text_lines:
                    line = line.split(",")

                    # we split the date and time attributes to combine them into a datetime object
                    datesplit = line[5].split("-")
                    timesplit = line[6].split(":")
                    d = date(int(datesplit[0]), int(datesplit[1]), int(datesplit[2]))
                    t = time(int(timesplit[0]), int(timesplit[1]), int(timesplit[2]))
                    date_time = datetime.combine(d, t)
                    date_time_str = date_time.strftime("%Y-%m-%d %H:%M:%S") 

                    # we create a new trackpoint object and append it to the track_points list
                    # we skip the 2nd attribute in the line because it is not relevant
                    track_point: TrackPoint = TrackPoint(new_activity.id, line[0], line[1], line[3], line[4], date_time_str)
                    # we append the trackpoint to the track_points list
                    track_points.append(track_point.toJson())

                    # update the start tackpoint of the activity if the current trackpoint date_time is smaller than the start_trackpoint date_time
                    if (start_trackpoint == None or track_point.date_time < start_trackpoint.date_time):
                        start_trackpoint = track_point

                    # update the end trackpoint of the activity if the current trackpoint date_time is bigger than the end_trackpoint date_time
                    if (end_trackpoint == None or track_point.date_time > end_trackpoint.date_time):
                         end_trackpoint = track_point
                
                f.close()
                    
                    
                # when when have gone through all the trackpoints we set the start and end date_time of the activity
                new_activity.set_start_date_time(start_trackpoint.date_time)
                new_activity.set_end_date_time(end_trackpoint.date_time)

                # if the user has labels we iterate through the labels list to find the label that matches the start and end date_time of the activity
                # if the label matches we add the transportation mode to the activity
                if (new_user.hasLabel):
                    for label in labels:
                        if (new_activity.start_date_time == label.start_time and new_activity.end_date_time == label.end_time):
                            new_activity.set_transportation_mode(label.mode_of_transportation)
                            break
                # we append the activity to the activities list
                activities.append(new_activity.toJson())


           
            # when we have gone through all the files in the Trajectory folder we add the user, activities and trackpoints to the database
            self.addToDatabase(new_user, activities, track_points)
            print("Added user:" + str(i) + " to the list, with activites and trackpoints")
            print("---------------------------")
            
   
    def addToDatabase(self,user: User, activities: list[Activity], trackpoints: list[TrackPoint]):
        print("Adding user to the database")
        collection = self.db["users"]
        collection.insert_one(user.toJson())

        
        if (len(activities) > 0):
            print("Adding activities to the database")
            collection = self.db["activities"]
            collection.insert_many(activities)

        if (len(trackpoints) > 0):
            print("Adding trackpoints to the database")
            collection = self.db["trackpoints"]
            collection.insert_many(trackpoints)

    def create_colls(self):
        # Create USER COLLECTION
        collection = self.db.create_collection("users")    

        print("Create user collection", collection)

        # Create ACTIVITY COLLECTION
        collection = self.db.create_collection("activities")

        print("Create activity collection", collection)

        # Create TRACKPOINT COLLECTION
        collection = self.db.create_collection("trackpoints")
        print("Create trackpoint collection", collection)

    # this function drops all tables in the database to reset it
    def reset(self):
        collection = self.db["users"]
        collection.drop()
        collection = self.db["activities"]
        collection.drop()
        collection = self.db["trackpoints"]
        collection.drop()
        


def main():
    program = None
    try:
        program = PopulateDB()
        program.reset()
        program.create_colls()
        program.walkFiles()
        program.present()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
