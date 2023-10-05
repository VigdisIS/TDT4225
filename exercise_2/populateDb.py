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

class Activity:
    def __init__(self, id, user_id):
        self.id = str(uuid.uuid4())
        self.secondary_id = id
        self.user_id = user_id
        self.transportation_mode = None
        self.start_date_time = None
        self.end_date_time = None
    
    def set_transportation_mode(self, transport_mode):
        self.transportation_mode = transport_mode
    
    def set_start_date_time(self, start_date_time):
        self.start_date_time = start_date_time

    def set_end_date_time(self, end_date_time):
        self.end_date_time = end_date_time

class TrackPoint:
    def __init__(self, activity_id, lat, lon, altitude, date_days, date_time):
        self.activity_id = activity_id
        self.lat = lat
        self.lon = lon
        self.altitude = altitude
        self.date_days = date_days
        self.date_time = date_time
    

class Label:
    def __init__(self, start_time, end_time, mode_of_transportation):
        self.start_time = start_time
        self.end_time = end_time
        self.mode_of_transportation = mode_of_transportation


class PopulateDB:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor
    
    def present(self):
        print('USER TABLE:')
        query = '''
            select
                *
            from user
            limit 10;
        '''
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print(tabulate(result, headers=self.cursor.column_names))

        print('ACTIVITY TABLE:')
        query = '''
            select
                *
            from activity
            limit 10;
        '''
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print(tabulate(result, headers=self.cursor.column_names))  

        print('TRACKPOINT TABLE:')
        query = '''
            select
                *
            from trackpoint
            limit 10;
        '''
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print(tabulate(result, headers=self.cursor.column_names))  

    def create_tables(self):
            user_query = """CREATE TABLE user (
                    id INTEGER PRIMARY KEY,
                    has_labels BOOLEAN
                    )"""
            activity_query = """
                    CREATE TABLE activity (
                    id VARCHAR(100) PRIMARY KEY,
                    secondary_id BIGINT,
                    user_id INTEGER ,
                    FOREIGN KEY (user_id) REFERENCES user(id),
                    transportation_mode TEXT,
                    start_date_time DATETIME,
                    end_date_time DATETIME
                    )"""
            trackpoint_query = """
                    CREATE TABLE trackpoint (
                    id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    activity_id VARCHAR(100),
                    FOREIGN KEY (activity_id) REFERENCES activity(id),
                    lat DOUBLE,
                    lon DOUBLE,
                    altitude INTEGER,
                    date_days DOUBLE,
                    date_time DATETIME
                    )
                    """
            print("Creating user table...")
            self.cursor.execute(user_query)
            print("User table created")
            print("------------------------")
            print("Creating activity table...")
            self.cursor.execute(activity_query)
            print("Activity table created")
            print("------------------------")
            print("Creating trackpoint table...")
            self.cursor.execute(trackpoint_query)
            print("Trackpoint table created")
            print("------------------------")
            self.db_connection.commit()

    def walkFiles(self):
        # walk through all 181 folders with user date
        for i in range(0, 182):
            # for each folder create a new user
            print("User: " + str(i))
            new_user = User(i)
            # add the correct start to the path of the folder based on the user id
            if i < 10:
                path = base_path + "00" + str(i)
            elif i < 100:
                path = base_path + "0" + str(i)
            else:
                path = base_path + str(i)

            # initialize empty folder for track_points and activities for the user
            track_points = []
            activities = []
            labels = []
            # walk through all the files inside that folder
            if os.listdir(path).__contains__("labels.txt"):
                new_user.setHasLabel(True)
                label_path = path + "/labels.txt"
                f = open(label_path, "r")
                label_path_lines = f.readlines()[1:]
                for line in label_path_lines:
                    split_line = line.replace("\n", "").replace("\t", " ").split(" ")
                    start_datesplit = split_line[0].split("/")
                    start_timesplit = split_line[1].split(":")
                    d_start = date(int(start_datesplit[0]), int(start_datesplit[1]), int(start_datesplit[2]))
                    t_start = time(int(start_timesplit[0]), int(start_timesplit[1]), int(start_timesplit[2]))

                    start_datetime = datetime.combine(d_start, t_start)
                            
                    end_datesplit = split_line[2].split("/")
                    end_timesplit = split_line[3].split(":")

                    d_end = date(int(end_datesplit[0]), int(end_datesplit[1]), int(end_datesplit[2]))
                    t_end = time(int(end_timesplit[0]), int(end_timesplit[1]), int(end_timesplit[2]))

                    end_datetime = datetime.combine(d_end, t_end)

                    mode_of_transportation = split_line[4]

                    new_label: Label = Label(start_datetime, end_datetime, mode_of_transportation)

                    labels.append(new_label)


                # if the folder contains a labels.txt file, set the user hasLabel to true
                # this is to make sure that we only search for labels to activities when the user data is labeled
                    

                # go through each individual file in the files
            for filename in os.listdir(path + "/Trajectory"):
                # if the filename is labels.txt skip that file
                if(filename == "labels.txt"):
                        continue
                # set the id of the activity to the filename
                new_id = filename.split(".")[0]

                # initialize a new activity object
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
                    
                start_trackpoint: TrackPoint = None
                end_trackpoint: TrackPoint = None
                # for each line in the file we split the line by "," and create a new trackpoint object
                for line in file_text_lines:
                    line = line.split(",")

                    # we split the date and time attributes to combine them into a datetime object
                    datesplit = line[5].split("-")
                    timesplit = line[6].split(":")
                    d = date(int(datesplit[0]), int(datesplit[1]), int(datesplit[2]))
                    t = time(int(timesplit[0]), int(timesplit[1]), int(timesplit[2]))
                    date_time = datetime.combine(d, t)


                        # we create a new trackpoint object and append it to the track_points list
                    track_point: TrackPoint = TrackPoint(new_activity.id, line[0], line[1], line[3], line[4], date_time)
                    track_points.append((track_point.activity_id, track_point.lat, track_point.lon, track_point.altitude, track_point.date_days, track_point.date_time))

                    # update the start tackpoint of the activity if the current trackpoint date_time is smaller than the start_trackpoint date_time
                    if (start_trackpoint == None or track_point.date_time < start_trackpoint.date_time):
                        start_trackpoint = track_point

                    # update the end trackpoint of the activity if the current trackpoint date_time is bigger than the end_trackpoint date_time
                    if (end_trackpoint == None or track_point.date_time > end_trackpoint.date_time):
                         end_trackpoint = track_point
                    
                f.close()
                    
                    

                new_activity.set_start_date_time(start_trackpoint.date_time)
                new_activity.set_end_date_time(end_trackpoint.date_time)

                if (new_user.hasLabel):
                    for label in labels:
                        if (new_activity.start_date_time == label.start_time and new_activity.end_date_time == label.end_time):
                            new_activity.set_transportation_mode(label.mode_of_transportation)
                            break
                activities.append((new_activity.id, new_activity.secondary_id, new_activity.user_id, new_activity.transportation_mode, new_activity.start_date_time, new_activity.end_date_time))
            self.addToDatabase(new_user, activities, track_points)
            print("Added user:" + str(i) + " to the database, with activites and trackpoints")
            print("---------------------------")
            
   
    def addToDatabase(self,user: User, activities: list[Activity], trackpoints: list[TrackPoint]):
        add_user = ''' INSERT INTO user(id,has_labels)
              VALUES(%s,%s) '''
        self.cursor.execute(add_user, (user.id, user.hasLabel))
        add_activity = ''' INSERT INTO activity(id,secondary_id,user_id,transportation_mode,start_date_time,end_date_time)
              VALUES(%s,%s,%s,%s,%s,%s) '''
        self.cursor.executemany(add_activity, activities)
        add_trackpoint = ''' INSERT INTO trackpoint(activity_id,lat,lon,altitude,date_days,date_time)
              VALUES(%s,%s,%s,%s,%s,%s) '''
        self.cursor.executemany(add_trackpoint, trackpoints)
        self.db_connection.commit()

    def reset(self):
        print("Dropping trackpoint table")  
        self.cursor.execute("DROP TABLE IF EXISTS trackpoint")
        print("Trackpoint table dropped")
        print("------------------------")
        print("Dropping activity table")
        self.cursor.execute("DROP TABLE IF EXISTS activity")
        print("Activity table dropped")
        print("------------------------")
        print("Dropping user table")
        self.cursor.execute("DROP TABLE IF EXISTS user")
        print("User table dropped")
        print("------------------------")
        self.connection.db_connection.commit()


def main():
    program = None
    try:
        program = PopulateDB()
        program.reset()
        program.create_tables()
        program.walkFiles()
        program.present()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
