from DbConnector import DbConnector
from haversine import haversine, Unit
from tabulate import tabulate

class Part2:
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def present(self, task_query):
        self.cursor.execute(task_query)
        result = self.cursor.fetchall()
        print(tabulate(result, headers=self.cursor.column_names))  
    
    def task1(self):
        # Count the number of distinct users, activities and trackpoints
        print("Task 1:")
        return '''
            select
                count(distinct u.id) as "number_of_users",
                count(distinct a.id) as "number_of_activities",
                count(distinct t.id) as "number_of_trackpoints"
            from user u
            left join activity a
                on u.id = a.user_id
            left join trackpoint t
                on a.id = t.activity_id;
            '''
    
    def task2(self):
        # We first create a CTE where each user has a count of their trackpoints
        # by joining the user table with the activity table and the trackpoint 
        # table.
        # We then calculate the average, maximum and minimum number of 
        # trackpoints per user
        print("Task 2:")
        return '''
            with total_trackpoints as (
                select
                    user.id as user_id,
                    count(trackpoint.id) as total_trackpoints
                from user
                left join activity 
                    on user.id = activity.user_id
                left join trackpoint 
                    on activity.id = trackpoint.activity_id
                group by user.id
            )
            select 
                avg(total_trackpoints) as average_trackpoints,
                max(total_trackpoints) as maximum_trackpoints,
                min(total_trackpoints) as minimum_trackpoints
            from total_trackpoints;
            '''

    def task3(self):
        # We count the number of activities registered per user, and order the
        # resulting table by this count limited to only the top 15 by 
        # descending order.
        print("Task 3:")
        return '''
            select 
                user_id, 
                count(id) as number_of_activities
            from activity 
            group by user_id 
            order by number_of_activities desc limit 15;
            '''

    def task4(self):
        # We select all distinct users from the activity table where the
        # transportation mode is bus.
        print("Task 4:")
        return '''
            select distinct
                user_id
            from activity
            where transportation_mode = 'bus';
            '''

    def task5(self):
        # We select the user_id and count the number of different transportation
        # modes registered per user, and order the resulting table by this count
        # limited to only the top 10 by descending order.
        print("Task 5:")
        return '''
            select 
                user_id,
                count(distinct transportation_mode) as distinct_transportation_modes
            from activity
            group by user_id
            order by distinct_transportation_modes desc limit 10;
            '''
    
    def task6(self):
        # We create a CTE where we select all the rows from the activity table
        # where the combination of secondary_id, transportation_mode, 
        # start_date_time and end_date_time is duplicated. We then count the
        # number of rows in this CTE, and get the total number of times an
        # activity is duplicated.
        print("Task 6:")
        return '''
        with duplicates as (
            select 
                secondary_id, 
                transportation_mode,
                start_date_time,
                end_date_time,
                count(*)
            from activity
            group by secondary_id, transportation_mode, start_date_time, end_date_time
            having count(*) > 1)
        select
            count(*) as "number of duplicated activities"
        from duplicates;
            '''
    
    def task7_a(self):
        # The datediff() function will give the number of days between two
        # dates. If the difference is 0, the activity is done in the same day,
        # i.e. there is no difference between the dates. We select the number
        # of distinct users from the activity table where the difference between
        # the start_date_time and end_date_time is not 0, i.e. the activity is
        # started one day, and ended in another.
        print("Task 7:")
        print("a)")
        return '''
            select
                count(distinct user_id) as users
            from activity
            where datediff(start_date_time, end_date_time) != 0;
            '''
    
    def task7_b(self):
        # We perform he same where clause as in task 7a, but we also select the
        # transportation_mode, user_id and the difference between the 
        # start_date_time and end_date_time. We order the resulting table by
        # user_id.
        print("Task 7:")
        print("b)")
        return '''
            select
                transportation_mode,
                user_id,
                timediff(end_date_time, start_date_time) as duration
            from activity
            where datediff(start_date_time, end_date_time) != 0
            order by user_id;
            '''

    def task8(self):
        # We select all the rows from the trackpoint table, and join it with the
        # activity table on the activity_id to get access to information such as
        # user_id, activity_id, lat, lon and date_time.
        print("Task 8:")
        query = '''
            select 
                a1.user_id,
                t1.activity_id as activity_id,
                t1.lat as lat,
                t1.lon as lon,
                t1.date_time as date_time
            from trackpoint as t1
            left join activity as a1 
                on t1.activity_id = a1.id
            order by user_id asc;
            '''
        self.cursor.execute(query)
        rows = self.cursor.fetchall()

        # We create an empty hashmap where we will store the rows that are close
        # in space, decided in the loop below.
        close_users = {}
        # We iterate through the rows, comparing the current row to the next,
        # always checking that a user isn't compared to itself.
        for i in range(len(rows)):
            if rows[i] != rows[-1] and rows[i][0] != rows[i+1][0]:
                for j in range(i+1, len(rows)):
                    user1 = rows[i]
                    user2 = rows[j]

                    # Calculate distance using Haversine formula
                    loc1 = (user1[2], user1[3])
                    loc2 = (user2[2], user2[3])
                    distance = haversine(loc1, loc2, unit='m')

                    # If the distance is less than 50 meters, we add the row to
                    # the hashmap.
                    if distance <= 50:
                        close_users[f"{i}{j}"] = {
                            "user_1": user1[0],
                            "date_time_1": user1[4],
                            "user_2": user2[0],
                            "date_time_2": user2[4],
                            "distance": distance
                        }

        # Create a temporary table to store the hashmap data
        create_temp_table_query = '''
            create temporary table activity_distance_temp (
                user_1 int,
                date_time_1 datetime,
                user_2 int,
                date_time_2 datetime,
                distance float
            )
            '''
        self.cursor.execute(create_temp_table_query)

        # Insert the hashmap data into the temporary table
        for key in close_users:
            insert_query = f'''
                insert into activity_distance_temp (
                    user_1, 
                    date_time_1, 
                    user_2, 
                    date_time_2, 
                    distance
                ) values (
                    '{close_users[key]['user_1']}', 
                    '{close_users[key]['date_time_1']}',
                    '{close_users[key]['user_2']}',
                    '{close_users[key]['date_time_2']}',
                    {close_users[key]['distance']}
                )
                '''
            self.cursor.execute(insert_query)

        # Create another two temporary tables, since MySQL doesn't allow us to 
        # use the same temporary table twice in the same query, and we want to 
        # get the union of the two user columns to deduce the number of users 
        # that at some point have been close in time and space. We also only
        # get the users that are close in time from the ones that are already
        # confirmed to be close in space.
        create_temp_user1_query = '''
            create temporary table user_1_temp as
            select
                user_1 as user_id
            from activity_distance_temp
            where abs(timestampdiff(second, date_time_1, date_time_2)) <= 30;
            '''
        self.cursor.execute(create_temp_user1_query)

        create_temp_user2_query = '''
            create temporary table user_2_temp as
            select
                user_2 as user_id
            from activity_distance_temp
            where abs(timestampdiff(second, date_time_1, date_time_2)) <= 30;
            '''
        self.cursor.execute(create_temp_user2_query)
        
        # We return the number of users that are close in time and space by
        # counting the number of distinct users in the union of the two
        # temporary tables.
        return '''
            select 
                count(distinct user_id) as "# of users close in time and space"
            from (
                select
                    user_id 
                from user_1_temp
                union
                select
                    user_id 
                from user_2_temp
            ) as users_count;
            '''     

    def task9(self):
        # We create a CTE where we select the activity_id, and the sum of the
        # difference between the altitude and the previous altitude, if the
        # altitude is not null and the altitude is greater than the previous
        # altitude. We then select the user_id and the sum of the total
        # increase in altitude per user, and order the resulting table by the
        # sum of the total increase in altitude per user, limited to the top 15
        # by descending order.
        print("Task 9:")
        return '''
            with total_increase_per_activity as (
                select 
                    activity_id,
                    sum(case
                            when altitude is not null and altitude > prev_altitude
                            then altitude - prev_altitude
                            else 0 
                        end) 
                    as total_increase
                from (
                    select 
                        activity_id,
                        case 
                            when altitude = -777 
                            then null
                            else altitude * 0.3048 -- convert feet to meters
                        end as altitude,
                        lag(altitude * 0.3048) over (partition by activity_id order by date_time) as prev_altitude
                    from trackpoint
                ) as altitude_diff
                group by activity_id
            )
            select
                user_id as id,
                sum(total_increase) as "total meters gained per user"
            from activity
            left join total_increase_per_activity 
                on activity.id = total_increase_per_activity.activity_id
            group by user_id
            order by sum(total_increase) desc limit 15;
            '''

    def task10(self):
        # We select the transportation_mode, activity_id, lat, lon and date_time
        # from the activity table, and join it with the trackpoint table on the
        # activity_id.
        print("Task 10:")
        query = '''
            select 
                transportation_mode,
                activity_id,
                lat,
                lon,
                date_time
            from activity
            left join trackpoint 
                on activity.id = trackpoint.activity_id
            where transportation_mode is not null
            '''
        self.cursor.execute(query)
        activities = self.cursor.fetchall()

        # We create an empty hashmap where we will store the rows after
        # calculating the distance between each trackpoint in each activity.
        hashmap = {}

        # We iterate through the rows, creating a hashmap where the key is the
        # activity_id, and the value is a list of tuples containing the lat and
        # lon of each trackpoint in the activity.
        for i in range(len(activities)):
            if activities[i][1] not in hashmap:
                hashmap[activities[i][1]] = []
                new_tuple = (activities[i][2], activities[i][3])
                if hashmap[activities[i][1]] is not None:
                    hashmap[activities[i][1]].append(new_tuple)
                else:
                    hashmap[activities[i][1]] = [new_tuple]
            else:
                new_tuple = (activities[i][2], activities[i][3])
                if hashmap[activities[i][1]] is not None:
                    hashmap[activities[i][1]].append(new_tuple)
                else:
                    hashmap[activities[i][1]] = [new_tuple]

        # We create an empty hashmap where we will store the activity_id as key,
        # and the total distance of the activity as value.
        activities_length = {}

        # We iterate through the hashmap, calculating the distance between each
        # trackpoint in each activity, and adding the distance to the hashmap
        # created above.
        for key in hashmap:
            if key not in activities_length:
                activities_length[key] = 0
            else:
                activities_length[key] = 0
            for i in range(1, len(hashmap[key])):
                activities_length[key] += haversine(hashmap[key][i-1], hashmap[key][i], unit='m')

        # Create a temporary table to store the hashmap data
        create_temp_table_query = '''
            create temporary table activity_distance_temp (
                activity_id varchar(255),
                value float
            )
            '''
        self.cursor.execute(create_temp_table_query)

        # Insert the hashmap data into the temporary table
        for activity_id, value in activities_length.items():
            insert_query = f'''
                insert into activity_distance_temp (
                    activity_id, 
                    value
                ) values (
                    '{activity_id}', 
                    {value}
                )
                '''
            self.cursor.execute(insert_query)

        # We create a CTE where we select the transportation_mode, user_id,
        # activity_id, start_date_time, end_date_time, value and the row number
        # of the value column, partitioned by the transportation_mode, ordered
        # by the value column in descending order. The row number serves as an 
        # order where for each different transportation mode, the row with the
        # highest value becomes the first row, so it gets the lowest row number.
        # We then select the transportation_mode, user_id, activity_id,
        # start_date_time, end_date_time, duration and value from the CTE, where
        # the row number is 1, i.e. the row with the highest value for each
        # transportation mode.
        return '''
            with temp as (
                select 
                    t1.transportation_mode,
                    t1.user_id,
                    t1.id,
                    t1.start_date_time,
                    t1.end_date_time,
                    t2.value,
                    row_number() over (partition by t1.transportation_mode order by t2.value desc) row_num
                from activity as t1
                left join activity_distance_temp as t2 
                    on t1.id = t2.activity_id
                where t1.transportation_mode is not null
            )
            select 
                transportation_mode,
                user_id,
                id,
                start_date_time,
                end_date_time,
                timestampdiff(hour, start_date_time, end_date_time) as duration,
                value
            from temp
            where row_num = 1
        '''
        
    def task11(self):
        # We create a CTE where we select the activity_id, date_time and the
        # previous date_time, partitioned by the activity_id, ordered by the
        # date_time. We use the lag() function, which looks at the previous row
        # with regards to the partition and order, and returns the value of the
        # date_time in that row. We then select the user_id and the count of the
        # activity_id, where the difference between the date_time and the
        # previous date_time is greater than or equal to 5 minutes, i.e. the
        # activity has at least one instance where to consecutive trackpoints
        # were registered with a time difference of at least 5 minutes.
        # We order the resulting table by the user_id.
        print("Task 11:")
        return '''
        with prev_timestamp_per_activity as (
            select 
                activity_id,
                date_time,
                lag(date_time) over (partition by activity_id order by date_time) as prev_timestamp
            from trackpoint
        ),
        invalid_activities_count as (
            select distinct 
                user_id,
                activity_id
            from prev_timestamp_per_activity
            left join activity 
                on activity.id = activity_id
            where timestampdiff(minute, prev_timestamp, date_time) >= 5
            order by user_id asc
        )
        select 
            user_id,
            count(activity_id) as "number of invalid activities"
        from invalid_activities_count
        group by user_id
        '''

    def task12(self):
        # We create a CTE where we select the user_id, transportation_mode and
        # the count of the transportation_mode, partitioned by the user_id and
        # the transportation_mode to get the count of each transportation_mode.
        # We then create another CTE where we select the user_id and the max
        # count of the transportation_mode, partitioned by the user_id to get
        # the max count of each user. We then join the two CTEs on the user_id
        # and the count of the transportation_mode, to get the transportation
        # mode with the max count for each user. We then create another CTE
        # where we select the user_id, transportation_mode, the count of the
        # transportation_mode and the row number of the count of the
        # transportation_mode, partitioned by the user_id and ordered by the
        # count of the transportation_mode in descending order and the
        # transportation_mode in ascending order. This is to get the 
        # transportation mode that was registered most times per user, and also
        # in the case a transportation mode was registered equal amounts of
        # times, a secondary alphabetic order would prioritize one of them.
        # We then select the user_id, transportation_mode and the count of the
        # transportation_mode from the CTE, where the row number is 1, i.e. the
        # row with the highest count for each user.
        print("Task 12:")
        return '''
            with most_used_transportation_mode as (
                select 
                    user_id, 
                    transportation_mode, 
                    count(*) as times_transportation_used
                from activity
                where transportation_mode is not null
                group by user_id, transportation_mode
            ),
            max_transportation_mode_used_count as (
                select 
                    user_id, 
                    max(times_transportation_used) as max_times_transportation_used
                from most_used_transportation_mode
                group by user_id
            ),
            most_used_per_user as (
                select 
                    count.user_id, 
                    count.transportation_mode, 
                    count.times_transportation_used
                from most_used_transportation_mode count
                join max_transportation_mode_used_count max 
                    on count.user_id = max.user_id 
                    and count.times_transportation_used = max.max_times_transportation_used
            ),
            most_used_per_user_ranked as (
                select 
                    user_id,
                    transportation_mode,
                    times_transportation_used,
                    row_number() over (partition by user_id order by times_transportation_used desc, transportation_mode asc) as row_num
                from most_used_per_user
            )
            select
                user_id,
                transportation_mode as "most_used_transportation_mode",
                times_transportation_used as "# times transportation mode used"
            from most_used_per_user_ranked
            where row_num = 1;
        '''

def main():
    program = None
    try:
        program = Part2()
        program.present(program.task1())
        program.present(program.task2())
        program.present(program.task3())
        program.present(program.task4())
        program.present(program.task5())
        program.present(program.task6())
        program.present(program.task7_a())
        program.present(program.task7_b())
        program.present(program.task8())
        program.present(program.task9())
        program.present(program.task10())
        program.present(program.task11())
        program.present(program.task12())
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()

if __name__ == '__main__':
    main()
