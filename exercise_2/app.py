from DbConnector import DbConnector
from haversine import haversine, Unit
from datetime import datetime
from collections import defaultdict
from tabulate import tabulate

# TODO: Add comments to the code
class Part2:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def present(self):
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print(tabulate(result))  
    
    def task1(self):
        print("Task 1:")
        query = '''
            select
                count(id) as number_of_trackpoints
            from trackpoint
            union all
            select 
                count(id) as number_of_activities
            from activity
            union all
            select
                count(id) as number_of_users
            from user;
            '''
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print(tabulate(result))  
    
    def task2(self):
        print("Task 2:")
        # TODO: avg/min/max per user per activity or for all users?
        query  = '''
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
        self.cursor.execute(query )
        result = self.cursor.fetchall()
        print(tabulate(result, headers=self.cursor.column_names))  

    def task3(self):
        print("Task 3:")
        query  = '''
            select 
                user_id, 
                count(user_id) as number_of_activities
            from activity 
            group by user_id 
            order by number_of_activities desc limit 15;
            '''
        self.cursor.execute(query )
        result = self.cursor.fetchall()
        print(tabulate(result, headers=self.cursor.column_names))  

    def task4(self):
        print("Task 4:")
        query = '''
            select distinct
                user_id
            from activity
            where transportation_mode = 'bus';
            '''
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print(tabulate(result, headers=self.cursor.column_names))  

    def task5(self):
        print("Task 5:")
        # query = "SELECT user_id, COUNT(DISTINCT transportation_mode), COUNT(transportation_mode) FROM activity GROUP BY user_id";
        query = '''
            select 
                user_id,
                count(distinct transportation_mode) as distinct_transportation_modes
            from activity
            group by user_id
            order by distinct_transportation_modes desc limit 10;
            '''
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print(tabulate(result, headers=self.cursor.column_names))  
    
    def task6(self):
        # TODO: what is meant by activity? I.e. distinct activity ids?
        print("Task 6:")
        query = '''
            select 
                id, 
                count(*)
            from activity
            group by id
            having count(*) > 1;
            '''
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print(tabulate(result, headers=self.cursor.column_names))  
    
    def task7(self):
        print("Task 7:")
        print("a)")
        query = '''
            select
                count(distinct user_id) as users
            from activity
            where datediff(start_date_time, end_date_time) != 0;
            '''
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print(tabulate(result, headers=self.cursor.column_names))  
        
        print("b)")
        query = '''
            select
                user_id,
                transportation_mode,
                timediff(end_date_time, start_date_time) as duration
            from activity
            where datediff(start_date_time, end_date_time) != 0;
            '''
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print(tabulate(result, headers=self.cursor.column_names))  

    def task8(self):
        # TODO: try and optimalize by utlizing temporary tables
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

        close_users = []
        for i in range(len(rows)):
            if rows[i][0] not in close_users and rows[i] != rows[-1] and rows[i][0] != rows[i+1][0]:
                for j in range(i+1, len(rows)):
                    user1 = rows[i]
                    user2 = rows[j]
                    if user2[0] not in close_users and abs((user1[4] - user2[4]).total_seconds()) <= 30:
                        # Calculate distance using Haversine formula
                        loc1 = (user1[2], user1[3])
                        loc2 = (user2[2], user2[3])
                        distance = haversine(loc1, loc2, unit='m')

                        if distance <= 50:
                            close_users.append(user1[0])
                            close_users.append(user2[0])
                            continue
        print(close_users)
        # TODO: use tabulate to format output
        print(tabulate(close_users, headers=["Close users"]))  

    def task9(self):
        print("Task 9:")
        query = '''
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
                        case when altitude = -777 
                            then null
                            else altitude * 0.3048 
                        end as altitude,
                        lag(altitude * 0.3048) over (partition by activity_id order by date_time) as prev_altitude
                    from trackpoint
                ) as subquery
                group by activity_id
            )
            select
                user_id,
                sum(total_increase) as "total meters gained"
            from activity
            left join total_increase_per_activity 
                on activity.id = total_increase_per_activity.activity_id
            group by user_id
            order by sum(total_increase) desc limit 15;
            '''
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print(tabulate(result, headers=self.cursor.column_names))

    def task10(self):
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

        hashmap = {}

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

        activities_length = {}

        for key in hashmap:
            if key not in activities_length:
                activities_length[key] = 0
            else:
                activities_length[key] = 0
            for i in range(1, len(hashmap[key])):
                activities_length[key] += haversine(hashmap[key][i-1], hashmap[key][i], unit='m')

        # Create a temporary table
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
                insert into activity_distance_temp (activity_id, value) values ('{activity_id}', {value})
                '''
            self.cursor.execute(insert_query)

        select_query = '''
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
        self.cursor.execute(select_query)
        # Fetch all rows from the result of the SELECT query
        result = self.cursor.fetchall()
        print(tabulate(result, headers=self.cursor.column_names))
        
    def task11(self):
        # Select all activity_ids
        print("Task 11:")
        query = '''
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
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print(tabulate(result, headers=self.cursor.column_names))

    def task12(self):
        print("Task 12:")
        query = '''
            with most_used_transportation_mode as (
                select 
                    user_id, 
                    transportation_mode, 
                    count(*) as mode_count
                from activity
                where transportation_mode is not null
                group by user_id, transportation_mode
                having mode_count = (
                    select max(mode_count)
                    from (
                        select 
                            user_id, 
                            transportation_mode, 
                            count(*) as mode_count
                        from activity
                        where transportation_mode is not null
                        group by user_id, transportation_mode  
                    ) as subquery
                where subquery.user_id = activity.user_id
                )
            )
            select 
                user_id,
                transportation_mode as "most_used_transportation_mode"
            from most_used_transportation_mode
            order by user_id asc;
        '''
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print(tabulate(result, headers=self.cursor.column_names))

def main():
    program = None
    try:
        program = Part2()
        # program.task1()
        # program.task2()
        # program.task3()
        # program.task4()
        # program.task5()
        # program.task6()
        # program.task7()
        # program.task8()
        # program.task9()
        # program.task10()
        # program.task11()
        # program.task12()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()

if __name__ == '__main__':
    main()
