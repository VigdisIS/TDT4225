from DbConnector import DbConnector

class Part2:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def present(self):
         self.cursor.execute("SELECT * FROM trackpoint")
         rows = self.cursor.fetchall()
         for row in rows:
            print(row)
    
    def task1(self):
        print("Task 1:")
        self.cursor.execute("SELECT COUNT(id) AS NumberOfTrackpoints FROM trackpoint")
        rows = self.cursor.fetchall()
        print("Number of Trackpoints")
        print(rows[0])
        print("----------------")
        self.cursor.execute("SELECT COUNT(id) AS NumberOfActivities FROM activity")
        rows = self.cursor.fetchall()
        print("Number of Activites")
        print(rows[0])
        print("----------------")

        self.cursor.execute("SELECT COUNT(id) AS NumberOfUsers FROM user")
        rows = self.cursor.fetchall()
        print("Number of Users")
        print(rows[0])    
        print("----------------")
    
    def task2(self):
        print("Task 2")
        x = "SELECT transportation_mode, COUNT('transportation_mode') FROM activity GROUP BY transportation_mode ";
        self.cursor.execute(x)
        rows = self.cursor.fetchall()
        for row in rows:
            print(row)

        print("Task 2 test")
        x = "SELECT * FROM trackpoint";
        self.cursor.execute(x)
        rows = self.cursor.fetchone()
        for row in rows:
            print(row)    

    def task3(self):
        print("Task 3")
        x = "SELECT user_id, COUNT(user_id) FROM activity GROUP BY user_id ORDER BY COUNT(user_id) DESC ";
        self.cursor.execute(x)
        rows = self.cursor.fetchall()
        for row in rows[0: 15]:
            print(row)

    def task4(self):
        print("Task 4")
        x = "SELECT COUNT(DISTINCT user_id), transportation_mode FROM activity WHERE transportation_mode = 'bus' ";
        self.cursor.execute(x)
        rows = self.cursor.fetchall()
        for row in rows:
            print(row)

    def task5(self):
        print("Task 5")
        x = "SELECT user_id, COUNT(DISTINCT transportation_mode), COUNT(transportation_mode) FROM activity GROUP BY user_id";
        self.cursor.execute(x)
        rows = self.cursor.fetchall()
        for row in rows:
            print(row)


def main():
    program = None
    try:
        program = Part2()
        #program.task1()
        #program.task2()
        #program.task3()
        #program.task4()
        program.task5()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
