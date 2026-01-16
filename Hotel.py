from datetime import datetime
import pandas as pd
import sqlite3
from sqlite3 import Error

#####################
# 1. TABLE CREATION #
#####################

def create_tables():
    db = sqlite3.connect('hotel.db')
    c = db.cursor()

    # Create the queries
    customer = '''CREATE TABLE customer (
                ID VARCHAR(4) PRIMARY KEY,
                Name TEXT NOT NULL,
                Gender TEXT,
                Mobile INTEGER,
                Address VARCHAR(100));'''

    room = '''CREATE TABLE room (
            BookingID VARCHAR(10) PRIMARY KEY,
            RoomNo INTEGER,
            CheckIn VARCHAR(10) NOT NULL,
            CheckOut VARCHAR(10) NOT NULL,
            CustomerID VARCHAR(4) NOT NULL);'''

    services = '''CREATE TABLE services (
                ServiceID INTEGER PRIMARY KEY,
                ServiceName TEXT,
                Price REAL,
                EmployeeID INTEGER);'''

    employee = '''CREATE TABLE employee (
                ID INTEGER,
                Mobile INTEGER,
                Name VARCHAR(64));'''

    booked_services = '''CREATE TABLE booked_services (
                    BookingID VARCHAR(10),
                    ServiceID INTEGER);'''

    table = [customer,room,services,employee,booked_services]
    for i in range(len(table)):
        c.execute(table[i])
        db.commit()
    return




# Import data from Hotel2NF-Customer.csv
def import_customers():
    db = sqlite3.connect('hotel.db')
    c = db.cursor()
    data = pd.read_csv('Hotel2NF-Customer.csv')
    df = pd.DataFrame(data)
    try:
        for row in df.itertuples():
            c.execute('''
                    INSERT INTO customer (ID,Name,Gender,Mobile,Address)
                    VALUES (?,?,?,?,?)''',
                      (row.ID,
                      row.Name,
                      row.Gender,
                      row.Mobile,
                      row.Address))
        db.commit()
    except Error as e:
        print(e)
    return

    

#Import data from Hotel2NF-Room.csv
def import_rooms():
    db = sqlite3.connect('hotel.db')
    c = db.cursor()
    data = pd.read_csv('Hotel3NF-Room.csv')
    df = pd.DataFrame(data)
    try:
        for row in df.itertuples():
            c.execute('''
                    INSERT INTO room (BookingID,RoomNo,CheckIn,CheckOut,CustomerID)
                    VALUES (?,?,?,?,?)''',
                      (row.BookingID,
                       row.RoomID,
                       row.CheckIn,
                       row.CheckOut,
                       row.CustomerID))
        db.commit()
    except Error as e:
        print(e)
    return



#Hotel3NF-Services.csv
def import_services():
    db = sqlite3.connect('hotel.db')
    c = db.cursor()
    data = pd.read_csv('Hotel2NFServices.csv')
    df = pd.DataFrame(data)
    try:
        for row in df.itertuples():
            c.execute('''
                    INSERT INTO services (ServiceID,ServiceName,Price,EmployeeID)
                    VALUES (?,?,?,?)''',
                      (row.ServiceID,
                       row.ServiceName,
                       row.Price,
                       row.EmployeeID))
        db.commit()
    except Error as e:
        print(e)
    return


#Hotel3NF-BookedServices.csv
def import_booked_services():
    db = sqlite3.connect('hotel.db')
    c = db.cursor()
    data = pd.read_csv('Hotel3NFBookedServices.csv')
    df = pd.DataFrame(data)
    try:
        for row in df.itertuples():
            c.execute('''
                    INSERT INTO booked_services (BookingID,ServiceID)
                    VALUES (?,?)''',
                      (row.BookingID,
                       row.ServiceID))
        db.commit()
    except Error as e:
        print(e)
    return



#Import data from Hotel3NF-Employee.csv
def import_employees():
    db = sqlite3.connect('hotel.db')
    c = db.cursor()
    data = pd.read_csv('Hotel3NF-Employee.csv')
    df = pd.DataFrame(data)
    try:
        for row in df.itertuples():
            c.execute('''
                    INSERT INTO employee (ID,Mobile,Name)
                    VALUES (?,?,?)''',
                      (row.ID,
                       row.Mobile,
                       row.Name))
        db.commit()
    except Error as e:
        print(e)
    return





def init_db():
    '''
    Setup the Database. Creates a new DB 'Hotel.db' and replace
    and pre-existing data
    '''
    db = sqlite3.connect('hotel.db')
    c = db.cursor()

    #remove all existing data
    drop1 = "DROP TABLE IF EXISTS customer"
    drop2 = "DROP TABLE IF EXISTS room"
    drop3 = "DROP TABLE IF EXISTS services"
    drop4 = "DROP TABLE IF EXISTS employee"
    drop5 = "DROP TABLE IF EXISTS booked_services"
    tables = [drop1,drop2,drop3,drop4,drop5]
    for i in range(len(tables)):
        c.execute(tables[i])
        db.commit()

    #create empty tables
    create_tables()

    #import data set into the csv files
    import_customers()
    import_rooms()
    import_services()
    import_employees()
    import_booked_services()

    db.commit()
    db.close()
    return "Data successfully imported"




def main():
    # init_db()

    while True:
        db=sqlite3.connect("hotel.db")
        c = db.cursor()
        print(10*'-','HOTEL QUERY PROGRAM',10*'-')
        print('0. Quit')
        print('1. List all Customers and retrieval of their details')
        print('2. List all Rooms and retrieve their details')
        print('3. List all employees and retrieve their details')
        print('4. Show all Services')
        print('5. Show all Bookings')
        print('6. Show Bookings with Customer Names')
        print('7. Show Booking with room details')
        print('8. Show services in each booking')
        print(10*'-','Aggregate Queries',10*'-')
        print('9. Count Total Customers')
        print('10. Count Total Bookings')
        print('11. Count bookings per customer')
        print('12. Average Stay Length')
        print('13. Total Revenue from services')
        print('14. Revenue per Employee')
        print('15. Most Popular Service')


        option = str(input("Enter search option: "))

        
        if option == '1':
            query = '''SELECT * FROM customer'''
            c.execute(query)
            data = c.fetchall()
            for row in data:
                print(row)

        elif option == '2':
            query = '''SELECT * FROM room'''
            c.execute(query)
            data = c.fetchall()
            for row in data:
                print(row)

        elif option == '3':
            query = '''SELECT * FROM employee'''
            c.execute(query)
            employees = c.fetchall()
            for employee in employees:
                print(employee)
        
        elif option == '4':
            query = '''SELECT * FROM services'''
            c.execute(query)
            services = c.fetchall()
            for service in services:
                print(service)

        elif option == '5':
            query = '''SELECT * FROM booked_services'''
            c.execute(query)
            bookings = c.fetchall()
            for booking in bookings:
                print(booking)

        # Show booking by customer name
        elif option == '6':
            name = input("Enter customer name: ")

            # Parameter substitution
            query = '''SELECT c.ID, c.Name, r.BookingID, r.RoomNo
                       FROM customer c
                       JOIN room r ON c.ID = r.CustomerID
                       WHERE c.Name = ?
                    '''
            # Pass variable as tuple
            c.execute(query, (name,))
            data = c.fetchall()

            for row in data:
                print(row)

        # 7. Show Booking with room details
        elif option == '7':
            query = '''
                SELECT c.ID, c.Name, r.BookingID, r.RoomNo, r.CheckIn, r.CheckOut
                FROM Customer c
                JOIN Room r ON c.ID = r.CustomerID
            '''

        # Show services in each booking
        elif option == '8':
            name = str(input("Enter customer name: "))
            query = '''
                SELECT r.BookingID, r.RoomNo, c.Name, s.ServiceName, s.Price
                FROM room r
                JOIN customer c ON r.CustomerID = c.ID
                JOIN booked_services bs ON r.BookingID = bs.BookingID
                JOIN services s ON bs.ServiceID = s.ServiceID
                WHERE c.Name = ?
            '''

            c.execute(query, (name,))
            rows = c.fetchall()
            for row in rows:
                print(row)


        # 9. Count Total Customers
        elif option == '9':
            query = '''SELECT COUNT(*) FROM customer'''
            c.execute(query)
            total_customers = c.fetchone()[0]
            print("Total Customers:", total_customers)
    

        # 10 .Count Total Bookings
        elif option == '10':
            query = '''SELECT COUNT(*) FROM room'''
            c.execute(query)
            total_rooms = c.fetchone()[0]
            print("Total Room Bookings:", total_rooms)

        # 11. Count bookings per customer
        elif option == '11':
            query = '''SELECT c.ID, c.Name, COUNT(r.BookingID) AS NumBookings
                       FROM Customer c
                       JOIN Room r ON c.ID = r.CustomerID
                       GROUP BY c.ID, c.Name
                    '''
            c.execute(query)
            rows = c.fetchall()

            for row in rows: 
                print("CustomerID:", row[0], "| Name:", row[1], "| Bookings:", row[2])


        # 12. Average Stay Length
        elif option == '12':
            query = '''
                    SELECT CheckIn, CheckOut FROM Room 
                    WHERE CheckIn IS NOT NULL 
                    AND CheckOut IS NOT NULL
                    '''
            c.execute(query)
            rows = c.fetchall()

            total_days = 0
            counter = 0

            for checkin, checkout in rows:
                try:
                    # Parse dates as YYYY-MM-DD
                    check_in = datetime.strptime(checkin, "%d/%m/%Y")
                    check_out = datetime.strptime(checkout, "%d/%m/%Y")
                    
                    # Calculate difference in days per row
                    stay_length = (check_out - check_in).days
                    total_days += stay_length    # Append per-row stay length to the total days
                    counter += 1
                except Exception as e:
                    print("Date parsing error:", e)

                # Prevent divide-by-zero cases
                if counter > 0:
                    avg_stay = total_days / counter
                    print("Average Stay Length (days):", round(avg_stay, 2))
                else:
                    print("No valid bookings found.")

        # 13. Total Revenue from services
        elif option == '13':
            query = '''
                    SELECT SUM(s.Price)
                    FROM booked_services bs
                    JOIN services s ON bs.ServiceID = s.ServiceID
                    '''
            c.execute(query)
            total_revenue = c.fetchone()[0]

            if total_revenue is not None:
                print("Total Service Revenue from Bookings:", round(total_revenue, 2))
            else:
                print("No services bookings found.")

         # 14. Revenue per Employee
        elif option == '14':
            query = ''' SELECT e.ID AS EmployeeID, e.Name 
                    AS EmployeeName, SUM(s.Price) 
                    AS RevenueGenerated 
                    FROM booked_services bs JOIN Services s 
                    ON bs.ServiceID = s.ServiceID 
                    JOIN Employee e 
                    ON s.EmployeeID = e.ID 
                    GROUP BY e.ID, e.Name 
                    '''
            c.execute(query)
            rows = c.fetchall()
        
            for row in rows: 
                print("EmployeeID:", row[0], "| Name:", row[1], "| Revenue:", round(row[2], 2))
    
        # 15. Most Popular Service
        elif option == '15':
            query = ''' 
                SELECT s.ServiceName, 
                COUNT(bs.ServiceID) 
                AS TimesBooked 
                FROM booked_services bs 
                JOIN Services s 
                ON bs.ServiceID = s.ServiceID 
                GROUP BY s.ServiceName 
                ORDER BY TimesBooked 
                DESC 
                ''' 
            c.execute(query) 
            rows = c.fetchall() 
            for row in rows: 
                print("Service:", row[0], "| Times Booked:", row[1])           
        
        # End
        elif option == '0':
            break

main()



