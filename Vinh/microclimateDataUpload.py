import mysql.connector
import sys
import csv
import os
from mysql.connector import Error
import getpass

def fileReader(folderPath):
    filePaths = []
    for root, dirs, files in os.walk(folderPath):
        for file in files:
            filePath = os.path.join(root, file)
            filePaths.append(filePath)
    return filePaths
            
# Function to create a database connection
def create_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        if connection.is_connected():
            print("Connection successful!")
    except Error as e:
        print(f"The error '{e}' occurred")
    
    return connection

def readCSV(filePath):  # code to read csv files
    dataArray = []
    with open(filePath, mode='r', newline='') as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
            dataArray.append(row)
    return dataArray

# Function to execute a read query
def execute_read_query(connection, query, data=None):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query, data)
        result = cursor.fetchall()
        return result
    except Error as e:
        print(f"The error '{e}' occurred")
        
#checks if sensor exists based on serialNumber
def sensorSerialExist(conn, serialNumber):
    result = sensorIDrequestSerial(conn, serialNumber)
    if result:
        return result
    else:
        print("sensor doesn't exist, adding new sensor ")
        insertSensor(serialNumber, conn)
        return sensorIDrequest(conn, serialNumber)
#check sensor exists based on name
def sensorIDrequestName(conn, name):
    sensorIDQuery = "SELECT idSensor FROM sensors WHERE name = %s;"   
    result = execute_read_query(conn, sensorIDQuery, (serialNumber,))
    return result[0][0] if result else None
#check sensor exists based on serialNumber
def sensorIDrequestSerial(conn, serialNumber):
    sensorIDQuery = "SELECT idSensor FROM sensors WHERE name = %s;"   
    result = execute_read_query(conn, sensorIDQuery, (serialNumber,))
    return result[0][0] if result else None

def execute_query(connection, query, data=None):
    cursor = connection.cursor()
    print(query)
    print(data)
    try:
        cursor.execute(query, data)
        connection.commit()
        print("Query executed successfully")
    except Error as e:
        print(f"The error '{e}' occurred")
#adds all of tinytag data for 1 csv file
def addTinyTagRecords(connection, data, sensorID):
    columns = [col.replace(" ", "") for col in data[4][2:]]
    columns_str = "Time, idSensor, " + ", ".join(columns)
    placeholders = ", ".join(["%s"] * (len(columns) + 2))
    insertDataQuery = f"""
    INSERT INTO Data ({columns_str})
    VALUES ({placeholders})
    """
    for row in data[5:]:
        dateTime = row[1].replace("/", "-")
        if row[1][2] not in "1234567890":
            dateTime = dateTime[6:10] + dateTime[2:6] + dateTime[0:2] + dateTime[10:]
        if len(dateTime) < 19:
            dateTime += ":00"
        dataToInsert = [dateTime, sensorID]
        for i in range(2, len(row)):
            dataToInsert.append(row[i].split(" ", 1)[0])
        execute_query(connection, insertDataQuery, tuple(dataToInsert))
#adds all data in 1 csv file for soil moisture and precipitation
def addSoilRecords(connection, data, sensorID):
    placeholders = ", ".join(["%s"] * (9))
    insertDataQuery = f"""
    INSERT INTO site_sensors_data (Time, idSensor, theta, temperature, rainfall, power)
    VALUES ({placeholders})
    """
    for row in data[2:]:
        dateTime = row[0].replace("/", "-")
        if row[0][2] not in "1234567890":
            dateTime = dateTime[6:10] + dateTime[2:6] + dateTime[0:2] + dateTime[10:]
        if len(dateTime) < 19:
            dateTime += ":00"
        dataToInsert = [dateTime, sensorID]
        for i in [1,3,6,7]:#column 1 is theta, 3 temperature, 6 is rainfall, 7 is power
            newData = row[i]
            if "INF" in newData: newData = None
            dataToInsert.append(newData)
        execute_query(connection, insertDataQuery, tuple(dataToInsert))
#adds all data in a folder for soil and rain automatically
#only works for sensors in if else statement
def addAllSoilRecords(folderPath, connection):
    filePaths = fileReader(folderPath)
    for filePath in filePaths:
        if filePath.endswith(".csv"):
            print(filePath)
            data = readCSV(filePath)
            if "NTUB-A" in  filePath.upper():
                sensorID = 21
            elif "NTUB-B" in  filePath.upper():
                sensorID = 22
            elif "SP3" in  filePath.upper():
                sensorID = 23
            elif "SP2" in  filePath.upper():
                sensorID = 24
            addSoilRecords(connection, data, sensorID)
                
#adds all tiny tag records in a folder
def addAllTinyTagRecords(folderPath, connection):
    filePaths = fileReader(folderPath)
    for filePath in filePaths:
        if filePath.endswith(".csv"):
            print(filePath)
            data = readCSV(filePath)
            sensorID = sensorSerialExist(conn, serialNumber)
            addTinyTagRecords(connection, data, sensorID)

def linkSensorSite(sensors, siteID, conn):
    serialNumbersPlaceholder = ', '.join(['%s'] * len(sensors))
    updateQuery = f"""
    UPDATE sensors
    SET idSite = %s
    WHERE serialNumber IN ({serialNumbersPlaceholder})
    """
    dataToInsert = [siteID] + sensors
    execute_query(conn, updateQuery, dataToInsert)
#code to insert a new sensor
def insertSensor(serialNumber, conn):
    name = input("What is the sensor name? ")
    siteID = input("What is the siteID of the sensor? (input 'null' if unknown) ")
    siteID = None if siteID.lower() == "null" else siteID
    if serialNumber == None:
        serialNumber = input("What is the serialNumber of the sensor? ('null' if unknown or invalid) ")
    serialNumber = None if serialNumber.lower() == "null" else siteID
    insertQuery = "INSERT INTO sensors (name, idSite, serialNumber) VALUES (%s, %s, %s)"
    dataToInsert = (name, siteID, serialNumber)
    execute_query(conn, insertQuery, dataToInsert)

def addSite(conn):
    print("Adding new site")
    name = input("What is the site name? ")
    updateQuery = "INSERT INTO site (name) VALUES (%s);"
    execute_query(conn, updateQuery, (name,))
    
def deleteQuery(connection, table, column, nameORID, TimeStart, TimeEnd):
    dataToInsert = [nameORID]
    TimeCondition= ""
    if TimeStart and TimeEnd:
        TimeCondition = f" AND TIME between %s and %s"
        dataToInsert.append(TimeStart)
        dataToInsert.append(TimeEnd)
    Query = f"DELETE FROM {table} WHERE {column} = %s{TimeCondition};"
    print(Query)
    print(dataToInsert)
    certain = input ("are you sure you want to continue with deletion? Y/N? ")
    if certain.upper() != "Y":
        return
    execute_query(connection, Query, (dataToInsert))
    
def deleteSiteOrSensor(connection, siteOrSensor, table):
    print(f"Deleting {siteOrSensor} based on Name or ID chosen")
    
    # Ask for ID first
    nameOrID = input(f"What is the {siteOrSensor} ID? (input 'null' if unknown): ")
    
    if nameOrID.lower() == "null":
        # If ID is unknown, ask for the name
        nameOrID = input(f"What is the {siteOrSensor} name? (input 'null' if unknown): ")
        
        if nameOrID.lower() == "null":
            # If both ID and name are unknown, print an error message
            print(f"{siteOrSensor} ID or {siteOrSensor} Name required for deletion")
            return
        deleteQuery(connection, table, "name", nameOrID, None, None)
    else:
        column = "id"+table
        deleteQuery(connection, table, column, nameOrID, None, None)
        
        
def addSoil(connection):
    choices = ("""Choose 1 to add soil/precipitation data using sensorID or name
Choose 2 to add add soil/precipitation data automatically (inconsistent results)

""")
    choice = input(choices)
    folderPath = input("Enter the folder/file path: ")
    if choice == 2:
        print("Adding any file containting SP1, SP2, NTUB-A, NTUB-B")
        folderPath = input("Enter the folder/file path: ")
        addAllSoilRecords(folderPath, connection)
        
    elif choice == 1:
        filePaths = fileReader(folderPath)
        for filePath in filePaths:
            sensorID = input("What is the Sensor ID? (input 'null' if unknown) ")
            if sensorID.lower() == "null":
                name = input("What is the Sensor name? (input 'null' if unknown) ")
                if name.lower() == "null":
                    print("Sensor ID or Sensor Name required for adding records to database. File Skipped")
                else:
                    data = readCSV(filePath)
                    addSoilRecords(sensor)
    else:print("Invalid choice!!!")
        
        
def deleteSensorData(connection, table):
    print("Deleting data based on Name, SerialNumber or SensorID chosen")
    # Ask for ID first
    ID = input("What is the sensor ID? (input 'null' if unknown): ")
    if ID.lower() == "null":
        
        # If ID is unknown, ask for the name
        ID = input("What is the sensor name? (input 'null' if unknown): ") 
        if ID.lower() == "null":
             
            ID = input("What is the Serial Number? (input 'null' if unknown): ") 
            if ID.lower() == "null":
                # If both ID and name are unknown, print an error message
                print(" Name, SerialNumber or SensorID required for deletion")
                return
            else: ID= sensorIDrequestSerial(conn, SerialNumber)
        else: ID= sensorIDrequestName(conn, name)
            
    StartDateTime = input("Start date and time for deletion?") 
    EndDateTime = input("End date and time for deletion?")
    deleteQuery(connection, table, "sensorID", nameORID, StartDateTime, EndDateTime)
        
        
def choiceAdd(connection):
    options = ("""You have decided to add new data into the database

Choose 1 to add new National Trust sites to the database
Choose 2 to add new sensors to the database
Choose 3 to add tiny tag sensor data to the database
Choose 4 to add new soil moisture and precipitation data to the database
Choose B to go back
              
""")
    while True:
        choice = input(options)
        if choice == "1":  # Site
            addSite(connection)
        elif choice == "2":  # Sensor
            insertSensor(None, connection)
        elif choice == "3":  # Tiny tag
            folderPath = input("Enter the folder/file path: ")
            addAllTinyTagRecords(folderPath, connection)
        elif choice == "4":  # Soil and rain
            addSoil(connection)
        elif choice.upper() == "B":  # Back
            print("Going back...")
            return  # Exiting the loop
        else:
            print("Invalid input!!")

def choiceDelete(connection):
    options = ("""You have decided to remove data from the database

Choose 1 to remove National Trust sites from the database
Choose 2 to remove sensors from the database
Choose 3 to remove tinytag sensor data from the database
Choose 4 to remove soil data from the database
Choose B to go back
              
""")
    while True:
        choice = input(options)
        if choice == "1":  # Site
            deleteSiteOrSensor(connection, "site", "site")
        elif choice == "2":  # Sensor
            deleteSiteOrSensor(connection,"sensor", "sensors")
        elif choice == "3":  # Tiny tag
            folderPath = input("Enter the folder/file path: ")
            deleteSensorData(connection, "data")
        elif choice == "4":  # Tiny tag
            folderPath = input("Enter the folder/file path: ")
            deleteSensorData(connection, "site_sensor_data")
        elif choice.upper() == "B":  # Back
            print("Going back...")
            return  # Exiting the loop
        else:
            print("Invalid input!!")
            
def choiceUpdate(conn):
    print("What would you like to update?")
    print("choose 1 to Update Sensor Information")
    print("choose 2 to Update Site Information")
    choice = input("Enter the number of your choice: ")

    if choice == '1':
        sensorID = input("Enter the Sensor ID you want to update: ")
        column = input("Which column would you like to update?: ")
        update_query = f"UPDATE Sensors SET {column} = %s WHERE idSensor = %s"
        value = input("New value?: ")
        execute_query(conn, update_query, (value, sensorID))

    elif choice == '2':
        siteID = input("Enter the Site ID you want to update: ")
        new_name = input("Enter the new site name: ")
        update_query = "UPDATE site SET name = %s WHERE idSite = %s"
        execute_query(conn, update_query, (new_name, siteID))
        
    else:
        print("Invalid choice.")

    print("Update complete.")
    
def main():
    host_name = "127.0.0.1"
    user_name = "root"
    db_name = "microclimatedatabase"
    user_password = "Aseriesofrandomwords1"
    
    # Establish a connection
    connection = create_connection(host_name, user_name, user_password, db_name)
    options =("""What would you like to do?
Choose 1 to add new sites/sensors/data
Choose 2 to update existing sites/sensors
Choose 3 to remove sites/sensors/data from the database
Choose Q to Quit

""")

    while True:
        choice = input(options)
        if choice == "1": #insert new data into the database
            choiceAdd(connection)
        elif choice == "2":#update existing data in the database
            choiceUpdate(connection)
        elif choice == "3":#remove existing data from the database
            choiceDelete(connection)
        elif choice.upper() == "Q": #quit
            print("quitting")
        # Close the connection
            if connection.is_connected():
                connection.close()
                print("The connection is closed")
            sys.exit()
        else: print("Invalid input!!")
main()