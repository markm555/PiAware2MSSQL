

# Date written: 5/22/3030
# Author: Mark Moore
# update: 5/22/2020 code running
#
# This program will pull json data from a Raspberry Pi running piaware software, parse the json out into individual rows and columns and write the data to a SQL database in real time.

import requests
import json
import pyodbc
import datetime
import time

# define url to pull json from piaware

url = "http://yourpiaware/dump1090-fa/data/aircraft.json"

# establish connection to SQL Server and set up cursor
# print("Connecting to SQL")
conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=YourSQLServer;'  # Works for on-premises SQL Server or Azure SQL Server
                      'Database=YourDatabase;'
                      'UID=YourUserName;'      # Currently set up for SQL Authentication to use Windows Authentication you will have to specify tha the connection is trusted
                      'PWD=YourPassword;')
cursor = conn.cursor()

# Main proccessing loop to parse the json to a python dictionary and pull out just the aircraft data.
# Pull out each column and write the columns to SQL, then convert the dictionary containing just the 
# aircraft data back to json and write it into Azure Service Bus.
while True:

    # get request to pull the json data from the piawre computer
    response = requests.get(url)
    
    # convert response to unicode
    data = response.text
    
    # parse the json and convert it into a python dictionary

    parsed = json.loads(data)

    now = parsed["now"]
    messages = parsed["messages"]

    
    # aircraft will contain many rows.  It will contain 1 row per aircraft piaware could see when we sent the get request
    aircraft = parsed["aircraft"]

    # get date.time to use as a column in the database
    currentdt=datetime.datetime.now()
    
    # print(total)

    # loop through rows in aircraft returned from piaware
    # the if statement disregards incomplete records sometimes only partial records are recieved from the transponder
    # pull individual values out of each record and assign the value to variable

    i=0
    while i<len(aircraft):

        record=aircraft[i]
        # Loop through records as a dictionary and populate columns for only complete records
        if "hex" in aircraft[i] and "squawk" in aircraft[i] and "flight" in aircraft[i] and "lat" in aircraft[i] and "lon" in aircraft[i]  and "nucp" in aircraft[i]  and "seen_pos" in aircraft[i]  and "altitude" in aircraft[i]  and "vert_rate" in aircraft[i]  and "track" in aircraft[i]  and "speed" in aircraft[i]  and "category" in aircraft[i]  and "mlat" in aircraft[i]  and "tisb" in aircraft[i]  and "messages" in aircraft[i]  and "seen" in aircraft[i]  and "rssi" in aircraft[i]:
            hex = aircraft[i]['hex']              
            squawk = aircraft[i]['squawk']           # responder id
            flight = aircraft[i]['flight']           # flight number
            lat = aircraft[i]['lat']                 # latitude
            lon = aircraft[i]['lon']                 # longitude
            nucp = aircraft[i]['nucp']               # how strong was the signal from the transponder, higher is better
            seen_pos = aircraft[i]['seen_pos']
            altitude = aircraft[i]['altitude']       # altitude
            vr = aircraft[i]['vert_rate']            # vertical rate negative number are descending positive numbers are ascending
            track = aircraft[i]['track']             # heading as in degrees of the compass
            speed = aircraft[i]['speed']             # speed in knots
            category = aircraft[i]['category']
            mlat = aircraft[i]['mlat']               # boolean (true/ false) if the lon and lat were calcuated.  Requires three or more connected stations
            tisb = aircraft[i]['tisb']
            messages = aircraft[i]['messages']       
            seen = aircraft[i]['seen']
            rssi = aircraft[i]['rssi']

            # convert speed from knots to mph
            speed=speed*1.15078

            # Write to SQL
            cursor.execute("insert into [Piawaredb].[dbo].[KDFW2](dt,hex,squawk,flight, lat, lon, nucp, seen_pos, altitude, vr, track, speed, category, messages, seen, rssi) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", currentdt, hex, squawk, flight, lat, lon, nucp, seen_pos, altitude, vr, track, speed, category, messages, seen, rssi)
            conn.commit()

        i=i+1






    
