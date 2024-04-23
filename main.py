# Database Mgt Sys Final Project
# Kate Halushka and Josie Libbon
# Spring 2024

# Dataset: https://think.cs.vt.edu/corgis/datasets/json/airlines/airlines.json

# TODO: acquire data from internet
import requests, json
import psycopg2
import psycopg2.extras
import pandas as pd

# TODO: connect to DB

# Connect to the database
connection = psycopg2.connect(host='dbms-final-project.cb6yyy428jlt.us-east-1.rds.amazonaws.com',
                             user='postgres',
                             port=5432,
                             database='airportdata',
                             password='dbstudent')
connection.set_session(readonly=False, autocommit=True)

cursor = connection.cursor(cursor_factory = psycopg2.extras.RealDictCursor)


# TODO: structure data and input into DB
data_endpoint = 'https://think.cs.vt.edu/corgis/datasets/json/airlines/airlines.json'
unstructured_data = requests.get(data_endpoint)
json_data = unstructured_data.json()

codes = []

# Iterate through JSON data
for item in json_data:
    airport_code = item['Airport']['Code']
    airport_name = item['Airport']['Name']
    time_label = item['Time']['Label']
    month = item['Time']['Month']
    month_name = item['Time']['Month Name']
    year = item['Time']['Year']
    delays_carrier = item['Statistics']['# of Delays']['Carrier']
    delays_late_aircraft = item['Statistics']['# of Delays']['Late Aircraft']
    delays_nas = item['Statistics']['# of Delays']['National Aviation System']
    delays_security = item['Statistics']['# of Delays']['Security']
    delays_weather = item['Statistics']['# of Delays']['Weather']
    carriers_names = item['Statistics']['Carriers']['Names']
    carriers_total = item['Statistics']['Carriers']['Total']
    flights_cancelled = item['Statistics']['Flights']['Cancelled']
    flights_delayed = item['Statistics']['Flights']['Delayed']
    flights_diverted = item['Statistics']['Flights']['Diverted']
    flights_on_time = item['Statistics']['Flights']['On Time']
    flights_total = item['Statistics']['Flights']['Total']
    minutes_delayed_carrier = item['Statistics']['Minutes Delayed']['Carrier']
    minutes_delayed_late_aircraft = item['Statistics']['Minutes Delayed']['Late Aircraft']
    minutes_delayed_nas = item['Statistics']['Minutes Delayed']['National Aviation System']
    minutes_delayed_security = item['Statistics']['Minutes Delayed']['Security']
    minutes_delayed_total = item['Statistics']['Minutes Delayed']['Total']
    minutes_delayed_weather = item['Statistics']['Minutes Delayed']['Weather']

    # Insert into Airport table
    try:
        cursor.execute("INSERT INTO airport (code, name) VALUES (%s, %s)", (airport_code, airport_name))
        print(f"Airport with code '{airport_code}' added to database.")
    except psycopg2.IntegrityError as e:
        # Handle duplicate key error
        print(f"Airport with code '{airport_code}' already exists.")

    # Insert into Time table
    cursor.execute("INSERT INTO time (label, month, month_name, year) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING", (time_label, month, month_name, year))

    # Insert into Statistics table
    cursor.execute("""INSERT INTO statistics (airport_code, time_label, delays_carrier, delays_late_aircraft, delays_national_aviation_system,
                      delays_security, delays_weather, carriers_names, carriers_total, flights_cancelled, flights_delayed, flights_diverted,
                      flights_on_time, flights_total, minutes_delayed_carrier, minutes_delayed_late_aircraft, minutes_delayed_national_aviation_system,
                      minutes_delayed_security, minutes_delayed_total, minutes_delayed_weather)
                      VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                      (airport_code, time_label, delays_carrier, delays_late_aircraft, delays_nas, delays_security, delays_weather,
                       carriers_names, carriers_total, flights_cancelled, flights_delayed, flights_diverted, flights_on_time, flights_total,
                       minutes_delayed_carrier, minutes_delayed_late_aircraft, minutes_delayed_nas, minutes_delayed_security,
                       minutes_delayed_total, minutes_delayed_weather))

    # Commit the transaction
    connection.commit()

# Close cursor and connection
cursor.close()
connection.close()





# TODO: perform analysis on data using DB