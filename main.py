# Database Mgt Sys Final Project
# Kate Halushka and Josie Libbon
# Spring 2024

# Dataset: https://think.cs.vt.edu/corgis/datasets/json/airlines/airlines.json

import requests, json
import psycopg2
import psycopg2.extras
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

data_empty = False


# TODO: structure data and input into DB
def populate_data():
    # TODO: acquire data from internet
    data_endpoint = 'https://think.cs.vt.edu/corgis/datasets/json/airlines/airlines.json'
    unstructured_data = requests.get(data_endpoint)
    json_data = unstructured_data.json()

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
            cursor.execute("INSERT INTO airports (code, name) VALUES (%s, %s)", (airport_code, airport_name))
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




# TODO: perform analysis on data using DB
def analyze_data():
    # do analytics
    # find overall worst and best delay and cancellation rates by month and airport 
    cancelled_rates = {}
    delayed_rates = {}
    cursor.execute("SELECT airport_code, time_label, flights_cancelled, flights_delayed, flights_total FROM statistics ORDER BY flights_cancelled DESC")
    result = cursor.fetchall()
    for res in result:
        key = f"{res['airport_code']} {res['time_label']}"
        cancelled_rates[key] = res['flights_cancelled']/res['flights_total']
        delayed_rates[key] = res['flights_delayed']/res['flights_total']
    crs = list(cancelled_rates.values())
    idx = np.argmax(crs)
    max_c_airport = list(cancelled_rates.keys())[idx]
    print(max_c_airport)
    print("Max cancellation rate was "+ str(max(crs))+ "%")
    drs = list(delayed_rates.values())
    idx = np.argmax(drs)
    max_d_airport = list(delayed_rates.keys())[idx]
    print(max_d_airport)
    print("Max delay rate was "+ str(max(drs))+ "%")
    idx = np.argmin(crs)
    min_c_airport = list(cancelled_rates.keys())[idx]
    print(min_c_airport)
    print("Min cancellation rate was "+ str(min(crs))+ "%")
    idx = np.argmin(drs)
    min_d_airport = list(delayed_rates.keys())[idx]
    print(min_d_airport)
    print("Min delay rate was "+ str(min(drs))+ "%")
    cursor.execute(f"SELECT * FROM statistics WHERE airport_code LIKE '{min_c_airport[0:3]}' AND time_label LIKE '{min_c_airport[4:]}'")
    stats = cursor.fetchone()
    print(stats['flights_cancelled'])
    print(stats['flights_total'])

    # create histogram of delays and cancellations by month across all airports in a selected year
    year = 2012
    cursor.execute(f"""SELECT airport_code, time_label, flights_cancelled, flights_delayed FROM statistics JOIN time on statistics.time_label = time.label WHERE time.year = {year}""")
    stats = cursor.fetchall()
    dels = {}
    cancels = {}
    for stat in stats:
        if stat['time_label'] not in dels:
            dels[stat['time_label']] = stat['flights_delayed']
        else:
            dels[stat['time_label']] += stat['flights_delayed']
        if stat['time_label'] not in cancels:
            cancels[stat['time_label']] = stat['flights_cancelled']
        else:
            cancels[stat['time_label']] += stat['flights_cancelled']
    # make dicts into lists to be put into grouped bar chart
    labels = list(dels.keys())
    bars = {
        "Delays": list(dels.values()),
        "Cancellations": list(cancels.values())
    }
    x = np.arange(len(labels))  # the label locations
    width = 0.25  # the width of the bars
    multiplier = 0

    fig, ax = plt.subplots(layout='constrained')

    for attribute, measurement in bars.items():
        offset = width * multiplier
        rects = ax.bar(x + offset, measurement, width, label=attribute)
        ax.bar_label(rects, padding=3)
        multiplier += 1
    
    # Add some text for labels, title and custom x-axis tick labels, etc.
    ax.set_ylabel('Number of delayed/cancelled flights')
    ax.set_title('Delays and Cancellations per Month')
    ax.set_xticks(x + width, labels)
    ax.legend(loc='upper left')
    ax.set_ylim(0, max(max(bars['Delays']), max(bars['Cancellations']))+ 1000)

    plt.show()


        






# TODO: connect to DB
connection = psycopg2.connect(host='dbms-final-project.cb6yyy428jlt.us-east-1.rds.amazonaws.com',
                                user='postgres',
                                port=5432,
                                database='airportdata',
                                password='dbstudent')
connection.set_session(readonly=False, autocommit=True)

cursor = connection.cursor(cursor_factory = psycopg2.extras.RealDictCursor)

if(data_empty):
    populate_data()

analyze_data()

# Close cursor and connection
cursor.close()
connection.close()