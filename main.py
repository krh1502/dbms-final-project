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
from scipy import stats

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

def get_extreme_rates():
    # find overall worst and best delay and cancellation rates by month and airport
    cancelled_rates = {}
    delayed_rates = {}
    cursor.execute("SELECT airport_code, time_label, flights_cancelled, flights_delayed, flights_total FROM statistics")
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

def plot_flight_data_by_year(year):
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
    ax.set_title(f'Delays and Cancellations per Month in {year}')
    ax.set_xticks(x + width, labels)
    ax.legend(loc='upper left')
    ax.set_ylim(0, max(max(bars['Delays']), max(bars['Cancellations']))+ 1000)

    plt.show()

def plot_flight_data_by_month(month):
    cursor.execute(f"""SELECT airport_code, time_label, flights_cancelled, flights_delayed, time.year FROM statistics JOIN time on statistics.time_label = time.label WHERE time.month = {month}""")
    statistics = cursor.fetchall()
    dels = {}
    cancels = {}
    for stat in statistics:
        if stat['year'] not in dels:
            dels[stat['year']] = stat['flights_delayed']
        else:
            dels[stat['year']] += stat['flights_delayed']
        if stat['year'] not in cancels:
            cancels[stat['year']] = stat['flights_cancelled']
        else:
            cancels[stat['year']] += stat['flights_cancelled']
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
    ax.set_xlabel('Year')
    ax.set_title('Delays and Cancellations in December by Year')
    ax.set_xticks(x + width, labels)
    ax.legend(loc='upper left')
    ax.set_ylim(0, max(max(bars['Delays']), max(bars['Cancellations']))+ 1000)

    plt.show()

def best_and_worst_airports():
    # show non-on-time flights by airport
    cursor.execute(f"""SELECT airport_code, SUM(flights_cancelled) as cancels, SUM(flights_delayed) as delays, SUM(flights_total) as total FROM statistics GROUP BY airport_code ORDER BY airport_code ASC""")
    airport_data = cursor.fetchall()
    airport_dict = {}
    for airport in airport_data:
        airport_dict[airport['airport_code']] = (airport['cancels'] + airport['delays']) / airport['total']
    rates = list(airport_dict.values())
    average_rate = np.average(rates)
    worst_airport = list(airport_dict.keys())[np.argmax(rates)]
    best_airport = list(airport_dict.keys())[np.argmin(rates)]
    print("The airport with the lowest rate of on-time flights is: " + worst_airport + " with a rate of " + "{:.2%}".format(1-max(rates)) + " of flights on time.")
    print("The airport with the highest rate of on-time flights is: " + best_airport + " with a rate of " + "{:.2%}".format(1-min(rates)) + " of flights on time.")
    print("The average on-time rate across all airports is "+ "{:.2%}".format(1-average_rate))

def average_delay_times():
    query = """
        select sum(delays_carrier) as delays_carrier,
        sum(delays_late_aircraft) as delays_late_aircraft,
        sum(delays_national_aviation_system) as delays_national_aviation_system,
        sum(delays_security) as delays_security,
        sum(delays_weather) as delays_weather,
        sum(minutes_delayed_carrier) as minutes_delayed_carrier,
        sum(minutes_delayed_late_aircraft) as minutes_delayed_late_aircraft,
        sum(minutes_delayed_national_aviation_system) as minutes_delayed_national_aviation_system,
        sum(minutes_delayed_security) as minutes_delayed_security,
        sum(minutes_delayed_weather) as minutes_delayed_weather
        from statistics;
        """
    cursor.execute(query)
    delay_totals = cursor.fetchone()
    print("The average time a flight is late for carrier delays is "+ "{:.2f}".format(delay_totals['minutes_delayed_carrier']/delay_totals['delays_carrier']) + " minutes.")
    print("The average time a flight is late for aircraft delays is "+ "{:.2f}".format(delay_totals['minutes_delayed_late_aircraft']/delay_totals['delays_late_aircraft']) + " minutes.")
    print("The average time a flight is late for national aviation system delays is "+ "{:.2f}".format(delay_totals['minutes_delayed_national_aviation_system']/delay_totals['delays_national_aviation_system']) + " minutes.")
    print("The average time a flight is late for security delays is "+ "{:.2f}".format(delay_totals['minutes_delayed_security']/delay_totals['delays_security']) + " minutes.")
    print("The average time a flight is late for weather delays is "+ "{:.2f}".format(delay_totals['minutes_delayed_weather']/delay_totals['delays_weather']) + " minutes.")

def most_frequent_delay_type_by_airport():
    query = """
            select airport_code,
            sum(delays_carrier) as delays_carrier,
            sum(delays_late_aircraft) as delays_late_aircraft,
            sum(delays_national_aviation_system) as delays_national_aviation_system,
            sum(delays_security) as delays_security,
            sum(delays_weather) as delays_weather
            from statistics
            group by airport_code
            order by airport_code ASC;
            """
    cursor.execute(query)
    airports = cursor.fetchall()
    for airport in airports:
        reasons = {'Carrier': airport['delays_carrier'], 
                   'Late Aircraft': airport['delays_late_aircraft'],
                   'National Aviation System': airport['delays_national_aviation_system'],
                   'Security': airport['delays_security'],
                   'Weather': airport['delays_weather'],}
        most_frequent_key = max(zip(reasons.values(), reasons.keys()))[1]
        print(f"The most frequent reason for delays at {airport['airport_code']} is {most_frequent_key}.")
    query = """
            select
            sum(delays_carrier) as "Carrier",
            sum(delays_late_aircraft) as "Late Aircraft",
            sum(delays_national_aviation_system) as "National Aviation System",
            sum(delays_security) as "Security",
            sum(delays_weather) as "Weather"
            from statistics
            """
    cursor.execute(query)
    total_reasons = cursor.fetchone()
    most_frequent_key = max(zip(total_reasons.values(), total_reasons.keys()))[1]
    print(f"The most frequent reason for delays across all airports is {most_frequent_key}.")



# TODO: perform analysis on data using DB
def analyze_data():
    # do analytics
    # get_extreme_rates()
    # plot_flight_data_by_year(2014)
    # numerical month (1=january, etc)
    # plot_flight_data_by_year(12)
    # best_and_worst_airports()
    #average_delay_times()
    most_frequent_delay_type_by_airport()
    # is delta actually the most delayed airline?



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