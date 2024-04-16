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

for item in json_data:
    if item['Airport']['Code'] in codes:
        continue
    else:
        name = item['Airport']['Name']
        if "'" in item['Airport']['Name']:
            name = name.replace("'", "''")
        cursor.execute(f"INSERT INTO airports (code, name) VALUES('{item['Airport']['Code']}', '{name}')")
        connection.commit()
        codes.append(item['Airport']['Code'])

connection.close()





# TODO: perform analysis on data using DB