# Database Mgt Sys Final Project
# Kate Halushka and Josie Libbon
# Spring 2024

# Dataset: https://think.cs.vt.edu/corgis/datasets/json/airlines/airlines.json

# TODO: acquire data from internet
import requests, json

data_endpoint = 'https://think.cs.vt.edu/corgis/datasets/json/airlines/airlines.json'

unstructured_data = requests.get(data_endpoint)
json_data = unstructured_data.json()

# TODO: connect to DB

# TODO: structure data and input into DB

# TODO: perform analysis on data using DB