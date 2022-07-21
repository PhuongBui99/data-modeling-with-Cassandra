#!/usr/bin/env python
# coding: utf-8

# # Part I. ETL Pipeline for Pre-Processing the Files

# ## PLEASE RUN THE FOLLOWING CODE FOR PRE-PROCESSING THE FILES

# #### Import Python packages 
# Import Python packages 
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv


# #### Creating list of filepaths to process original event csv data files
# checking your current working directory
print(os.getcwd())

# Get your current folder and subfolder event data
filepath = os.getcwd() + '/event_data'

# Create a for loop to create a list of files and collect each filepath
for root, dirs, files in os.walk(filepath):
    
# join the file path and roots with the subdirectories using glob
    file_path_list = glob.glob(os.path.join(root,'*'))
    #print(file_path_list)


# #### Processing the files to create the data file csv that will be used for Apache Casssandra tables
# initiating an empty list of rows that will be generated from each file
full_data_rows_list = [] 
    
# for every filepath in the file path list 
for f in file_path_list:

# reading csv file 
    with open(f, 'r', encoding = 'utf8', newline='') as csvfile: 
        # creating a csv reader object 
        csvreader = csv.reader(csvfile) 
        next(csvreader)
        
 # extracting each data row one by one and append it        
        for line in csvreader:
            #print(line)
            full_data_rows_list.append(line) 
            
# uncomment the code below if you would like to get total number of rows 
#print(len(full_data_rows_list))
# uncomment the code below if you would like to check to see what the list of event data rows will look like
#print(full_data_rows_list)

# creating a smaller event data csv file called event_datafile_full csv that will be used to insert data into the \
# Apache Cassandra tables
csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
    writer = csv.writer(f, dialect='myDialect')
    writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',                'level','location','sessionId','song','userId'])
    for row in full_data_rows_list:
        if (row[0] == ''):
            continue
        writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))


# check the number of rows in your csv file
with open('event_datafile_new.csv', 'r', encoding = 'utf8') as f:
    print(sum(1 for line in f))


# # Part II. Complete the Apache Cassandra coding portion of your project. 
# 
# ## Now you are ready to work with the CSV file titled <font color=red>event_datafile_new.csv</font>, located within the Workspace directory.  The event_datafile_new.csv contains the following columns: 
# - artist 
# - firstName of user
# - gender of user
# - item number in session
# - last name of user
# - length of the song
# - level (paid or free song)
# - location of the user
# - sessionId
# - song title
# - userId
# 
# The image below is a screenshot of what the denormalized data should appear like in the <font color=red>**event_datafile_new.csv**</font> after the code above is run:<br>
# 
# <img src="images/image_event_datafile_new.jpg">

# ## Begin writing your Apache Cassandra code in the cells below

# #### Creating a Cluster

# This should make a connection to a Cassandra instance your local machine 
# (127.0.0.1)

from cassandra.cluster import Cluster
cluster = Cluster(['127.0.0.1'])

# To establish connection and begin executing queries, need a session
session = cluster.connect()


# #### Create Keyspace
# TO-DO: Create a Keyspace 
try:
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS udacity 
    WITH REPLICATION = 
    { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
)

except Exception as e:
    print(e)


# #### Set Keyspace
# TO-DO: Set KEYSPACE to the keyspace specified above
try:
    session.set_keyspace('udacity')
except Exception as e:
    print(e)


# ### Now we need to create tables to run the following queries. Remember, with Apache Cassandra you model the database tables on the queries you want to run.

# ## Create queries to ask the following three questions of the data
# 
# ### 1. Give me the artist, song title and song's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 4
# 
# 
# ### 2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
#     
# 
# ### 3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
# 
# 
# 

# ##How to create primary key
# Create a primary key on sessionid and iteminsession, sessionid is the partitioning key which will partition the date in way so that all the records for same session id will be stored in same partition.

## TO-DO: Query 1:  Give me the artist, song title and song's length in the music app history that was heard during \
## sessionId = 338, and itemInSession = 4
'''
To get artist, song title, song's length in the music app history that has sessionId = 338 and itemInSession = 4
We need to create a table with 5 columns Session_Id, Item_Number, Artist_Name, Song_Title, Song_Length
'''
query1 = "CREATE TABLE IF NOT EXISTS songs "
query1 = query1 + "(Session_Id double, Item_Number double, Artist varchar, Song_Title varchar, Song_Length double , PRIMARY KEY(Session_Id, Item_Number))"
try:
    session.execute(query1)
except Exception as e:
    print(e)

# We have provided part of the code to set up the CSV file. Please complete the Apache Cassandra code below#
file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
## TO-DO: Assign the INSERT statements into the `query` variable
        query = "INSERT INTO songs (Session_Id, Item_Number, Artist, Song_Title, Song_Length)"
        query = query + "VALUES (%s, %s, %s, %s, %s)"
        ## TO-DO: Assign which column element should be assigned for each column in the INSERT statement.
        ## For e.g., to INSERT artist_name and user first_name, you would change the code below to `line[0], line[1]`
        session.execute(query, (int(line[8]), int(line[3]), line[0], line[9], float(line[5])))


# #### Do a SELECT to verify that the data have been inserted into each table
## TO-DO: Add in the SELECT statement to verify the data was entered into the table
querySelect = "SELECT Artist, Song_Title, Song_Length FROM songs WHERE Session_Id = 338 AND Item_Number = 4"

try:
    result = session.execute(querySelect)
except Exception as e:
    print(e)


# ### COPY AND REPEAT THE ABOVE THREE CELLS FOR EACH OF THE THREE QUESTIONS

# ##How to create primary key
# Create a primary key on sessionid, itemInSession and userid, sessionid and userId are the partitioning keys which will partition
# in way so that all the records for same user id, sessionid will be stored in same partition.

## TO-DO: Query 2: Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name)\
## for userid = 10, sessionid = 182
'''
To get artist, itemInSession, first name, last name of user in the music app history that has userid = 10, sessionid = 182
We need to create a table with 6 columns: User_Id, Session_Id, Item_Number, Artist, First_Name, Last_Name
'''
query2 = "CREATE TABLE IF NOT EXISTS users "
query2 = query2 + "(User_Id int, Session_Id int, Item_Number int, Artist varchar,  First_Name varchar, Last_Name varchar, PRIMARY KEY((User_Id, Session_Id), Item_Number))"
try:
    session.execute(query2)
except Exception as e:
    print(e)

##This is for read data from file and then insert them into users table
file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
        query = "INSERT INTO users (User_Id, Session_Id, Item_Number, Artist, First_Name, Last_Name)"
        query = query + "VALUES (%s, %s, %s, %s, %s, %s)"
        session.execute(query, (int(line[10]), int(line[8]), int(line[3]), line[0], line[1], line[4]))

##This query is for selecting records from users table

querySelect1 = "SELECT Artist, Item_Number, First_Name, Last_Name FROM users WHERE User_Id = 10 AND Session_Id = 182"

try:
    session.execute(querySelect1)
except Exception as e:
    print(e)


# ##How to create primary key
# Create a primary key on Song_Title and User_Id, User_Id is the partitioning key which will 
# partition the user in way so that all the records for same user id will be stored in same partition.


## TO-DO: Query 3: Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
'''
To get first name, last name of user in the music app history that has song title is 'All Hands Against His Own'
We need to create a table with 4 columns: User_Id, Song_Title, First_Name, Last_Name
'''

query3 = "CREATE TABLE IF NOT EXISTS listenSong "
query3 = query3 + "(Song_Title varchar, User_Id int, First_Name varchar, Last_Name varchar, PRIMARY KEY(Song_Title, User_Id))"
try:
    session.execute(query3)
except Exception as e:
    print(e)


##This is for read data from file and then insert them into listenSong table
file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
        query = "INSERT INTO listenSong (Song_Title, User_Id, First_Name, Last_Name)"
        query = query + "VALUES (%s, %s, %s, %s)"
        session.execute(query, (line[9], int(line[10]), line[1], line[4]))


##This query is for selecting records from listenSong table
querySelect2 = "SELECT First_Name, Last_Name,FROM listenSong WHERE Song_Title = 'All Hands Against His Own' "
try:
    session.execute(querySelect2)
except Exception as e:
    print(e)


# ### Drop the tables before closing out the sessions
## TO-DO: Drop the table before closing out the sessions

query4 = "DROP TABLE songs"
query5 = "DROP TABLE users"
query6 = "DROP TABLE listenSong"

try:
    session.execute(query4)
    session.execute(query5)
    session.execute(query6)
except Exception as e:
    print(e)


# ### Close the session and cluster connection¶

session.shutdown()
cluster.shutdown()




