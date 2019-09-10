# Import Python packages 
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv

# checking current working directory
print(os.getcwd())

# Get current folder and subfolder event data
filepath = os.getcwd() + '/event_data'

# create a list of files and collect each filepath
for root, dirs, files in os.walk(filepath):
    
# join the file path and roots with the subdirectories
    file_path_list = glob.glob(os.path.join(root,'*'))
    #print(file_path_list)

# Processing the files to create the data file csv that will be used for Apache Casssandra tables	
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
            
print(len(full_data_rows_list))
#print(full_data_rows_list)

# creating a event data csv file called event_datafile_full csv that will be used to insert data into the \
# tables
csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
    writer = csv.writer(f, dialect='myDialect')
    writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',\
                'level','location','sessionId','song','userId'])
    for row in full_data_rows_list:
        if (row[0] == ''):
            continue
        writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))

# check the number of rows in csv file
with open('event_datafile_new.csv', 'r', encoding = 'utf8') as f:
    print(sum(1 for line in f))
	
# connection to a Cassandra instance
from cassandra.cluster import Cluster
cluster = Cluster()

try:
    session = cluster.connect()
except Exception as e:
    print(e)

# Keyspace 
try:
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS sparkify 
    WITH REPLICATION = 
    { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
)

except Exception as e:
    print(e)

try:
    session.set_keyspace('sparkify')
except Exception as e:
    print(e)
	
# Creating/inserting table number one In this query, I used sessionid and iteminsession as the partition key
#Table for query artist, song title and length, sessionId = x, and itemInSession = y
query = "CREATE TABLE IF NOT EXISTS session_table "
query = query + "(sessionid int, iteminsession int, artist text, song text, length decimal, \
PRIMARY KEY (sessionid, iteminsession))"
print(query)
try:
    session.execute(query)
except Exception as e:
    print(e) 

file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
# INSERT statement
        query = "INSERT INTO session_table (sessionid, iteminsession, artist, song, length)"
        query = query + " VALUES (%s, %s, %s, %s, %s)"
        try:
            session.execute(query, (int(line[8]), int(line[3]), line[0], line[9], float(line[5])))
        except Exception as e:
            print(e)
			
# query artist, song title and length, filtering by sessionId and itemInSession
query = "select artist, song, length from session_table where sessionid=194 and iteminsession=3"
print(query)
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
for row in rows:
    print(row)
    print (row.artist, row.song, row.length)
	
# Creating/inserting table number two In this query, I used userid and sessionid as the 
# partition key and iteminsession as my clustering key. Each partition is uniquely identified
# by userid and sessionid while iteminsession was used to uniquely identify the rows 
# within a partition to sort the data.
# Table for query name of artist, song (sorted by itemInSession) and user (first and last name)
query = "CREATE TABLE IF NOT EXISTS artist_song_user_table "
query = query + "(userid int, sessionid int, iteminsession int, artist text, song text, firstname text, lastname text, \
PRIMARY KEY ((userid, sessionid), iteminsession))"
print(query)
try:
    session.execute(query)
except Exception as e:
    print(e)
	
file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
        query = "INSERT INTO artist_song_user_table (userid, sessionid, iteminsession, artist, song, firstname, lastname)"
        query = query + " VALUES (%s, %s, %s, %s, %s, %s, %s)"
        try:
            session.execute(query, (int(line[10]), int(line[8]), int(line[3]), line[0], line[9], line[1], line[4]))
        except Exception as e:
            print(e)

# query artist, song title and user name, filtered by userid and sessionid 
query = "select artist, song, firstname, lastname from artist_song_user_table WHERE userid=10 and sessionid=182"
print(query)
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print (row.artist, row.song, row.firstname, row.lastname)
	
# Table for query username filtered by song title
query = "CREATE TABLE IF NOT EXISTS username_song "
query = query + "(song text, userid int, firstname text, lastname text, \
PRIMARY KEY (song, userid))"
print(query)
try:
    session.execute(query)
except Exception as e:
    print(e)
	
file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
# INSERT statement
        query = "INSERT INTO username_song (song, userid, firstname, lastname)"
        query = query + " VALUES (%s, %s, %s, %s)"
        try:
            session.execute(query, (line[9], int(line[10]), line[1], line[4]))
        except Exception as e:
            print(e)
			
# every user who listened to the song 'All Hands Against His Own'
query = "select firstname, lastname from username_song WHERE song='All Hands Against His Own'"
print(query)
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print (row.firstname, row.lastname)
	
#just for testing purpose
query = "select firstname, lastname from username_song limit 10"
print(query)
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print (row.firstname, row.lastname)
	
# Drop the table before closing out the sessions
query = "drop table session_table"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
	
query = "drop table artist_song_user_table"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
	
query = "drop table username_song"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
	
session.shutdown()
cluster.shutdown()