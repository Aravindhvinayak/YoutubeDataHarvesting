# YoutubeDataHarvesting
By using this code, we can get the Youtube channel details by entering corresponding channel_id and we can save the fetched data and get values from database for predefined set of queries 
# Objective:
To fetch the YouTube data by entering channel_id in the User Interface and get the corresponding data. 
Also proving storage button to save the fetched data in the My-SQL database. Finally get the results for predefined questions.
# Version 1.0.0 Last Update: 14 December 2024
# Required Packages:
As of version 1.0.0, the program is written for python 3.7. It uses the following non-default packages:
```
  streamlit
  googleapiclient.discovery
  googleapiclient.errors
  mysql.connector
  datetime
  pandas
```
# Run the Project:
Then, activate the environment with “current directory address of file where it was available” followed by streamlit run Home.py.
# Program Usage:
```
In function ## get_channel_info(channel_id,youtube) ## where we need to pass parameters,
 channel_id - valid YouTube channel_id
 youtube - build object of youtube api with API_Key”
to get the corresponding channel details.
```
```
In Function ## insert_data(con,cursor,l) ## we need to pass the parametes,
o	Con - connection details with db
o	Cursor – details of cursor where it is pointing
o	L – Data to save (should be in list of tuple data)
to save the data into the db
```



