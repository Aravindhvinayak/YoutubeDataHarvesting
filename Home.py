import streamlit as st
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import mysql.connector
from datetime import *
import pandas as pd

# functions:
def get_channel_info(channel_id,youtube):   
    #building request:
    request = youtube.channels().list(
            part='snippet,contentDetails,statistics,status',
            id=channel_id,
            maxResults=50 )
    #executing request
    try:
        response = request.execute()
        tem =response.get('items',None)
        if tem:
            channel_data = dict(Channel_Name = response['items'][0]['snippet'].get('title',None),
                            Channel_Id = response['items'][0].get('id',None),
                            Subcription_count = response['items'][0]['statistics'].get('subscriberCount',None),
                            Channel_views = response['items'][0]['statistics'].get('viewCount',None),
                            Channel_Description = response['items'][0]['snippet'].get('description',None),
                            Channel_Status = response['items'][0]['status'].get('privacyStatus',None),
                            Playlist_ids = response['items'][0]['contentDetails']['relatedPlaylists'].get('uploads',None),
                            Video_count = response['items'][0]['statistics'].get('videoCount',None))
        
            return channel_data
        else:
            return None
    except HttpError as e:
        return None

#get youtube playlist data:
def get_playlist_data(youtube,channel_id):
    #building request:
    request = youtube.playlists().list(
            part = 'snippet,id',
            channelId = channel_id,
            maxResults = 50 )
    try:
        playlist_data = request.execute()
        tem = playlist_data.get('items',None)
        if tem:
            all_playlist_data=[]
            for i in range(len(playlist_data['items'])):
                playlist_dict_data = dict( Playlist_id = playlist_data['items'][i].get('id',None),
                                        Channel_id = playlist_data['items'][i]['snippet'].get('channelId',None),
                                        Playlist_Name = playlist_data['items'][i]['snippet'].get('title',None),
                                        maxResults = 25 
                )
                all_playlist_data.append(playlist_dict_data)
                
            next_playlist_pagetoken = playlist_data.get('nextPageToken')
            nxt_playlist_page = True
            while nxt_playlist_page:
                if next_playlist_pagetoken is None:
                    nxt_playlist_page = False
                else:
                    request = youtube.playlists().list(
                        part = 'snippet,id',
                        channelId = channel_id,
                        maxResults = 50 )
                    playlist_data = request.execute()
                
                    all_playlist_data=[]
                    for i in range(len(playlist_data['items'])):
                        playlist_dict_data = dict( Playlist_id = playlist_data['items'][i].get('id',None),
                                                Channel_id = playlist_data['items'][i]['snippet'].get('channelId',None),
                                                Playlist_Name = playlist_data['items'][i]['snippet'].get('title',None),
                                                maxResults = 25
                            
                        )
                        all_playlist_data.append(playlist_dict_data)
                    next_playlist_pagetoken = playlist_data.get('nextPageToken')
            return all_playlist_data
        else:
            return None
    except HttpError as e:
        return None

#get video id 
def get_video_ids(youtube,Playlist_ids):
    #building request:
    request = youtube.playlistItems().list(
        part='snippet,contentDetails,id,status',
        playlistId=Playlist_ids,
        maxResults=50 )
    try:
        response = request.execute()
        tem = response.get('items',None)
        video_id=[]
        if tem:
            for i in range(len(response['items'])):
                Video_l_id = response['items'][i]['contentDetails'].get('videoId',None)
                video_id.append(Video_l_id)

            next_page_token = response.get('nextPageToken') # using get because if nothing we get in response then it will return as none
            more_pages = True

            while more_pages:
                if next_page_token is None:
                    more_pages = False
                else:
                    request = youtube.playlistItems().list(
                            part='snippet,contentDetails,id,status',
                            playlistId=Playlist_ids,
                            maxResults=50,
                            pageToken = next_page_token)
                    response = request.execute()
                    
                    for i in range(len(response['items'])):
            
                        Video_l_id = response['items'][i]['contentDetails'].get('videoId',None)
                        video_id.append(Video_l_id)
                
                    next_page_token = response.get('nextPageToken')
            return video_id
        else:
            return None  
    except HttpError as e:
        return None

#get videos data
def get_videos_data (youtube,video_ids):

    videos_data=[]
    try:
        for i in range(0, len(video_ids), 50): # request accepts only 50 so we are passing 50 in each loop
            #building request
            request = youtube.videos().list(
                part="snippet,contentDetails,statistics",
                id=','.join(video_ids[i:i+50])
            )
            response = request.execute()
            tem = response.get('items',None)
            if tem:
                for item in response['items']:
                    video_dic_data = dict(Video_id = item.get('id',None),
                                        Video_Name = item['snippet'].get('title',None),
                                        Video_Description = item['snippet'].get('description',None),
                                        Channel_id = item['snippet'].get('channelId',None),
                                        Published_data = item['snippet'].get('publishedAt',None),
                                        Video_view_count = item['statistics'].get('viewCount',0),
                                        Video_like_count = item['statistics'].get('likeCount',0),
                                        Video_Favorite_count = item['statistics'].get('favoriteCount',0),
                                        Video_comment_count = item['statistics'].get('commentCount',0),
                                        Video_duration = item['contentDetails'].get('duration',None),
                                        Video_thumbnail = item['snippet']['thumbnails']['default'].get('url',None),
                                        Video_caption_status = item['contentDetails'].get('caption',None)
                    )
                    videos_data.append(video_dic_data)
            else:
                 videos_data = None   
        return videos_data
    except HttpError as e:
        return None
    
#get comment details
def get_comments_details(youtube,channel_id):

    #building request
    request = youtube.commentThreads().list(
            part ='id,snippet',
            allThreadsRelatedToChannelId=channel_id,
             maxResults=100 )
    try:
        outt = request.execute()
        tem = outt.get('items',None)
        if tem:
            all_comments = []
            for i in range(len(outt['items'])):
                da = dict(
                    comment_id= outt['items'][i].get('id',None),
                    video_id= outt['items'][i]['snippet'].get('videoId',None),
                    comment_text= outt['items'][i]['snippet']['topLevelComment']['snippet'].get('textOriginal',None),
                    comment_author= outt['items'][i]['snippet']['topLevelComment']['snippet'].get('authorDisplayName',None),
                    comment_published_date = outt['items'][i]['snippet']['topLevelComment']['snippet'].get('publishedAt',None)
                )
                all_comments.append(da)

            nxt_pg_tkn = outt.get('nextPageToken')
            nxt_page = True

            while nxt_page:
                if nxt_pg_tkn is None:
                    nxt_page = False
                else:
                    request = youtube.commentThreads().list(
                            part ='id,snippet',
                            allThreadsRelatedToChannelId=channel_id,
                            pageToken = nxt_pg_tkn,
                            maxResults=100 )
                    outt = request.execute()
                    for i in range(len(outt['items'])):
                        da = dict(
                            comment_id= outt['items'][i].get('id',None),
                            video_id= outt['items'][i]['snippet'].get('videoId',None),
                            comment_text= outt['items'][i]['snippet']['topLevelComment']['snippet'].get('textOriginal',None),
                            comment_author= outt['items'][i]['snippet']['topLevelComment']['snippet'].get('authorDisplayName',None),
                            comment_published_date = outt['items'][i]['snippet']['topLevelComment']['snippet'].get('publishedAt',None)
                        )
                        all_comments.append(da)
                        nxt_pg_tkn = outt.get('nextPageToken')
                        
            return all_comments
        else:
            return None
    except HttpError as e:
        return None

#SQL code
#making connection
def make_connection():
    con = mysql.connector.connect(
        host='localhost',
        user='root',
        password='12345678'
    )
    return con

#creating db and make connection
def create_db(con):
    if (con.is_connected()):
        cursor = con.cursor()
        cursor.execute("show databases")
        for i in cursor:
            if i[0] == 'youtubedataharvesting':
                var3 = "use youtubedataharvesting"
                cursor.execute(var3)
                return cursor
                break
                
        else:
            var = "create database if not exists youtubedataharvesting"
            cursor.execute(var)
            var3 = "use youtubedataharvesting"
            cursor.execute(var3)
            return cursor
    else:
        er = "Not connected"
        return er

#Initial Checking condition
def initial_check(cursor,user_input):
    query0 = "select channel_id from channel"
    cursor.execute(query0)
    temp = [i[0] for i in cursor if user_input == i[0]]
    return temp

def create_tables(cursor):
    
    var4 ="""create table if not exists Channel(
                                                 channel_name varchar(255) COMMENT 'Name of the channel',
                                                 channel_id varchar(255) COMMENT 'Unique Identifier for the channel',                                             
                                                 channel_subcription_count INT COMMENT 'Subcription count the channel',
                                                 channel_views INT COMMENT 'Number of views for the channel',
                                                 channel_description TEXT COMMENT ' Description of the channel',
                                                 channel_status varchar(255) COMMENT 'Status of the channel',
                                                 channel_playlist_ids varchar(255) COMMENT 'Playlist of the channel',
                                                 video_count INT COMMENT 'Total no of videos in the channel'
                                                 )"""
    cursor.execute(var4)
    
    var6="""create table if not exists Playlist(
                                                 playlist_id varchar(255) COMMENT 'Individual playlist id of the channel',
                                                 channel_id varchar(255) COMMENT 'Unique Identifier for the channel',                                             
                                                 Playlist_Name varchar(255) COMMENT 'Play list name in the channel',
                                                 maxResults INT COMMENT 'max result the channel'
                                                 )"""
    cursor.execute(var6)
    var7="""create table if not exists VideoData(
                                                  Video_id varchar(255) COMMENT 'Unique Identifier for the video',
                                                  Video_Name varchar(255) COMMENT 'Name of the video',
                                                  Video_Description TEXT COMMENT 'Description of the Video',
                                                  channel_id varchar(255) COMMENT 'Unique Identifier for the channel',
                                                  Published_data DATE COMMENT 'Published date of the video',
                                                  Video_view_count INT COMMENT 'Number of views for the video',
                                                  Video_like_count INT COMMENT ' Number of like count of the video',
                                                  Video_Favorite_count INT COMMENT 'Favorite count of the video',
                                                  Video_comment_count INT COMMENT 'Comment count of the video',
                                                  Video_duration TIME COMMENT 'Duration of the video',
                                                  Video_thumbnail varchar(255) COMMENT 'Thumbnail of the video',
                                                  Video_caption_status BOOL COMMENT 'Caption status'
                                                  )"""
    cursor.execute(var7)
    
    var8="""create table if not exists CommentData(
                                                    comment_id varchar(255) COMMENT 'Unique Identifier for the comment',
                                                    Video_id varchar(255) COMMENT 'Unique Identifier for the video',
                                                    comment_text TEXT COMMENT 'Comment of the video',
                                                    comment_author varchar(255) COMMENT 'Comment made by of the Video',
                                                    comment_published_date DATE COMMENT'Commented date'
                                                    )"""
    cursor.execute(var8)
    #print('tables created successfully')
    b = "pass"
    return b

#calling tables and inserting data
def insert_data(con,cursor,l):
    dicdf = l
    var = "show tables"
    cursor.execute(var)
    check_list = ["channel","commentdata","playlist","videodata"]
    have_list = []
    for i in cursor:
        have_list.append(i[0])
    if ( check_list == have_list ):
        for name, df in dicdf.items():
            if name == 'channel_info_df':
                query = "insert into Channel values(%s,%s,%s,%s,%s,%s,%s,%s)"
                data=[]
                for index in df.index:
                    row=df.loc[index].values
                    row=(str(row[0]),str(row[1]),int(row[2]),int(row[3]),str(row[4]),str(row[5]),str(row[6]),int(row[7]))
                    data.append(row)
                cursor.executemany(query,data)
                con.commit()
                
            elif name == 'playlist_data_df':    
                query = "insert into Playlist values(%s,%s,%s,%s)"
                data=[]
                for index in df.index:
                    row=df.loc[index].values
                    row=(str(row[0]),str(row[1]),str(row[2]),int(row[3]))
                    data.append(row)
                cursor.executemany(query,data)
                con.commit()
                
            elif name == 'video_data_df': 
                query = "insert into videodata values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                data=[]
                for index in df.index:
                    row=df.loc[index].values
                    if (type(row[4]) == str):
                        published_date=datetime.fromisoformat(row[4])
                        published_date=f"{published_date.year}-{published_date.month}-{published_date.day}"
                    else:
                        published_date = None
                    duration=row[9]
                    if (type(duration)== str):
                        if (duration.find('M') != -1) & (duration.find('S') != -1):
                            minu=int(duration[duration.index('T')+1 : duration.index('M')])
                            sec=int(duration[duration.index('M')+1 : duration.index('S')])
                            up_tim = f"{minu}:{sec}"
                        elif duration.find('M') == -1:
                            sec=int(duration[duration.index('T')+1 : duration.index('S')])
                            up_tim = f"{00}:{sec}"
                        elif duration.find('S') == -1:
                            minu=int(duration[duration.index('T')+1 : duration.index('M')])
                            up_tim = f"{minu}:{00}"
                    else:
                        up_tim = None
                    row=(str(row[0]),str(row[1]),str(row[2]),str(row[3]),published_date,int(row[5]),int(row[6]),int(row[7]),int(row[8]),up_tim,str(row[10]),bool(row[11]))
                    data.append(row)
                cursor.executemany(query,data)
                con.commit()
                
            elif name == 'comment_details_df':        
                query = "insert into CommentData values(%s,%s,%s,%s,%s)"
                data=[]
                for index in df.index:
                    row=df.loc[index].values
                    if (type(row[4]) == str):
                        published_date=datetime.fromisoformat(row[4])
                        published_date=f"{published_date.year}-{published_date.month}-{published_date.day}"
                    else:
                        published_date = None
                    row=(str(row[0]),str(row[1]),str(row[2]),str(row[3]),published_date)
                    data.append(row)
                cursor.executemany(query,data)
                con.commit()
        con.close()
        b = "Data Inserted"
        return b   
    else:
        cret = create_tables(cursor)
        for name, df in dicdf.items():
            if name == 'channel_info_df':
                query = "insert into Channel values(%s,%s,%s,%s,%s,%s,%s,%s)"
                data=[]
                for index in df.index:
                    row=df.loc[index].values
                    row=(str(row[0]),str(row[1]),int(row[2]),int(row[3]),str(row[4]),str(row[5]),str(row[6]),int(row[7]))
                    data.append(row)
                cursor.executemany(query,data)
                con.commit()
                
            elif name == 'playlist_data_df':    
                query = "insert into Playlist values(%s,%s,%s,%s)"
                data=[]
                for index in df.index:
                    row=df.loc[index].values
                    row=(str(row[0]),str(row[1]),str(row[2]),int(row[3]))
                    data.append(row)
                cursor.executemany(query,data)
                con.commit()
                
            elif name == 'video_data_df': 
                query = "insert into videodata values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                data=[]
                for index in df.index:
                    row=df.loc[index].values
                    if (type(row[4]) == str):
                        published_date=datetime.fromisoformat(row[4])
                        published_date=f"{published_date.year}-{published_date.month}-{published_date.day}"
                    else:
                        published_date = None
                    duration=row[9]
                    if (type(duration)== str):
                        if (duration.find('M') != -1) & (duration.find('S') != -1):
                            minu=int(duration[duration.index('T')+1 : duration.index('M')])
                            sec=int(duration[duration.index('M')+1 : duration.index('S')])
                            up_tim = f"{minu}:{sec}"
                        elif duration.find('M') == -1:
                            sec=int(duration[duration.index('T')+1 : duration.index('S')])
                            up_tim = f"{00}:{sec}"
                        elif duration.find('S') == -1:
                            minu=int(duration[duration.index('T')+1 : duration.index('M')])
                            up_tim = f"{minu}:{00}"
                    else:
                        up_tim = None
                    row=(str(row[0]),str(row[1]),str(row[2]),str(row[3]),published_date,int(row[5]),int(row[6]),int(row[7]),int(row[8]),up_tim,str(row[10]),bool(row[11]))
                    data.append(row)
                cursor.executemany(query,data)
                con.commit()
                
            elif name == 'comment_details_df':        
                query = "insert into CommentData values(%s,%s,%s,%s,%s)"
                data=[]
                for index in df.index:
                    row=df.loc[index].values
                    if (type(row[4]) == str):
                        published_date=datetime.fromisoformat(row[4])
                        published_date=f"{published_date.year}-{published_date.month}-{published_date.day}"
                    else:
                        published_date = None
                    row=(str(row[0]),str(row[1]),str(row[2]),str(row[3]),published_date)
                    data.append(row)
                cursor.executemany(query,data)
                con.commit()
        con.close()
        b = "Data Inserted"
        return b
    
#Functions to fetch the data from db: 
def query1(cursor):
    query1 = "select Video_Name,channel_name from VideoData left join Channel on VideoData.channel_id=channel.channel_id"
    cursor.execute(query1)
    lis=[]
    for data in cursor:
        lis.append(data)
    return lis
 
def query2(cursor):
    query2 = "select channel_name,video_count from Channel where video_count =(select max(video_count) from Channel)"
    cursor.execute(query2)
    lis=[]
    for data in cursor:
        lis.append(data)
    return lis 

def query3(cursor):
    query3 = "select channel_name,Video_Name from VideoData left join Channel on VideoData.channel_id=channel.channel_id order by Video_view_count DESC LIMIT 10"
    cursor.execute(query3)
    lis=[]
    for data in cursor:
        lis.append(data)
    return lis 

def query4(cursor):
    query4 = "select Video_Name,Video_comment_count from VideoData"
    cursor.execute(query4)
    lis=[]
    for data in cursor:
        lis.append(data)
    return lis 

def query5(cursor):
    query5 = "select Video_Name,channel_name from VideoData inner join Channel on VideoData.channel_id=channel.channel_id where Video_like_count =(select max(Video_like_count) from VideoData)"
    cursor.execute(query5)
    lis=[]
    for data in cursor:
        lis.append(data)
    return lis 

def query6(cursor):
    query6 = "select  Video_Name,Video_like_count from VideoData "
    cursor.execute(query6)
    lis=[]
    for data in cursor:
        lis.append(data)
    return lis 

def query7(cursor):
    query7 = "select channel_name,channel_views from Channel"
    cursor.execute(query7)
    lis=[]
    for data in cursor:
        lis.append(data)
    return lis 

def query8(cursor):
    query8 = "select DISTINCT channel_name from Channel cross join VideoData where Published_data like '%2022%'"
    cursor.execute(query8)
    lis=[]
    for data in cursor:
        lis.append(data)
    return lis 
        
def query9(cursor):
    query9 = "SELECT channel_name, AVG(Video_duration) FROM channel JOIN VideoData ON channel.channel_id = VideoData.channel_id GROUP BY channel_name"
    cursor.execute(query9)
    lis=[]
    for data in cursor:
        lis.append(data)
    return lis 

def query10(cursor):
    query10 = "SELECT Video_Name, channel_name FROM channel JOIN VideoData ON channel.channel_id = VideoData.channel_id order by video_comment_count desc;"
    cursor.execute(query10)
    lis=[]
    for data in cursor:
        lis.append(data)
    return lis
def clear_text():
    st.session_state["user_input"]=""
st.set_page_config(
    page_title="Home Page"
    )
with st.sidebar:
    st.info("Project 1 : **Youtube Data Harvesting**  \n Batch : **MA28**  \n Student Name : **Aravindh D**",icon="🧑‍💻")
    st.success("*Select the* ***Search Tab*** in the main screen to serach and get the channel details*",icon="🔍")
    st.success("*Select the* ***Store Data Tab*** in the main screen to store the fetched channel details*",icon="📁")
    st.success("*Select the* ***Queries Tab*** in the main screen to fetch the channel details for selected queries*",icon="💡")

st.title("Welcome!")

st.markdown("*This is an application in which you can fetch channel data by entering its channel ID, store the fetched data and get data for selected queries*")

tab1, tab2, tab3 = st.tabs(["Search", "Store Data", "Queries"])
with tab1:
    #st.header("Get Youtube channel info")
  with st.container(border=True):
    if "user_input" not in st.session_state: 
        st.session_state["user_input"] =""
    user_input = st.text_input("Channel ID:",st.session_state["user_input"],placeholder='Enter youtube channel id')
    col1, col2 = st.columns(2)
    with col1:
      button1 = st.button("Search")  
    with col2:
      button2 = st.button("Clear",on_click=clear_text())

if button1:
  if user_input:
     make_conn = make_connection()
     var_create_db = create_db(make_conn)
     data_exist = initial_check(var_create_db,user_input)
     if data_exist:
         st.info(f"Data exists for entered channel_id {user_input}.\n Please enter the different channel_id and search !",icon="✨")
     else:
        api_key = 'AIzaSyAn5QOoKEJYL_MI1MRdhsHV1Ajd_0bfmuk'
        youtube = build('youtube', 'v3', developerKey=api_key)
        # to get channel info
        channel_info = get_channel_info(user_input,youtube)
        if channel_info:
            index=[0]
            channel_info_df = pd.DataFrame(channel_info,index=index)
            # assign current playlistid
            playlist_id = channel_info['Playlist_ids']

            # to fetch youtube data    
            playlist_data = get_playlist_data(youtube,user_input)   
            video_ids = get_video_ids(youtube,playlist_id)
            video_data = get_videos_data (youtube,video_ids)
            comment_details = get_comments_details(youtube,user_input)

            #Get channel information as data frame
            playlist_data_df = pd.DataFrame(playlist_data)
            video_data_df = pd.DataFrame(video_data)
            comment_details_df = pd.DataFrame(comment_details)

            #adding all dataframe in single dictionary
            all_df_dict = {
                'channel_info_df':channel_info_df,
                'playlist_data_df':playlist_data_df,
                'video_data_df':video_data_df,
                'comment_details_df':comment_details_df
            }
            st.session_state.all_df = all_df_dict
            st.write ("You have entered Youtube Channel ID as : ",user_input)
            if channel_info or playlist_data or video_data or comment_details:
                st.success("Data Fetched successfully",icon="💫")
            else:
                st.error(f"Failed to fetch data / Entered channel id **{user_input}** was not found !",icon="🪲")
        else:
            st.error(f"Entered channel id **{user_input}** is invalid or No Data found",icon="🪦")
  
  else:
    st.warning("Please Enter channnel ID and search!",icon="✍️")

if button2:
    user_input = ""
        
with tab2:
    with st.container(border=True):
      st.write("Click on Store button to save the fetched data in db")
      store = st.button("Store")
      if store:
        if st.session_state.all_df:
            try:
                make_conn = make_connection()
                var_create_db = create_db(make_conn)
                var_insert_data = insert_data(make_conn,var_create_db,st.session_state.all_df)
                if var_insert_data == 'Data Inserted':
                    st.success("Data was saved successfully",icon="👍")
                else:
                    st.error("Data was not saved",icon="🐞")
            except mysql.connector.Error as err:
                st.error(f"Error: {err}",icon="🐞")
                make_conn.rollback()
            finally:
                make_conn.close()
        else:
            st.warning("Please search and get the channel details in the **Search Tab** and try to store data",icon="🛒")
        st.session_state.all_df=""

with tab3:
    make_conn = make_connection()
    var_create_db = create_db(make_conn)
    with st.container(border=True):
      st.text("Question 1: What are the names of all the videos and their corresponding channels ")
      button4 = st.button("Get Answer", key='1',use_container_width=True)
      if button4:
          get_ans = query1(var_create_db)
          dfa1 = pd.DataFrame(get_ans,columns=['Videos_Name','Channel'])
          st.dataframe(dfa1)   
    with st.container(border=True):
      st.text("Question 2: Which channels have the most number of videos, and how many videos do they have ")
      button5 = st.button("Get Answer", key='2',use_container_width=True)
      if button5:
          get_ans = query2(var_create_db)
          dfa2 = pd.DataFrame(get_ans,columns=['Channel','Video Count'])
          st.dataframe(dfa2)
    with st.container(border=True):
      st.text("Question 3: What are the top 10 most viewed videos and their respective channels ")
      button6 = st.button("Get Answer", key='3',use_container_width=True)
      if button6:
          get_ans = query3(var_create_db)
          dfa3 = pd.DataFrame(get_ans,columns=['Channel Name','Most viewed videos'])
          st.dataframe(dfa3)
    with st.container(border=True):
      st.text("Question 4: How many comments were made on each video, and what are their corresponding video names ")
      button7 = st.button("Get Answer", key='4',use_container_width=True)
      if button7:
          get_ans = query4(var_create_db)
          dfa4 = pd.DataFrame(get_ans,columns=['Videos Name','Comment count'])
          st.dataframe(dfa4)
    with st.container(border=True):
      st.text("Question 5: Which videos have the highest number of likes, and what are their corresponding channel names")
      button8 = st.button("Get Answer", key='5',use_container_width=True)
      if button8:
          get_ans = query5(var_create_db)
          dfa5 = pd.DataFrame(get_ans,columns=['Video Name','Channel Name'])
          st.dataframe(dfa5)
    with st.container(border=True):
      st.text("Question 6: What is the total number of likes and dislikes for each video, and what are their corresponding video names")
      button9 = st.button("Get Answer", key='6',use_container_width=True)
      if button9:
          get_ans = query6(var_create_db)
          dfa6 = pd.DataFrame(get_ans,columns=['Video Name','Likes'])
          st.dataframe(dfa6)
    with st.container(border=True):
      st.text("Question 7: What is the total number of views for each channel, and what are their corresponding channel names")
      button10 = st.button("Get Answer", key='7',use_container_width=True)
      if button10:
          get_ans = query7(var_create_db)
          dfa7 = pd.DataFrame(get_ans,columns=['Channel Name','Total Views'])
          st.dataframe(dfa7)
    with st.container(border=True):
      st.text("Question 8: What are the names of all the channels that have published videos in the year 2022")
      button11 = st.button("Get Answer", key='8',use_container_width=True)
      if button11:
          get_ans = query8(var_create_db)
          dfa8 = pd.DataFrame(get_ans,columns=['Channel Name'])
          st.dataframe(dfa8)
    with st.container(border=True):
      st.text("Question 9: What is the average duration of all videos in each channel, and what are their corresponding channel names")
      button12 = st.button("Get Answer", key='9',use_container_width=True)
      if button12:
          get_ans = query9(var_create_db)
          dfa9 = pd.DataFrame(get_ans,columns=['Channel Name','Average Duration'])
          st.dataframe(dfa9)
    with st.container(border=True):
      st.text("Question 10: Which videos have the highest number of comments, and what are their corresponding channel names")
      button13 = st.button("Get Answer", key='10',use_container_width=True)
      if button13:
          get_ans = query10(var_create_db)
          dfa10 = pd.DataFrame(get_ans,columns=['Videos Name','Channel Name'])
          st.dataframe(dfa10)

