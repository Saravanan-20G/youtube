from googleapiclient.discovery import build
import googleapiclient.discovery
import psycopg2
import pymongo
import pandas as pd
import streamlit as st

#Api Key Connect
def api():
  api_key = "AIzaSyA80-8IbFkfHmRpjwIRA42IrxOtClMZuw0"
  youtube = googleapiclient.discovery.build("youtube", "v3", developerKey=api_key)

  return youtube

youtube=api()

#Database Connect
import certifi
ca = certifi.where()
client = pymongo.MongoClient(
"mongodb+srv://saravanan:San123456@cluster0.z7u66ej.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0", tlsCAFile=ca)
db = client['Youtube_data']
collection = db['channel_details']

#Sql connect 
mydb = psycopg2.connect(host='localhost', user='postgres', password='123456', database='youtube_data', port=5432)
cursor = mydb.cursor()

#Channel Information

def channel_info(channel_id):
    res = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    ).execute()
    channel_det = {
        'Channel_Name' : res['items'][0]['snippet'].get('title'),
        'Channel_Id' : res['items'][0].get('id'),
        'Subscribers' : res['items'][0]['statistics'].get('subscriberCount'),
        'Views' : res['items'][0]['statistics'].get('viewCount'),
        'Total_videos':res['items'][0]['statistics']['videoCount'],
        'Channel_Description' : res['items'][0]['snippet'].get('description'),
        'Playlist_Id' : res['items'][0]['contentDetails']['relatedPlaylists'].get('uploads')
    }
    return channel_det

#Playlist Information
def playlists(channel_id):
    playlists = {}
    next_page_token = None
    while True:
        res = youtube.playlists().list(
            part ='snippet,contentDetails',
            channelId=channel_id,
            maxResults = 50,
            pageToken = next_page_token
        ).execute()
        for i in (res['items']):
            per_playlist_info = {
                'Channel_Id':i['snippet'].get('channelId'),
                'Playlist_Id':i.get('id'),
                'Playlist_Name':i['snippet'].get('title'),
                'Video_Count':i['contentDetails'].get('itemCount')
            }
            playlists[per_playlist_info['Playlist_Id']] = per_playlist_info
        next_page_token = res.get('nextPageToken')
        if next_page_token is None:
            break
    return playlists


#Video ID's Getting
def fetch_video_ids(playlist_id):
    video_ids_list = []
    next_page_token = None
    while True:
        res = youtube.playlistItems().list(
            part ='snippet',
            playlistId = playlist_id,
            maxResults = 50,
            pageToken = next_page_token
        ).execute()
        for i in range(len(res['items'])):
            video_ids_list.append(res['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = res.get('nextPageToken')
        if next_page_token is None:
            break
    return video_ids_list

     


#Video Information
def video_info(video_ids):
    video_info = {}
    for video in video_ids:
        try:
            res = youtube.videos().list(
                part='snippet,contentDetails,statistics',
                id=video
            ).execute()
            if 'items' in res and res['items']:
                per_video_info = {
                    'Channel_Name': res['items'][0]['snippet'].get('channelTitle'),
                    'Channel_Id': res['items'][0]['snippet'].get('channelId'),
                    'Video_Id': res['items'][0].get('id'),
                    'Title': res['items'][0]['snippet'].get('title'),
                    'Tags':res['items'][0]['snippet'].get('tags'),
                    'Thumbnail': res['items'][0]['snippet']['thumbnails']['default'].get('url'),
                    'Description': res['items'][0]['snippet'].get('description'),
                    'Published_date': res['items'][0]['snippet'].get('publishedAt').replace("T", " ").replace("Z", ""),
                    'Duration': res['items'][0]['contentDetails'].get('duration'),
                    'View_Count': res['items'][0]['statistics'].get('viewCount'),
                    'Like_Count': res['items'][0]['statistics'].get('likeCount'),
                    'Comment_Count': res['items'][0]['statistics'].get('commentCount'),
                    'Favorite_Count': res['items'][0]['statistics'].get('favoriteCount'),
                    'Definition':res['items'][0]['contentDetails']['definition'],
                    'Caption_Status': res['items'][0]['contentDetails'].get('caption')}
                   
                video_info[per_video_info['Video_Id']] = per_video_info
            else:
                print(f"No data found for video ID: {video}")
        except Exception as e:
            print(f"Error occurred for video ID: {video}, Error: {e}")
    return video_info

#Comments Information
from googleapiclient.errors import HttpError

def video_comments(video_id):
    comment_details = []

    try:
        response = youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            maxResults=50
        ).execute()

        for comment in response['items']:
            try:
                per_comment_info = {
                    'Video_Id': comment['snippet'].get('videoId'),
                    'Comment_Id': comment['snippet']['topLevelComment'].get('id'),
                    'Comment_Text': comment['snippet']['topLevelComment']['snippet'].get('textOriginal'),
                    'Comment_Author': comment['snippet']['topLevelComment']['snippet'].get('authorDisplayName'),
                    'Comment_PublishedAt': comment['snippet']['topLevelComment']['snippet'].get('publishedAt').replace("T", " ").replace("Z", "")
                }
                comment_details.append(per_comment_info)
            except KeyError as ke:
                print(f"KeyError: {ke}. Skipping comment.")
            except Exception as ex:
                print(f"Error processing comment: {ex}. Skipping comment.")
    except HttpError as e:
        if e.resp.status == 403:
            print(f"Comments are disabled for video: {video_id}")
        else:
            print(f"Error fetching comments for video {video_id}: {e}")
    except Exception as ex:
        print(f"Error fetching comments for video {video_id}: {ex}")

    return comment_details

#Complete Information
def complete_channel_info(channel_id):
    channel_info_ = channel_info(channel_id)
    playlists_ = playlists(channel_id)
    playlist_id = channel_info_["Playlist_Id"]
    video_ids_ = fetch_video_ids(playlist_id)
    video_info_ = video_info(video_ids_)
    
    comment_info_ = {}
    for video_id in video_ids_:
        comment_info_[video_id] = video_comments(video_id)
   
    collection = db["channel_details"]
    
    collection.insert_one({
        'Channel_Info': channel_info_,
        'Playlist_Info': playlists_,
        'Playlist_Id': playlist_id,
        'Video_Ids': video_ids_,
        'Video_Information': video_info_,
        'Video_Comments': comment_info_
    })
    
   
    return complete_channel_info



#Channels Table
def channels_tables():
    
    create_query='''create table if not exists channels(Channel_Name varchar(100),
                                                            channel_Id varchar(80) primary key,
                                                            Subscribers bigint,
                                                            Views bigint,
                                                            Total_Videos int,
                                                            Channel_Description text,
                                                            Playlist_Id varchar(80))'''
    cursor.execute(create_query)
    mydb.commit()

ch_list = []
db = client["Youtube_data"]
collection = db["channel_details"]

ch_data = collection.find({}, {"_id": 0, "Channel_Info": 1})
for data in ch_data:
    if "Channel_Info" in data:
        ch_list.append(data["Channel_Info"])

df = pd.DataFrame(ch_list)

for index,row in df.iterrows():
    insert_query='''insert into channels(Channel_Name, 
                                    channel_Id,
                                    Subscribers,
                                    Views,
                                    Total_Videos,
                                    Channel_Description,
                                    Playlist_Id)
                                    
                                    values(%s,%s,%s,%s,%s,%s,%s)
                                    ON CONFLICT (channel_Id) DO NOTHING'''
    values=(row["Channel_Name"],
            row["Channel_Id"],
            row["Subscribers"],
            row["Views"],
            row["Total_videos"],
            row["Channel_Description"],
            row["Playlist_Id"])

    cursor.execute(insert_query,values)
    mydb.commit()


#Video Table
def videos_table():
    create_query = '''
        CREATE TABLE IF NOT EXISTS videos (
            Channel_Name VARCHAR(100),
            Channel_Id VARCHAR(100),
            Video_Id VARCHAR(30) PRIMARY KEY,
            Title VARCHAR(150),
            Tags TEXT,
            Thumbnail VARCHAR(200),
            Description TEXT,
            Published_date TIMESTAMP,
            Duration INTERVAL,
            Views BIGINT,
            Likes BIGINT,
            Comments INT,
            Favorite_Count INT,
            Definition VARCHAR(10),
            Caption_status VARCHAR(50)
        )
    '''
    cursor.execute(create_query)
    mydb.commit()


vi_list = []
db = client["Youtube_data"]
collection = db["channel_details"]

vi_data = collection.find({}, {"_id": 0, "Video_Information": 1})

if vi_data and "Video_Information" in vi_data:
    ids = list(vi_data['Video_Information'])
    
    for i in ids:
        vi_list.append(vi_data['Video_Information'][i])

df2 = pd.DataFrame(vi_list)


for index,row in df2.iterrows():
    insert_query = '''insert into videos(Channel_Name,
                                          Channel_Id,
                                          Video_Id,
                                          Title,
                                          Tags,
                                          Thumbnail,
                                          Description,
                                          Published_date,
                                          Duration,
                                          Views,
                                          Likes,
                                          Comments,
                                          Favorite_Count,
                                          Definition,
                                          Caption_status)
                                      values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                                      ON CONFLICT (Video_Id) DO NOTHING'''
    values = (row["Channel_Name"],
              row["Channel_Id"],
              row["Video_Id"],
              row["Title"],
              row["Tags"],
              row["Thumbnail"],
              row["Description"],
              row["Published_date"],
              row["Duration"],
              row["View_Count"],
              row["Like_Count"],
              row["Comment_Count"],
              row["Favorite_Count"],
              row["Definition"],
              row["Caption_Status"])

    
    cursor.execute(insert_query, values)
    mydb.commit()

#Comments Table 
def comments_table():
        create_query='''create table if not exists comments(Video_Id varchar(50),
                                                        Comment_Id varchar(100) primary key,
                                                        Comment_Text text,
                                                        Comment_Author varchar(150),
                                                        Comment_Published timestamp
                                                        )'''

        cursor.execute(create_query)
        mydb.commit()

com_list = []
db = client["Youtube_data"]
collection = db["channel_details"]

com_data = collection.find({}, {"_id": 0, "Video_Comments": 1})

if com_data and "Video_Comments" in com_data:
        for video_id, comments in com_data['Video_Comments'].items():
                for comment in comments:
                        com_list.append({
                        "Video_Id": video_id,
                        "Comment_Id": comment.get("Comment_Id", ""),
                        "Comment_Text": comment.get("Comment_Text", ""),
                        "Comment_Author": comment.get("Comment_Author", ""),
                        "Comment_PublishedAt": comment.get("Comment_PublishedAt", "")
                        })
df3 = pd.DataFrame(com_list)

for index,row in df3.iterrows():
        insert_query='''insert into comments(Video_Id,
                                                Comment_Id,
                                                Comment_Text,
                                                Comment_Author,
                                                Comment_Published)
                                                
                                                values(%s,%s,%s,%s,%s)
                                                ON CONFLICT (Comment_Id) DO NOTHING'''
        
        values=(row["Video_Id"],
                row["Comment_Id"],
                row["Comment_Text"],
                row["Comment_Author"],
                row["Comment_PublishedAt"])
        cursor.execute(insert_query,values)
        mydb.commit()
#Tables
def tables():
    channels_tables()
    videos_table()
    comments_table()

    return "Tables created...."


#streamlit part


st.header('YouTube Data Harvesting and Warehousing',divider='rainbow')

option = st.sidebar.selectbox("Select",('','Data Store', 'Migrate', 'Questions'))

channel_id=st.sidebar.text_input("Enter the channel ID")

st.write('You selected:', option)

if option=="Data Store":
    ch_list = []
    db = client["Youtube_data"]
    collection = db["channel_details"]

    ch_data = collection.find({}, {"_id": 0, "Channel_Info": 1})
    for data in ch_data:
        if "Channel_Info" in data:
            ch_list.append(data["Channel_Info"])

    df = pd.DataFrame(ch_list)
    st.write(df)

    if channel_id in ch_list:
        st.success("Channel Details of the given channel id already exists")
        
    else:
        insert=complete_channel_info(channel_id) 
        st.success(insert)
        
elif option=="Migtate":
    
    Table=tables()
    st.success(Table)

if option=="Questions":
    question=st.sidebar.selectbox("Select your question",("1. What are the names of all the videos and their corresponding channels?",
                                              "2. Which channels have the most number of videos, and how many videos do they have?",
                                              "3. What are the top 10 most viewed videos and their respective channels?",
                                              "4. How many comments were made on each video, and what are their corresponding video names?",
                                              "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
                                              "6. What is the total number of likes for each video, and what are their corresponding video names?",
                                              "7. What is the total number of views for each channel, and what are their corresponding channel names?",
                                              "8. What are the names of all the channels that have published videos in the year 2022?",
                                              "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                              "10. Which videos have the highest number of comments, and what are their corresponding channel names?"))

    if question=="1. What are the names of all the videos and their corresponding channels?":
        query1='''select title as videos,channel_name as channelname from videos'''
        cursor.execute(query1)
        mydb.commit()
        t1=cursor.fetchall()
        df=pd.DataFrame(t1,columns=["Title","Channel_Name"])
        st.write(df)

    elif question=="2. Which channels have the most number of videos, and how many videos do they have?":
        query2='''select channel_name as channelname,total_videos as no_videos from channels
                order by total_videos desc'''
        cursor.execute(query2)
        mydb.commit()
        t2=cursor.fetchall()
        df2=pd.DataFrame(t2,columns=["Channel_Name","Total_videos"])
        st.write(df2)

    elif question=="3. What are the top 10 most viewed videos and their respective channels?":
        query3='''select views as views,channel_name as channelname,title as videotitle from videos
                    where views is not null order by views desc limit 10'''
        cursor.execute(query3)
        mydb.commit()
        t3=cursor.fetchall()
        df3=pd.DataFrame(t3,columns=["View_Count","Channel_Name","Title"])
        st.write(df3)


    elif question=="4. How many comments were made on each video, and what are their corresponding video names?":
        query4='''select comments as no_comments,title as videotitle from videos
                    where comments is not null'''
        cursor.execute(query4)
        mydb.commit()
        t4=cursor.fetchall()
        df4=pd.DataFrame(t4,columns=["Comment_Count","Title"])
        st.write(df4)

    elif question=="5. Which videos have the highest number of likes, and what are their corresponding channel names?":
        query5='''select title as videotitle,channel_name as channelname,likes as likecount
                    from videos where likes is not null order by likes desc'''
        cursor.execute(query5)
        mydb.commit()
        t5=cursor.fetchall()
        df5=pd.DataFrame(t5,columns=["Title","Channel_Name","Like_Count"])
        st.write(df5)

    elif question=="6. What is the total number of likes for each video, and what are their corresponding video names?":
        query6='''select likes as likecount,title as videotitle from videos'''
        cursor.execute(query6)
        mydb.commit()
        t6=cursor.fetchall()
        df6=pd.DataFrame(t6,columns=["Like_Count","Title"])
        st.write(df6)

    elif question=="7. What is the total number of views for each channel, and what are their corresponding channel names?":
        query7='''select channel_name as channeltitle,views as totalview from channels'''
        cursor.execute(query7)
        mydb.commit()
        t7=cursor.fetchall()
        df7=pd.DataFrame(t7,columns=["Channel_Name","Views"])
        st.write(df7)

    elif question=="8. What are the names of all the channels that have published videos in the year 2022?":
        query8='''select title as video_title,published_date as videorelease,channel_name as channelname from videos
                    where extract(year from published_date)=2022'''
        cursor.execute(query8)
        mydb.commit()
        t8=cursor.fetchall()
        df8=pd.DataFrame(t8,columns=["Title","Published_date","Channel_Name"])
        st.write(df8)

    elif question=="9. What is the average duration of all videos in each channel, and what are their corresponding channel names?":
        query9='''select channel_name as channelname,AVG(duration) as averageduration from videos group by channel_name'''
        cursor.execute(query9)
        mydb.commit()
        t9=cursor.fetchall()
        df9=pd.DataFrame(t9,columns=["Channel_Name","Duration"])
        st.write(df9)


    elif question=="10. Which videos have the highest number of comments, and what are their corresponding channel names?":
        query10='''select title as videotitle, channel_name as channelname,comments as comments from videos
                    where comments is not null order by comments desc'''
        cursor.execute(query10)
        mydb.commit()
        t10=cursor.fetchall()
        df10=pd.DataFrame(t10,columns=["Title","Channel_Name","Comment_Count"])
        st.write(df10)


