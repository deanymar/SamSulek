import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'
import openpyxl
import os
from googleapiclient.discovery import build
from IPython.display import JSON
from datetime import date
from datetime import datetime
import isodate
import pypyodbc as odbc
import re
api_key = ''
all_data = []
api_service_name = "youtube"
api_version = "v3"
    # Get credentials and create an API client
youtube = build(
            api_service_name, api_version, developerKey=api_key)
def requestData(youtube,channel_id):
    all_data = []
    request = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=channel_id
            )
    response = request.execute()
    JSON(response)
    for item in response['items']:
        data = {'channelName': item['snippet']['title'],
            'totalViews' : item['statistics']['viewCount'],
            'totalSub' : item['statistics']['subscriberCount'],
            'numofVideos' : item['statistics']['videoCount'],
            'playListId' : item['contentDetails']['relatedPlaylists']['uploads']
           }
        break;
    all_data.append(data)
    return (pd.DataFrame(all_data))
def get_video_ids(youtube, playlist_id):
    video_ids = []
    request = youtube.playlistItems().list(
        part="snippet,contentDetails",
        playlistId=playlist_id,
        maxResults = 50
    )
    response = request.execute()
    for item in response['items']:
        video_ids.append(item['contentDetails']['videoId'])

    next_page_token = response.get('nextPageToken')
    while next_page_token is not None:
        request = youtube.playlistItems().list(
                    part='contentDetails',
                    playlistId = playlist_id,
                    maxResults = 50,
                    pageToken = next_page_token)
        response = request.execute()

        for item in response['items']:
            video_ids.append(item['contentDetails']['videoId'])

        next_page_token = response.get('nextPageToken')

    return video_ids
def get_video_details(youtube,video_ids):
    all_video_info = []
    for i in range (0,len(video_ids),50):
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=','.join(video_ids[i:i+50])
        )
        response = request.execute()
        for video in response['items']:
            video_info_keep = {'snippet':['channelTitle','title','publishedAt'],
                          'statistics' : ['viewCount','likeCount','commentCount'],
                          'contentDetails' : ['duration']}

            video_info = {}
            video_info['video_id'] = video['id']
            for key in video_info_keep.keys():
                for value in video_info_keep[key]:
                    try:
                        video_info[value] = video[key][value]
                    except:
                        video_info[value] = None
            all_video_info.append(video_info)
    return pd.DataFrame(all_video_info)
def clean_data(df):
    df.dropna(inplace=True)
    ##changes dates to days of week and from datetime to regular date
    df['publishedAt'] = pd.to_datetime(df['publishedAt'])
    df['publishedDay'] =df['publishedAt'].dt.day_name()
    # df['publishedAt'] = pd.to_datetime(df['publishedAt']).dt.date
    df['publishedAt'] = df['publishedAt'].dt.strftime('%Y-%m-%d')
    df = df[['channelID','video_id', 'channelTitle', 'title', 'publishedAt', 'publishedDay', 'viewCount','likeCount', 'commentCount', 'duration']]
    ##Changes the duration of the video into minutes
    df['durationMinutes'] = df['duration'].apply(lambda x: isodate.parse_duration(x))
    df['durationMinutes'] = df['durationMinutes'].astype('timedelta64[s]')
    df['durationMinutes'] = round(df['durationMinutes']/60,1)
    ##changes final columns to numeric
    numeric_cols = ['viewCount','likeCount','commentCount','durationMinutes']
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce', axis=0)

    return df
def clean_titles(title):
    # Convert to lowercase
    title = title.str.lower()
    return title
def updateDatabase(connection_string, sql_check_table,sql_create_table, sql_truncate, sql_insert, records):
    try:
        conn = odbc.connect(connection_string)
        cursor = conn.cursor()

        # Check if the table exists
        cursor.execute(sql_check_table)
        table_exists = cursor.fetchone()[0]
        
        if not table_exists:
            # Create the table if it does not exist
            cursor.execute(sql_create_table)
            print("Table Created.")
        else:
            print("Table already exists.")

        # Truncate the table
        cursor.execute(sql_truncate)
        
        # Insert new records
        cursor.executemany(sql_insert, records)

        # Commit the transaction
        conn.commit()
        print('Table Updated')
        
    except Exception as e:
        print("Error:", e)
        conn.rollback()
    finally:
        try:
            cursor.close()
            conn.close()
        except Exception as e:
            print("Error during cleanup:", e)
## SamSulek Data
channel_id = "UCAuk798iHprjTtwlClkFxMA"
SamSulek_channel_stats = requestData(youtube,channel_id)
playlist_id = requestData(youtube,channel_id)['playListId'].values[0]
SamSulek_video_ids = get_video_ids(youtube,playlist_id)
SamSulek_video_df = get_video_details(youtube,SamSulek_video_ids)
SamSulek_video_df['channelID'] = channel_id
SamSulek_video_df_clean = clean_data(SamSulek_video_df)
SamSulek_video_df_clean['title'] = clean_titles(SamSulek_video_df_clean['title'] )
records_video = SamSulek_video_df_clean.values.tolist()
records_channelData = SamSulek_channel_stats.values.tolist()
## Uploading Data to SQL Server
server = 'LAPTOP-IBU46V5E\\DEAN;'
database = 'SamSulekData;'
driver = 'ODBC Driver 17 for SQL Server'
connection_string = (
    f"DRIVER={{{driver}}};"
    f"SERVER={server};"
    f"DATABASE={database};"
    f"Trusted_Connection=yes;"
)
# Define SQL queries for ChannelData
sql_check_table_channel = '''
    IF OBJECT_ID('dbo.ChannelData', 'U') IS NOT NULL
        SELECT 1 AS table_exists
    ELSE
        SELECT 0 AS table_exists
'''

sql_create_table_channel = '''
    CREATE TABLE ChannelData (
        channelName NVARCHAR(200), 
        totalViews INT, 
        totalSub INT, 
        numofVideos INT, 
        channelId NVARCHAR(200)
    )
'''


sql_truncate_channel = '''
    TRUNCATE TABLE ChannelData ;
'''

sql_insert_channel = '''
    INSERT INTO ChannelData
    VALUES (?, ?, ?, ?, ?)
'''
# Update ChannelData table
updateDatabase(connection_string,sql_check_table_channel ,sql_create_table_channel, sql_truncate_channel, sql_insert_channel, records_channelData)


# Define SQL queries for VideoData
sql_check_table_video = '''
    IF OBJECT_ID('dbo.VideoData', 'U') IS NOT NULL
        SELECT 1 AS table_exists
    ELSE
        SELECT 0 AS table_exists
'''

sql_create_table_video = '''
    CREATE TABLE VideoData (
        channelID VARCHAR(255),
        video_id VARCHAR(255),
        channelTitle VARCHAR(255),
        title VARCHAR(255),
        publishedAt VARCHAR(255),
        publishedDay VARCHAR(255),
        viewCount INT,
        likeCount INT,
        commentCount INT,
        duration VARCHAR(255),
        durationMinutes FLOAT
    )
'''

sql_truncate_video = '''
    TRUNCATE TABLE VideoData;
'''

sql_insert_video = '''
    INSERT INTO VideoData
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
'''

# Update VideoData table
updateDatabase(connection_string, sql_check_table_video,sql_create_table_video, sql_truncate_video, sql_insert_video, records_video)
