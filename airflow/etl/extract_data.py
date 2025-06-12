import os
import json
from googleapiclient.discovery import build
from dotenv import load_dotenv

# Load env vars once
load_dotenv()

API_KEY = os.getenv("API_KEY")
CHANNEL_ID = 'UCG5qGWdu8nIRZqJ_GgDwQ-w'


def get_youtube_client():
    if not API_KEY:
        raise ValueError("API Key not accessed.")
    return build('youtube', 'v3', developerKey=API_KEY)


# get playlist id from channel
def get_playlist_id(youtube, channel_id):
    try:
        response = youtube.channels().list(
            id=channel_id,
            part='contentDetails'
        ).execute()
        if not response['items']:
            print("Channel not found")

        return response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    except Exception as e:
        print(f"Getting Playlist ID error: {e}")


# get a list of video IDs
def get_video_ids(youtube, playlist_id):
    video_ids = []
    next_pg_token = None
    try:
        while True:
            response = youtube.playlistItems().list(
                part='contentDetails',
                playlistId=playlist_id,
                maxResults=50,
                pageToken=next_pg_token
            ).execute()

            for item in response['items']:
                video_ids.append(item['contentDetails']['videoId'])
            next_pg_token = response.get('nextPageToken')

            if not next_pg_token:
                break
        return video_ids
    except Exception as e:
        print(f"Getting videoId error: {e}")


# extract data from each video id
def get_video_data(youtube, video_ids):
    video_data = []
    try:
        for i in range(0, len(video_ids), 50):
            batch_ids = video_ids[i:i + 50]
            response = youtube.videos().list(
                part='snippet,statistics',
                id=','.join(batch_ids)
            ).execute()

            for item in response['items']:
                video_data.append({
                    'videoId': item['id'],
                    'title': item['snippet']['title'],
                    'publishedAt': item['snippet']['publishedAt'],
                    'viewCount': item['statistics'].get('viewCount', 0),
                    'likeCount': item['statistics'].get('likeCount', 0),
                    'commentCount': item['statistics'].get('commentCount', 0)
                })
        return video_data
    except Exception as e:
        print(f"Extracting video data error: {e}")


# push data into a JSON file
def write_to_file(video_data):
    try:
        filepath = "/home/deecodes/pl-youtube-analytics/docs/youtube_data.json"
        if os.path.exists(filepath):
            with open(filepath, 'r') as f:
                data = json.load(f)
                if not isinstance(data, list):
                    print("Warning: Existing data is not a list. Resetting.")
                    data = []
        else:
            data = []

        data.extend(video_data)

        with open(filepath, 'w') as f:
            json.dump(data, f, indent=4)
        print("Data loaded into file successfully!")
    except Exception as e:
        print(f"Loaded data into JSON file error: {e}")


# Airflow PythonOperator entrypoint
def extract_task():
    print("Data Extraction from YouTube API is starting...")

    youtube = get_youtube_client()
    print("YouTube client initialized.")

    playlist_id = get_playlist_id(youtube, CHANNEL_ID)
    print("Playlist ID extracted. Extracting video IDs...")

    video_ids = get_video_ids(youtube, playlist_id)
    print("Video IDs extracted. Extracting video data now...")

    video_data = get_video_data(youtube, video_ids)
    print("Video data extracted. Writing to file now...")

    write_to_file(video_data)
