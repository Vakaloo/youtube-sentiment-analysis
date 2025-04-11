from dotenv import load_dotenv
import os
import pandas as pd
import time 
import googleapiclient.discovery as build
from googleapiclient.errors import HttpError


def load_youtube_key():
    """Load the YouTube API key from an environment variable."""

    load_dotenv()
    YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
    if YOUTUBE_API_KEY: 
        print("API key loaded successfully!")
    else:
        print("Error: API Key not found. Check your .env file.")

    api_service_name = "youtube"
    api_version = "v3"

    # Initialize the YouTube API client
    youtube = build.build(api_service_name, 
                          api_version, 
                          developerKey=YOUTUBE_API_KEY)

    return youtube

def load_video_response(video_id, youtube):
    """Load the video response using the YouTube API."""
    try:
        video_response = youtube.videos().list(
            part = "id,snippet,contentDetails,statistics",
            id = video_id, # Replace with the actual video ID
        ).execute()   

        if not video_response:
            print("No response from the API.")
    
        return video_response
    
    except HttpError as e: 
        print(f"An error occurred: {e}")
        return None
    
def info_video(video_response):
    """Extract video information from the response."""
    video_data = video_response['items'][0]

    video_snippet = video_data['snippet']
    video_statistics = video_data['statistics']
    video_content_details = video_data['contentDetails']

    # Create a dictionrary for the data of the video
    video_info = {
        'Channel_name': video_snippet['channelTitle'],
        'video_id': video_data['id'],
        'video_url': f"https://www.youtube.com/watch?v={video_data['id']}",
        'video_title': video_data['snippet']['title'],
        'video_publication_date': video_data['snippet']['publishedAt'],
        'video_duration': video_content_details['duration'],
        'video_view_count': int(video_statistics['viewCount']),
        'video_like_count': int(video_statistics['likeCount']),
        'video_comment_count': int(video_statistics['commentCount'])
    }

    return video_info

def fetch_comments(video_id, youtube, video_info):
    """Fetch comments from a YouTube video."""
    all_comments = []
    next_page_token = None
    max_numner_of_comments = video_info['video_comment_count']
    # max_numner_of_comments = 1000
    try:
        start_time = time.time()
        while len(all_comments) < max_numner_of_comments:
            response = youtube.commentThreads().list(
                part='snippet,replies',            
                videoId=video_id,             
                maxResults=min(100, max_numner_of_comments - len(all_comments)),                    
                textFormat='plainText',
                pageToken=next_page_token,
            ).execute()
            # If no comment threads are found, break the loop
            if not response.get('items'):
                print("No comment threads found.")
                break

            for item in response['items']:
                comment = item['snippet']['topLevelComment']['snippet']

                comment_info = {
                    'comment_id': item['id'],
                    'author': comment['authorDisplayName'],
                    'author_id': comment.get('authorChannelId', {}).get('value', ''),
                    'text': comment['textDisplay'],
                    'like_count': comment['likeCount'],
                    'published_at': comment['publishedAt'],
                    'updated_at': comment['updatedAt'],
                }

                all_comments.append(comment_info)

                if len(all_comments) >= max_numner_of_comments:
                    print("Reached the maximum number of comments.")
                    break

                # Check if there are more comments to fetch
                next_page_token = response.get('nextPageToken')
                if not next_page_token:
                    break
                
                # Sleep to avoid hitting rate limits
                # time.sleep(0.5)
        end_time = time.time()
        total_time = end_time - start_time
        print(f"Time taken to fetch comments: {round(total_time,2)} seconds")
        print(f"The total number of comments is {len(all_comments)}")

        return all_comments
    
    except HttpError as e:
        print(f"An HTTP error {e.resp.status} occurred:\n{e.content}")

        return None

def save_comments(all_comments, video_info):
    """Save comments to a CSV file."""
    
    comments_df = pd.DataFrame(all_comments)

    folder_path = '\\youtube-sentiment-analysis\\data'
    video_info['video_title']

    csv_file_path = os.path.join(folder_path, f"{video_info['Channel_name']}_{video_info['video_title']}_comments.csv")

    # Save the video comments to a CSV file
    comments_df.to_csv(csv_file_path, index=False)
    print(f'Video comments with title: {video_info["video_title"]} saved to CSV file: {csv_file_path}')

    return csv_file_path


if __name__ == "__main__":
    # --- Configuration ---
    VIDEO_ID_TO_ANALYZE = "UDVOuh-LDx8"  # Replace with the actual YouTube video ID

    # --- Main Execution ---
    print("--- Starting YouTube Comment Scraper ---")

    # 1. Load the YouTube API key
    youtube_api = load_youtube_key()
    if not youtube_api:
        print("Script terminated due to API key issue.")
        exit()

    # 2. Load the video response
    print(f"\n--- Fetching video information for ID: {VIDEO_ID_TO_ANALYZE} ---")
    video_response = load_video_response(VIDEO_ID_TO_ANALYZE, youtube_api)
    if not video_response:
        print("Could not retrieve video information. Please check the video ID.")
        exit()

    # 3. Extract video information
    print("\n--- Extracting video details ---")
    video_info = info_video(video_response)
    print("Video information loaded successfully:")
    for key, value in video_info.items():
        print(f"  {key}: {value}")

    # 4. Fetch comments
    print("\n--- Fetching comments ---")
    all_video_comments = fetch_comments(VIDEO_ID_TO_ANALYZE, youtube_api, video_info)
    if all_video_comments is not None:
        # 5. Save comments to CSV
        print("\n--- Saving comments to CSV ---")
        csv_file = save_comments(all_video_comments, video_info)
        if csv_file:
            print(f"\n--- Task Completed Successfully ---")
            print(f"All comments for video '{video_info['video_title']}' have been fetched and saved to: {csv_file}")
        else:
            print("\n--- Task Completed with a Warning ---")
            print("Comments were fetched but could not be saved to a CSV file.")
    else:
        print("\n--- Task Completed with an Error ---")
        print("Could not fetch comments for the video.")