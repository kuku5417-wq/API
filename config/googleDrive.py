import os
from googleapiclient.discovery import build
from google.auth import default

SAMPLE_FOLDER_ID = os.environ.get("GOOGLE_DRIVE_SAMPLE_FOLDER_ID")

def get_drive_service():
    creds, _ = default(scopes=["https://www.googleapis.com/auth/drive"])
    return build("drive", "v3", credentials=creds)
