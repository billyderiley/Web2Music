import pygame
import time
import requests
from io import BytesIO

class SpotifyPreviewPlayer:
    def __init__(self):
        # Initialize pygame mixer
        pygame.mixer.init()

    def play_spotify_preview(self, preview_url):
        """Play a 30-second preview of a Spotify track."""
        if preview_url is None:
            print("No preview available.")
            return

        # Fetch the MP3 file from the preview URL
        response = requests.get(preview_url)
        audio_data = BytesIO(response.content)

        # Load and play the audio
        pygame.mixer.music.load(audio_data)
        pygame.mixer.music.play()

        # Keep the program running while the music plays
        while pygame.mixer.music.get_busy():
            time.sleep(1)

