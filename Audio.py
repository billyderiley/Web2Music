import pygame
import time
import requests
from io import BytesIO

class SpotifyPreviewPlayer:
    def __init__(self):
        # Initialize pygame mixer
        pygame.mixer.init()
        pygame.init()
        # Create a minimal window for event handling
        self.screen = pygame.display.set_mode((200, 100))
        pygame.display.set_caption("Spotify Preview Player")
        self.Spotify_Dataframe = None

    def set_spotify_dataframe(self, Spotify_Dataframe):
        self.Spotify_Dataframe = Spotify_Dataframe

    def preview_menu(self,):
        current_index = 0
        play_new_track = True
        preview_urls = self.Spotify_Dataframe['Spotify_Preview_Url'].tolist()
        running = True
        pygame.font.init()
        font = pygame.font.SysFont("Arial", 20)
        while running:
            for event in pygame.event.get():
                if event.type == pygame.KEYDOWN:
                    if event.key == pygame.K_RIGHT:
                        current_index = (current_index + 1) % len(preview_urls)
                        play_new_track = True
                    elif event.key == pygame.K_LEFT:
                        current_index = (current_index - 1) % len(preview_urls)
                        play_new_track = True
                    elif event.key == pygame.K_SPACE:
                        if pygame.mixer.music.get_busy():
                            pygame.mixer.music.pause()
                        else:
                            pygame.mixer.music.unpause()
                    elif event.key == pygame.K_RETURN:
                        running = False
                        pygame.quit()
                        return
                elif event.type == pygame.QUIT:
                    running = False

            if play_new_track and preview_urls:
                pygame.mixer.music.stop()
                track_info = (f"Playing preview: {self.Spotify_Dataframe['Spotify_Name'][current_index]} - "
                              f"{self.Spotify_Dataframe['Spotify_Artist'][current_index]} "
                              f"({self.Spotify_Dataframe['Spotify_Album'][current_index]}, "
                              f"{self.Spotify_Dataframe['Spotify_Release_Date'][current_index]}) "
                              f"URL: {self.Spotify_Dataframe['Spotify_Preview_Url'][current_index]}")
                print(track_info)
                self.play_spotify_preview(preview_urls[current_index])
                play_new_track = False

                # Clear screen
                self.screen.fill((0, 0, 0))

                # Render text
                text_surface = font.render(track_info, True, (255, 255, 255))
                # Blit text to screen
                self.screen.blit(text_surface, (10, 10))

                # Update the display
                pygame.display.update()
            pygame.time.wait(100)  # 100 ms delay to reduce CPU usage

        pygame.quit()

    def play_spotify_preview(self, preview_url):
        if preview_url is None:
            print("No preview available.")
            return

        # Fetch the MP3 file from the preview URL
        response = requests.get(preview_url)
        audio_data = BytesIO(response.content)

        # Load and play the audio
        pygame.mixer.music.load(audio_data)
        pygame.mixer.music.play()


