import pandas as pd
import pafy
import vlc
import tkinter as tk
from tkinter import messagebox
import ast

class YoutubeAudioPlayer:
    def __init__(self, dataframe):
        self.dataframe = dataframe
        self.filtered_df = self.filter_df_with_youtube_links()
        self.vlc_instance = vlc.Instance()
        self.player = self.vlc_instance.media_player_new()
        self.current_index = 0
        self.user_selected_rows = pd.DataFrame()

        self.current_row_index = 0
        self.current_video_index_within_row = 0



    def filter_df_with_youtube_links(self):
        # Filter rows where 'Discogs_Tracks_Youtube' has non-empty lists
        return self.dataframe[self.dataframe['Discogs_Tracks_Youtube'].astype(bool)]

    def play_audio(self, video_id_or_url):
        try:
            # Check if it's just a video ID or a full URL
            if len(video_id_or_url) == 11:
                url = f"https://www.youtube.com/watch?v={video_id_or_url}"
            else:
                url = video_id_or_url

            video = pafy.new(url)

            # Check if there is an available audio stream
            best_audio = video.getbestaudio()
            if best_audio is None:
                raise ValueError("No audio stream found for this video")

            media = self.vlc_instance.media_new(best_audio.url)
            self.player.set_media(media)
            self.player.play()
        except Exception as e:
            print(f"Error playing video {url}: {e}")
            # Here you can add logic to handle the error, e.g., skipping to the next video

    def save_current_audio_info(self):
        # Save the row corresponding to the current audio being played
        current_row = self.filtered_df.iloc[self.current_index:self.current_index + 1]
        self.user_selected_rows = pd.concat([self.user_selected_rows, current_row], ignore_index=True)

    def next_audio(self):
        # Loop until a valid entry is found or end of DataFrame is reached
        while self.current_row_index < len(self.filtered_df):
            try:
                next_audio_urls = self.get_audio_url(self.current_row_index)
                # Play the next video if there is one in the list
                if self.current_video_index_within_row < len(next_audio_urls):
                    next_audio_url = next_audio_urls[self.current_video_index_within_row]
                    self.play_audio(next_audio_url)
                    self.current_video_index_within_row += 1
                    return
                else:
                    # Move to the next row if all videos in the current row are played
                    self.current_row_index += 1
                    self.current_video_index_within_row = 0
            except ValueError:
                # Skip to the next row if no valid entry is found
                self.current_row_index += 1

        # If the end of the DataFrame is reached, stop playing
        print("No more audio to play.")

    def next_audio_old(self):
        # Get the list of URLs for the current row
        try:
            next_audio_urls = self.get_audio_url(self.current_row_index)
        except ValueError as e:
            while ValueError:
                self.current_row_index += 1
                next_audio_urls = self.get_audio_url(self.current_row_index)
                if next_audio_urls is not None:
                    break

        if self.current_video_index_within_row < len(next_audio_urls):
            # Get the URL for the next video in the current row
            next_audio_url = next_audio_urls[self.current_video_index_within_row]
            self.play_audio(next_audio_url)
            # Increment the video index within the row for the next call
            self.current_video_index_within_row += 1
        else:
            # If we've played all videos in the current row, move to the next row
            self.current_row_index += 1
            self.current_video_index_within_row = 0
            if self.current_row_index < len(self.filtered_df):
                # Automatically play the first video of the next row
                self.next_audio()
            else:
                print("No more audio to play.")
    def next_audio_old(self):
        # Check if there are more videos in the current row
        youtube_entry = self.filtered_df.iloc[self.current_row_index]['Discogs_Tracks_Youtube']
        youtube_list = ast.literal_eval(youtube_entry)

        if self.current_video_index_within_row < len(youtube_list) - 1:
            # Move to the next video in the current row
            self.current_video_index_within_row += 1
        else:
            # Move to the next row and reset the video index
            self.current_row_index += 1
            self.current_video_index_within_row = 0

        if self.current_row_index < len(self.filtered_df):
            # Get the URL for the next video
            next_audio_urls = self.get_audio_url(self.current_row_index)

            self.play_audio(next_audio_urls)
        else:
            print("No more audio to play.")

    def next_audio_old2(self):
        # Check if there are more videos in the current row
        if self.current_video_index_within_row < len(
                ast.literal_eval(self.filtered_df.iloc[self.current_row_index]['Discogs_Tracks_Youtube'])) - 1:
            self.current_video_index_within_row += 1
        else:
            # Move to the next row and reset the video index
            self.current_row_index += 1
            self.current_video_index_within_row = 0

        if self.current_row_index < len(self.filtered_df):
            next_audio_url = self.get_audio_url()
            self.play_audio(next_audio_url)
        else:
            print("No more audio to play.")

    def next_audio_old(self):
        # Logic to play the next audio
        self.current_index += 1
        if self.current_index >= len(self.filtered_df):
            self.current_index = 0  # Loop back to the first audio
        next_audio_url = self.get_audio_url(self.current_index)
        self.play_audio(next_audio_url)

    def previous_audio(self):
        # Check if there are videos before the current one in the current row
        if self.current_video_index_within_row > 0:
            # Move to the previous video in the current row
            self.current_video_index_within_row -= 1
        else:
            # Move to the previous row
            self.current_row_index -= 1
            if self.current_row_index < 0:
                # If we're at the first row, loop around to the last row
                self.current_row_index = len(self.filtered_df) - 1
            # Get the last video index of the new current row
            prev_audio_urls = self.get_audio_url(self.current_row_index)
            self.current_video_index_within_row = len(prev_audio_urls) - 1

        # Get the URL for the current video
        prev_audio_url = self.get_audio_url(self.current_row_index)[self.current_video_index_within_row]
        self.play_audio(prev_audio_url)

    def previous_audio_old(self):
        # Logic to play the previous audio
        self.current_index -= 1
        if self.current_index < 0:
            self.current_index = len(self.filtered_df) - 1  # Loop to the last audio
        prev_audio_url = self.get_audio_url(self.current_index)
        self.play_audio(prev_audio_url)

    def pause_audio(self):
        self.player.pause()

    def stop_audio(self):
        self.player.stop()

    def get_audio_url_old(self, index):
        # Extract the URL; this will depend on how the YouTube URLs are stored within the list
        youtube_entry = self.filtered_df.iloc[index]['Discogs_Tracks_Youtube']
        url = youtube_entry.split(',')[1]  # Placeholder for actual URL extraction logic
        return url

    def get_audio_url_old2(self, index):
        youtube_entry = self.filtered_df.iloc[index]['Discogs_Tracks_Youtube']

        # Safely evaluate the string as a list
        try:
            youtube_list = ast.literal_eval(youtube_entry)
        except ValueError:
            raise ValueError(f"Unable to parse entry as list: {youtube_entry}")

        # Iterate through the evaluated list and extract the URL
        for item in youtube_list:
            parts = item.split(',')
            if len(parts) >= 2:
                url = parts[1].strip().strip("'").strip('"')
                if url.startswith("https://www.youtube.com/watch?v=") and len(url.split('=')[-1]) == 11:
                    return url.split('=')[-1]  # Return only the video ID
                elif len(url) == 11:
                    return url  # Return the part if it's a video ID

        raise ValueError(f"No valid YouTube URL or ID found in entry: {youtube_entry}")

    def get_audio_url_old3(self):
        youtube_entry = self.filtered_df.iloc[self.current_row_index]['Discogs_Tracks_Youtube']
        youtube_list = ast.literal_eval(youtube_entry)

        if self.current_video_index_within_row < len(youtube_list):
            item = youtube_list[self.current_video_index_within_row]
            url = item.split(',')[1].strip().strip("'").strip('"')
            return url.split('=')[-1]
        else:
            raise ValueError("No more videos in the current row.")

    def get_audio_url(self, index):
        youtube_entry = self.filtered_df.iloc[index]['Discogs_Tracks_Youtube']

        # Check if youtube_entry is NaN or None
        if pd.isna(youtube_entry) or youtube_entry is None:
            raise ValueError(f"No YouTube entry found for row {index}")

        # Remove surrounding brackets and split by ',' to get the individual 'title, url' entries
        youtube_entry = youtube_entry.strip("[]")
        video_entries = [entry.strip().strip("'") for entry in youtube_entry.split("', '")]

        urls = []
        for entry in video_entries:
            # Split each 'title, url' entry by ',' to separate title and URL
            parts = entry.split(',')
            if len(parts) == 2:
                # The second part is the URL
                url = parts[1].strip()
                if url.startswith("https://www.youtube.com/watch?v=") and len(url.split('=')[-1]) == 11:
                    urls.append(url)  # Append the URL to the list
                elif len(url) == 11:
                    urls.append(f"https://www.youtube.com/watch?v={url}")  # Append the full URL to the list

        if not urls:
            raise ValueError(f"No valid YouTube URLs found in entry: {youtube_entry}")

        return urls  # Return the list of URLs

    def get_user_selected_dataframe(self):
        return self.user_selected_rows

class YoutubeAudioPlayerGUI:
    def __init__(self, master, youtube_player):
        self.master = master
        self.youtube_player = youtube_player
        self.master.title("Youtube Audio Player")

        self.play_button = tk.Button(master, text='Play', command=self.play_audio)
        self.play_button.pack(side=tk.LEFT)

        self.pause_button = tk.Button(master, text='Pause', command=self.pause_audio)
        self.pause_button.pack(side=tk.LEFT)

        self.next_button = tk.Button(master, text='Next', command=self.next_audio)
        self.next_button.pack(side=tk.LEFT)

        self.prev_button = tk.Button(master, text='Previous', command=self.prev_audio)
        self.prev_button.pack(side=tk.LEFT)

        self.save_button = tk.Button(master, text='Save', command=self.save_audio_info)
        self.save_button.pack(side=tk.LEFT)

        """self.playback_scale = tk.Scale(master, from_=0, to=100, orient=tk.HORIZONTAL, command=self.on_seek)
        self.playback_scale.pack(fill=tk.X, expand=True)
        self.update_playback_scale()"""

        """self.is_user_interacting = False
        self.playback_scale = tk.Scale(master, from_=0, to=100, orient=tk.HORIZONTAL, command=self.on_seek)
        self.playback_scale.pack(fill=tk.X, expand=True)

        # Bind events for the slider
        self.bind_slider_events()"""

        # Update the playback scale periodically
        self.update_playback_scale()

    def on_seek(self, val):
        if self.is_user_interacting:
            duration = self.youtube_player.player.get_length() / 1000  # Duration in seconds
            seek_position = duration * float(val) / 100  # Calculate the seek position
            self.youtube_player.player.set_time(int(seek_position * 1000))  # Seek to the position

    def update_playback_scale(self):
        if self.youtube_player.player.is_playing():
            duration = self.youtube_player.player.get_length() / 1000  # Duration in seconds
            if duration > 0:
                position = self.youtube_player.player.get_time() / 1000  # Current position in seconds
                self.is_user_interacting = False
                self.playback_scale.set((position / duration) * 100)  # Update the scale position
                self.is_user_interacting = True
        self.master.after(1000, self.update_playback_scale)  # Update every second

    # Add event bindings for user interaction
    def bind_slider_events(self):
        self.playback_scale.bind("<ButtonPress-1>", lambda event: setattr(self, "is_user_interacting", True))
        self.playback_scale.bind("<ButtonRelease-1>", lambda event: setattr(self, "is_user_interacting", False))

    def play_audio(self):
        url = self.youtube_player.get_audio_url(self.youtube_player.current_index)
        self.youtube_player.play_audio(url)

    def pause_audio(self):
        self.youtube_player.pause_audio()

    def next_audio(self):
        self.youtube_player.next_audio()

    def prev_audio(self):
        self.youtube_player.previous_audio()

    def save_audio_info(self):
        self.youtube_player.save_current_audio_info()
        messagebox.showinfo("Save", "Audio info saved.")