from datetime import datetime

class SpotifyScraper():
    def __init__(self, sp):
        self.sp = sp

    def get_artist_metrics(self, artist_id):
        """Retrieve popularity and followers metrics for a given artist."""
        artist_info = self.sp.artist(artist_id)
        artist_popularity = artist_info['popularity']
        artist_followers = artist_info['followers']['total']
        return artist_popularity, artist_followers