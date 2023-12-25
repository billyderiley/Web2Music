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

    def get_spotify_metadata_from_track(self, track, album_name=None, album_release_date=None):
        #print(track.keys())


        """Get Spotify metadata from a track object."""
        metadata = {
            'Spotify_ID': track.get('id', 'Unknown ID'),
            'Spotify_Name': track.get('name', 'Unknown Name'),
            'Spotify_Artist': track.get('artists', [{'name': 'Unknown Artist'}])[0]['name'],
            'Spotify_Album': track.get('album').get('name', 'Unknown Album') if album_name is None else album_name,
            'Spotify_Release_Date': track.get('album').get('release_date', 'Unknown Release Date') if album_release_date is None else album_release_date,
            'Spotify_Popularity': track.get('popularity', 0),
            'Spotify_Duration': track.get('duration_ms', 0),
            'Spotify_Preview_Url': track.get('preview_url', 'No Preview Available')
        }
        return metadata