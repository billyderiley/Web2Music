from datetime import datetime

class SpotifyScraper():

    @staticmethod
    def get_spotify_metadata_from_aggregated_albums(aggregated_albums):
        aggregated_metadata = []
        for album in aggregated_albums:
            aggregated_metadata = aggregated_metadata.append(SpotifyScraper.get_spotify_metadata_from_album(album))
        return aggregated_metadata

    @staticmethod
    def get_spotify_metadata_from_album(album_obj, album_name=None, album_release_date=None):

        """Get Spotify metadata from a track object."""
        metadata = {
            'Spotify_Album_ID': album_obj.get('id', 'Unknown ID'),
            'Spotify_Album_Name': album_obj.get('album_name', 'Unknown Name'),
            'Spotify_Album_Artists': album_obj.get('artists'),
            'Spotify_Album_Artist_IDs': album_obj.get('artist_ids'),
            'Spotify_Album_Tracks': album_obj.get('tracks', 'Unknown Tracks'),
            'Spotify_Album_Release_Date': album_obj.get('album_release_date'),
            'Spotify_Album_Genres': album_obj.get('album_genres'),
        }
        return metadata

    @staticmethod
    def get_spotify_metadata_from_track(
            track, u_id, album_id=None, album_name=None, album_release_date=None,
            album_artists=None, album_artist_ids=None):

        """Get Spotify metadata from a track object."""
        metadata = {
            'u_id' : u_id,
            'Spotify_Track_ID': track.get('id'),
            'Spotify_Track_Name': track.get('name', 'Unknown Name'),
            'Spotify_Track_Artist': track.get('artists', [{'name': 'Unknown Artist'}])[0]['name'],
            'Spotify_Track_Artist_ID': track.get('artists', [{'id': 'Unknown Artist ID'}])[0]['id'],
            'Spotify_Track_Duration': track.get('duration_ms'),
            'Spotify_Track_Preview_Url': track.get('preview_url', 'No Preview Available'),
            'Spotify_Track_Number': track.get('track_number'),
            'Spotify_Album_ID': album_id,
            'Spotify_Album_Name': album_name,
            'Spotify_Album_Release_Date': album_release_date,
            'Spotify_Album_Artists': ','.join(album_artists),
            'Spotify_Album_Artist_IDs': ','.join(album_artist_ids)
        }
        return metadata

    @staticmethod
    def remove_characters_from_string(string):
        remove_characters = ['/', '\\', '?', '%', '*', ':', '|', '"', '<', '>', '.']
        for character in remove_characters:
            string = string.replace(character, '')
        return string

    def __init__(self, sp):
        self.sp = sp

    def get_artist_metrics(self, artist_id):
        """Retrieve popularity and followers metrics for a given artist."""
        artist_info = self.sp.artist(artist_id)
        artist_popularity = artist_info['popularity']
        artist_followers = artist_info['followers']['total']
        return artist_popularity, artist_followers




