from ScrapeDataHandler import DataHandler

import os

import hashlib
class MasterUIDUniverse:
    def __init__(self, master_uid_universe, data_handler):
        self.master_uid_universe = master_uid_universe
        self.data_handler = data_handler

        self.master_u_id_Dataframe_path = "master_u_id_Dataframe.csv"
        self.master_u_id_Dataframe = (
            self.data_handler.loadfromCSV(self.master_u_id_Dataframe_path)) \
            if os.path.exists(self.master_u_id_Dataframe_path) \
            else self.data_handler.create_new_u_id_dataframe()

        self.master_Spotify_Dataframe_path = "master_Spotify_Dataframe.csv"
        self.master_Spotify_Dataframe = (
            self.data_handler.loadfromCSV(self.master_Spotify_Dataframe_path)) \
            if os.path.exists(self.master_Spotify_Dataframe_path) \
            else self.data_handler.create_new_spotify_dataframe()

        self.master_Discogs_Dataframe_path = "master_Discogs_Dataframe.csv"
        self.master_Discogs_Dataframe = (
            self.data_handler.loadfromCSV(self.master_Discogs_Dataframe_path)) \
            if os.path.exists(self.master_Discogs_Dataframe_path) \
            else DataHandler.create_new_discogs_dataframe()

    @staticmethod
    def draw_out_lifeforce(dataframe):
        # Process each row to generate u_ids based on relationships
        for index, row in dataframe.iterrows():
            # Example: Generate a u_id for a combination of artist and album
            artist_album_uid = MasterUIDUniverse.generate_hashed_id(row['artist'], row['album'], seed=123)
            # Store the generated u_id in the dataframe
            dataframe.at[index, 'artist_album_uid'] = artist_album_uid

            # Additional logic to generate u_ids for other combinations

        return dataframe

    def create_new_discogs_dataframe(data=None, seed=0):
        if data is not None:
            for row in data:
                row['u_id'] = generate_hashed_id(row['artist'], row['album'], row['track_name'], seed=seed)
        return pd.DataFrame(data, columns=[...])  # Specify your columns

    def generate_hashed_id(*args, seed=0):
        """Generates a hashed ID for given inputs and a seed."""
        hash_input = f"{seed}_" + "_".join(str(arg) for arg in args)
        return hashlib.sha256(hash_input.encode()).hexdigest()

    def encode_duration(start_time, end_time, seed=0):
        """Encodes a duration into a unique ID."""
        duration_str = f"{start_time}-{end_time}"
        return generate_hashed_id(duration_str, seed=seed)


    def get_master_uid_universe(self):
        return self.master_uid_universe

    def set_master_uid_universe(self, master_uid_universe):
        self.master_uid_universe = master_uid_universe