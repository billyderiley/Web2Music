#from ScrapeDataHandler import DataHandler
import pandas as pd
import random
import string

import hashlib
class MasterUIDUniverse:
    def __init__(self):
        #self.master_uid_universe = master_uid_universe
        #self.data_handler = data_handler

        self.master_u_id_Dataframe_path = "master_u_id_Dataframe.csv"
        self.master_u_id_Dataframe = None
        # check if master_u_id_Dataframe csv exists as a file and if not create it
        if self.load_master_u_id_Dataframe() is None:
            self.create_new_u_id_dataframe(data=None, in_file=True)


    """
    Loading methods
    """

    def loadfromCSV(self, csv_file):
        try:
            return pd.read_csv(csv_file)
        except FileNotFoundError:
            return None

    """
    
    """

    def set_master_u_id_Dataframe(self, df):
        """Set the master u_id DataFrame."""
        self.master_u_id_Dataframe = df

    def load_master_u_id_Dataframe(self):
        return  self.loadfromCSV(self.master_u_id_Dataframe_path)

    """
        Master u_id Dataframe
        """

    def create_new_u_id_dataframe(self, data=None, in_file=False):
        """Create an empty u_id DataFrame."""
        u_id_df = pd.DataFrame(data if data else None,
                               columns=['u_id_track', 'u_id_release', 'u_id_artist', 'u_id_label', 'u_id_genre'])
        if in_file:
            self.save_dataframe(u_id_df, self.master_u_id_Dataframe_path)
        else:
            return u_id_df

    def save_dataframe(self, dataframe, path):
        dataframe.to_csv(path, index=False)
        print(f"DataFrame saved to {path}")

    def check_u_id_in_master_list(self, artist_name, album_name=None, track_name=None):
        """
        Checks if a u_id exists in the master_u_id_list based on artist name and either album name or track name.
        Returns the u_id if found, otherwise None.
        """
        # Stream the master_u_id_Dataframe in chunks
        for chunk in pd.read_csv(self.master_u_id_Dataframe_path, chunksize=1000):  # Adjust chunksize as needed
            if album_name:
                matched_row = chunk[(chunk['u_id_artist'] == artist_name) & (chunk['u_id_release'] == album_name)]
            elif track_name:
                matched_row = chunk[(chunk['u_id_artist'] == artist_name) & (chunk['u_id_track'] == track_name)]
            else:
                raise ValueError("Either album_name or track_name must be provided")

            if not matched_row.empty:
                u_id = matched_row['u_id'].iloc[0]
                return u_id
        return None

    def generate_unique_id(self, artist_name, album_name=None, track_name=None):
        """
        Generates a random unique identifier based on artist name and either album name or track name.
        """
        if album_name:
            base_string = artist_name + album_name
        elif track_name:
            base_string = artist_name + track_name
        else:
            raise ValueError("Either album_name or track_name must be provided")

        length = 10  # Adjust the length as needed
        characters = string.ascii_letters + string.digits
        u_id = base_string + ''.join(random.choice(characters) for _ in range(length))
        return u_id

    def check_url_in_master_list_old(self, url):
        """
        Checks if a URL exists in the master_u_id_list.
        Returns the u_id and its type if found, otherwise (None, None).
        """
        # Stream the master_u_id_Dataframe in chunks
        for chunk in pd.read_csv(self.master_u_id_Dataframe_path, chunksize=1000):  # Adjust chunksize as needed
            matched_row = chunk[chunk['url_column_name'] == url]  # Replace 'url_column_name' with actual column name
            if not matched_row.empty:
                u_id = matched_row['u_id_column_name'].iloc[0]  # Replace 'u_id_column_name' with actual column name
                u_id_type = 'type_to_determine'  # Determine the type of u_id
                return u_id, u_id_type
        return None, None

    def generate_unique_id_old(self, exclusion_list=None):
        """Generates a random unique identifier."""
        length = 10  # Adjust the length as needed
        characters = string.ascii_letters + string.digits
        u_id = ''.join(random.choice(characters) for _ in range(length))
        if exclusion_list is not None:
            while u_id in exclusion_list:
                u_id = ''.join(random.choice(characters) for _ in range(length))
        return u_id


    def get_master_u_id_tracklist(self):
        if self.master_u_id_Dataframe is None:
            try:
                self.load_master_u_id_Dataframe()
            except FileNotFoundError:
                u_id_df = self.create_new_u_id_dataframe()
                self.set_master_u_id_Dataframe(u_id_df)
        return self.master_u_id_Dataframe['u_id_track'].tolist()

    def Track_Release_Dataframe_to_master_u_id_Dataframe(self):
        if self.Track_Release_Dataframe is not None:
            if self.master_u_id_Dataframe is not None:
                self.master_u_id_Dataframe = pd.concat([self.master_u_id_Dataframe, self.Track_Release_Dataframe], ignore_index=True).drop_duplicates(
                    subset=['u_id', 'Discogs_Tracks_Artists', 'Discogs_Tracks', 'Discogs_Tracks_Durations',
                            'Discogs_Tracks_Positions'])
            else:
                self.master_u_id_Dataframe = self.Track_Release_Dataframe
            self.save_dataframe(self.master_u_id_Dataframe, self.master_u_id_Dataframe_path)
            print(f"Master u_id DataFrame saved to {self.master_u_id_Dataframe_path}")
        else:
            print("No Master u_id DataFrame to save.")


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