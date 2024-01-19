import sqlite3
import pickle
from concurrent.futures import ThreadPoolExecutor, as_completed
import inspect
from collections import defaultdict
import datetime

class DatabaseManager:
    def __init__(self, db_path='soup_urls.db'):
        self.db_path = db_path
        self.create_db_tables()
        self.remove_old_entries()

    def create_db_tables(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Create table for storing soup objects
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS soups (
                url TEXT PRIMARY KEY, 
                soup_object TEXT, 
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Create table for storing user interaction history
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_interaction_history (
    method_name TEXT,
    call_location TEXT,
    user_input TEXT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
        ''')
        # Check if 'call_location' column exists and add it if not
        cursor.execute("PRAGMA table_info(user_interaction_history)")
        columns = [info[1] for info in cursor.fetchall()]
        if 'call_location' not in columns:
            cursor.execute("ALTER TABLE user_interaction_history ADD COLUMN call_location TEXT")

        cursor.execute('''
                    CREATE TABLE IF NOT EXISTS spotify_search_history (
                        query TEXT,
                        request_type TEXT,
                        response_type TEXT,
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        response TEXT
                    )
                ''')

        # Create table for logging specific Spotify API responses
        cursor.execute('''
                    CREATE TABLE IF NOT EXISTS spotify_response_log (
                        entity_type TEXT,
                        entity_data TEXT,
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')

        conn.commit()
        conn.close()

    def save_soup_data(self, url, soup_object):
        serialized_soup = pickle.dumps(soup_object)
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("INSERT OR REPLACE INTO soups (url, soup_object, timestamp) VALUES (?, ?, CURRENT_TIMESTAMP)", (url, serialized_soup))
        conn.commit()
        conn.close()

    def load_soup_data(self, url):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT soup_object FROM soups WHERE url = ?", (url,))
        data = cursor.fetchone()
        conn.close()
        return pickle.loads(data[0]) if data else None

    def save_user_interaction(self, method_name, call_location, user_input):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO user_interaction_history (method_name, call_location, user_input) 
            VALUES (?, ?, ?)""",
            (method_name, call_location, user_input))
        conn.commit()
        conn.close()
    def load_recent_user_interaction(self, method_name):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT user_input FROM user_interaction_history WHERE method_name = ? ORDER BY timestamp DESC LIMIT 1", (method_name,))
        data = cursor.fetchone()
        conn.close()
        return data[0] if data else None


    """
    Code from previous BaseScraper
    
    
    """

    def create_db_table(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        # Create the table with an additional timestamp column
        cursor.execute('''
                CREATE TABLE IF NOT EXISTS soups (
                    url TEXT PRIMARY KEY, 
                    soup_object TEXT, 
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
        """cursor.execute('''CREATE TABLE IF NOT EXISTS soups 
                          (url TEXT PRIMARY KEY, soup_object TEXT)''')"""
        conn.commit()
        conn.close()

    def save_data_to_db(self, url, soup_object):
        # Serialize the soup object
        serialized_soup = pickle.dumps(soup_object)
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("INSERT OR REPLACE INTO soups (url, soup_object, timestamp) VALUES (?, ?, CURRENT_TIMESTAMP)",
                       (url, serialized_soup))
        conn.commit()
        conn.close()

    def get_data_from_db(self, url):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT soup_object, timestamp FROM soups WHERE url = ?", (url,))
        data = cursor.fetchone()
        conn.close()

        if data:
            soup_object, timestamp = data
            soup_object = pickle.loads(soup_object)
            timestamp = datetime.datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
            return soup_object, timestamp
        else:
            return None, None

    def delete_data_from_db(self, url):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM soups WHERE url = ?", (url,))
        conn.commit()
        conn.close()


    def divide_into_batches(self, dataframe, batch_size):
        # Divide dataframe into batches of rows
        for i in range(0, len(dataframe), batch_size):
            yield dataframe.iloc[i:i + batch_size]

    def get_contextual_method_name(self):
        """
        Infers the method name and the class it belongs to.
        """
        frame = inspect.currentframe()
        method_name = frame.f_back.f_code.co_name
        class_name = frame.f_back.f_locals["self"].__class__.__name__
        return f"{class_name}.{method_name}"

    def get_call_location(self):
        """
        Determines the location where the UserInteraction method was called from.
        Looks two frames back in the call stack to find the actual caller.
        """
        frame = inspect.currentframe()
        # Get the frame of the method that called get_call_location (i.e., a method in UserInteraction)
        user_interaction_frame = frame.f_back
        # Get the frame of the actual caller method
        caller_frame = user_interaction_frame.f_back if user_interaction_frame else None

        if caller_frame:
            method_name = caller_frame.f_code.co_name
            class_name = caller_frame.f_locals.get('self',
                                                   None).__class__.__name__ if 'self' in caller_frame.f_locals else None

            if class_name:
                return f"{class_name}.{method_name}"
            else:
                return method_name
        else:
            return "Unknown location"

    """
    Methods for handling input_history
    """


    def save_user_interaction(self, method_name, call_location, user_input):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO user_interaction_history (method_name, call_location, user_input) 
            VALUES (?, ?, ?)""",
                       (method_name, call_location, user_input))
        conn.commit()
        conn.close()


    def load_user_interaction_history(self, method_name, call_location):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT user_input FROM user_interaction_history 
            WHERE method_name = ? 
            AND call_location = ?
            ORDER BY timestamp DESC""",
                       (method_name, call_location))
        data = cursor.fetchall()
        conn.close()
        return [item[0] for item in data]

    """
    Code for managing Spotify related SQL stuff
    """

    def save_spotify_api_request(self, query, request_type, response_type, response):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO spotify_search_history (query, request_type, response_type, response) 
            VALUES (?, ?, ?, ?)""",
                       (query, request_type, response_type, pickle.dumps(response)))
        conn.commit()
        conn.close()

    def check_existing_spotify_request(self, query, request_type):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT response FROM spotify_search_history 
            WHERE query = ? AND request_type = ?
            ORDER BY timestamp DESC LIMIT 1""",
                       (query, request_type))
        data = cursor.fetchone()
        conn.close()
        return pickle.loads(data[0]) if data else None

    def log_spotify_api_response(self, entity_type, entity_data):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO spotify_response_log (entity_type, entity_data) 
            VALUES (?, ?)""",
                       (entity_type, pickle.dumps(entity_data)))
        conn.commit()
        conn.close()


    """
    Database Maintainer
    """

    def remove_old_entries(self):
        """
        Removes entries from all tables in the database that are older than one day.
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Get the names of all tables in the database
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()

        # For each table, delete rows where the timestamp is older than one day
        for table in tables:
            table_name = table[0]
            cursor.execute(f"""
                DELETE FROM {table_name}
                WHERE timestamp < datetime('now', '-1 day')
            """)

        conn.commit()
        conn.close()