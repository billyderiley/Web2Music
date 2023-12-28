import sqlite3
import pickle
from concurrent.futures import ThreadPoolExecutor, as_completed
import inspect

class DatabaseManager:
    def __init__(self, db_path='soup_urls.db'):
        self.db_path = db_path
        self.create_db_tables()

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
        cursor.execute("SELECT soup_object FROM soups WHERE url = ?", (url,))
        data = cursor.fetchone()
        conn.close()
        if data:
            # Unpickle the soup object
            soup_object = pickle.loads(data[0])
            return soup_object
        else:
            return None



        from concurrent.futures import ThreadPoolExecutor, as_completed

    def execute_in_batches(self, urls, action, batch_size=5):
        with ThreadPoolExecutor(max_workers=batch_size) as executor:
            # Submit each URL to the executor
            future_to_url = {executor.submit(action, url): url for url in urls}

            # Process the results as they complete
            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    future.result()  # This is where the action function is executed
                    # print(f"Loaded webpage: {future.result()}")  # Print the URL returned by the action function
                except Exception as exc:
                    print(f"{url} generated an exception: {exc}")

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