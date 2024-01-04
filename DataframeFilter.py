import pandas as pd
import random
import string
from collections import Counter
from UserInteraction import UserInteraction
import Levenshtein as lv



class DataframeFilter(UserInteraction):
    def __init__(self, dataframe, master_u_ids_list=None):
        super().__init__()
        self.dataframe = dataframe
        self.filtered_dataframe = None

        #UserInteraction_obj = UserInteraction()

    def get_column_by_name(self, dataframe, column_name):
        """
        Returns the specified column from a DataFrame.

        :param dataframe: pandas DataFrame.
        :param column_name: The name of the column to retrieve.
        :return: pandas Series representing the specified column.
        """
        return dataframe[column_name]

    def process_column_values(self, column):
        """
        Processes a DataFrame column by concatenating all string values, separating them by commas,
        and then splitting them into individual unique values along with their frequency.

        :param column: pandas Series representing a DataFrame column.
        :return: Dictionary of unique string values with their frequency.
        """
        # Concatenate all non-null values, separated by commas, and split into a list
        concatenated_values = column.dropna().astype(str).str.cat(sep=',')
        # Split the concatenated string into a list
        split_values = concatenated_values.split(',')
        # Count the occurrences of each value
        value_counts = Counter(value.strip() for value in split_values)
        return value_counts

    def process_column_values_backup(self, column):
        """
        Processes a DataFrame column by concatenating all string values, separating them by commas,
        and then splitting them into individual unique values.

        :param column: pandas Series representing a DataFrame column.
        :return: List of unique string values.
        """
        # Concatenate all non-null values, separated by commas, and split into a list
        concatenated_values = column.dropna().astype(str).str.cat(sep=',')
        # Split the concatenated string into a list and get unique values
        unique_values = pd.Series(concatenated_values.split(',')).unique()
        # Strip whitespace and return the list
        return [value.strip() for value in unique_values]


    def filter(self, column, value, include=True):
        if include:
            return self.dataframe[self.dataframe[column].astype(str).str.contains(value)]
        else:
            return self.dataframe[~self.dataframe[column].astype(str).str.contains(value)]

    def user_interaction_filter(self):
        df = self.dataframe if self.filtered_dataframe is None or self.filtered_dataframe.empty else self.dataframe
        selected_column = self.select_column()
        selected_values = self.select_values(df, selected_column)

        # Apply both exclusive and inclusive filters
        filtered_exclusive = self.apply_filter(df, selected_column, selected_values, exclusive=True)
        filtered_inclusive = self.apply_filter(df, selected_column, selected_values, exclusive=False)

        # Present options to the user
        print("\nSelect the DataFrame to keep:")
        print("1: Original DataFrame, size: ", df.shape)
        print("2: Filtered DataFrame (Exclusive), size: ", filtered_exclusive.shape)
        print("3: Filtered DataFrame (Inclusive), size: ", filtered_inclusive.shape)
        choice = int(self.get_user_input("Enter your choice (1, 2, or 3): "))

        if choice == 1:
            self.filtered_dataframe = df
        elif choice == 2:
            self.filtered_dataframe = filtered_exclusive
        elif choice == 3:
            self.filtered_dataframe = filtered_inclusive
        else:
            print("Invalid choice. Keeping the original DataFrame.")

        print("New DataFrame Size: ", self.filtered_dataframe.shape)

        """if self.ask_filter_by_unique_values():
            min_count, max_count = self.get_min_max_counts()
            self.filter_rows_by_unique_values(selected_column, min_count, max_count)"""

    def get_min_max_counts(self):
        min_count = int(self.get_user_input("Enter the minimum number of unique values per row: "))
        max_count = int(self.get_user_input("Enter the maximum number of unique values per row: "))
        return min_count, max_count

    def ask_filter_by_unique_values(self):
        return self.get_user_input(
            "Do you want to filter rows based on the number of unique values? (yes/no): ").lower() == 'yes'

    def select_column(self):
        columns = self.dataframe.columns.tolist()
        for i, column in enumerate(columns, 1):
            print(f"{i}: {column}")
        column_choice = int(self.get_user_input("Enter the number of the column to filter by: ")) - 1
        return columns[column_choice]

    """def select_value(self, selected_column):
        column_data = self.get_column_by_name(self.dataframe, selected_column)
        unique_values_with_freq = self.process_column_values(column_data)
        sorted_items = sorted(unique_values_with_freq.items(), key=lambda item: item[1], reverse=False)
        for i, (value, freq) in enumerate(sorted_items, start=1):
            print(f"{i}: {value} ({freq})")
        value_choice = int(self.get_user_input("Enter the number of the value to filter by: ")) - 1
        if 0 <= value_choice < len(sorted_items):
            return sorted_items[value_choice][0]
        else:
            print("Invalid value number.")
            return None"""

    def select_values(self, df, selected_column):
        column_data = self.get_column_by_name(df, selected_column)
        unique_values_with_freq = self.process_column_values(column_data)
        sorted_items = sorted(unique_values_with_freq.items(), key=lambda item: item[1], reverse=False)

        for i, (value, freq) in enumerate(sorted_items, start=1):
            print(f"{i}: {value} ({freq})")
        value_choices = self.get_user_input("Enter the numbers or ranges (e.g., 1, 3, 5-8, 11) of values to filter by (comma-separated): ")

        selected_values = []
        for choice in value_choices.split(','):
            choice = choice.strip()
            if '-' in choice:
                # Handle range selection with flexible start and stop
                try:
                    start, end = [int(num.strip()) - 1 for num in choice.split('-')]
                    start, end = sorted([start, end])  # Ensure start is less than or equal to end
                    selected_values.extend([sorted_items[i][0] for i in range(start, end + 1)])
                except (ValueError, IndexError):
                    print(f"Invalid range: {choice}. Skipping.")
            else:
                # Handle individual selection
                try:
                    choice_index = int(choice) - 1
                    if 0 <= choice_index < len(sorted_items):
                        selected_values.append(sorted_items[choice_index][0])
                except ValueError:
                    print(f"Invalid selection: {choice}. Skipping.")
        print("Selected values: ", selected_values)
        return selected_values

    """def apply_filter(self, selected_column, selected_value):
        if selected_value is not None:
            filtered_in = self.handle_user_selection(self.dataframe, selected_column, selected_value, filter_in=True)
            filtered_out = self.handle_user_selection(self.dataframe, selected_column, selected_value, filter_in=False)
            print("\nSelect the DataFrame to keep:")
            print("1: Original DataFrame, size: ", self.dataframe.shape)
            print("2: Filtered In DataFrame, size: ", filtered_in.shape)
            print("3: Filtered Out DataFrame, size: ", filtered_out.shape)
            choice = int(self.get_user_input("Enter your choice (1, 2, or 3): "))
            if choice == 1:
                self.filtered_dataframe = self.dataframe
            elif choice == 2:
                self.filtered_dataframe = filtered_in
            elif choice == 3:
                self.filtered_dataframe = filtered_out
            else:
                print("Invalid choice. Keeping the original DataFrame.")
            print("New DataFrame Size: ", self.filtered_dataframe.shape)"""

    """def apply_filter(self, selected_column, selected_values, exclusive=False):
        if selected_values:
            if exclusive:
                # Exclusive: Rows must contain only any of the selected values (and no others)
                mask = self.dataframe[selected_column].apply(
                    lambda x: set(str(x).split(',')).issubset(set(selected_values))
                )
            else:
                # Inclusive: Rows must contain at least one of the selected values (but can contain others)
                mask = self.dataframe[selected_column].apply(
                    lambda x: any(val in str(x).split(',') for val in selected_values)
                )

            filtered_df = self.dataframe[mask]
        else:
            print("No valid values selected. Keeping the original DataFrame.")
            filtered_df = self.dataframe

        self.filtered_dataframe = filtered_df
        print("New DataFrame Size: ", self.filtered_dataframe.shape)"""

    def apply_filter(self, df, selected_column, selected_values, exclusive=False):
        selected_values_set = set(selected_values)  # Convert to set for efficient lookup

        if exclusive:
            # Exclusive: Include rows where every item is in the selected values
            mask = df[selected_column].apply(
                lambda x: set(DataFrameUtility.clean_and_split(x)).issubset(selected_values_set)
            )
        else:
            # Inclusive: Include rows that contain at least one of the selected values
            mask = df[selected_column].apply(
                lambda x: any(val in selected_values_set for val in DataFrameUtility.clean_and_split(x))
            )

        return df[mask]



    def filter_rows_by_unique_values(self, column_name, min_count, max_count):
        def count_unique_values(row):
            #print(type(row))
            if isinstance(row, str):
                values = row.split(',')
                return len(set(values))
            else:
                # Handle the case where the row is not a string (e.g., NaN or numeric)
                return 0  # or some appropriate default value
        df = self.dataframe if self.filtered_dataframe is None or self.filtered_dataframe.empty else self.filtered_dataframe
        mask = df[column_name].apply(count_unique_values).between(min_count, max_count)
        self.filtered_dataframe = df[mask]

    def dataframe_alteration_menu(self):
        while True:
            print("\nDataFrame Alteration Menu:")
            print("1: Filter by Column Value")
            print("2: Filter Rows by Unique Value Count")
            print("3: [Other DataFrame alterations]")
            print("4: Exit Menu")

            choice = self.get_user_input("Enter your choice: ")

            if choice == '1':
                self.user_interaction_filter()
            elif choice == '2':
                self.filter_by_unique_value_count()
            elif choice == '3':
                # Add other DataFrame alteration methods here
                pass
            elif choice == '4':
                print("Exiting menu.")
                break
            else:
                print("Invalid choice. Please try again.")

    def filter_by_unique_value_count(self):
        selected_column = self.select_column()
        if self.ask_filter_by_unique_values():
            min_count, max_count = self.get_min_max_counts()
            self.filter_rows_by_unique_values(selected_column, min_count, max_count)
            print("Filtered DataFrame based on unique value count.")
            print("New DataFrame Size: ", self.filtered_dataframe.shape)

    def handle_user_selection(self, dataframe, column_name, selected_value, filter_in=True):
        def row_contains_value(row, value):
            #print(value)
            """
            Checks if the given value is present in the row. This function can be specialized further
            as needed.

            :param row: The row data from the DataFrame.
            :param value: The value to check for.
            :return: True if the value is found in the row, False otherwise.
            """
            if isinstance(row, str):
                # Assuming the row could have comma-separated values
                values = row.split(',')
                values = [val.strip() for val in values]
                #print(values)
                return value in values
            else:
                # For non-string rows, direct comparison
                return row == value
        """
        Filters the DataFrame based on the user's selection.

        :param dataframe: pandas DataFrame.
        :param column_name: The name of the column to filter by.
        :param selected_value: The value to filter by.
        :param filter_in: If True, keep rows with the value; if False, exclude them.
        :return: A new filtered DataFrame.
        """
        if filter_in:
            # Include rows where the column contains the value
            filtered_df = dataframe[dataframe[column_name].apply(lambda row: row_contains_value(row, selected_value))]
        else:
            # Exclude rows where the column contains the value
            filtered_df = dataframe[~dataframe[column_name].apply(lambda row: row_contains_value(row, selected_value))]

        return filtered_df

        """# Extract unique values
        unique_values = pd.Series(
            self.dataframe[selected_column].dropna().astype(str).str.cat(sep=',').split(',')).unique()

        # Display unique values
        for i, value in enumerate(unique_values, 1):
            print(f"{i}: {value.strip()}")

        # Get user input for value choice
        value_choice = int(input("Enter the number of the value to filter by: ")) - 1
        if value_choice < 0 or value_choice >= len(unique_values):
            print("Invalid selection. Please try again.")
            return

        selected_value = unique_values[value_choice].strip()

        # Ask whether to include or exclude the rows
        filter_option = input("Type 'include' to keep rows with this value or 'exclude' to remove them: ").lower()

        # Filter the DataFrame
        self.filtered_dataframe = self.filter(selected_column, selected_value, include=(filter_option == 'include'))
        print("Filter applied. The filtered DataFrame is stored in 'self.filtered_dataframe'.")
        DataHandler.save_dataframe(dataframe=self.filtered_dataframe, save_as_file_name="filtered_dataframe.csv")"""


    def user_select_value(self, unique_values):
        """
    Prompts the user to select a value from the list of unique values.

    :param unique_values: List of unique values (keys from the dictionary).
    :return: The selected value.
    """
        while True:
            try:
                #user_choice = int(input("Enter the number corresponding to your choice: ")) - 1
                user_choice = int(self.get_user_input( "Enter the number corresponding to your choice: ")) - 1
                if 0 <= user_choice < len(unique_values):
                    return unique_values[user_choice]
                else:
                    print("Invalid selection. Please try again.")
            except ValueError:
                print("Please enter a valid number.")




    """
    Methods for getting information from the dataframe
    """

    def get_search_items(self, dataframe ,search_columns, keep_unique_ids=True, master_u_ids_list=None):
        """
        Extracts values from specified columns of the DataFrame and returns them as a list of tuples.

        :param column_names: List of column names to extract data from.
        :param keep_unique_ids: Boolean flag to determine if unique IDs should be added.
        :return: List of tuples with values from the specified columns.
        """
        if keep_unique_ids:
            if not master_u_ids_list:
                master_u_ids_list = []
            if 'u_id' not in dataframe.columns:
                dataframe['u_id'] = [self.generate_unique_id(master_u_ids_list) for _ in range(len(dataframe))]
            search_columns = ['u_id'] + search_columns

        if not all(column in dataframe.columns for column in search_columns):
            print("One or more specified columns are not in the DataFrame.")
            return []

        search_items = []
        print(search_columns)
        print(len(dataframe))
        print(dataframe.shape)
        for _, row in dataframe.iterrows():
            item = tuple(DataframeFilter.normalize_str([row[column] for column in search_columns]))
            #item = tuple(self.remove_url_unfriendly_characters(str(row[column])) for column in search_columns)
            print(f"item: {item}")
            search_items.append(item)
        print(f"search_items: {search_items}")
        return search_items

    @staticmethod
    def generate_unique_id(exclusion_list=None):
        """Generates a random unique identifier."""
        length = 10  # Adjust the length as needed
        characters = string.ascii_letters + string.digits
        id = ''.join(random.choice(characters) for _ in range(length))
        if exclusion_list is not None:
            while id in exclusion_list:
                id = ''.join(random.choice(characters) for _ in range(length))
        return id

    def get_search_items_backup(self, column_names, keep_unique_ids=True):
        """
        Extracts values from specified columns of the DataFrame and returns them as a list of tuples.

        :param column_names: List of column names to extract data from.
        :return: List of tuples with values from the specified columns.
        """
        if keep_unique_ids:
            # add unique_id to column_names
            column_names = ['u_id'] + column_names

        if not all(column in self.dataframe.columns for column in column_names):
            print(f"One or more specified columns are not in the DataFrame.")
            # add a new column to the dataframe and fill these with random values between 1 and 1000 with alpha characters too
            #self.dataframe['u_id'] = [random.randint(1, 100) for _ in range(len(self.dataframe.index))]
        search_items = []
        for _, row in self.dataframe.iterrows():

            item = tuple(self.remove_url_unfriendly_characters(row[column]) for column in column_names)
            print(f"item: {item}")
            search_items.append(item)

        return search_items

    """
    Methods for comparing strings
    """
    @staticmethod
    def normalize_str(to_normalize_strings):
        """
        Normalizes a list of strings by converting them to lowercase and removing whitespace.
        Also removes any URL-unfriendly characters

        :param to_normalize_strings: List of strings to normalize.
        :return: List of normalized strings.
        """
        normalized_strings = []
        for l_string in to_normalize_strings:
            normalized_string = str(l_string).lower().strip()
            normalized_string = DataframeFilter.remove_url_unfriendly_characters(normalized_string)
            normalized_strings.append(normalized_string)
        return normalized_strings


    @staticmethod
    def remove_url_unfriendly_characters(l_string):
        remove_characters = ['/', '\\', '?', '%', '*', ':', '|', '"', '<', '>', '.']
        for character in remove_characters:
            l_string = l_string.replace(character, '')
        return l_string

    @staticmethod
    def similarity(str1, str2):
        if str1 is None or str2 is None:
            return 0  # Return 0 similarity if either string is None
        return lv.ratio(str(str1).lower(), str(str2).lower())




class DataFrameUtility:
    @staticmethod
    def clean_and_split(x):
        """
        Splits the string by comma and strips spaces from each element.

        :param x: The string to be split and cleaned.
        :return: A list of cleaned strings.
        """
        return [val.strip() for val in str(x).split(',')]

    @staticmethod
    def divide_into_batches( dataframe, batch_size):
        # Split the DataFrame into smaller batches
        batches = [dataframe.iloc[i:i + batch_size] for i in range(0, len(dataframe), batch_size)]
        return batches

