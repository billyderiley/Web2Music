import pandas as pd
from collections import Counter
from ScrapeDataHandler import DataHandler
class DataframeFilter:
    def __init__(self, dataframe):
        self.dataframe = dataframe
        self.filtered_dataframe = None

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
        # Display columns and get user choice
        columns = self.dataframe.columns.tolist()
        for i, column in enumerate(columns, 1):
            print(f"{i}: {column}")

        column_choice = int(input("Enter the number of the column to filter by: ")) - 1
        selected_column = columns[column_choice]
        column_data = self.get_column_by_name(self.dataframe, selected_column)
        unique_values_with_freq = self.process_column_values(column_data)

        # Get the total number of items
        total_items = len(unique_values_with_freq)

        # Sort the items by frequency in descending order
        sorted_items = sorted(unique_values_with_freq.items(), key=lambda item: item[1], reverse=True)

        # Print the sorted unique values with frequency
        for i, (value, freq) in enumerate(sorted_items, start=1):
            print(f"{i}: {value} ({freq})")

        # Get user's selection based on the index
        selected_value = self.user_select_value(list(unique_values_with_freq.keys()))
        filtered_in = self.handle_user_selection(self.dataframe, selected_column, selected_value, filter_in=True)
        filtered_out = self.handle_user_selection(self.dataframe, selected_column, selected_value, filter_in=False)

        # Prompt user to select which DataFrame to keep
        print("\nSelect the DataFrame to keep:")
        print("1: Original DataFrame, size: ", self.dataframe.shape)
        print("2: Filtered In DataFrame, size: ", filtered_in.shape)
        print("3: Filtered Out DataFrame, size: ", filtered_out.shape)
        choice = int(input("Enter your choice (1, 2, or 3): "))

        if choice == 1:
            self.filtered_dataframe = self.dataframe
        elif choice == 2:
            self.filtered_dataframe = filtered_in
        elif choice == 3:
            self.filtered_dataframe = filtered_out
        else:
            print("Invalid choice. Keeping the original DataFrame.")

        print("New DataFrame Size: ", self.dataframe.shape)

    def handle_user_selection(self, dataframe, column_name, selected_value, filter_in=True):
        def row_contains_value(row, value):
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
                user_choice = int(input("Enter the number corresponding to your choice: ")) - 1
                if 0 <= user_choice < len(unique_values):
                    return unique_values[user_choice]
                else:
                    print("Invalid selection. Please try again.")
            except ValueError:
                print("Please enter a valid number.")


