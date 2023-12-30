from DatabaseManager import DatabaseManager
from prompt_toolkit import prompt

from prompt_toolkit.history import InMemoryHistory
from prompt_toolkit import prompt
from prompt_toolkit.history import InMemoryHistory
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
from prompt_toolkit.styles import Style
from prompt_toolkit.shortcuts import CompleteStyle
from prompt_toolkit.completion import WordCompleter
import inspect


class UserInteraction(DatabaseManager):
    def __init__(self):

        super().__init__()
        self.history = InMemoryHistory()
        self.fancy_prompt=False

    def load_history_into_prompt(self, method_name, call_location):
        history_data = self.load_user_interaction_history(method_name, call_location)
       # print(history_data)
        #print(method_name)
        for input_line in history_data:
            self.history.append_string(input_line)

    # create a basic get_user_input method that will be used for testing withoutr fancy prompt


    def get_user_input(self, prompt_message):
        method_name = self.get_contextual_method_name()
        call_location = self.get_call_location()
        print(f"Call Location: {call_location} and Method Name: {method_name}")

        if self.fancy_prompt:
            self.load_history_into_prompt(method_name, call_location)

            # Define a custom style for the prompt
            style = Style.from_dict({
                'prompt': 'ansigreen',
                '': 'ansiblue'
            })

            # Use WordCompleter for auto-completion based on history
            completer = WordCompleter([record for record in self.history.get_strings()], ignore_case=True)
            #completer = WordCompleter(['test'], ignore_case=True)

            # Prompt with auto-suggestions, styling, and auto-completion
            user_input = prompt(
                prompt_message,
                history=self.history,
                auto_suggest=AutoSuggestFromHistory(),
                style=style,
                completer=completer,
                complete_style=CompleteStyle.MULTI_COLUMN
            )
            self.save_user_interaction(method_name, call_location, user_input)
        else:
            user_input = input(prompt_message)
        return user_input

    def select_csv_file(self, csv_files):
        if not csv_files:
            print("No CSV files found in the current directory.")
            return None

        for i, file in enumerate(csv_files, 1):
            print(f"{i}: {file}")

        file_index = int(self.get_user_input("Enter the number of the CSV file to load: ")) - 1

        if 0 <= file_index < len(csv_files):
            method_name = self.get_contextual_method_name()
            call_location = self.get_call_location()
            print("call location")
            print(call_location)
            self.save_user_interaction(method_name, call_location, csv_files[file_index])
            return csv_files[file_index]
        else:
            print("Invalid file number.")
            return None