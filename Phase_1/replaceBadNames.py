"""
replaceBadNames.py replaces specific strings in text files within a folder hierarchy.

This script processes all text files in a specified folder (and its subfolders) and replaces occurrences
of a target string with a replacement string. It supports multi-threaded processing for improved performance.

Steps:
    - Iterates through a folder and its subfolders to locate all .txt files.
    - Replaces the target string with the replacement string in each file's content.
    - Provides two modes: replacing specific string pairs or standardizing names by replacing spaces with hyphens.

Functions:
    - replace_string_in_file: Handles the replacement of a target string in a single file.
    - process_folder: Processes all .txt files in a folder and its subfolders.

Usage:
    Configure the folder to search, the target string, and the replacement string before running the script.
    For bulk replacements, provide a list of tuples (target, replacement) or names for standardization.
"""
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

# Set the working directory to the script's location
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)


def replace_string_in_file(file_path, target_string, replacement_string):
    """
    Replace occurrences of a target string with a replacement string in a text file.

    Args:
        file_path (str): Path to the text file.
        target_string (str): String to be replaced.
        replacement_string (str): String to replace the target string.
    """
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            file_contents = f.read()

        # Only replace strings after a specific section in the file
        if (
            target_string
            in file_contents[
                file_contents.find("Past Performance Running Line Preview"):
            ]
        ):
            updated_contents = file_contents.replace(target_string, replacement_string)

            if updated_contents != file_contents:
                with open(file_path, "w", encoding="utf-8") as f:
                    f.write(updated_contents)

                print(f"Replaced '{target_string}' in {file_path}")

    except Exception as e:
        print(f"Error processing {file_path} with target string '{target_string}': {e}")


def process_folder(folder_name, target_string, replacement_string):
    """
    Process all text files in a folder and replace occurrences of a target string.

    Args:
        folder_name (str): Name of the folder to process.
        target_string (str): String to be replaced.
        replacement_string (str): String to replace the target string.
    """
    folder_path = os.path.join(os.getcwd(), folder_name)
    tasks = []

    with ThreadPoolExecutor() as executor:
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                if file.endswith(".txt"):
                    file_path = os.path.join(root, file)
                    tasks.append(
                        executor.submit(
                            replace_string_in_file,
                            file_path,
                            target_string,
                            replacement_string,
                        )
                    )

        for future in as_completed(tasks):
            future.result()


if __name__ == "__main__":
    """
    Main script execution. Processes a folder of text files, replacing specified strings. Configure 'folder_to_search',
    'target_string', and 'replacement_string' before running.
    """
    folder_to_search = "text_files"

    # Replace specific strings with a replacement in bulk
    both_names = []  # List of tuples [(target, replacement), ...]
    names = []  # List of names to standardize by replacing spaces with hyphens

    # Standardize names by replacing spaces with hyphens
    with ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(process_folder, folder_to_search, name, name.replace(" ", "-"))
            for name in names
        ]

        for future in as_completed(futures):
            future.result()

    # Process specific target-replacement pairs
    """with ThreadPoolExecutor() as executor:
        futures = [executor.submit(process_folder, folder_to_search, name[0], name[1]) for name in both_names]

        # Wait for all tasks to complete
        for future in as_completed(futures):
            future.result()"""
