"""
processAllToCSV.py extracts race data from text files and compiles it into a CSV file.

This script processes folders of text files derived from race files, extracts relevant race information from each file, and
saves the data to an output CSV file, all_race_data.csv. It skips files or segments that are invalid, contain cancellation
messages, or have an unsupported race type.

Steps:
    - Splits the text of each file into manageable segments using a predefined delimiter.
    - Skips segments with cancellation messages or invalid race types.
    - Extracts race data using the getRaces function and appends file-level information.
    - Compiles extracted data into a structured format, with sorting and column reordering.
    - Writes the processed data to all_race_data.csv.
    - Logs files that contain no valid data for further review.

Functions:
    - splitText: Splits file content into manageable text segments.
    - process_single_file: Processes a single text file, extracting relevant data.
    - process_files: Iterates through all files in a folder and compiles the data.

Usage:
    Execute the script directly to process files in the 'text_files' folder and output the results to 'all_race_data.csv'.
    Ensure the folder path and output file name are configured as needed before execution.
"""

import os
import pandas as pd
from getRaces import getRaces
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# Set the working directory to the script's location
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

# Thread-safe lock for shared data access
all_data_lock = threading.Lock()

# Global list to store extracted race data
all_data = []

# Global list to track files for which no valid data was found
files_not_found = []


def splitText(full_text):
    """
    Split the input text into segments based on a delimiter.

    Args:
        full_text (str): The full text to be split.

    Returns:
        list: List of text segments.
    """
    return [segment.strip() for segment in full_text.split("All Rights Reserved.")[:-1]]


def process_single_file(file_path):
    """
    Process a single text file, extract relevant race data, and store it.

    Args:
        file_path (str): Path to the text file being processed.
    """
    file_name = os.path.basename(file_path)
    file_number = int(file_name.split("_")[0])

    try:
        # Read the contents of the file
        with open(file_path, "r", encoding="utf-8") as file:
            full_text = file.read()

        # Split the text into segments for processing
        split_texts = splitText(full_text)
        file_data_found = False

        for segment in split_texts:
            # Skip segments containing cancellation messages
            if (
                "Cancelled - Weather" in segment
                or "Cancelled - Management Decision" in segment
                or "Cancelled - Track Conditions" in segment
                or "Cancelled - Equipment Malfunction" in segment
                or "CANCELLED - Thoroughbred" in segment
                or "CANCELLED - Quarter Horse" in segment
                or "CANCELLED" in segment[: segment.find("Race ") + 30]
                or "declared no contest" in segment
            ):
                continue

            # Extract race data from the segment
            data_list = getRaces(segment)

            if data_list == "Invalid Race Type":
                print("Invalid race type found in " + file_path)
                continue

            if isinstance(data_list, list):
                for data in data_list:
                    if isinstance(data, dict):
                        file_data_found = True
                        # Add the file number to each data dictionary
                        data["file_number"] = file_number
                        # Append data to the global list in a thread-safe manner
                        with all_data_lock:
                            all_data.append(data)

        if not file_data_found:
            # Track files with no valid data
            files_not_found.append(file_name)

    except Exception as exc:
        # Print error message for file processing issues
        print(f"Error processing file {file_name}: {exc}")


def process_files(folder_path, output_csv_file):
    """
    Process all text files in the specified folder and write extracted data to a CSV file.

    Args:
        folder_path (str): Path to the folder containing text files.
        output_csv_file (str): Path to the output CSV file.
    """
    if not os.path.isdir(folder_path):
        print(f"The folder {folder_path} does not exist.")
        return

    # Walk through the folder to find all text files
    for root, _, files in os.walk(folder_path):
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [
                executor.submit(process_single_file, os.path.join(root, filename))
                for filename in files
                if filename.endswith(".txt")
            ]

            # Handle exceptions from the futures
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as exc:
                    print(f"Error processing file: {exc}")

    # Sort and write the extracted data to a CSV file
    if all_data:
        sorted_data = sorted(
            all_data,
            key=lambda x: (
                datetime.strptime(
                    x.get("date", "January 1, 1970"), "%B %d, %Y"
                ),
                int(
                    x.get("file_number", 0)
                ),
                int(
                    x.get("race_number", 0)
                ),
            ),
        )
        df = pd.DataFrame(sorted_data)
        # Reorder columns to place file_number and race_number first
        df = df[
            ["file_number", "race_number"]
            + [col for col in df.columns if col not in ["file_number", "race_number"]]
        ]
        df.to_csv(output_csv_file, index=False)
        print(f"Data successfully written to {output_csv_file}")
    else:
        print("No data to write.")

    # Print list of files with no data
    if files_not_found:
        print("No data found for files:")
        for f in files_not_found:
            print(f)


if __name__ == "__main__":
    """
    Main script execution. Processes text files in the 'text_files' folder and
    writes the extracted race data to 'all_race_data.csv'.
    """
    folder_path = "text_files"
    output_csv_file = "all_race_data.csv"
    process_files(folder_path, output_csv_file)
