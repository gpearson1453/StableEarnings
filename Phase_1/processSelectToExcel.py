"""
processSelectToExcel.py extracts selected race data from text files and compiles it into an Excel file.

This script processes a folder of text files derived from race files, extracts relevant race information from each file,
and saves the data to an output Excel file, select_race_data.xlsx. It skips files or segments that are invalid, contain
cancellation messages, or have an unsupported race type.

Steps:
    - Splits the text of each file into manageable segments using a predefined delimiter.
    - Skips segments with cancellation messages or invalid race types.
    - Extracts race data using the getRaces function and appends file-level information.
    - Compiles extracted data into a structured format, with sorting and column reordering.
    - Writes the processed data to select_race_data.xlsx with frozen panes for improved readability.
    - Logs files that contain no valid data for further review.

Functions:
    - splitText: Splits file content into manageable text segments.
    - process_single_file: Processes a single text file, extracting relevant data.
    - process_files: Iterates through all files in a folder and compiles the data into an Excel file.

Usage:
    Execute the script directly to process files in the specified folder and output the results to select_race_data.xlsx.
    Ensure the folder path and output file name are configured as needed before execution.
"""
import os
import pandas as pd
from getRaces import getRaces
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

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


def process_files(folder_path, output_excel_file):
    """
    Process all text files in the specified folder and write extracted data to an Excel file.

    Args:
        folder_path (str): Path to the folder containing text files.
        output_excel_file (str): Path to the output Excel file.
    """
    if not os.path.isdir(folder_path):
        print(f"The folder {folder_path} does not exist.")
        return

    # Walk through the folder to find all text files
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [
            executor.submit(process_single_file, os.path.join(folder_path, filename))
            for filename in os.listdir(folder_path)
            if filename.endswith(".txt")
        ]

        # Handle exceptions from the futures
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as exc:
                print(f"Error processing file: {exc}")

    # Sort and write the extracted data to an Excel file
    if all_data:
        sorted_data = sorted(
            all_data, key=lambda x: (x.get("file_number", 0), x.get("race_number", 0))
        )
        df = pd.DataFrame(sorted_data)
        # Reorder columns to place file_number and race_number first
        df = df[
            ["file_number", "race_number"]
            + [col for col in df.columns if col not in ["file_number", "race_number"]]
        ]

        with pd.ExcelWriter(output_excel_file, engine="xlsxwriter") as writer:
            df.to_excel(writer, index=False)
            worksheet = writer.sheets["Sheet1"]
            worksheet.freeze_panes(1, 0)  # Freeze the header row
        print(f"Data successfully written to {output_excel_file}")
    else:
        print("No data to write.")

    # Print list of files with no data
    if files_not_found:
        print("No data found for files:")
        for f in files_not_found:
            print(f)


if __name__ == "__main__":
    """
    Main script execution. Processes text files in the specified folder and
    writes the extracted race data to an Excel file.
    """
    folder_path = r"text_files\2020-06"
    # folder_path = 'testing_files'
    # folder_path = r'text_files\temp'
    output_excel_file = "select_race_data.xlsx"
    process_files(folder_path, output_excel_file)
