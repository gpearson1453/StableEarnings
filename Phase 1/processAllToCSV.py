import os
import pandas as pd
from getRaces import getRaces
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from mappings import stupid_horse_names, fixed, save_mappings  # Import the stupid_horse_names and fixed dictionaries and save_mappings

# A lock to control access to shared data (all_data) to avoid race conditions during multi-threading
all_data_lock = threading.Lock()

# A shared list to store all data extracted from the files
all_data = []

# A list to keep track of files where no data was found
files_not_found = []

def splitText(full_text):
    """
    Split the full text of a text file into segments based on the `split_phrase`, trimming extra spaces from each segment.

    Returns a list of cleaned segments.
    """
    return [segment.strip() for segment in full_text.split('All Rights Reserved.')[:-1]]

def apply_stupid_horse_name_fixes(folder_path):
    """
    Replace problematic horse names in text files based on the stupid_horse_names dictionary
    and remove replaced names from the dictionary. Track the replaced names in the `fixed` dictionary.
    """
    for file_name in os.listdir(folder_path):
        if file_name.endswith(".txt") and file_name in stupid_horse_names:
            file_path = os.path.join(folder_path, file_name)

            # Read the current content of the text file
            with open(file_path, 'r', encoding='utf-8') as text_file:
                file_content = text_file.read()

            # Apply all the replacements for the current file
            replacements_made = []
            for old_name, new_name in stupid_horse_names[file_name]:
                file_content = file_content.replace(old_name, new_name)
                replacements_made.append((old_name, new_name))

            # Write the modified content back to the file
            with open(file_path, 'w', encoding='utf-8') as text_file:
                text_file.write(file_content)

            # After replacements, move the replaced names to the `fixed` dictionary
            if file_name not in fixed:
                fixed[file_name] = []

            fixed[file_name].extend(replacements_made)

            # Remove the replaced names from `stupid_horse_names`
            del stupid_horse_names[file_name]

            # Save updated mappings to JSON
            save_mappings({"stupid_horse_names": stupid_horse_names, "fixed": fixed})

            print(f"Replaced names in: {file_name}")


def process_single_file(file_path):
    """
    Process a single text file by reading its content, splitting it into segments, 
    and calling getRaces on each segment to extract relevant data.
    """
    file_name = os.path.basename(file_path)
    file_number = int(file_name.split('_')[0])  # Extract file number from the file name

    try:
        # Open and read the entire text file content
        with open(file_path, 'r', encoding='utf-8') as file:
            full_text = file.read()

        # Split the full text into segments using the splitText function
        split_texts = splitText(full_text)

        file_data_found = False  # Flag to indicate if any valid data is found in this file

        # Process each text segment, skipping any that mention cancellation
        for segment in split_texts:
            if 'Cancelled - Weather' in segment or 'Cancelled - Management Decision' in segment:
                continue  # Skip segments that refer to canceled events

            # Extract race data from the segment using getRaces
            data_list = getRaces(segment)

            if data_list == 'Invalid Race Type':
                continue  # Skip invalid race types

            if isinstance(data_list, list):
                # Iterate over each dictionary (race data) in the data_list
                for data in data_list:
                    if isinstance(data, dict):  # Ensure the item is a dictionary
                        file_data_found = True  # Mark that valid data was found
                        data['file_number'] = file_number  # Add the file number to the data
                        # Append the data to the global list using a lock to avoid race conditions
                        with all_data_lock:
                            all_data.append(data)

        # If no data was found, record the file name
        if not file_data_found:
            files_not_found.append(file_name)

    except Exception as exc:
        print(f"Error processing file {file_name}: {exc}")

def process_files(folder_path, output_csv_file):
    """
    Process all text files in all folders inside the specified directory, extract data using the process_single_file method, 
    and write the results to a CSV file.
    """
    if not os.path.isdir(folder_path):
        print(f"The folder {folder_path} does not exist.")
        return

    # Recursively process files in all subdirectories
    for root, _, files in os.walk(folder_path):
        apply_stupid_horse_name_fixes(root)

        # Use multi-threading to process files concurrently
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = []
            for filename in files:
                if filename.endswith(".txt"):  # Expect text files
                    file_path = os.path.join(root, filename)
                    futures.append(executor.submit(process_single_file, file_path))

            for future in as_completed(futures):
                try:
                    future.result()  # Raise exceptions if any occurred during file processing
                except Exception as exc:
                    print(f"Error processing file: {exc}")

    # If data has been successfully extracted, write it to a CSV file
    if all_data:
        # Sort the extracted data by file number and race number
        sorted_data = sorted(all_data, key=lambda x: (x.get('file_number', 0), x.get('race_number', 0)))

        # Convert the sorted data into a Pandas DataFrame
        df = pd.DataFrame(sorted_data)

        # Reorder columns to ensure 'file_number' is first and 'race_number' is second
        df = df[['file_number', 'race_number'] + [col for col in df.columns if col not in ['file_number', 'race_number']]]

        # Write the DataFrame to a CSV file
        df.to_csv(output_csv_file, index=False)
        print(f"Data successfully written to {output_csv_file}")
    else:
        print("No data to write.")

    # Print out files that had no data if applicable
    if files_not_found:
        print("No data found for files:")
        for f in files_not_found:
            print(f)

if __name__ == "__main__":
    folder_path = 'text_files'
    output_csv_file = 'all_race_data.csv'
    process_files(folder_path, output_csv_file)
