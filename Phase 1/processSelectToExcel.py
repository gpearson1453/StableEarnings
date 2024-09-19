import os
import pandas as pd
from getRaces import getRaces
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from mappings import stupid_horse_names, fixed, save_mappings

# A lock to control access to shared data (all_data) to avoid race conditions during multi-threading
all_data_lock = threading.Lock()

# A shared list to store all data extracted from the files
all_data = []

# A list to keep track of files where no data was found
files_not_found = []

def splitText(full_text):
    """
    Splits the full text of a file into segments based on the 'All Rights Reserved.' marker, 
    trimming extra spaces from each segment.

    Parameters:
    - full_text (str): The entire text content of the file.

    Returns:
    - list: A list of cleaned text segments.
    """
    return [segment.strip() for segment in full_text.split('All Rights Reserved.')[:-1]]

def apply_stupid_horse_name_fixes(folder_path):
    """
    Replace problematic horse names in text files based on the stupid_horse_names dictionary
    and track the replaced names in the 'fixed' dictionary. The changes are saved to a JSON file.

    Parameters:
    - folder_path (str): The path of the folder containing text files to be processed.
    """
    for file_name in os.listdir(folder_path):
        if file_name.endswith(".txt") and file_name in stupid_horse_names:
            file_path = os.path.join(folder_path, file_name)

            with open(file_path, 'r', encoding='utf-8') as text_file:
                file_content = text_file.read()

            replacements_made = []
            for old_name, new_name in stupid_horse_names[file_name]:
                file_content = file_content.replace(old_name, new_name)
                replacements_made.append((old_name, new_name))

            with open(file_path, 'w', encoding='utf-8') as text_file:
                text_file.write(file_content)

            if file_name not in fixed:
                fixed[file_name] = []
            fixed[file_name].extend(replacements_made)

            del stupid_horse_names[file_name]
            save_mappings({"stupid_horse_names": stupid_horse_names, "fixed": fixed})

            print(f"Replaced names in: {file_name}")

def process_single_file(file_path):
    """
    Processes a single text file by reading its content, splitting it into segments,
    and extracting relevant race data using the getRaces function.

    Parameters:
    - file_path (str): The path of the text file to be processed.
    """
    file_name = os.path.basename(file_path)
    file_number = int(file_name.split('_')[0])  # Extract the file number from the file name

    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            full_text = file.read()

        split_texts = splitText(full_text)
        file_data_found = False

        for segment in split_texts:
            if 'Cancelled - Weather' in segment or 'Cancelled - Management Decision' in segment:
                continue  # Skip canceled events

            data_list = getRaces(segment)

            if data_list == 'Invalid Race Type':
                continue  # Skip invalid race types

            if isinstance(data_list, list):
                for data in data_list:
                    if isinstance(data, dict):
                        file_data_found = True
                        data['file_number'] = file_number
                        with all_data_lock:
                            all_data.append(data)

        if not file_data_found:
            files_not_found.append(file_name)

    except Exception as exc:
        print(f"Error processing file {file_name}: {exc}")

def process_files(folder_path, output_excel_file):
    """
    Processes all text files in a specified folder, extracts data using process_single_file, 
    and writes the results to an Excel file.

    Parameters:
    - folder_path (str): The path of the directory containing the text files.
    - output_excel_file (str): The name of the output Excel file where extracted data will be saved.
    """
    if not os.path.isdir(folder_path):
        print(f"The folder {folder_path} does not exist.")
        return

    apply_stupid_horse_name_fixes(folder_path)

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(process_single_file, os.path.join(folder_path, filename))
                   for filename in os.listdir(folder_path) if filename.endswith(".txt")]

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as exc:
                print(f"Error processing file: {exc}")

    if all_data:
        sorted_data = sorted(all_data, key=lambda x: (x.get('file_number', 0), x.get('race_number', 0)))
        df = pd.DataFrame(sorted_data)
        df = df[['file_number', 'race_number'] + [col for col in df.columns if col not in ['file_number', 'race_number']]]
        
        with pd.ExcelWriter(output_excel_file, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False)
            worksheet = writer.sheets['Sheet1']
            worksheet.freeze_panes(1, 0)  # Freeze the first row for better readability
        print(f"Data successfully written to {output_excel_file}")
    else:
        print("No data to write.")

    if files_not_found:
        print("No data found for files:")
        for f in files_not_found:
            print(f)

if __name__ == "__main__":
    folder_path = r'text_files\January2020'
    output_excel_file = 'select_race_data.xlsx'
    process_files(folder_path, output_excel_file)
