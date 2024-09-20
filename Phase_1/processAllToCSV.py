import os
import pandas as pd
from getRaces import getRaces
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from mappings import stupid_horse_names, fixed, save_mappings

# Set the working directory to the script's location
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

# A lock to control access to shared data (all_data) to avoid race conditions during multi-threading
all_data_lock = threading.Lock()

# A shared list to store all data extracted from the files
all_data = []

# A list to keep track of files where no data was found
files_not_found = []

def splitText(full_text):
    return [segment.strip() for segment in full_text.split('All Rights Reserved.')[:-1]]

def apply_stupid_horse_name_fixes(folder_path):
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
    file_name = os.path.basename(file_path)
    file_number = int(file_name.split('_')[0])

    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            full_text = file.read()

        split_texts = splitText(full_text)
        file_data_found = False

        for segment in split_texts:
            if 'Cancelled - Weather' in segment or 'Cancelled - Management Decision' in segment:
                continue

            data_list = getRaces(segment)

            if data_list == 'Invalid Race Type':
                continue

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

def process_files(folder_path, output_csv_file):
    if not os.path.isdir(folder_path):
        print(f"The folder {folder_path} does not exist.")
        return

    for root, _, files in os.walk(folder_path):
        apply_stupid_horse_name_fixes(root)

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(process_single_file, os.path.join(root, filename))
                       for filename in files if filename.endswith(".txt")]

            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as exc:
                    print(f"Error processing file: {exc}")

    if all_data:
        sorted_data = sorted(all_data, key=lambda x: (x.get('file_number', 0), x.get('race_number', 0)))
        df = pd.DataFrame(sorted_data)
        df = df[['file_number', 'race_number'] + [col for col in df.columns if col not in ['file_number', 'race_number']]]
        df.to_csv(output_csv_file, index=False)
        print(f"Data successfully written to {output_csv_file}")
    else:
        print("No data to write.")

    if files_not_found:
        print("No data found for files:")
        for f in files_not_found:
            print(f)

if __name__ == "__main__":
    folder_path = 'text_files'
    output_csv_file = 'all_race_data.csv'
    process_files(folder_path, output_csv_file)
