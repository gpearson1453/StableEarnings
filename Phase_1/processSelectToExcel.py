import os
import pandas as pd
from getRaces import getRaces
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

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

def process_single_file(file_path):
    file_name = os.path.basename(file_path)
    file_number = int(file_name.split('_')[0])

    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            full_text = file.read()

        split_texts = splitText(full_text)
        file_data_found = False

        for segment in split_texts:
            if ('Cancelled - Weather' in segment 
                or 'Cancelled - Management Decision' in segment 
                or 'Cancelled - Track Conditions' in segment
                or 'Cancelled - Equipment Malfunction' in segment
                or 'CANCELLED - Thoroughbred' in segment
                or 'CANCELLED - Quarter Horse' in segment
                or 'declared no contest' in segment):
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

def process_files(folder_path, output_excel_file):
    if not os.path.isdir(folder_path):
        print(f"The folder {folder_path} does not exist.")
        return

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
            worksheet.freeze_panes(1, 0)
        print(f"Data successfully written to {output_excel_file}")
    else:
        print("No data to write.")

    if files_not_found:
        print("No data found for files:")
        for f in files_not_found:
            print(f)

if __name__ == "__main__":
    #folder_path = r'text_files\2022-07-B'
    #folder_path = 'testing_files'
    folder_path = r'text_files\temp'
    output_excel_file = 'select_race_data.xlsx'
    process_files(folder_path, output_excel_file)
