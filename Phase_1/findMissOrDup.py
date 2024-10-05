import os
import pandas as pd

def find_duplicates(file_path):
    # Set the working directory to the script's location
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)
    
    # Load the Excel file
    df = pd.read_excel(file_path)

    # Check for duplicates by race_id and horse_name
    duplicates = df[df.duplicated(subset=['race_id', 'horse_name'], keep=False)]

    # Print file numbers of duplicates
    if not duplicates.empty:
        duplicate_pairs = duplicates[['file_number', 'race_id', 'horse_name']]
        print("Duplicate entries found:")
        print(duplicate_pairs)
    else:
        print("No duplicates found.")

if __name__ == "__main__":
    file_path = 'select_race_data.xlsx'  # Adjust the file path if needed
    find_duplicates(file_path)
