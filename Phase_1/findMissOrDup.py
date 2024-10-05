import os
import pandas as pd
import re

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

def check_tracks_in_excel(file_path, text_block):
    # Set the working directory to the script's location
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)

    # Load the Excel file
    df = pd.read_excel(file_path)

    # Split the text block into lines
    lines = text_block.strip().splitlines()
    
    # Extract the date from the first line
    date_line = lines[0].strip()
    date_pattern = r'(\w+), (\w+) (\d{1,2}) (\d{4})'
    match = re.match(date_pattern, date_line)
    if not match:
        print("Invalid date format")
        return
    day_name, month, day, year = match.groups()
    formatted_date = f'{month} {day}, {year}'

    # Check for each track name
    track_names = [line.strip() for line in lines[1:]]  # All lines after the date
    for track in track_names:
        search_pattern = f'{formatted_date}_{track}'
        if not df['race_id'].str.contains(search_pattern, na=False).any():
            print(f"No match found for {search_pattern}")

if __name__ == "__main__":
    file_path = 'select_race_data.xlsx'  # Adjust the file path if needed
    date_track_blocks = []
    
    for text_block in date_track_blocks:
        check_tracks_in_excel(file_path, text_block)
        
    find_duplicates(file_path)
