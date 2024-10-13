import os
import pandas as pd
import re

# Set the working directory to the script's location
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

def load_excel(file_path):
    """
    Loads the Excel file into a pandas DataFrame.
    
    Args:
        file_path (str): The path to the Excel file.

    Returns:
        pd.DataFrame: DataFrame containing the contents of the Excel file.
    """
    try:
        df = pd.read_excel(file_path)
        return df
    except FileNotFoundError:
        print(f"File not found: {file_path}")
        return None

def find_duplicates(df):
    """
    Finds and prints duplicate entries in the DataFrame based on 'race_id' and 'horse_name'.
    
    Args:
        df (pd.DataFrame): DataFrame containing race data.
    """
    # Identify duplicates based on race_id and horse_name
    duplicates = df[df.duplicated(subset=['race_id', 'horse_name'], keep=False)]
    
    if not duplicates.empty:
        # If duplicates exist, print the file number, race ID, and horse name for the duplicates
        print("Duplicate entries found:")
        duplicate_pairs = duplicates[['file_number', 'race_id', 'horse_name']]
        print(duplicate_pairs)
    else:
        print("No duplicates found.")

def extract_date_from_text(text_block):
    """
    Extracts the date from the first line of the given text block.
    
    Args:
        text_block (str): Text block where the first line contains the date.

    Returns:
        str: Formatted date in 'Month Day, Year' format if successful, otherwise None.
    """
    lines = text_block.strip().splitlines()
    date_line = lines[0].strip()  # First line is assumed to be the date
    date_pattern = r'(\w+), (\w+) (\d{1,2}) (\d{4})'
    
    # Match the date pattern
    match = re.match(date_pattern, date_line)
    if match:
        day_name, month, day, year = match.groups()
        return f'{month} {day}, {year}'
    else:
        print("Invalid date format in the text block.")
        return None

def check_tracks_in_excel(df, text_block):
    """
    Checks if each track name from the text block is present in the DataFrame for the corresponding date.
    
    Args:
        df (pd.DataFrame): DataFrame containing race data.
        text_block (str): Text block where the first line is a date and the subsequent lines are track names.
    """
    missing = False
    
    # Extract the date from the text block
    formatted_date = extract_date_from_text(text_block)
    if not formatted_date:
        return

    # Extract the track names from the text block (all lines after the date)
    track_names = [line.strip() for line in text_block.strip().splitlines()[1:]]

    # Check if each track name exists in the DataFrame for the corresponding date
    for track in track_names:
        search_pattern = f'{formatted_date}_{track}'
        if not df['race_id'].str.contains(search_pattern, na=False).any():
            print(f"No match found for {search_pattern}")
            missing = True
            
    return missing

def main(file_path, date_track_blocks):
    """
    Main function to load the Excel file, check for duplicates, and verify tracks in the text blocks.
    
    Args:
        file_path (str): Path to the Excel file.
        date_track_blocks (list): List of text blocks, each containing a date and track names.
    """
    # Load the Excel file
    df = load_excel(file_path)
    if df is None:
        return

    any_missing = False
    
    # Check for track name matches based on date and track blocks
    for text_block in date_track_blocks:
        if check_tracks_in_excel(df, text_block):
            any_missing = True
    if not any_missing:
        print("No files missing.")

    # Check for duplicates in the DataFrame
    find_duplicates(df)

if __name__ == "__main__":
    # Define the path to the Excel file
    file_path = 'select_race_data.xlsx'
    
    # Define the list of text blocks with dates and track names (currently empty, to be populated later)
    date_track_blocks = ['''Monday, January 1 2024
AQUEDUCT
CAMARERO RACE TRACK
FAIR GROUNDS
GOLDEN GATE FIELDS
GULFSTREAM PARK
MAHONING VALLEY RACE COURSE
OAKLAWN PARK
PARX RACING
SANTA ANITA PARK''',
'''Tuesday, January 2 2024
MAHONING VALLEY RACE COURSE
PARX RACING''',
'''Wednesday, January 3 2024
HOLLYWOOD CASINO AT CHARLES TOWN RACES
MAHONING VALLEY RACE COURSE
PARX RACING
TAMPA BAY DOWNS
TURFWAY PARK''',
'''Thursday, January 4 2024
AQUEDUCT
DELTA DOWNS
FAIR GROUNDS
GULFSTREAM PARK
HOLLYWOOD CASINO AT CHARLES TOWN RACES
MAHONING VALLEY RACE COURSE
TURFWAY PARK''',
'''Friday, January 5 2024
AQUEDUCT
CAMARERO RACE TRACK
DELTA DOWNS
FAIR GROUNDS
GOLDEN GATE FIELDS
GULFSTREAM PARK
HOLLYWOOD CASINO AT CHARLES TOWN RACES
LAUREL PARK
OAKLAWN PARK
SAM HOUSTON RACE PARK
SANTA ANITA PARK
SUNLAND PARK
TAMPA BAY DOWNS
TURFWAY PARK''',
'''Saturday, January 6 2024
AQUEDUCT
CAMARERO RACE TRACK
DELTA DOWNS
FAIR GROUNDS
GOLDEN GATE FIELDS
GULFSTREAM PARK
HOLLYWOOD CASINO AT CHARLES TOWN RACES
LAUREL PARK
LOS ALAMITOS
OAKLAWN PARK
SAM HOUSTON RACE PARK
SANTA ANITA PARK
SUNLAND PARK
TAMPA BAY DOWNS
TURFWAY PARK''',
'''Sunday, January 7 2024
AQUEDUCT
CAMARERO RACE TRACK
FAIR GROUNDS
GOLDEN GATE FIELDS
GULFSTREAM PARK
LAUREL PARK
LOS ALAMITOS
OAKLAWN PARK
SAM HOUSTON RACE PARK
SANTA ANITA PARK
SUNLAND PARK
TAMPA BAY DOWNS''',
'''Monday, January 8 2024
MAHONING VALLEY RACE COURSE
PARX RACING''',
'''Tuesday, January 9 2024
LOUISIANA DOWNS
MAHONING VALLEY RACE COURSE
PARX RACING''',
'''Wednesday, January 10 2024
GULFSTREAM PARK
HOLLYWOOD CASINO AT CHARLES TOWN RACES
LOUISIANA DOWNS
MAHONING VALLEY RACE COURSE
PARX RACING
PENN NATIONAL
TAMPA BAY DOWNS
TURFWAY PARK''',
'''Thursday, January 11 2024
AQUEDUCT
CAMARERO RACE TRACK
DELTA DOWNS
FAIR GROUNDS
GULFSTREAM PARK
HOLLYWOOD CASINO AT CHARLES TOWN RACES
LOUISIANA DOWNS
MAHONING VALLEY RACE COURSE
PENN NATIONAL
SUNLAND PARK
TURFWAY PARK''',
'''Friday, January 12 2024
AQUEDUCT
CAMARERO RACE TRACK
DELTA DOWNS
FAIR GROUNDS
GOLDEN GATE FIELDS
GULFSTREAM PARK
HOLLYWOOD CASINO AT CHARLES TOWN RACES
LAUREL PARK
OAKLAWN PARK
PENN NATIONAL
SAM HOUSTON RACE PARK
SANTA ANITA PARK
SUNLAND PARK
TAMPA BAY DOWNS
TURFWAY PARK''',
'''Saturday, January 13 2024
AQUEDUCT
CAMARERO RACE TRACK
DELTA DOWNS
FAIR GROUNDS
GOLDEN GATE FIELDS
GULFSTREAM PARK
HOLLYWOOD CASINO AT CHARLES TOWN RACES
LAUREL PARK
LOS ALAMITOS
OAKLAWN PARK
SAM HOUSTON RACE PARK
SANTA ANITA PARK
SUNLAND PARK
TAMPA BAY DOWNS
TURFWAY PARK''',
'''Sunday, January 14 2024
AQUEDUCT
CAMARERO RACE TRACK
GOLDEN GATE FIELDS
GULFSTREAM PARK
LAUREL PARK
LOS ALAMITOS
OAKLAWN PARK
SAM HOUSTON RACE PARK
SANTA ANITA PARK
SUNLAND PARK
TAMPA BAY DOWNS''',
'''Monday, January 15 2024
CAMARERO RACE TRACK
FAIR GROUNDS
GOLDEN GATE FIELDS
MAHONING VALLEY RACE COURSE
PARX RACING
SAM HOUSTON RACE PARK
SANTA ANITA PARK''',
'''Tuesday, January 16 2024
LOUISIANA DOWNS
MAHONING VALLEY RACE COURSE
PARX RACING''',
'''Wednesday, January 17 2024
GULFSTREAM PARK
HOLLYWOOD CASINO AT CHARLES TOWN RACES
LOUISIANA DOWNS
MAHONING VALLEY RACE COURSE
PARX RACING
PENN NATIONAL
TAMPA BAY DOWNS
TURFWAY PARK''',
'''Thursday, January 18 2024
AQUEDUCT
DELTA DOWNS
FAIR GROUNDS
GULFSTREAM PARK
HOLLYWOOD CASINO AT CHARLES TOWN RACES
LOUISIANA DOWNS
MAHONING VALLEY RACE COURSE
PENN NATIONAL
SUNLAND PARK
TURFWAY PARK''',
'''Friday, January 19 2024
AQUEDUCT
CAMARERO RACE TRACK
DELTA DOWNS
FAIR GROUNDS
GOLDEN GATE FIELDS
GULFSTREAM PARK
HOLLYWOOD CASINO AT CHARLES TOWN RACES
LAUREL PARK
OAKLAWN PARK
PENN NATIONAL
SAM HOUSTON RACE PARK
SANTA ANITA PARK
SUNLAND PARK
TAMPA BAY DOWNS
TURFWAY PARK''',
'''Saturday, January 20 2024
AQUEDUCT
CAMARERO RACE TRACK
DELTA DOWNS
FAIR GROUNDS
GOLDEN GATE FIELDS
GULFSTREAM PARK
HOLLYWOOD CASINO AT CHARLES TOWN RACES
LAUREL PARK
LOS ALAMITOS
OAKLAWN PARK
SAM HOUSTON RACE PARK
SANTA ANITA PARK
SUNLAND PARK
TAMPA BAY DOWNS
TURFWAY PARK''',
'''Sunday, January 21 2024
AQUEDUCT
CAMARERO RACE TRACK
FAIR GROUNDS
GOLDEN GATE FIELDS
GULFSTREAM PARK
LAUREL PARK
LOS ALAMITOS
OAKLAWN PARK
SAM HOUSTON RACE PARK
SANTA ANITA PARK
SUNLAND PARK
TAMPA BAY DOWNS''',
'''Monday, January 22 2024
MAHONING VALLEY RACE COURSE
PARX RACING''',
'''Tuesday, January 23 2024
LOUISIANA DOWNS
MAHONING VALLEY RACE COURSE
PARX RACING''',
'''Wednesday, January 24 2024
GULFSTREAM PARK
HOLLYWOOD CASINO AT CHARLES TOWN RACES
LOUISIANA DOWNS
MAHONING VALLEY RACE COURSE
PARX RACING
PENN NATIONAL
TAMPA BAY DOWNS
TURFWAY PARK''',
'''Thursday, January 25 2024
CAMARERO RACE TRACK
DELTA DOWNS
FAIR GROUNDS
GULFSTREAM PARK
HOLLYWOOD CASINO AT CHARLES TOWN RACES
LOUISIANA DOWNS
MAHONING VALLEY RACE COURSE
PENN NATIONAL
SUNLAND PARK
TURFWAY PARK''',
'''Friday, January 26 2024
AQUEDUCT
CAMARERO RACE TRACK
DELTA DOWNS
FAIR GROUNDS
GOLDEN GATE FIELDS
GULFSTREAM PARK
HOLLYWOOD CASINO AT CHARLES TOWN RACES
LAUREL PARK
OAKLAWN PARK
PENN NATIONAL
SAM HOUSTON RACE PARK
SANTA ANITA PARK
SUNLAND PARK
TAMPA BAY DOWNS
TURFWAY PARK''',
'''Saturday, January 27 2024
AQUEDUCT
CAMARERO RACE TRACK
DELTA DOWNS
FAIR GROUNDS
GOLDEN GATE FIELDS
GULFSTREAM PARK
HOLLYWOOD CASINO AT CHARLES TOWN RACES
LAUREL PARK
LOS ALAMITOS
OAKLAWN PARK
SAM HOUSTON RACE PARK
SANTA ANITA PARK
SUNLAND PARK
TAMPA BAY DOWNS
TURFWAY PARK''',
'''Sunday, January 28 2024
AQUEDUCT
CAMARERO RACE TRACK
FAIR GROUNDS
GOLDEN GATE FIELDS
GULFSTREAM PARK
LAUREL PARK
LOS ALAMITOS
OAKLAWN PARK
SAM HOUSTON RACE PARK
SANTA ANITA PARK
SUNLAND PARK
TAMPA BAY DOWNS''',
'''Monday, January 29 2024
MAHONING VALLEY RACE COURSE
PARX RACING
TURF PARADISE''',
'''Tuesday, January 30 2024
LOUISIANA DOWNS
MAHONING VALLEY RACE COURSE
PARX RACING
TURF PARADISE''',
'''Wednesday, January 31 2024
GULFSTREAM PARK
HOLLYWOOD CASINO AT CHARLES TOWN RACES
LOUISIANA DOWNS
MAHONING VALLEY RACE COURSE
PARX RACING
PENN NATIONAL
TAMPA BAY DOWNS
TURF PARADISE
TURFWAY PARK''']

    # Run the main function
    main(file_path, date_track_blocks)
