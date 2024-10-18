import csv
import os

# Variable to determine the year cutoff
train_test_start_year = 2022  # Set this to the desired cutoff year

# Define paths
parent_folder = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
phase_1_folder = os.path.join(parent_folder, 'Phase_1')
csv_file_path = os.path.join(phase_1_folder, 'all_race_data.csv')

phase_2_folder = os.path.join(parent_folder, 'Phase_2')
setup_csv_path = os.path.join(phase_2_folder, 'setup.csv')
traintest_csv_path = os.path.join(phase_2_folder, 'traintest.csv')

# Function to determine the year from a date string in the format 'Month Date, Year'
def extract_year_from_date(date_string):
    try:
        return int(date_string.split(",")[1].strip())  # Extract the year
    except (IndexError, ValueError):
        return None  # Return None if the date is not in the correct format

# Open the input and output files
with open(csv_file_path, mode='r', newline='', encoding='utf-8') as csvfile, \
     open(setup_csv_path, mode='w', newline='', encoding='utf-8') as setup_csvfile, \
     open(traintest_csv_path, mode='w', newline='', encoding='utf-8') as traintest_csvfile:

    csvreader = csv.reader(csvfile)
    setup_writer = csv.writer(setup_csvfile)
    traintest_writer = csv.writer(traintest_csvfile)

    # Process the header
    header = next(csvreader)
    setup_writer.writerow(header)  # Write the header to both CSV files
    traintest_writer.writerow(header)

    # Process each row
    for row in csvreader:
        date_string = row[2]  # Assuming the date is in the third column (index 2)
        year = extract_year_from_date(date_string)
        
        if year is not None:
            if year < train_test_start_year:
                setup_writer.writerow(row)
            else:
                traintest_writer.writerow(row)
        else:
            print(f"Warning: Couldn't parse date in row: {row}")

print("CSV processing complete. Data split into setup.csv and traintest.csv.")
