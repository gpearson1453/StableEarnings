import csv
import os
import threading
import dataMethods as dm
import uuid

# Variable to determine the year cutoff
train_test_start_year = 2022  # Set this to the desired cutoff year

# Define paths
parent_folder = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
phase_1_folder = os.path.join(parent_folder, "Phase_1")
csv_file_path = os.path.join(phase_1_folder, "all_race_data.csv")

phase_2_folder = os.path.join(parent_folder, "Phase_2")
setup_csv_path = os.path.join(phase_2_folder, "setup.csv")
traintest_csv_path = os.path.join(phase_2_folder, "traintest.csv")
testing_csv_path = os.path.join(phase_2_folder, "testing.csv")


# Function to determine the year from a date string in the format 'Month Date, Year'
def extract_year_from_date(date_string):
    try:
        return int(date_string.split(",")[1].strip())  # Extract the year
    except (IndexError, ValueError):
        return None  # Return None if the date is not in the correct format


# Function to convert the columns 12-24 using dm.convertTime
def convert_columns_in_file(file_path):
    temp_file_path = file_path + "_temp"
    with open(file_path, mode="r", newline="", encoding="utf-8") as infile, open(
        temp_file_path, mode="w", newline="", encoding="utf-8"
    ) as outfile:

        csvreader = csv.reader(infile)
        csvwriter = csv.writer(outfile)

        # Process header
        header = next(csvreader)
        csvwriter.writerow(header)

        # Process each row
        for row in csvreader:
            for i in range(11, 24):  # Columns 12-24 are indexed 11-23
                try:
                    row[i] = dm.convertTime(row[i])
                except Exception as e:
                    print(f"Error converting time in row {row}: {e}")
            try:
                row[8] = dm.convertTemp(row[8])
            except Exception as e:
                print(f"Error converting temp in row {row}: {e}")
            try:
                row[2] = dm.convertDate(row[2])
            except Exception as e:
                print(f"Error converting date in row {row}: {e}")
            csvwriter.writerow(row)

    # Replace the original file with the converted one
    os.replace(temp_file_path, file_path)


# Splitting the CSV data first
with open(csv_file_path, mode="r", newline="", encoding="utf-8") as csvfile, open(
    setup_csv_path, mode="w", newline="", encoding="utf-8"
) as setup_csvfile, open(
    traintest_csv_path, mode="w", newline="", encoding="utf-8"
) as traintest_csvfile, open(
    testing_csv_path, mode="w", newline="", encoding="utf-8"
) as testing_csvfile:

    csvreader = csv.reader(csvfile)
    setup_writer = csv.writer(setup_csvfile)
    traintest_writer = csv.writer(traintest_csvfile)
    testing_writer = csv.writer(testing_csvfile)

    # Process the header
    header = next(csvreader)
    header.extend(
        ["pos_gain", "late_pos_gain", "last_pos_gain", "speed", "race_id"]
    )  # Add new columns to header
    setup_writer.writerow(header)
    traintest_writer.writerow(header)
    testing_writer.writerow(header)
    race_id = str(uuid.uuid4())
    prev_file_num_race_num = (-1, -1)

    # Process each row and split based on year and month
    for row in csvreader:
        if (row[0], row[1]) != prev_file_num_race_num:
            race_id = str(uuid.uuid4())
            prev_file_num_race_num = (row[0], row[1])

        # Check conditions for track surface and horse type
        if row[6] not in ["Dirt", "Turf", "AWT"] or row[5] not in [
            "Thoroughbred",
            "Quarter Horse",
        ]:
            continue  # Skip rows that don't meet the criteria

        date_string = row[2]  # Assuming the date is in the third column (index 2)
        year = extract_year_from_date(date_string)

        # Calculate new column values
        if row[17] == "N/A":
            speed = None
        else:
            speed = float(row[10]) / dm.convertTime(row[17])

        if row[28] in ["---", "N/A"]:
            row[28] = None

        figs = row[29].split(", ")
        while "---" in figs:
            figs.remove("---")
        l = len(figs)
        if l % 2 != 0:
            print(f"Warning: Couldn't parse figs in row: {row}")
        if l < 4:
            pos_gain, late_pos_gain, last_pos_gain = None, None, None
        elif l == 4:
            pos_gain = int(figs[0]) - int(figs[-2])
            late_pos_gain, last_pos_gain = None, None
        elif l == 6:
            pos_gain = int(figs[0]) - int(figs[-2])
            last_pos_gain = int(figs[-4]) - int(figs[-2])
            late_pos_gain = None
        elif l < 10:
            pos_gain = int(figs[0]) - int(figs[-2])
            last_pos_gain = int(figs[-4]) - int(figs[-2])
            late_pos_gain = int(figs[-6]) - int(figs[-2])
        else:
            pos_gain = int(figs[0]) - int(figs[-2])
            last_pos_gain = int(figs[-4]) - int(figs[-2])
            late_pos_gain = int(figs[-8]) - int(figs[-2])

        # Add new values to the row
        row.extend([pos_gain, late_pos_gain, last_pos_gain, speed, race_id])

        # Write the row to the appropriate file
        if date_string.startswith("August") and year == 2020:
            testing_writer.writerow(row)
        elif year is not None:
            if year < train_test_start_year:
                setup_writer.writerow(row)
            else:
                traintest_writer.writerow(row)
        else:
            print(f"Warning: Couldn't parse date in row: {row}")

# Now that the CSVs are split, apply threading to handle the conversion
setup_thread = threading.Thread(target=convert_columns_in_file, args=(setup_csv_path,))
traintest_thread = threading.Thread(
    target=convert_columns_in_file, args=(traintest_csv_path,)
)
testing_thread = threading.Thread(
    target=convert_columns_in_file, args=(testing_csv_path,)
)

# Start all threads
setup_thread.start()
traintest_thread.start()
testing_thread.start()

# Wait for all threads to complete
setup_thread.join()
traintest_thread.join()
testing_thread.join()

print("CSV processing complete.")
