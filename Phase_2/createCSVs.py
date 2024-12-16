"""
createCSVs.py extracts data from all_race_data.csv into three files, testing.csv, setup.csv, and traintest.csv.

This script takes race data from a large input file, processes it into the required format, and calculates additional fields
like position gains and race speed. Data is divided into three categories:
    - Setup data (before the train-test start year)
    - Train/Test data (from the train-test start year onward)
    - Testing data (specific conditions like dates in August 2022)

Steps:
    - Read data from the input CSV file.
    - Calculate new fields such as position gains, speed, and race IDs.
    - Write rows to the appropriate output file based on the year and testing conditions.
    - Perform column conversions (e.g., time, temperature, and date) using threaded processing.

Functions:
    - extract_year_from_date: Extracts the year from a date string.
    - convert_columns_in_file: Converts specific columns in a CSV file for time, temperature, and date.

Usage:
    Ensure the input file path and output folder paths are correctly configured before running the script.
    Execute the script to generate three output CSV files.
"""
import csv
import os
import threading
import dataMethods as dm
import uuid

# Define cutoff between setup and traintest
train_test_start_year = 2022

# Define folder and file paths
parent_folder = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
phase_1_folder = os.path.join(parent_folder, "Phase_1")
csv_file_path = os.path.join(phase_1_folder, "all_race_data.csv")

phase_2_folder = os.path.join(parent_folder, "Phase_2")
setup_csv_path = os.path.join(phase_2_folder, "setup.csv")
traintest_csv_path = os.path.join(phase_2_folder, "traintest.csv")
testing_csv_path = os.path.join(phase_2_folder, "testing.csv")


def extract_year_from_date(date_string):
    """
    Extract the year from a date string.

    Args:
        date_string (str): The date string in the format 'Month Day, Year'.

    Returns:
        int or None: The extracted year, or None if parsing fails.
    """
    try:
        return int(date_string.split(",")[1].strip())
    except (IndexError, ValueError):
        return None


def convert_columns_in_file(file_path):
    """
    Convert specific columns in a CSV file (e.g., time, temperature, date).

    Args:
        file_path (str): Path to the CSV file to be converted.
    """
    temp_file_path = file_path + "_temp"
    with open(file_path, mode="r", newline="", encoding="utf-8") as infile, open(
        temp_file_path, mode="w", newline="", encoding="utf-8"
    ) as outfile:

        csvreader = csv.reader(infile)
        csvwriter = csv.writer(outfile)

        header = next(csvreader)
        csvwriter.writerow(header)

        for row in csvreader:
            for i in range(11, 24):
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

    os.replace(temp_file_path, file_path)


if __name__ == "__main__":
    """
    Main function to process the CSV file and generate output files for setup, train/test, and testing datasets.
    """
    # Open input and output files
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

        # Write headers with additional fields
        header = next(csvreader)
        header.extend(
            ["pos_gain", "late_pos_gain", "last_pos_gain", "speed", "race_id"]
        )
        setup_writer.writerow(header)
        traintest_writer.writerow(header)
        testing_writer.writerow(header)
        race_id = str(uuid.uuid4())
        prev_file_num_race_num = (-1, -1)

        for row in csvreader:
            # Assign new race_id for each unique file and race number combination
            if (row[0], row[1]) != prev_file_num_race_num:
                race_id = str(uuid.uuid4())
                prev_file_num_race_num = (row[0], row[1])

            # Skip invalid surface types or breeds
            if row[6] not in ["Dirt", "Turf", "AWT"] or row[5] not in [
                "Thoroughbred",
                "Quarter Horse",
            ]:
                continue

            date_string = row[2]
            year = extract_year_from_date(date_string)

            # Calculate speed if valid time is available
            if row[17] == "N/A":
                speed = None
            else:
                speed = float(row[10]) / dm.convertTime(row[17])

            # Clean up position figures
            if row[28] in ["---", "N/A"]:
                row[28] = None

            figs = row[29].split(", ")
            while "---" in figs:
                figs.remove("---")
            figs_length = len(figs)

            # Calculate positional gains
            if figs_length % 2 != 0:
                print(f"Warning: Couldn't parse figs in row: {row}")
            if figs_length < 4:
                pos_gain, late_pos_gain, last_pos_gain = None, None, None
            elif figs_length == 4:
                pos_gain = int(figs[0]) - int(figs[-2])
                late_pos_gain, last_pos_gain = None, None
            elif figs_length == 6:
                pos_gain = int(figs[0]) - int(figs[-2])
                last_pos_gain = int(figs[-4]) - int(figs[-2])
                late_pos_gain = None
            elif figs_length < 10:
                pos_gain = int(figs[0]) - int(figs[-2])
                last_pos_gain = int(figs[-4]) - int(figs[-2])
                late_pos_gain = int(figs[-6]) - int(figs[-2])
            else:
                pos_gain = int(figs[0]) - int(figs[-2])
                last_pos_gain = int(figs[-4]) - int(figs[-2])
                late_pos_gain = int(figs[-8]) - int(figs[-2])

            # Add calculated fields to the row
            row.extend([pos_gain, late_pos_gain, last_pos_gain, speed, race_id])

            # Write rows to the appropriate file
            if date_string.startswith("August") and year == 2022:
                testing_writer.writerow(row)
            elif year is not None:
                if year < train_test_start_year:
                    setup_writer.writerow(row)
                else:
                    traintest_writer.writerow(row)
            else:
                print(f"Warning: Couldn't parse date in row: {row}")

    # Start threaded column conversion
    setup_thread = threading.Thread(target=convert_columns_in_file, args=(setup_csv_path,))
    traintest_thread = threading.Thread(
        target=convert_columns_in_file, args=(traintest_csv_path,)
    )
    testing_thread = threading.Thread(
        target=convert_columns_in_file, args=(testing_csv_path,)
    )

    setup_thread.start()
    traintest_thread.start()
    testing_thread.start()

    setup_thread.join()
    traintest_thread.join()
    testing_thread.join()

    print("CSV processing complete.")
