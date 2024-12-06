import os
import pandas as pd
from getRaces import getRaces
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

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
    return [segment.strip() for segment in full_text.split("All Rights Reserved.")[:-1]]


def process_single_file(file_path):
    file_name = os.path.basename(file_path)
    file_number = int(file_name.split("_")[0])

    try:
        with open(file_path, "r", encoding="utf-8") as file:
            full_text = file.read()

        split_texts = splitText(full_text)
        file_data_found = False

        for segment in split_texts:
            if (
                "Cancelled - Weather" in segment
                or "Cancelled - Management Decision" in segment
                or "Cancelled - Track Conditions" in segment
                or "Cancelled - Equipment Malfunction" in segment
                or "CANCELLED - Thoroughbred" in segment
                or "CANCELLED - Quarter Horse" in segment
                or "CANCELLED" in segment[: segment.find("Race ") + 30]
                or "declared no contest" in segment
            ):
                continue

            data_list = getRaces(segment)

            if data_list == "Invalid Race Type":
                print("Invalid race type found in " + file_path)
                continue

            if isinstance(data_list, list):
                for data in data_list:
                    if isinstance(data, dict):
                        file_data_found = True
                        data["file_number"] = file_number
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
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [
                executor.submit(process_single_file, os.path.join(root, filename))
                for filename in files
                if filename.endswith(".txt")
            ]

            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as exc:
                    print(f"Error processing file: {exc}")

    if all_data:
        sorted_data = sorted(
            all_data,
            key=lambda x: (
                datetime.strptime(
                    x.get("date", "January 1, 1970"), "%B %d, %Y"
                ),  # Parse 'Month Date, Year' format
                int(
                    x.get("file_number", 0)
                ),  # Ensure file_number is treated as an integer
                int(
                    x.get("race_number", 0)
                ),  # Ensure race_number is treated as an integer
            ),
        )
        df = pd.DataFrame(sorted_data)
        df = df[
            ["file_number", "race_number"]
            + [col for col in df.columns if col not in ["file_number", "race_number"]]
        ]
        df.to_csv(output_csv_file, index=False)
        print(f"Data successfully written to {output_csv_file}")
    else:
        print("No data to write.")

    if files_not_found:
        print("No data found for files:")
        for f in files_not_found:
            print(f)


if __name__ == "__main__":
    folder_path = "text_files"
    output_csv_file = "all_race_data.csv"
    process_files(folder_path, output_csv_file)
