"""
addTrainTest.py processes traintest.csv (or testing.csv) to populate the database with trainable and testable data.

This script reads data from a CSV file, builds caches for weather and track state encoding, and inserts
rows into the database in batches. Data is split into training and testing datasets based on a calculated test ratio. The
script also normalizes data values, encodes weather and track state values, and handles progress tracking.

Unlike addSetup.py, which processes all track data first, then performances and races, then horses, etc., this file adds all
new data and updates concurrently. This is because a training or testing data point must be created using data from other
fields from a specific point in time. For example, if we are trying to train the model to predict a horse's performance in a
certain race, we cannot train the model using the horse's win rate at the end of that year because the model will not have
access to that type of future data when it is being used to predict an upcoming race. Instead, training and testing data
should contain all entities' statistical profiles at the time just before the race, which is when the model would actualy be
used to make the predictions. Therefore, updates to the database must happen for each race, rather than processing all data
for a certain entity type at once and then moving on to the next.

Steps:
    - Reset relevant database tables if specified.
    - Build caches for weather and track state encodings.
    - Calculate the ratio for testable rows based on odds data.
    - Process rows in the CSV file and insert data into Trainables and Testables tables.
    - Batch process queries to optimize database insertion.

Functions:
    - buildCaches: Builds caches for weather and track state encodings.
    - getTestRatio: Calculates the ratio for determining test data rows.
    - encodeWeather: Encodes normalized weather data.
    - encodeTrackState: Encodes normalized track state data.
    - pushBatches: Processes and commits batches of database queries.
    - formatTime: Formats elapsed time into hours, minutes, and seconds.
    - clockCheck: Displays progress updates based on rows processed.
    - addTrainTestToDB: Main function to process the traintest.csv file and populate the database.

Usage:
    Execute this script to populate the database with trainable and testable data.
    Ensure the path to traintest.csv is correct before running the script.
"""
import csv
import dataMethods as dm
import os
import time
from decimal import Decimal
import queue
import random

# Set script directory to ensure relative paths work correctly
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

# Global variables for batch processing and caching
conn = None  # Database connection object
batch_queue = queue.Queue()  # Queue to hold batches of queries
start_time = time.time()  # Start time for progress tracking
total_rows = -1  # Total rows in the CSV file
weather_cache = {}  # Cache for weather encoding
valid_weather_num = 0  # Counter for valid weather entries
track_state_cache = {}  # Cache for track state encoding
valid_track_state_num = 0  # Counter for valid track state entries
test_ratio = 0.0  # Ratio for determining test data rows


def buildCaches(csv_reader):
    """
    Build caches for weather and track state encoding based on their occurrences in the data.

    Args:
        csv_reader (csv.DictReader): Reader for the CSV file to process rows.
    """
    global weather_cache, valid_weather_num, track_state_cache, valid_track_state_num

    prev_file_num_race_num = (-1, -1)

    for row in csv_reader:

        # We only want to update the cache with each new race because all rows pertaining to the same race automatically
        # have the same weather and track_state.
        if (row["file_number"], row["race_number"]) != prev_file_num_race_num:

            w = dm.normalize(row["weather"])
            ts = dm.normalize(row["track_state"])

            # Both encodeWeather and encodeTrackState will rely on their respective caches to determine encodings, where a
            # cache entry is of the form:
            # key: weather or track_state value (w or ts)
            # value: [int, int] ([weather or track state count, encoding value])

            # If the length of w <= 2, there was most likely an error, meaning this weather is invalid and won't be added to
            # the cache, so we can just skip it.
            if len(w) > 2:

                # If w is already in the weather cache, we want to increment the count for that weather type by one.
                if w in weather_cache:

                    weather_cache[w][0] += 1

                    # If the count for this weather type is now 5, this is enough occurrences of a weather type for it to be
                    # considered valid, so we will increment valid_weather_num and assign this new number to be the encoded
                    # weather value.
                    if weather_cache[w][0] == 5:

                        valid_weather_num += 1

                        weather_cache[w][1] = valid_weather_num

                # If w is not in the weather cache, then we can add it with a count of 1 and an encoded value of 0, which
                # will be changed when enough occurrences are found.
                else:
                    weather_cache[w] = [1, 0]

            # If the length of ts <= 2, there was most likely an error, meaning this track state is invalid and won't be
            # added to the cache, so we can just skip it.
            if len(ts) > 2:

                # If ts is already in the track state cache, we want to increment the count for that track state type by one.
                if ts in track_state_cache:

                    track_state_cache[ts][0] += 1

                    # If the count for this track state type is now 5, this is enough occurrences of a track state type for
                    # it to be considered valid, so we will increment valid_track_state_num and assign this new number to be
                    # the encoded track state value.
                    if track_state_cache[ts][0] == 5:

                        valid_track_state_num += 1

                        track_state_cache[ts][1] = valid_track_state_num

                # If ts is not in the track state cache, then we can add it with a count of 1 and an encoded value of 0,
                # which will be changed when enough occurrences are found.
                else:
                    track_state_cache[ts] = [1, 0]

            # Update prev_file_num_race_num to represent this race to avoid adding more weather and track state values for
            # this race.
            prev_file_num_race_num = (row["file_number"], row["race_number"])


def getTestRatio(csv_reader):
    """
    Calculate the ratio of testable rows based on the presence of odds data.

    The data should be divided into roughly 80% training data and 20% testing data. However, all testing data must have a
    valid odds field, and some races don't have odds listed for the horses, so data from these races must automatically be
    designated as training data. This method then calculates the ratio of the remaining data, which do have valid odds
    fields, that will be designated as testing data in order for the ratio of all the data to remain roughly 80-20.

    Args:
        csv_reader (csv.DictReader): Reader for the CSV file to process rows.

    Returns:
        float: Ratio of test data rows.
    """
    prev_file_num_race_num = (-1, -1)
    odds_count = 0
    total = 0

    for row in csv_reader:
        if (row["file_number"], row["race_number"]) != prev_file_num_race_num:
            if row["odds"] != "N/A":
                odds_count += 1
            total += 1
            prev_file_num_race_num = (row["file_number"], row["race_number"])

    return (0.2 * total) / odds_count


def encodeWeather(normalized_w):
    """
    Encode weather data based on the weather cache.

    Args:
        normalized_w (str): Normalized weather string.

    Returns:
        int: Encoded weather value.
    """
    # If the length of normalized_w <= 2, there was most likely an error, meaning this weather is invalid and gets an
    # encoded value of 0.
    if len(normalized_w) <= 2:
        return 0
    else:
        return weather_cache[normalized_w][1]


def encodeTrackState(normalized_ts):
    """
    Encode track state data based on the track state cache.

    Args:
        normalized_ts (str): Normalized track state string.

    Returns:
        int: Encoded track state value.
    """
    # If the length of normalized_ts <= 2, there was most likely an error, meaning this track state is invalid and gets an
    # encoded value of 0.
    if len(normalized_ts) <= 2:
        return 0
    else:
        return track_state_cache[normalized_ts][1]


def pushBatches():
    """
    Process and commit batches of database queries stored in the batch queue.

    This function handles and gives an approximate location of the problematic row in the csv file and retries database
    connection if necessary.
    """
    global batch_queue, total_rows, start_time
    b_total = 0
    conn = dm.local_connect("StableEarnings")  # Establish connection to the database
    start_time = time.time()  # Reset the start time
    try:
        while not batch_queue.empty():
            try:
                with conn.cursor() as cur:
                    b, row_num = batch_queue.get()  # Retrieve the batch and row number
                    b_total += len(b)
                    for query, params in b:
                        cur.execute(query, params)  # Execute each query in the batch
                    conn.commit()  # Commit the batch to the database
                    if row_num:
                        if b_total >= 50000:
                            print(f"{b_total} queries processed.")
                            clockCheck(row_num, total_rows)  # Display progress
                            b_total = 0
                    else:
                        print("Reset processed.")

            except Exception as e:
                conn.rollback()  # Rollback in case of an error
                print(f"Error occurred in batch: {e} at around row {row_num}")
                if conn.closed:
                    conn = dm.connect_db("defaultdb")  # Reconnect if the connection is closed

    finally:
        if conn and not conn.closed:
            conn.close()
            print("Connection closed after batch processing.")


def formatTime(seconds):
    """
    Format elapsed time in hours, minutes, and seconds.

    Args:
        seconds (float): Elapsed time in seconds.

    Returns:
        str: Formatted time string.
    """
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    seconds = int(seconds % 60)
    return f"{hours}h {minutes}m {seconds}s"


def clockCheck(row_num, total_rows):
    """
    Display progress information based on elapsed time and rows processed.

    Args:
        row_num (int): Number of rows processed so far.
        total_rows (int): Total number of rows to process.
    """
    elapsed_time = time.time() - start_time
    rows_per_sec = row_num / elapsed_time
    estimated_time_left = (total_rows - row_num) / rows_per_sec

    formatted_elapsed_time = formatTime(elapsed_time)
    formatted_time_left = formatTime(estimated_time_left)

    print(
        f"Rows processed: {row_num}/{total_rows}.     " +
        f"Time elapsed: {formatted_elapsed_time}     Estimated time remaining: {formatted_time_left}"
    )


def addTrainTestToDB(file_path, reset):
    """
    Process traintest.csv to add trainable and testable data to the database.

    Args:
        file_path (str): Path to the CSV file.
        reset (bool): If True, reset relevant database tables before processing.
    """
    global batch_queue, total_rows, weather_cache, track_state_cache, test_ratio
    try:
        print("Adding Trainables, Testables, and Updates to DB.")

        batch = []  # Initialize a batch for queries

        if reset:
            # Drop and recreate the Trainables and Testables tables if reset is True
            batch.append((dm.dropTrainables(), None))
            batch.append((dm.createTrainables(), None))
            batch_queue.put((batch.copy(), None))
            batch.clear()
            print("Trainables table reset.")
            batch.append((dm.dropTestables(), None))
            batch.append((dm.createTestables(), None))
            batch_queue.put((batch.copy(), None))
            batch.clear()
            print("Testables table reset.")

        # Count total rows in the file for progress tracking
        with open(file_path, mode="r") as csv_file:
            total_rows = sum(1 for row in csv_file) - 1

        # Reopen file to execute buildCaches(), getTestRatio(), and to read data rows
        with open(file_path, mode="r") as csv_file:
            csv_reader = csv.DictReader(csv_file)  # Create csv_reader for buildCaches()

            buildCaches(csv_reader)  # Build weather and track state caches

            # Recreate csv_reader for getTestRatio()
            csv_file.seek(0)
            csv_reader = csv.DictReader(csv_file)

            test_ratio = getTestRatio(csv_reader)

            # Recreate csv_reader for data addition
            csv_file.seek(0)
            csv_reader = csv.DictReader(csv_file)

            prev_file_num_race_num = (-1, -1)  # Track the last file number  and race number processed
            prev_file_num = -1  # Track the last file number processed

            for row_num, row in enumerate(csv_reader, start=1):
                # If batch reaches size 1000, process the batch
                if len(batch) >= 1000:
                    batch_queue.put((batch.copy(), row_num))
                    batch.clear()

                track_n_name = dm.normalize(row["location"])  # Normalize track location name
                owner_n_name = dm.normalize(row["owner"])  # Normalize owner name
                horse_n_name = dm.normalize(row["horse_name"])  # Normalize horse name
                jockey_n_name = dm.normalize(row["jockey"])  # Normalize jockey name
                trainer_n_name = dm.normalize(row["trainer"])  # Normalize trainer name

                if (row["file_number"], row["race_number"]) != prev_file_num_race_num:
                    # Use test ratio to determine if this race will be used for training or testing
                    test = random.random() < test_ratio and row["odds"] != "N/A"

                    # Add or update a Race
                    batch.append(
                        dm.addRace(
                            row["race_id"],
                            row["file_number"],
                            track_n_name,
                            row["race_number"],
                            row["date"],
                            row["race_type"],
                            row["surface"],
                            row["weather"],
                            row["temp"],
                            row["track_state"],
                            Decimal(row["distance(miles)"]),
                            Decimal(row["final_time"]) if row["final_time"] else None,
                            Decimal(row["speed"]) if row["speed"] else None,
                            (
                                Decimal(row["fractional_a"])
                                if row["fractional_a"]
                                else None
                            ),
                            (
                                Decimal(row["fractional_b"])
                                if row["fractional_b"]
                                else None
                            ),
                            (
                                Decimal(row["fractional_c"])
                                if row["fractional_c"]
                                else None
                            ),
                            (
                                Decimal(row["fractional_d"])
                                if row["fractional_d"]
                                else None
                            ),
                            (
                                Decimal(row["fractional_e"])
                                if row["fractional_e"]
                                else None
                            ),
                            (
                                Decimal(row["fractional_f"])
                                if row["fractional_f"]
                                else None
                            ),
                            Decimal(row["split_a"]) if row["split_a"] else None,
                            Decimal(row["split_b"]) if row["split_b"] else None,
                            Decimal(row["split_c"]) if row["split_c"] else None,
                            Decimal(row["split_d"]) if row["split_d"] else None,
                            Decimal(row["split_e"]) if row["split_e"] else None,
                            Decimal(row["split_f"]) if row["split_f"] else None,
                        )
                    )
                    prev_file_num_race_num = (row["file_number"], row["race_number"])

                if test:
                    # Add a Testable
                    batch.append(
                        dm.addTestable(
                            horse_n_name,
                            track_n_name,
                            jockey_n_name,
                            trainer_n_name,
                            owner_n_name,
                            row["surface"],
                            row["race_id"],
                            row["final_pos"],
                            row["race_type"],
                            Decimal(row["weight"]) if row["weight"] else None,
                            encodeWeather(dm.normalize(row["weather"])),
                            row["temp"],
                            encodeTrackState(dm.normalize(row["track_state"])),
                            Decimal(row["distance(miles)"]),
                            Decimal(row["odds"]) if row["odds"] != "N/A" else None,
                        )
                    )
                else:
                    # Add a Trainable
                    batch.append(
                        dm.addTrainable(
                            horse_n_name,
                            track_n_name,
                            jockey_n_name,
                            trainer_n_name,
                            owner_n_name,
                            row["surface"],
                            row["race_id"],
                            row["final_pos"],
                            row["race_type"],
                            Decimal(row["weight"]) if row["weight"] else None,
                            encodeWeather(dm.normalize(row["weather"])),
                            row["temp"],
                            encodeTrackState(dm.normalize(row["track_state"])),
                            Decimal(row["distance(miles)"]),
                            Decimal(0) if int(row["final_pos"]) == 1 else Decimal(100),
                            Decimal(0) if int(row["final_pos"]) <= 2 else Decimal(100),
                            Decimal(0) if int(row["final_pos"]) <= 3 else Decimal(100),
                        )
                    )

                if row["file_number"] != prev_file_num:
                    # Add or update a Track
                    batch.append(
                        dm.addTrack(
                            row["location"],
                            track_n_name,
                            (
                                Decimal(row["distance(miles)"])
                                / Decimal(row["final_time"])
                                if row["final_time"]
                                else None
                            ),
                        )
                    )
                    prev_file_num = row["file_number"]

                # Add or update a Horse
                batch.append(
                    dm.addHorse(
                        row["horse_name"],
                        horse_n_name,
                        Decimal(row["final_pos"]),
                        Decimal(row["pos_factor"]) if row["pos_factor"] else None,
                        Decimal(row["pos_gain"]) if row["pos_gain"] else None,
                        Decimal(row["late_pos_gain"]) if row["late_pos_gain"] else None,
                        Decimal(row["last_pos_gain"]) if row["last_pos_gain"] else None,
                        Decimal(row["speed"]) if row["speed"] else None,
                        track_n_name,
                        row["surface"],
                        Decimal(row["distance(miles)"]),
                        Decimal(row["total_horses"]),
                    )
                )

                # Add or update a Jockey
                batch.append(
                    dm.addJockey(
                        row["jockey"],
                        jockey_n_name,
                        Decimal(row["pos_gain"]) if row["pos_gain"] else None,
                        Decimal(row["late_pos_gain"]) if row["late_pos_gain"] else None,
                        Decimal(row["last_pos_gain"]) if row["last_pos_gain"] else None,
                        Decimal(row["final_pos"]),
                        Decimal(row["pos_factor"]) if row["pos_factor"] else None,
                        Decimal(row["speed"]) if row["speed"] else None,
                        track_n_name,
                    )
                )

                # Add or update a Trainer
                batch.append(
                    dm.addTrainer(
                        row["trainer"],
                        trainer_n_name,
                        Decimal(row["final_pos"]),
                        Decimal(row["pos_factor"]) if row["pos_factor"] else None,
                        Decimal(row["speed"]) if row["speed"] else None,
                        track_n_name,
                        row["surface"],
                        Decimal(row["distance(miles)"]),
                        Decimal(row["total_horses"]),
                    )
                )

                # Add or update a Owner
                batch.append(
                    dm.addOwner(
                        row["owner"],
                        owner_n_name,
                        Decimal(row["final_pos"]),
                        Decimal(row["pos_factor"]) if row["pos_factor"] else None,
                        Decimal(row["speed"]) if row["speed"] else None,
                        track_n_name,
                    )
                )

                # Add a Performance
                batch.append(
                    dm.addPerformance(
                        row["race_id"],
                        row["file_number"],
                        row["date"],
                        row["race_number"],
                        track_n_name,
                        horse_n_name,
                        row["program_number"],
                        Decimal(row["weight"]) if row["weight"] else None,
                        None if row["odds"] == "N/A" else Decimal(row["odds"]),
                        row["start_pos"] if row["start_pos"] else None,
                        Decimal(row["final_pos"]),
                        jockey_n_name,
                        trainer_n_name,
                        owner_n_name,
                        Decimal(row["pos_gain"]) if row["pos_gain"] else None,
                        Decimal(row["late_pos_gain"]) if row["late_pos_gain"] else None,
                        Decimal(row["last_pos_gain"]) if row["last_pos_gain"] else None,
                        Decimal(row["pos_factor"]) if row["pos_factor"] else None,
                        Decimal(row["speed"]) if row["speed"] else None,
                        "TESTING" if test else "TRAINING",
                    )
                )

                # Add or update an Owner-Trainer relationship
                batch.append(
                    dm.addOwnerTrainer(
                        owner_n_name,
                        trainer_n_name,
                        Decimal(row["final_pos"]),
                        Decimal(row["pos_factor"]) if row["pos_factor"] else None,
                        Decimal(row["speed"]) if row["speed"] else None,
                        track_n_name,
                    )
                )

                # Add or update a Horse-Track relationship
                batch.append(
                    dm.addHorseTrack(
                        horse_n_name,
                        track_n_name,
                        Decimal(row["final_pos"]),
                        Decimal(row["pos_factor"]) if row["pos_factor"] else None,
                        Decimal(row["speed"]) if row["speed"] else None,
                        row["surface"],
                    )
                )

                # Add or update a Jockey-Trainer relationship
                batch.append(
                    dm.addJockeyTrainer(
                        jockey_n_name,
                        trainer_n_name,
                        Decimal(row["final_pos"]),
                        Decimal(row["pos_factor"]) if row["pos_factor"] else None,
                        Decimal(row["speed"]) if row["speed"] else None,
                        track_n_name,
                    )
                )

                # Add or update a Horse-Jockey relationship
                batch.append(
                    dm.addHorseJockey(
                        horse_n_name,
                        jockey_n_name,
                        Decimal(row["final_pos"]),
                        Decimal(row["pos_factor"]) if row["pos_factor"] else None,
                        Decimal(row["speed"]) if row["speed"] else None,
                        track_n_name,
                    )
                )

                # Add or update a Horse-Trainer relationship
                batch.append(
                    dm.addHorseTrainer(
                        horse_n_name,
                        trainer_n_name,
                        Decimal(row["final_pos"]),
                        Decimal(row["pos_factor"]) if row["pos_factor"] else None,
                        Decimal(row["speed"]) if row["speed"] else None,
                        track_n_name,
                    )
                )

                # Add or update a Trainer-Track relationship
                batch.append(
                    dm.addTrainerTrack(
                        trainer_n_name,
                        track_n_name,
                        Decimal(row["final_pos"]),
                        Decimal(row["pos_factor"]) if row["pos_factor"] else None,
                        Decimal(row["speed"]) if row["speed"] else None,
                        row["surface"],
                    )
                )

            # Process any remaining queries in the batch
            if batch:
                batch_queue.put((batch.copy(), row_num))
                batch.clear()

        # Add fixes for bad testables to the end of the batch
        batch.append((dm.fixPerformances(), None))
        batch.append((dm.copyBadTestables(), None))
        batch.append((dm.deleteBadTestables(), None))
        batch_queue.put((batch.copy(), row_num))
        batch.clear()

    finally:
        pushBatches()  # Commit all remaining batches to the database


if __name__ == "__main__":
    """
    Main script execution for adding training and testing data to the database.

    This section executes all functions to populate the database tables with data from traintest.csv or testing.csv.
    """
    addTrainTestToDB("traintest.csv", True)
