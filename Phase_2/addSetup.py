"""
addSetup.py processes setup.csv (or testing.csv) to populate the database with setup data.

This script adds setup data, such as tracks, horses, jockeys, trainers, owners, races, and relationships between these
entities. Data is read from a CSV file and inserted into the corresponding database tables in batches for efficient
processing. Setup data is added before training and testing data to allow for horses, jockeys, etc. to have established
statistical profiles before the model is trained on how those profiles compare to each other.

Steps:
    - Optionally reset specific database tables before inserting new data.
    - Process rows from a CSV file in batches of 1000.
    - Normalize and transform data where needed (e.g., locations, horse names).
    - Add data for tracks, horses, jockeys, trainers, owners, races, and entity relationships.
    - Commit batches to the database to optimize performance and avoid memory overload.

Functions:
    - addTracksToDB: Adds track data to the database.
    - addHorsesToDB: Adds horse data to the database.
    - addJockeysToDB: Adds jockey data to the database.
    - addTrainersToDB: Adds trainer data to the database.
    - addOwnersToDB: Adds owner data to the database.
    - addRacesAndPerformancesToDB: Adds race and performance data to the database.
    - addOwnerTrainerToDB: Adds owner-trainer relationship data to the database.
    - addHorseTrackToDB: Adds horse-track relationship data to the database.
    - addJockeyTrainerToDB: Adds jockey-trainer relationship data to the database.
    - addHorseJockeyToDB: Adds horse-jockey relationship data to the database.
    - addHorseTrainerToDB: Adds horse-trainer relationship data to the database.
    - addTrainerTrackToDB: Adds trainer-track relationship data to the database.

Usage:
    Execute this script to populate the database with setup data. Ensure the CSV file path is correct.
"""
import csv
import dataMethods as dm
import os
import time
from decimal import Decimal
import queue

# Set script directory to ensure relative paths work correctly
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

# Global variables for batch processing
conn = None  # Database connection object
batch_queue = queue.Queue()  # Queue to hold batches of queries for processing
start_time = time.time()  # Start time for progress tracking
total_rows = -1  # Total rows in the CSV file


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


def addTracksToDB(file_path, reset):
    """
    Add track data to the database from a CSV file.

    Args:
        file_path (str): Path to the CSV file containing the data.
        reset (bool): If True, reset the tracks table before adding data.
    """
    global batch_queue, total_rows
    try:
        print("Adding Tracks to DB.")

        batch = []  # Initialize a batch for queries

        if reset:
            # Drop and recreate the Tracks table if reset is True
            batch.append((dm.dropTracks(), None))
            batch.append((dm.createTracks(), None))
            batch_queue.put((batch.copy(), None))
            batch.clear()
            print("Tracks table reset.")

        # Count total rows in the file for progress tracking
        with open(file_path, mode="r") as csv_file:
            total_rows = sum(1 for row in csv_file) - 1

        # Reopen file to read data rows
        with open(file_path, mode="r") as csv_file:
            csv_reader = csv.DictReader(csv_file)
            prev_file_num = -1  # Track the last file number processed

            for row_num, row in enumerate(csv_reader, start=1):
                # If file number changes and batch reaches size 1000, process the batch
                if row["file_number"] != prev_file_num:
                    if len(batch) >= 1000:
                        batch_queue.put((batch.copy(), row_num))
                        batch.clear()
                    track_n_name = dm.normalize(row["location"])  # Normalize track location name
                    batch.append(
                        # Add or update a Track
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
                    prev_file_num = row["file_number"]  # Update previous file number

            # Process any remaining queries in the batch
            if batch:
                batch_queue.put((batch.copy(), row_num))
                batch.clear()

    finally:
        pushBatches()  # Commit all remaining batches to the database


# All other functions for adding data (addRacesAndPerformancesToDB(), addHorses(), etc.) have similar structures.


def addRacesAndPerformancesToDB(file_path, reset):
    """
    Add race and performance data to the database from a CSV file.

    Args:
        file_path (str): Path to the CSV file containing the data.
        reset (bool): If True, reset the races and performances tables before adding data.
    """
    global batch_queue, total_rows
    try:
        print("Adding Races and Performances to DB.")

        batch = []

        if reset:
            batch.append((dm.dropRaces(), None))
            batch.append((dm.createRaces(), None))
            batch_queue.put((batch.copy(), None))
            batch.clear()
            print("Races table reset.")
            batch.append((dm.dropPerformances(), None))
            batch.append((dm.createPerformancesUseType(), None))
            batch.append((dm.createPerformances(), None))
            batch_queue.put((batch.copy(), None))
            batch.clear()
            print("Performances table reset.")

        with open(file_path, mode="r") as csv_file:
            total_rows = sum(1 for row in csv_file) - 1

        with open(file_path, mode="r") as csv_file:
            csv_reader = csv.DictReader(csv_file)
            prev_file_num_race_num = (-1, -1)

            for row_num, row in enumerate(csv_reader, start=1):
                if (row["file_number"], row["race_number"]) != prev_file_num_race_num:
                    if len(batch) >= 1000:
                        batch_queue.put((batch.copy(), row_num))
                        batch.clear()
                    track_n_name = dm.normalize(row["location"])
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
                owner_n_name = dm.normalize(row["owner"])
                horse_n_name = dm.normalize(row["horse_name"])
                jockey_n_name = dm.normalize(row["jockey"])
                trainer_n_name = dm.normalize(row["trainer"])
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
                        "SETUP",
                    )
                )

            if batch:
                batch_queue.put((batch.copy(), row_num))
                batch.clear()

    finally:
        pushBatches()


def addHorsesToDB(file_path, reset):
    """
    Add horse data to the database from a CSV file.

    Args:
        file_path (str): Path to the CSV file containing the data.
        reset (bool): If True, reset the horses table before adding data.
    """
    global batch_queue, total_rows
    try:
        print("Adding Horses to DB.")

        batch = []

        if reset:
            batch.append((dm.dropHorses(), None))
            batch.append((dm.createHorses(), None))
            batch_queue.put((batch.copy(), None))
            batch.clear()
            print("Horses table reset.")

        with open(file_path, mode="r") as csv_file:
            total_rows = sum(1 for row in csv_file) - 1

        with open(file_path, mode="r") as csv_file:
            csv_reader = csv.DictReader(csv_file)

            for row_num, row in enumerate(csv_reader, start=1):
                if len(batch) >= 1000:
                    batch_queue.put((batch.copy(), row_num))
                    batch.clear()
                track_n_name = dm.normalize(row["location"])
                horse_n_name = dm.normalize(row["horse_name"])
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

            if batch:
                batch_queue.put((batch.copy(), row_num))
                batch.clear()

    finally:
        pushBatches()


def addJockeysToDB(file_path, reset):
    """
    Add jockey data to the database from a CSV file.

    Args:
        file_path (str): Path to the CSV file containing the data.
        reset (bool): If True, reset the jockeys table before adding data.
    """
    global batch_queue, total_rows
    try:
        print("Adding Jockeys to DB.")

        batch = []

        if reset:
            batch.append((dm.dropJockeys(), None))
            batch.append((dm.createJockeys(), None))
            batch_queue.put((batch.copy(), None))
            batch.clear()
            print("Jockeys table reset.")

        with open(file_path, mode="r") as csv_file:
            total_rows = sum(1 for row in csv_file) - 1

        with open(file_path, mode="r") as csv_file:
            csv_reader = csv.DictReader(csv_file)

            for row_num, row in enumerate(csv_reader, start=1):
                if len(batch) >= 1000:
                    batch_queue.put((batch.copy(), row_num))
                    batch.clear()
                track_n_name = dm.normalize(row["location"])
                jockey_n_name = dm.normalize(row["jockey"])
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

            if batch:
                batch_queue.put((batch.copy(), row_num))
                batch.clear()

    finally:
        pushBatches()


def addTrainersToDB(file_path, reset):
    """
    Add trainer data to the database from a CSV file.

    Args:
        file_path (str): Path to the CSV file containing the data.
        reset (bool): If True, reset the trainers table before adding data.
    """
    global batch_queue, total_rows
    try:
        print("Adding Trainers to DB.")

        batch = []

        if reset:
            batch.append((dm.dropTrainers(), None))
            batch.append((dm.createTrainers(), None))
            batch_queue.put((batch.copy(), None))
            batch.clear()
            print("Trainers table reset.")

        with open(file_path, mode="r") as csv_file:
            total_rows = sum(1 for row in csv_file) - 1

        with open(file_path, mode="r") as csv_file:
            csv_reader = csv.DictReader(csv_file)

            for row_num, row in enumerate(csv_reader, start=1):
                if len(batch) >= 1000:
                    batch_queue.put((batch.copy(), row_num))
                    batch.clear()
                track_n_name = dm.normalize(row["location"])
                trainer_n_name = dm.normalize(row["trainer"])
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

            if batch:
                batch_queue.put((batch.copy(), row_num))
                batch.clear()

    finally:
        pushBatches()


def addOwnersToDB(file_path, reset):
    """
    Add owner data to the database from a CSV file.

    Args:
        file_path (str): Path to the CSV file containing the data.
        reset (bool): If True, reset the owners table before adding data.
    """
    global batch_queue, total_rows
    try:
        print("Adding Owners to DB.")

        batch = []

        if reset:
            batch.append((dm.dropOwners(), None))
            batch.append((dm.createOwners(), None))
            batch_queue.put((batch.copy(), None))
            batch.clear()
            print("Owners table reset.")

        with open(file_path, mode="r") as csv_file:
            total_rows = sum(1 for row in csv_file) - 1

        with open(file_path, mode="r") as csv_file:
            csv_reader = csv.DictReader(csv_file)

            for row_num, row in enumerate(csv_reader, start=1):
                if len(batch) >= 1000:
                    batch_queue.put((batch.copy(), row_num))
                    batch.clear()
                track_n_name = dm.normalize(row["location"])
                owner_n_name = dm.normalize(row["owner"])
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

            if batch:
                batch_queue.put((batch.copy(), row_num))
                batch.clear()

    finally:
        pushBatches()


def addOwnerTrainerToDB(file_path, reset):
    """
    Add owner-trainer relationship data to the database from a CSV file.

    Args:
        file_path (str): Path to the CSV file containing the data.
        reset (bool): If True, reset the owner-trainer table before adding data.
    """
    global batch_queue, total_rows
    try:
        print("Adding Owner-Trainer relationships to DB.")

        batch = []

        if reset:
            batch.append((dm.dropOwnerTrainer(), None))
            batch.append((dm.createOwnerTrainer(), None))
            batch_queue.put((batch.copy(), None))
            batch.clear()
            print("Owner-Trainer table reset.")

        with open(file_path, mode="r") as csv_file:
            total_rows = sum(1 for row in csv_file) - 1

        with open(file_path, mode="r") as csv_file:
            csv_reader = csv.DictReader(csv_file)

            for row_num, row in enumerate(csv_reader, start=1):
                if len(batch) >= 1000:
                    batch_queue.put((batch.copy(), row_num))
                    batch.clear()
                track_n_name = dm.normalize(row["location"])
                owner_n_name = dm.normalize(row["owner"])
                trainer_n_name = dm.normalize(row["trainer"])
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

            if batch:
                batch_queue.put((batch.copy(), row_num))
                batch.clear()

    finally:
        pushBatches()


def addHorseTrackToDB(file_path, reset):
    """
    Add horse-track relationship data to the database from a CSV file.

    Args:
        file_path (str): Path to the CSV file containing the data.
        reset (bool): If True, reset the horse-track table before adding data.
    """
    global batch_queue, total_rows
    try:
        print("Adding Horse-Track relationships to DB.")

        batch = []

        if reset:
            batch.append((dm.dropHorseTrack(), None))
            batch.append((dm.createHorseTrack(), None))
            batch_queue.put((batch.copy(), None))
            batch.clear()
            print("Horse-Track table reset.")

        with open(file_path, mode="r") as csv_file:
            total_rows = sum(1 for row in csv_file) - 1

        with open(file_path, mode="r") as csv_file:
            csv_reader = csv.DictReader(csv_file)

            for row_num, row in enumerate(csv_reader, start=1):
                if len(batch) >= 1000:
                    batch_queue.put((batch.copy(), row_num))
                    batch.clear()
                track_n_name = dm.normalize(row["location"])
                horse_n_name = dm.normalize(row["horse_name"])
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

            if batch:
                batch_queue.put((batch.copy(), row_num))
                batch.clear()

    finally:
        pushBatches()


def addJockeyTrainerToDB(file_path, reset):
    """
    Add jockey-trainer relationship data to the database from a CSV file.

    Args:
        file_path (str): Path to the CSV file containing the data.
        reset (bool): If True, reset the jockey-trainer table before adding data.
    """
    global batch_queue, total_rows
    try:
        print("Adding Jockey-Trainer relationships to DB.")

        batch = []

        if reset:
            batch.append((dm.dropJockeyTrainer(), None))
            batch.append((dm.createJockeyTrainer(), None))
            batch_queue.put((batch.copy(), None))
            batch.clear()
            print("Jockey-Trainer table reset.")

        with open(file_path, mode="r") as csv_file:
            total_rows = sum(1 for row in csv_file) - 1

        with open(file_path, mode="r") as csv_file:
            csv_reader = csv.DictReader(csv_file)

            for row_num, row in enumerate(csv_reader, start=1):
                if len(batch) >= 1000:
                    batch_queue.put((batch.copy(), row_num))
                    batch.clear()
                track_n_name = dm.normalize(row["location"])
                jockey_n_name = dm.normalize(row["jockey"])
                trainer_n_name = dm.normalize(row["trainer"])
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

            if batch:
                batch_queue.put((batch.copy(), row_num))
                batch.clear()

    finally:
        pushBatches()


def addHorseJockeyToDB(file_path, reset):
    """
    Add horse-jockey relationship data to the database from a CSV file.

    Args:
        file_path (str): Path to the CSV file containing the data.
        reset (bool): If True, reset the horse-jockey table before adding data.
    """
    global batch_queue, total_rows
    try:
        print("Adding Horse-Jockey relationships to DB.")

        batch = []

        if reset:
            batch.append((dm.dropHorseJockey(), None))
            batch.append((dm.createHorseJockey(), None))
            batch_queue.put((batch.copy(), None))
            batch.clear()
            print("Horse-Jockey table reset.")

        with open(file_path, mode="r") as csv_file:
            total_rows = sum(1 for row in csv_file) - 1

        with open(file_path, mode="r") as csv_file:
            csv_reader = csv.DictReader(csv_file)

            for row_num, row in enumerate(csv_reader, start=1):
                if len(batch) >= 1000:
                    batch_queue.put((batch.copy(), row_num))
                    batch.clear()
                track_n_name = dm.normalize(row["location"])
                jockey_n_name = dm.normalize(row["jockey"])
                horse_n_name = dm.normalize(row["horse_name"])
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

            if batch:
                batch_queue.put((batch.copy(), row_num))
                batch.clear()

    finally:
        pushBatches()


def addHorseTrainerToDB(file_path, reset):
    """
    Add horse-trainer relationship data to the database from a CSV file.

    Args:
        file_path (str): Path to the CSV file containing the data.
        reset (bool): If True, reset the horse-trainer table before adding data.
    """
    global batch_queue, total_rows
    try:
        print("Adding Horse-Trainer relationships to DB.")

        batch = []

        if reset:
            batch.append((dm.dropHorseTrainer(), None))
            batch.append((dm.createHorseTrainer(), None))
            batch_queue.put((batch.copy(), None))
            batch.clear()
            print("Horse-Trainer table reset.")

        with open(file_path, mode="r") as csv_file:
            total_rows = sum(1 for row in csv_file) - 1

        with open(file_path, mode="r") as csv_file:
            csv_reader = csv.DictReader(csv_file)

            for row_num, row in enumerate(csv_reader, start=1):
                if len(batch) >= 1000:
                    batch_queue.put((batch.copy(), row_num))
                    batch.clear()
                track_n_name = dm.normalize(row["location"])
                trainer_n_name = dm.normalize(row["trainer"])
                horse_n_name = dm.normalize(row["horse_name"])
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

            if batch:
                batch_queue.put((batch.copy(), row_num))
                batch.clear()

    finally:
        pushBatches()


def addTrainerTrackToDB(file_path, reset):
    """
    Add trainer-track relationship data to the database from a CSV file.

    Args:
        file_path (str): Path to the CSV file containing the data.
        reset (bool): If True, reset the trainer-track table before adding data.
    """
    global batch_queue, total_rows
    try:
        print("Adding Trainer-Track relationships to DB.")

        batch = []

        if reset:
            batch.append((dm.dropTrainerTrack(), None))
            batch.append((dm.createTrainerTrack(), None))
            batch_queue.put((batch.copy(), None))
            batch.clear()
            print("Trainer-Track table reset.")

        with open(file_path, mode="r") as csv_file:
            total_rows = sum(1 for row in csv_file) - 1

        with open(file_path, mode="r") as csv_file:
            csv_reader = csv.DictReader(csv_file)

            for row_num, row in enumerate(csv_reader, start=1):
                if len(batch) >= 1000:
                    batch_queue.put((batch.copy(), row_num))
                    batch.clear()
                track_n_name = dm.normalize(row["location"])
                trainer_n_name = dm.normalize(row["trainer"])
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

            if batch:
                batch_queue.put((batch.copy(), row_num))
                batch.clear()

    finally:
        pushBatches()


if __name__ == "__main__":
    """
    Main script execution for adding setup data to the database.

    This section executes all functions to populate the database tables with data from setup.csv or testing.csv.
    """
    # Testing
    """addTracksToDB('testing.csv', True)
    addHorsesToDB('testing.csv', True)
    addJockeysToDB('testing.csv', True)
    addTrainersToDB('testing.csv', True)
    addOwnersToDB('testing.csv', True)
    addRacesAndPerformancesToDB('testing.csv', True)

    addOwnerTrainerToDB('testing.csv', True)
    addHorseTrackToDB('testing.csv', True)
    addJockeyTrainerToDB('testing.csv', True)
    addHorseJockeyToDB('testing.csv', True)
    addHorseTrainerToDB('testing.csv', True)
    addTrainerTrackToDB('testing.csv', True)"""

    # Setup

    addTracksToDB("setup.csv", True)
    addHorsesToDB("setup.csv", True)
    addJockeysToDB("setup.csv", True)
    addTrainersToDB("setup.csv", True)
    addOwnersToDB("setup.csv", True)
    addRacesAndPerformancesToDB("setup.csv", True)

    addOwnerTrainerToDB("setup.csv", True)
    addHorseTrackToDB("setup.csv", True)
    addJockeyTrainerToDB("setup.csv", True)
    addHorseJockeyToDB("setup.csv", True)
    addHorseTrainerToDB("setup.csv", True)
    addTrainerTrackToDB("setup.csv", True)
