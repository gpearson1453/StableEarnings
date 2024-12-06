import csv
import dataMethods as dm
import os
import time
from decimal import Decimal
import queue
import uuid

# Set the working directory to the script's location
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

# Global variables
conn = None
batch_queue = queue.Queue()
start_time = time.time()  # Start time of the program
total_rows = -1


# Function to push batches to the database
def pushBatches():
    global batch_queue, total_rows, start_time
    b_total = 0
    conn = dm.local_connect("StableEarnings")  # Open the connection once at the start
    start_time = time.time()
    try:
        while not batch_queue.empty():
            try:
                with conn.cursor() as cur:
                    b, row_num = batch_queue.get()
                    b_total += len(b)
                    for query, params in b:
                        cur.execute(query, params)
                    conn.commit()
                    if row_num:
                        if b_total >= 50000:
                            print(f"{b_total} queries processed.")
                            clockCheck(row_num, total_rows)
                            b_total = 0
                    else:
                        print("Reset processed.")

            except Exception as e:
                conn.rollback()
                print(f"Error occurred in batch: {e} at around row {row_num}")
                # Reconnect if the connection is closed due to an error
                if conn.closed:
                    conn = dm.connect_db("defaultdb")

    finally:
        # Ensure connection is closed when all batches are finished
        if conn and not conn.closed:
            conn.close()
            print("Connection closed after batch processing.")


# Helper function to format elapsed time
def formatTime(seconds):
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    seconds = int(seconds % 60)
    return f"{hours}h {minutes}m {seconds}s"


# Progress tracker
def clockCheck(row_num, total_rows):
    elapsed_time = time.time() - start_time
    rows_per_sec = row_num / elapsed_time
    estimated_time_left = (total_rows - row_num) / rows_per_sec

    # Format elapsed time and estimated time left in hours, minutes, and seconds
    formatted_elapsed_time = formatTime(elapsed_time)
    formatted_time_left = formatTime(estimated_time_left)

    print(
        f"Rows processed: {row_num}/{total_rows}.     Time elapsed: {formatted_elapsed_time}     Estimated time remaining: {formatted_time_left}"
    )


def addTracksToDB(file_path, reset):
    global batch_queue, total_rows
    try:
        print("Adding Tracks to DB.")

        batch = []

        if reset:
            batch.append((dm.dropTracks(), None))
            batch.append((dm.createTracks(), None))
            batch_queue.put((batch.copy(), None))
            batch.clear()
            print("Tracks table reset.")

        # Get total number of rows in the CSV for progress tracking
        with open(file_path, mode="r") as csv_file:
            total_rows = sum(1 for row in csv_file) - 1  # Subtract 1 for the header row

        with open(file_path, mode="r") as csv_file:
            csv_reader = csv.DictReader(csv_file)
            prev_file_num = -1

            for row_num, row in enumerate(csv_reader, start=1):
                # Add logic to prepare each batch
                if row["file_number"] != prev_file_num:
                    if len(batch) >= 1000:
                        batch_queue.put((batch.copy(), row_num))
                        batch.clear()  # Clear for next batch
                    # Process and add track
                    track_n_name = dm.normalize(row["location"])
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

            # Push any remaining queries as the final batch
            if batch:
                batch_queue.put((batch.copy(), row_num))
                batch.clear()  # Clear for next batch

    finally:
        pushBatches()


def addRacesAndPerformancesToDB(file_path, reset):
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

        # Get total number of rows in the CSV for progress tracking
        with open(file_path, mode="r") as csv_file:
            total_rows = sum(1 for row in csv_file) - 1  # Subtract 1 for the header row

        with open(file_path, mode="r") as csv_file:
            csv_reader = csv.DictReader(csv_file)
            prev_file_num_race_num = (-1, -1)

            for row_num, row in enumerate(csv_reader, start=1):
                # Add logic to prepare each batch
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

            # Push any remaining queries as the final batch
            if batch:
                batch_queue.put((batch.copy(), row_num))
                batch.clear()  # Clear for next batch

    finally:
        pushBatches()


def addHorsesToDB(file_path, reset):
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

        # Get total number of rows in the CSV for progress tracking
        with open(file_path, mode="r") as csv_file:
            total_rows = sum(1 for row in csv_file) - 1  # Subtract 1 for the header row

        with open(file_path, mode="r") as csv_file:
            csv_reader = csv.DictReader(csv_file)

            for row_num, row in enumerate(csv_reader, start=1):
                # Add logic to prepare each batch
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

            # Push any remaining queries as the final batch
            if batch:
                batch_queue.put((batch.copy(), row_num))
                batch.clear()  # Clear for next batch

    finally:
        pushBatches()


def addJockeysToDB(file_path, reset):
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

        # Get total number of rows in the CSV for progress tracking
        with open(file_path, mode="r") as csv_file:
            total_rows = sum(1 for row in csv_file) - 1  # Subtract 1 for the header row

        with open(file_path, mode="r") as csv_file:
            csv_reader = csv.DictReader(csv_file)

            for row_num, row in enumerate(csv_reader, start=1):
                # Add logic to prepare each batch
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

            # Push any remaining queries as the final batch
            if batch:
                batch_queue.put((batch.copy(), row_num))
                batch.clear()  # Clear for next batch

    finally:
        pushBatches()


def addTrainersToDB(file_path, reset):
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

        # Get total number of rows in the CSV for progress tracking
        with open(file_path, mode="r") as csv_file:
            total_rows = sum(1 for row in csv_file) - 1  # Subtract 1 for the header row

        with open(file_path, mode="r") as csv_file:
            csv_reader = csv.DictReader(csv_file)

            for row_num, row in enumerate(csv_reader, start=1):
                # Add logic to prepare each batch
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

            # Push any remaining queries as the final batch
            if batch:
                batch_queue.put((batch.copy(), row_num))
                batch.clear()  # Clear for next batch

    finally:
        pushBatches()


def addOwnersToDB(file_path, reset):
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

        # Get total number of rows in the CSV for progress tracking
        with open(file_path, mode="r") as csv_file:
            total_rows = sum(1 for row in csv_file) - 1  # Subtract 1 for the header row

        with open(file_path, mode="r") as csv_file:
            csv_reader = csv.DictReader(csv_file)

            for row_num, row in enumerate(csv_reader, start=1):
                # Add logic to prepare each batch
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

            # Push any remaining queries as the final batch
            if batch:
                batch_queue.put((batch.copy(), row_num))
                batch.clear()  # Clear for next batch

    finally:
        pushBatches()


def addOwnerTrainerToDB(file_path, reset):
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

        # Get total number of rows in the CSV for progress tracking
        with open(file_path, mode="r") as csv_file:
            total_rows = sum(1 for row in csv_file) - 1  # Subtract 1 for the header row

        with open(file_path, mode="r") as csv_file:
            csv_reader = csv.DictReader(csv_file)

            for row_num, row in enumerate(csv_reader, start=1):
                # Add logic to prepare each batch
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

            # Push any remaining queries as the final batch
            if batch:
                batch_queue.put((batch.copy(), row_num))
                batch.clear()  # Clear for next batch

    finally:
        pushBatches()


def addHorseTrackToDB(file_path, reset):
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

        # Get total number of rows in the CSV for progress tracking
        with open(file_path, mode="r") as csv_file:
            total_rows = sum(1 for row in csv_file) - 1  # Subtract 1 for the header row

        with open(file_path, mode="r") as csv_file:
            csv_reader = csv.DictReader(csv_file)

            for row_num, row in enumerate(csv_reader, start=1):
                # Add logic to prepare each batch
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

            # Push any remaining queries as the final batch
            if batch:
                batch_queue.put((batch.copy(), row_num))
                batch.clear()  # Clear for next batch

    finally:
        pushBatches()


def addJockeyTrainerToDB(file_path, reset):
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

        # Get total number of rows in the CSV for progress tracking
        with open(file_path, mode="r") as csv_file:
            total_rows = sum(1 for row in csv_file) - 1  # Subtract 1 for the header row

        with open(file_path, mode="r") as csv_file:
            csv_reader = csv.DictReader(csv_file)

            for row_num, row in enumerate(csv_reader, start=1):
                # Add logic to prepare each batch
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

            # Push any remaining queries as the final batch
            if batch:
                batch_queue.put((batch.copy(), row_num))
                batch.clear()  # Clear for next batch

    finally:
        pushBatches()


def addHorseJockeyToDB(file_path, reset):
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

        # Get total number of rows in the CSV for progress tracking
        with open(file_path, mode="r") as csv_file:
            total_rows = sum(1 for row in csv_file) - 1  # Subtract 1 for the header row

        with open(file_path, mode="r") as csv_file:
            csv_reader = csv.DictReader(csv_file)

            for row_num, row in enumerate(csv_reader, start=1):
                # Add logic to prepare each batch
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

            # Push any remaining queries as the final batch
            if batch:
                batch_queue.put((batch.copy(), row_num))
                batch.clear()  # Clear for next batch

    finally:
        pushBatches()


def addHorseTrainerToDB(file_path, reset):
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

        # Get total number of rows in the CSV for progress tracking
        with open(file_path, mode="r") as csv_file:
            total_rows = sum(1 for row in csv_file) - 1  # Subtract 1 for the header row

        with open(file_path, mode="r") as csv_file:
            csv_reader = csv.DictReader(csv_file)

            for row_num, row in enumerate(csv_reader, start=1):
                # Add logic to prepare each batch
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

            # Push any remaining queries as the final batch
            if batch:
                batch_queue.put((batch.copy(), row_num))
                batch.clear()  # Clear for next batch

    finally:
        pushBatches()


def addTrainerTrackToDB(file_path, reset):
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

        # Get total number of rows in the CSV for progress tracking
        with open(file_path, mode="r") as csv_file:
            total_rows = sum(1 for row in csv_file) - 1  # Subtract 1 for the header row

        with open(file_path, mode="r") as csv_file:
            csv_reader = csv.DictReader(csv_file)

            for row_num, row in enumerate(csv_reader, start=1):
                # Add logic to prepare each batch
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

            # Push any remaining queries as the final batch
            if batch:
                batch_queue.put((batch.copy(), row_num))
                batch.clear()  # Clear for next batch

    finally:
        pushBatches()


if __name__ == "__main__":
    # Testing
    # addTracksToDB('testing.csv', True)
    # addHorsesToDB('testing.csv', True)
    # addJockeysToDB('testing.csv', True)
    # addTrainersToDB('testing.csv', True)
    # addOwnersToDB('testing.csv', True)
    # addRacesAndPerformancesToDB('testing.csv', True)

    # addOwnerTrainerToDB('testing.csv', True)
    # addHorseTrackToDB('testing.csv', True)
    # addJockeyTrainerToDB('testing.csv', True)
    # addHorseJockeyToDB('testing.csv', True)
    # addHorseTrainerToDB('testing.csv', True)
    # addTrainerTrackToDB('testing.csv', True)

    # Setup

    addTracksToDB("setup.csv", True)  # reset here
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
