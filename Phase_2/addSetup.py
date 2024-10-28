import csv
import dataMethods as dm
import os
import uuid
import time
from resetDatabase import resetDatabase
from decimal import Decimal

# Set the working directory to the script's location
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

# Global variables
conn = None
cur = None
batch_queries = []
track_cache = {}
horse_cache = {}
jockey_cache = {}
trainer_cache = {}
owner_cache = {}
start_time = time.time()  # Start time of the program

# Function to initialize the database connection and cursor
def init_db():
    global conn, cur
    conn = dm.connect_db()  # Establish the DB connection
    cur = conn.cursor()  # Initialize the cursor

# Function to close the database connection and cursor
def close_db():
    global conn, cur
    if cur is not None:
        cur.close()
    if conn is not None:
        conn.close()

# Function to push a single batch to the database
def push_batch_to_db(batch):
    try:
        with conn:
            with conn.cursor() as cur:
                for query, params in batch:
                    cur.execute(query, params)
                conn.commit()
        print(f"Batch of {len(batch)} processed.")
    except Exception as e:
        conn.rollback()
        print(f"Error occurred in batch: {e}")

# Helper function to format elapsed time
def format_time(seconds):
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
    formatted_elapsed_time = format_time(elapsed_time)
    formatted_time_left = format_time(estimated_time_left)

    print(f"Rows processed: {row_num}/{total_rows}.     Time elapsed: {formatted_elapsed_time}     Estimated time remaining: {formatted_time_left}")

# Add track to batch
def addTrack(track_name, distance, time):
    global track_cache
    n_name = dm.normalize(track_name)
    if n_name in track_cache:
        if time:
            batch_queries.append(dm.updateTrack(track_cache[track_name], Decimal(distance) / Decimal(time)))
        return track_cache[n_name]
    match_id = dm.check(n_name, cur, 'track', 'Tracks')
    if match_id:
        track_cache[track_name] = match_id
        if time:
            batch_queries.append(dm.updateTrack(match_id, Decimal(distance) / Decimal(time)))
        return match_id
    else:
        new_track_id = str(uuid.uuid4())
        batch_queries.append(dm.addNewTrack(new_track_id, track_name, n_name, Decimal(distance) / Decimal(time) if time else None))
        track_cache[track_name] = new_track_id
        return new_track_id

# Add horse to batch
def addHorse(name, pos, num_horses, pos_factor, pos_gain, late_pos_gain, last_pos_gain, surface, distance, speed, track_id):
    global horse_cache
    n_name = dm.normalize(name)
    if n_name in horse_cache:
        batch_queries.append(dm.updateHorse(horse_cache[name], pos, num_horses, pos_factor, pos_gain, late_pos_gain, last_pos_gain, surface, distance, speed, track_id))
        return horse_cache[n_name]
    match_id = dm.check(n_name, cur, 'horse', 'Horses')
    if match_id:
        horse_cache[name] = match_id
        batch_queries.append(dm.updateHorse(match_id, pos, num_horses, pos_factor, pos_gain, late_pos_gain, last_pos_gain, surface, distance, speed, track_id))
        return match_id
    else:
        new_horse_id = str(uuid.uuid4())
        batch_queries.append(dm.addNewHorse(name, n_name, new_horse_id, pos, pos_factor, pos_gain, late_pos_gain, last_pos_gain, speed, track_id, surface, distance))
        horse_cache[name] = new_horse_id
        return new_horse_id

# Main function to process CSV and manage batch processing
def process_batches(file_path):
    init_db()
    try:
        # Get total number of rows in the CSV for progress tracking
        with open(file_path, mode='r') as csv_file:
            total_rows = sum(1 for row in csv_file) - 1  # Subtract 1 for the header row

        with open(file_path, mode='r') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            prev_file_num = -1
            prev_file_num_race_num = (-1, -1)

            for row_num, row in enumerate(csv_reader, start=1):
                # Add logic to prepare each batch
                if row['file_number'] != prev_file_num:
                    if len(batch_queries) >= 1000:
                        # Push current batch to the database
                        push_batch_to_db(batch_queries[:])
                        batch_queries.clear()  # Clear for next batch
                        clockCheck(row_num, total_rows)
                    # Process and add track
                    track_id = addTrack(row['location'], Decimal(row['distance(miles)']), Decimal(row['final_time']) if row['final_time'] else None)
                    prev_file_num = row['file_number']
                if (row['file_number'], row['race_number']) != prev_file_num_race_num:
                    race_id = str(uuid.uuid4())
                    batch_queries.append(dm.addNewRace(
                        race_id, track_id, row['race_number'], row['date'], row['race_type'], row['surface'],
                        row['weather'], row['temp'], row['track_state'], Decimal(row['distance(miles)']),
                        Decimal(row['final_time']) if row['final_time'] else None, Decimal(row['speed']) if row['speed'] else None,
                        Decimal(row['fractional_a']) if row['fractional_a'] else None, Decimal(row['fractional_b']) if row['fractional_b'] else None,
                        Decimal(row['fractional_c']) if row['fractional_c'] else None, Decimal(row['fractional_d']) if row['fractional_d'] else None,
                        Decimal(row['fractional_e']) if row['fractional_e'] else None, Decimal(row['fractional_f']) if row['fractional_f'] else None,
                        Decimal(row['split_a']) if row['split_a'] else None, Decimal(row['split_b']) if row['split_b'] else None,
                        Decimal(row['split_c']) if row['split_c'] else None, Decimal(row['split_d']) if row['split_d'] else None,
                        Decimal(row['split_e']) if row['split_e'] else None, Decimal(row['split_f']) if row['split_f'] else None
                    ))
                    prev_file_num_race_num = (row['file_number'], row['race_number'])
                horse_id = addHorse(
                    row['horse_name'], Decimal(row['final_pos']), Decimal(row['total_horses']), 
                    Decimal(row['pos_factor']) if row['pos_factor'] else None, 
                    Decimal(row['pos_gain']) if row['pos_gain'] else None, 
                    Decimal(row['late_pos_gain']) if row['late_pos_gain'] else None, 
                    Decimal(row['last_pos_gain']) if row['last_pos_gain'] else None, 
                    row['surface'], Decimal(row['distance(miles)']), 
                    Decimal(row['speed']) if row['speed'] else None, track_id
                )

            # Push any remaining queries as the final batch
            if batch_queries:
                push_batch_to_db(batch_queries[:])
                batch_queries.clear()
                clockCheck(row_num, total_rows)

    finally:
        close_db()

if __name__ == "__main__":
    resetDatabase()
    init_db()  # Initialize the database connection and cursor
    try:
        process_batches('testing.csv')  # Run the setup process
    finally:
        close_db()  # Ensure the DB connection is closed after processing
