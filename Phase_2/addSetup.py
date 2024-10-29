import csv
import dataMethods as dm
import os
import time
from resetDatabase import resetDatabase
from decimal import Decimal
import threading
import queue

# Set the working directory to the script's location
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

# Global variables
conn = None
batch_queue = queue.Queue()
start_time = time.time()  # Start time of the program
batches_finished = False
batch = []
row_num = -1
total_rows = -1

# Function to initialize the database connection and cursor
def init_db():
    global conn
    conn = dm.connect_db()  # Establish the DB connection

# Function to close the database connection and cursor
def close_db():
    global conn
    with conn.cursor() as cur:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()

# Function to push batches to the database
def push_batches():
    global batch_queue, batches_finished, row_num, total_rows
    conn = None
    while not batches_finished or not batch_queue.empty():
        try:
            # Establish a connection for the current thread
            if conn is None or conn.closed:
                conn = dm.connect_db()  # Open a new connection
            with conn.cursor() as cur:
                b = batch_queue.get()
                for query, params in b:
                    cur.execute(query, params)
                conn.commit()
            print(f"Batch of {len(b)} processed.")
            clockCheck(row_num, total_rows)
        except Exception as e:
            if conn:
                conn.rollback()
            print(f"Error occurred in batch: {e}")
        finally:
            # Close connection if batches are finished
            if batches_finished and batch_queue.empty():
                if conn:
                    conn.close()
                    print("Database connection closed in push_batches.")


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

# Main function to process CSV and manage batch processing
def create_batches(file_path):
    global batch, batches_finished, batch_queue, row_num, total_rows
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
                    if len(batch) >= 1000:
                        batch_queue.put(batch.copy())
                        batch.clear()  # Clear for next batch
                    # Process and add track
                    track_n_name = dm.normalize(row['location'])
                    batch.append(dm.addTrack(row['location'], track_n_name, Decimal(row['distance(miles)']) / Decimal(row['final_time']) if row['final_time'] else None))
                    prev_file_num = row['file_number']
                if (row['file_number'], row['race_number']) != prev_file_num_race_num:
                    batch.append(dm.addNewRace(
                        track_n_name, row['race_number'], row['date'], row['race_type'], row['surface'],
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
                horse_n_name = dm.normalize(row['horse_name'])
                batch.append(dm.addHorse(
                    row['horse_name'], horse_n_name, Decimal(row['final_pos']), 
                    Decimal(row['pos_factor']) if row['pos_factor'] else None, 
                    Decimal(row['pos_gain']) if row['pos_gain'] else None, 
                    Decimal(row['late_pos_gain']) if row['late_pos_gain'] else None, 
                    Decimal(row['last_pos_gain']) if row['last_pos_gain'] else None, 
                    Decimal(row['speed']) if row['speed'] else None, track_n_name,
                    row['surface'], Decimal(row['distance(miles)']), 
                    Decimal(row['total_horses'])
                ))

            # Push any remaining queries as the final batch
            if batch:
                batch_queue.put(batch)
                batch.clear()  # Clear for next batch
                clockCheck(row_num, total_rows)

    finally:
        batches_finished = True
        close_db()

if __name__ == "__main__":
    resetDatabase()
    init_db()  # Initialize the database connection and cursor
    
    create_batches_thread = threading.Thread(target=create_batches, args=('testing.csv',))
    #create_batches_thread = threading.Thread(target=create_batches, args='setup.csv')
    
    push_batches_thread = threading.Thread(target=push_batches)
    
    try:
        create_batches_thread.start()
        push_batches_thread.start()
    finally:
        create_batches_thread.join()
        push_batches_thread.join()
