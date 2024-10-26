import csv
import dataMethods as dm
import os
import uuid
import time

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

# Function to push all batched queries to the database
def pushBatch():
    global conn, cur, batch_queries
    try:
        for query, params in batch_queries:
            cur.execute(query, params)
        conn.commit()  # Commit all the batched queries
        print(f"Successfully pushed {len(batch_queries)} queries.")
    except Exception as e:
        conn.rollback()  # Roll back if any query fails
        print(f"Error occurred: {e}")
    finally:
        batch_queries = []  # Clear the batch after pushing

def format_time(seconds):
    """Helper function to format time in hours, minutes, and seconds."""
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    seconds = int(seconds % 60)
    return f"{hours}h {minutes}m {seconds}s"

def clockCheck(row_num, total_rows):
    elapsed_time = time.time() - start_time
    rows_per_sec = row_num / elapsed_time
    estimated_time_left = (total_rows - row_num) / rows_per_sec

    # Format elapsed time and estimated time left in hours, minutes, and seconds
    formatted_elapsed_time = format_time(elapsed_time)
    formatted_time_left = format_time(estimated_time_left)

    print(f"Time elapsed: {formatted_elapsed_time}")
    print(f"Rows processed: {row_num}/{total_rows}")
    print(f"Estimated time remaining: {formatted_time_left}")

def addTrack(track_name, distance, time):
    global track_cache
    if track_name in track_cache:
        if time:
            batch_queries.append(dm.updateTrack(track_cache[track_name], float(distance) / float(time)))
        return track_cache[track_name]
    n_name = dm.normalize(track_name)
    match_id = dm.check(n_name, cur, 'track')
    if match_id:
        track_cache[track_name] = match_id
        if time:
            batch_queries.append(dm.updateTrack(match_id, float(distance) / float(time)))
        return match_id
    else:
        new_track_id = str(uuid.uuid4())
        batch_queries.append(dm.addNewTrack(new_track_id, track_name, n_name, float(distance) / float(time) if time else None))
        track_cache[track_name] = new_track_id
        return new_track_id
    
def addHorse(name, pos, num_horses, pos_factor, pos_gain, late_pos_gain, last_pos_gain, surface, distance, speed, track_id):
    global horse_cache
    if name in horse_cache:
        batch_queries.append(dm.updateHorse(horse_cache[name], pos, num_horses, pos_factor, pos_gain, late_pos_gain, last_pos_gain, surface, distance, speed, track_id))
        return horse_cache[name]
    n_name = dm.normalize(name)
    match_id = dm.check(n_name, cur, 'horse')
    if match_id:
        horse_cache[name] = match_id
        batch_queries.append(dm.updateHorse(match_id, pos, num_horses, pos_factor, pos_gain, late_pos_gain, last_pos_gain, surface, distance, speed, track_id))
        return match_id
    else:
        new_horse_id = str(uuid.uuid4())
        batch_queries.append(dm.addNewHorse(name, n_name, new_horse_id, pos, pos_factor, pos_gain, late_pos_gain, last_pos_gain, speed, track_id, surface, distance))
        horse_cache[name] = new_horse_id
        return new_horse_id

def addSetup(file_path):
    global cur, conn
    prev_file_num = -1
    prev_file_num_race_num = (-1, -1)

    # Get total number of rows in the CSV
    with open(file_path, mode='r') as csv_file:
        total_rows = sum(1 for row in csv_file) - 1  # Subtract 1 for the header row

    # Process the CSV
    with open(file_path, mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row_num, row in enumerate(csv_reader, start=1):
            if row['file_number'] != prev_file_num:
                if len(batch_queries) > 1000:
                    pushBatch()
                    clockCheck(row_num, total_rows)
                track_id = addTrack(row['location'], row['distance(miles)'], row['final_time'])
                prev_file_num = row['file_number']
            if (row['file_number'], row['race_number']) != prev_file_num_race_num:
                race_id = str(uuid.uuid4())
                speed = float(row['distance(miles)']) / float(row['final_time']) if row['final_time'] else None
                batch_queries.append(dm.addNewRace(race_id, track_id, row['race_number'], row['date'], row['race_type'], row['surface'], row['weather'], 
                                                   row['temp'], row['track_state'], row['distance(miles)'], 
                                                   row['final_time'] if row['final_time'] else None, 
                                                   speed,
                                                   row['fractional_a'] if row['fractional_a'] else None, 
                                                   row['fractional_b'] if row['fractional_b'] else None, 
                                                   row['fractional_c'] if row['fractional_c'] else None, 
                                                   row['fractional_d'] if row['fractional_d'] else None, 
                                                   row['fractional_e'] if row['fractional_e'] else None, 
                                                   row['fractional_f'] if row['fractional_f'] else None,
                                                   row['split_a'] if row['split_a'] else None, 
                                                   row['split_b'] if row['split_b'] else None, 
                                                   row['split_c'] if row['split_c'] else None, 
                                                   row['split_d'] if row['split_d'] else None, 
                                                   row['split_e'] if row['split_e'] else None, 
                                                   row['split_f'] if row['split_f'] else None))
                prev_file_num_race_num = (row['file_number'], row['race_number'])
            horse_id = addHorse(row['horse_name'], row['final_pos'], row['total_horses'], row['pos_factor'], row['pos_gain'], row['late_pos_gain'], 
                                row['last_pos_gain'], row['surface'], row['distance'], row['speed'], track_id)

    pushBatch()  # Push all the queries after processing the CSV

if __name__ == "__main__":
    init_db()  # Initialize the database connection and cursor
    try:
        addSetup('testing.csv')  # Run the setup process
        #addSetup('setup.csv')  # Run the setup process
    finally:
        close_db()  # Ensure the DB connection is closed after processing
