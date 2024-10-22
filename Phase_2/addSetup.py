import csv
import dataMethods as dm
import os
import uuid

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
        
def addTrack(track_name):
    global track_cache
    if track_name in track_cache:
        return track_cache[track_name]
    n_name = dm.normalize(track_name)
    match_id = dm.checkTrack(n_name, cur)  # Pass cursor
    if match_id:
        track_cache[track_name] = match_id
        return match_id
    else:
        new_track_id = str(uuid.uuid4())
        batch_queries.append(dm.addNewTrack(new_track_id, track_name, n_name))
        track_cache[track_name] = new_track_id
        return new_track_id

def addSetup(file_path):
    global cur, conn
    prev_file_num = -1
    prev_file_num_race_num = (-1, -1)
    
    with open(file_path, mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            if row['file_number'] != prev_file_num:
                if len(batch_queries) > 2000:
                    pushBatch()
                track_id = addTrack(row['location'])
                prev_file_num = row['file_number']
            if (row['file_number'], row['race_number']) != prev_file_num_race_num:
                race_id = str(uuid.uuid4())
                batch_queries.append(dm.addNewRace(race_id, track_id, row['race_number'], row['date'], row['race_type'], row['surface'], row['weather'], 
                                                   row['temp'], row['track_state'], row['distance(miles)'], 
                                                   row['final_time'] if row['final_time'] else None, 
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
                

    pushBatch()  # Push all the queries after processing the CSV

if __name__ == "__main__":
    init_db()  # Initialize the database connection and cursor
    try:
        addSetup('setup.csv')  # Run the setup process
    finally:
        close_db()  # Ensure the DB connection is closed after processing
