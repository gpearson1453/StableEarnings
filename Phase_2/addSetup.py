import csv
import dataMethods as dm
import os

# Set the working directory to the script's location
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

def addTrack(track_name, conn, cur, track_cache):
    if track_name in track_cache:
        return track_cache[track_name]

    n_name = dm.normalize(track_name)
    match_id = dm.checkTrack(n_name, cur)  # Pass cursor
    if match_id:
        track_cache[track_name] = match_id
        return match_id
    else:
        new_track_id = dm.addNewTrack(track_name, n_name, conn, cur)  # Pass connection and cursor
        track_cache[track_name] = new_track_id
        return new_track_id
    
def addSetup(file_path):
    conn = dm.connect_db()  # Open a single DB connection
    cur = conn.cursor()
    
    track_cache = {}  # Cache for track names to avoid repeated DB queries
    prev_file_num = -1
    prev_file_num_race_num = (-1, -1)
    
    with open(file_path, mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        
        for row in csv_reader:
            # Process the track only if it's a new file number
            if row['file_number'] != prev_file_num:
                track_id = addTrack(row['location'], conn, cur, track_cache)
                prev_file_num = row['file_number']
            if (row['file_number'], row['race_number']) != prev_file_num_race_num:
                race_id = dm.addNewRace(track_id, row['race_number'], dm.convertDate(row['date']), row['race_type'], row['surface'],
                                        row['weather'], dm.convertTemp(row['temp']), row['track_state'], row['distance(miles)'], 
                                        dm.convertTime(row['final_time']), dm.convertTime(row['fractional_a']), dm.convertTime(row['fractional_b']), 
                                        dm.convertTime(row['fractional_c']), dm.convertTime(row['fractional_d']), dm.convertTime(row['fractional_e']), 
                                        dm.convertTime(row['fractional_f']), dm.convertTime(row['split_a']), dm.convertTime(row['split_b']), dm.convertTime(row['split_c']), 
                                        dm.convertTime(row['split_d']), dm.convertTime(row['split_e']), dm.convertTime(row['split_f']), conn, cur)
                prev_file_num_race_num = (row['file_number'], row['race_number'])

            '''# Process the other entities (horses, jockeys, trainers) similarly
            if row['entity_type'] == 'horse':
                dm.addNewHorse(
                    name=row['name'],
                    avg_pos_factor=row.get('avg_pos_factor'),
                    st_dev_pos_factor=row.get('st_dev_pos_factor'),
                    avg_position_gain=row.get('avg_position_gain'),
                    st_dev_position_gain=row.get('st_dev_position_gain'),
                    avg_late_position_gain=row.get('avg_late_position_gain'),
                    avg_last_position_gain=row.get('avg_last_position_gain'),
                    ewma_perf_factor=row.get('ewma_perf_factor'),
                    most_recent_perf_factor=row.get('most_recent_perf_factor'),
                    ewma_dirt_perf_factor=row.get('ewma_dirt_perf_factor'),
                    ewma_turf_perf_factor=row.get('ewma_turf_perf_factor'),
                    ewma_awt_perf_factor=row.get('ewma_awt_perf_factor'),
                    distance_factor=row.get('distance_factor'),
                    conn=conn,  # Pass the connection
                    cur=cur  # Pass the cursor
                )
            # Add similar processing for jockeys, trainers, etc.'''

    conn.commit()  # Commit all changes after processing the entire CSV
    cur.close()
    conn.close()  # Close the DB connection after all rows are processed

if __name__ == "__main__":
    addSetup('setup.csv')
