import csv
import dataMethods as dm
import os
import time
from decimal import Decimal
import queue
import random

# Set the working directory to the script's location
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

# Global variables
conn = None
batch_queue = queue.Queue()
start_time = time.time()
total_rows = -1
weather_cache = {}
valid_weather_num = 0
track_state_cache = {}
valid_track_state_num = 0
test_ratio = 0.0

def buildCaches(csv_reader):
    
    global weather_cache, valid_weather_num, track_state_cache, valid_track_state_num
    
    prev_file_num_race_num = (-1, -1)
    
    for row in csv_reader:
        
        # we only want to update the cache with each new race because all rows pertaining 
        # to the same race automatically have the same weather and track_state
        if (row['file_number'], row['race_number']) != prev_file_num_race_num:
        
            w = dm.normalize(row['weather'])
            ts = dm.normalize(row['track_state'])
            
            # both encodeWeather and encodeTrackState will rely on their respective 
            # caches to determine encodings where a cache entry is of the form
                # key: weather or track_state value (w or ts)
                # value: [int, int] ([weather or track state count, encoding value])
            
            # if the length of w <= 2, there was most likely an error,
            # meaning this weather is invalid and wont be added to the cache
            # so we can just skip it
            if len(w) > 2:
                
                # if w is already in the weather cache we want to increment the count
                # for that weather type by one
                if w in weather_cache:
                    
                    weather_cache[w][0] += 1
                    
                    # if the count for this weather type is now 5, this is enough occurrences of a weather
                    # type for it to be considered valid, so we will increment valid_weather_num
                    # and assign this new number to be the encoded weather value
                    if weather_cache[w][0] == 5:
                        
                        valid_weather_num += 1
                        
                        weather_cache[w][1] = valid_weather_num
                
                # if w is not in the weather cache, then we can add it with a count of 1
                # and an encoded value of 0 which will be changed when enough occurrences are found
                else:
                    weather_cache[w] = [1, 0]
                    
            # if the length of ts <= 2, there was most likely an error,
            # meaning this track state is invalid and wont be added to the cache
            # so we can just skip it
            if len(ts) > 2:
                
                # if ts is already in the track state cache we want to increment the count
                # for that track state type by one
                if ts in track_state_cache:
                    
                    track_state_cache[ts][0] += 1
                    
                    # if the count for this track state type is now 5, this is enough occurrences of a track state
                    # type for it to be considered valid, so we will increment valid_track_state_num
                    # and assign this new number to be the encoded track state value
                    if track_state_cache[ts][0] == 5:
                        
                        valid_track_state_num += 1
                        
                        track_state_cache[ts][1] = valid_track_state_num
                
                # if ts is not in the track state cache, then we can add it with a count of 1
                # and an encoded value of 0 which will be changed when enough occurrences are found
                else:
                    track_state_cache[ts] = [1, 0]
                
            # update prev_file_num_race_num to represent this race to avoid 
            # adding more weather and track state values for this race
            prev_file_num_race_num = (row['file_number'], row['race_number'])

def getTestRatio(csv_reader):
    prev_file_num_race_num = (-1, -1)
    odds_count = 0
    total = 0
    
    for row in csv_reader:
        if (row['file_number'], row['race_number']) != prev_file_num_race_num:
            if row['odds'] != 'N/A':
                odds_count += 1
            total += 1
            prev_file_num_race_num = (row['file_number'], row['race_number'])
    
    return (0.2 * total) / odds_count

def encodeWeather(normalized_w):
    # if the length of normalized_w <= 2, there was most likely an error,
    # meaning this weather is invalid and gets an encoded value of 0
    if len(normalized_w) <= 2:
        return 0
    else:
        return weather_cache[normalized_w][1]

def encodeTrackState(normalized_ts):
    # if the length of normalized_ts <= 2, there was most likely an error,
    # meaning this track state is invalid and gets an encoded value of 0
    if len(normalized_ts) <= 2:
        return 0
    else:
        return track_state_cache[normalized_ts][1]

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

    print(f"Rows processed: {row_num}/{total_rows}.     Time elapsed: {formatted_elapsed_time}     Estimated time remaining: {formatted_time_left}")

def addTrainTestToDB(file_path, reset):
    global batch_queue, total_rows, weather_cache, track_state_cache, test_ratio
    try:
        print('Adding Trainables, Testables, and Updates to DB.')
        
        batch = []
        
        if reset:
            batch.append((dm.dropTrainables(), None))
            batch.append((dm.createTrainables(), None))
            batch_queue.put((batch.copy(), None))
            batch.clear()
            print('Trainables table reset.')
            batch.append((dm.dropTestables(), None))
            batch.append((dm.createTestables(), None))
            batch_queue.put((batch.copy(), None))
            batch.clear()
            print('Testables table reset.')
        
        # Get total number of rows in the CSV for progress tracking
        with open(file_path, mode='r') as csv_file:
            total_rows = sum(1 for row in csv_file) - 1  # Subtract 1 for the header row

        with open(file_path, mode='r') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            prev_file_num_race_num = (-1, -1)
            prev_file_num = -1
            
            buildCaches(csv_reader)
            
            csv_file.seek(0)  # Rewind again for second iteration
            csv_reader = csv.DictReader(csv_file)  # Reinitialize DictReader
            
            test_ratio = getTestRatio(csv_reader)
            
            csv_file.seek(0)  # Rewind again for second iteration
            csv_reader = csv.DictReader(csv_file)  # Reinitialize DictReader

            for row_num, row in enumerate(csv_reader, start=1):
                if len(batch) >= 1000:
                    batch_queue.put((batch.copy(), row_num))
                    batch.clear()  # Clear for next batch
                    
                track_n_name = dm.normalize(row['location'])
                owner_n_name = dm.normalize(row['owner'])
                horse_n_name = dm.normalize(row['horse_name'])
                jockey_n_name = dm.normalize(row['jockey'])
                trainer_n_name = dm.normalize(row['trainer'])
                
                if (row['file_number'], row['race_number']) != prev_file_num_race_num:
                    test = random.random() < test_ratio and row['odds'] != 'N/A'
                    batch.append(dm.addRace(
                        row['race_id'], row['file_number'], track_n_name, row['race_number'], row['date'], row['race_type'], row['surface'],
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
                
                if test:
                    batch.append(dm.addTestable(horse_n_name, track_n_name, jockey_n_name, trainer_n_name, 
                                                owner_n_name, row['surface'], row['race_id'], row['final_pos'], row['race_type'], 
                                                Decimal(row['weight']) if row['weight'] else None, 
                                                encodeWeather(dm.normalize(row['weather'])),
                                                row['temp'], 
                                                encodeTrackState(dm.normalize(row['track_state'])), 
                                                Decimal(row['distance(miles)']), 
                                                Decimal(row['odds']) if row['odds'] != 'N/A' else None))
                else:
                    batch.append(dm.addTrainable(horse_n_name, track_n_name, jockey_n_name, trainer_n_name, 
                                                owner_n_name, row['surface'], row['race_id'], row['final_pos'], row['race_type'], 
                                                Decimal(row['weight']) if row['weight'] else None, 
                                                encodeWeather(dm.normalize(row['weather'])),
                                                row['temp'], 
                                                encodeTrackState(dm.normalize(row['track_state'])), 
                                                Decimal(row['distance(miles)']), 
                                                Decimal(0) if int(row['final_pos']) == 1 else Decimal(100), 
                                                Decimal(0) if int(row['final_pos']) <= 2 else Decimal(100), 
                                                Decimal(0) if int(row['final_pos']) <= 3 else Decimal(100)))
                
                if row['file_number'] != prev_file_num:
                    batch.append(dm.addTrack(row['location'], track_n_name, Decimal(row['distance(miles)']) / Decimal(row['final_time']) if row['final_time'] else None))
                    prev_file_num = row['file_number']
                
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
                
                batch.append(dm.addJockey(row['jockey'], jockey_n_name, 
                                          Decimal(row['pos_gain']) if row['pos_gain'] else None, 
                                          Decimal(row['late_pos_gain']) if row['late_pos_gain'] else None, 
                                          Decimal(row['last_pos_gain']) if row['last_pos_gain'] else None, Decimal(row['final_pos']), 
                                          Decimal(row['pos_factor']) if row['pos_factor'] else None, 
                                          Decimal(row['speed']) if row['speed'] else None, track_n_name
                ))
                
                batch.append(dm.addTrainer(row['trainer'], trainer_n_name, Decimal(row['final_pos']),
                                           Decimal(row['pos_factor']) if row['pos_factor'] else None,
                                           Decimal(row['speed']) if row['speed'] else None, track_n_name,
                                           row['surface'], Decimal(row['distance(miles)']), Decimal(row['total_horses'])
                ))
                
                batch.append(dm.addOwner(row['owner'], owner_n_name, Decimal(row['final_pos']),
                                           Decimal(row['pos_factor']) if row['pos_factor'] else None,
                                           Decimal(row['speed']) if row['speed'] else None, track_n_name
                ))
                batch.append(dm.addPerformance(row['race_id'], row['file_number'], row['date'], row['race_number'], track_n_name, horse_n_name, row['program_number'], 
                                               Decimal(row['weight']) if row['weight'] else None, 
                                               None if row['odds'] == 'N/A' else Decimal(row['odds']), 
                                               row['start_pos'] if row['start_pos'] else None, 
                                               Decimal(row['final_pos']), 
                                               jockey_n_name, trainer_n_name, owner_n_name, 
                                               Decimal(row['pos_gain']) if row['pos_gain'] else None, 
                                               Decimal(row['late_pos_gain']) if row['late_pos_gain'] else None, 
                                               Decimal(row['last_pos_gain']) if row['last_pos_gain'] else None, 
                                               Decimal(row['pos_factor']) if row['pos_factor'] else None, 
                                               Decimal(row['speed']) if row['speed'] else None, 
                                               'TESTING' if test else 'TRAINING'
                ))
                
                batch.append(dm.addOwnerTrainer(owner_n_name, trainer_n_name, Decimal(row['final_pos']), 
                                             Decimal(row['pos_factor']) if row['pos_factor'] else None, 
                                             Decimal(row['speed']) if row['speed'] else None, 
                                             track_n_name
                ))
                
                batch.append(dm.addHorseTrack(horse_n_name, track_n_name, Decimal(row['final_pos']), 
                                             Decimal(row['pos_factor']) if row['pos_factor'] else None, 
                                             Decimal(row['speed']) if row['speed'] else None,
                                             row['surface']
                ))
                
                batch.append(dm.addJockeyTrainer(jockey_n_name, trainer_n_name, Decimal(row['final_pos']), 
                                             Decimal(row['pos_factor']) if row['pos_factor'] else None, 
                                             Decimal(row['speed']) if row['speed'] else None,
                                             track_n_name
                ))
                
                batch.append(dm.addHorseJockey(horse_n_name, jockey_n_name, Decimal(row['final_pos']), 
                                             Decimal(row['pos_factor']) if row['pos_factor'] else None, 
                                             Decimal(row['speed']) if row['speed'] else None, 
                                             track_n_name
                ))
                
                batch.append(dm.addHorseTrainer(horse_n_name, trainer_n_name, Decimal(row['final_pos']), 
                                             Decimal(row['pos_factor']) if row['pos_factor'] else None, 
                                             Decimal(row['speed']) if row['speed'] else None, 
                                             track_n_name
                ))
                
                batch.append(dm.addTrainerTrack(trainer_n_name, track_n_name, Decimal(row['final_pos']), 
                                             Decimal(row['pos_factor']) if row['pos_factor'] else None, 
                                             Decimal(row['speed']) if row['speed'] else None,
                                             row['surface']
                ))

            # Push any remaining queries as the final batch
            if batch:
                batch_queue.put((batch.copy(), row_num))
                batch.clear()  # Clear for next batch
    
        batch.append(dm.copyBadTestables())
        batch.append(dm.deleteBadTestables())
        batch_queue.put((batch.copy(), row_num))
        batch.clear()
        
        
    finally:
        pushBatches()

if __name__ == "__main__":
    addTrainTestToDB('traintest.csv', True)