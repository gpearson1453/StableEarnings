import psycopg2
import re
import unidecode
from fuzzywuzzy import fuzz
from datetime import datetime

# Establish connection to CockroachDB
def connect_db():
    return psycopg2.connect(
        dbname="defaultdb",
        user="molomala",
        password="aPyds3qPNhslU5xV8H-pMw",
        host="stable-earnings-3899.j77.aws-us-east-1.cockroachlabs.cloud",
        port="26257"
    )

# Generate a single query to update horse values based on the current database state
def updateHorse(horse_id, pos, num_horses, pos_factor, pos_gain, late_pos_gain, last_pos_gain, surface, distance, speed, track_id):
    alpha = 0.25
    d_factor_max_alpha = 0.5
    d_factor_alpha = (1 - ((pos - 1) / (num_horses - 1))) * d_factor_max_alpha
    
    query = """
        WITH TrackSpeed AS (
            SELECT ewma_speed 
            FROM Tracks 
            WHERE track_id = %s
        )
        UPDATE Horses
        SET total_races = total_races + 1,
        
            wins = CASE
                WHEN %s = 1 THEN wins + 1
                ELSE wins
            END,
            
            places = CASE
                WHEN %s < 3 THEN places + 1
                ELSE places
            END,
            
            shows = CASE
                WHEN %s < 4 THEN shows + 1
                ELSE shows
            END,
            
            ewma_pos_factor = CASE
                WHEN ewma_pos_factor IS NULL THEN %s
                ELSE (%s * %s + (1 - %s) * ewma_pos_factor)
            END,
            
            ewma_pos_gain = CASE
                WHEN ewma_pos_gain IS NULL THEN %s
                ELSE (%s * %s + (1 - %s) * ewma_pos_gain)
            END,
            
            ewma_late_pos_gain = CASE
                WHEN ewma_late_pos_gain IS NULL THEN %s
                ELSE (%s * %s + (1 - %s) * ewma_late_pos_gain)
            END,
            
            ewma_last_pos_gain = CASE
                WHEN ewma_last_pos_gain IS NULL THEN %s
                ELSE (%s * %s + (1 - %s) * ewma_last_pos_gain)
            END,
            
            perf_factor_count = CASE 
                WHEN %s IS NULL OR (SELECT ewma_speed FROM TrackSpeed) IS NULL THEN perf_factor_count
                ELSE perf_factor_count + 1
            END,
            
            ewma_perf_factor = CASE 
                WHEN %s IS NULL OR (SELECT ewma_speed FROM TrackSpeed) IS NULL THEN ewma_perf_factor
                ELSE (%s + 10 * ((%s / (SELECT ewma_speed FROM TrackSpeed)) - 1)) * %s + (1 - %s) * ewma_perf_factor
            END,
            
            recent_perf_factor = CASE 
                WHEN %s IS NULL OR (SELECT ewma_speed FROM TrackSpeed) IS NULL THEN recent_perf_factor
                ELSE %s + 10 * ((%s / (SELECT ewma_speed FROM TrackSpeed)) - 1)
            END,
            
            ewma_dirt_perf_factor = CASE 
                WHEN %s != 'Dirt' OR %s IS NULL OR (SELECT ewma_speed FROM TrackSpeed) IS NULL THEN ewma_dirt_perf_factor
                ELSE (%s + 10 * ((%s / (SELECT ewma_speed FROM TrackSpeed)) - 1)) * %s + (1 - %s) * ewma_dirt_perf_factor
            END,
            
            ewma_turf_perf_factor = CASE 
                WHEN %s != 'Turf' OR %s IS NULL OR (SELECT ewma_speed FROM TrackSpeed) IS NULL THEN ewma_turf_perf_factor
                ELSE (%s + 10 * ((%s / (SELECT ewma_speed FROM TrackSpeed)) - 1)) * %s + (1 - %s) * ewma_turf_perf_factor
            END,
            
            ewma_awt_perf_factor = CASE 
                WHEN %s != 'AWT' OR %s IS NULL OR (SELECT ewma_speed FROM TrackSpeed) IS NULL THEN ewma_awt_perf_factor
                ELSE (%s + 10 * ((%s / (SELECT ewma_speed FROM TrackSpeed)) - 1)) * %s + (1 - %s) * ewma_awt_perf_factor
            END,
            
            distance_factor = CASE
                WHEN %s < 2 THEN distance_factor
                ELSE (%s * %s + (1 - %s) * distance_factor)
            END
            
        WHERE horse_id = %s
    """
    values = (
        track_id,  # For CTE
        pos, pos, pos,  # For wins, places, shows
        pos_factor, pos_factor, alpha, alpha,  # ewma_pos_factor
        pos_gain, pos_gain, alpha, alpha,  # ewma_pos_gain
        late_pos_gain, late_pos_gain, alpha, alpha,  # ewma_late_pos_gain
        last_pos_gain, last_pos_gain, alpha, alpha,  # ewma_last_pos_gain
        speed,  # perf_factor_count
        speed, speed, pos_factor, alpha, alpha,  # ewma_perf_factor
        speed, pos_factor, speed,  # recent_perf_factor
        surface, speed, pos_factor, speed, alpha, alpha,  # ewma_dirt_perf_factor
        surface, speed, pos_factor, speed, alpha, alpha,  # ewma_turf_perf_factor
        surface, speed, pos_factor, speed, alpha, alpha,  # ewma_awt_perf_factor
        num_horses, distance, d_factor_alpha, d_factor_alpha,  # distance_factor
        horse_id  # horse_id in WHERE clause
    )
    return query, values


# Add a new horse to the Horses table
def addNewHorse(name, normalized_name, horse_id, pos, pos_factor, pos_gain, late_pos_gain, last_pos_gain, speed, track_id, surface, distance):
    query = """
    WITH TrackSpeed AS (
        SELECT ewma_speed 
        FROM Tracks 
        WHERE track_id = %s
    )
    INSERT INTO Horses (
        name, normalized_name, horse_id, total_races, wins, places, shows, ewma_pos_factor, 
        ewma_pos_gain, ewma_late_pos_gain, ewma_last_pos_gain, perf_factor_count, ewma_perf_factor, recent_perf_factor, 
        ewma_dirt_perf_factor, ewma_turf_perf_factor, ewma_awt_perf_factor, distance_factor
    )
    VALUES (
        %s, %s, %s, 1, -- name through total-races
        CASE WHEN %s = 1 THEN 1 ELSE 0, -- wins
        CASE WHEN %s < 3 THEN 1 ELSE 0, -- places
        CASE WHEN %s < 4 THEN 1 ELSE 0, -- shows
        %s, %s, %s, %s, -- ewma_pos_factor through ewma_last_pos_gain
        CASE WHEN %s IS NULL OR (SELECT ewma_speed FROM TrackSpeed) IS NULL 
            THEN 0
            ELSE 1 
        END,  -- perf_factor_count
        CASE WHEN %s IS NULL OR (SELECT ewma_speed FROM TrackSpeed) IS NULL 
            THEN NULL
            ELSE %s + 10 * ((%s / (SELECT ewma_speed FROM TrackSpeed)) - 1)
        END,  -- ewma_perf_factor
        CASE WHEN %s IS NULL OR (SELECT ewma_speed FROM TrackSpeed) IS NULL 
            THEN NULL
            ELSE %s + 10 * ((%s / (SELECT ewma_speed FROM TrackSpeed)) - 1)
        END,  -- recent_perf_factor
        CASE 
            WHEN %s = 'Dirt' AND %s IS NOT NULL AND (SELECT ewma_speed FROM TrackSpeed) IS NOT NULL
                THEN %s + 10 * ((%s / (SELECT ewma_speed FROM TrackSpeed)) - 1)
            ELSE NULL 
        END,  -- ewma_dirt_perf_factor
        CASE 
            WHEN %s = 'Turf' AND %s IS NOT NULL AND (SELECT ewma_speed FROM TrackSpeed) IS NOT NULL
                THEN %s + 10 * ((%s / (SELECT ewma_speed FROM TrackSpeed)) - 1)
            ELSE NULL 
        END,  -- ewma_turf_perf_factor
        CASE 
            WHEN %s = 'AWT' AND %s IS NOT NULL AND (SELECT ewma_speed FROM TrackSpeed) IS NOT NULL
                THEN %s + 10 * ((%s / (SELECT ewma_speed FROM TrackSpeed)) - 1)
            ELSE NULL 
        END,  -- ewma_awt_perf_factor
        %s  -- distance_factor
    )
    """
    values = (
        track_id,  # For CTE
        name, normalized_name, horse_id, # name through total-races
        pos, # wins
        pos, # places
        pos, # shows
        pos_factor, pos_gain, late_pos_gain, last_pos_gain,  # ewma_pos_factor through ewma_last_pos_gain
        speed,  # null check for perf_factor_count
        speed, pos_factor, speed,  # ewma_perf_factor
        speed, pos_factor, speed,  # recent_perf_factor
        surface, speed, pos_factor, speed,  # ewma_dirt_perf_factor
        surface, speed, pos_factor, speed,  # ewma_turf_perf_factor
        surface, speed, pos_factor, speed,  # ewma_awt_perf_factor
        distance  # distance_factor
    )
    return query, values




# Check for a track in the Tracks table
def check(normalized_name, cur, type):
    cur.execute("SELECT " + type + "_id, normalized_name FROM Tracks")
    for id, n_name in cur.fetchall():
        if n_name == normalized_name or is_similar(normalized_name, n_name):
            return id
    return None

# Generate a single query to update ewma_speed based on the current database value
def updateTrack(track_id, new_speed):
    alpha = 0.15
    # The query to update ewma_speed using the existing value in the same query
    query = """
        UPDATE Tracks
        SET ewma_speed = CASE
                            WHEN ewma_speed IS NULL THEN %s
                            ELSE %s * %s + (1 - %s)) * ewma_speed
                        END
        WHERE track_id = %s
    """
    values = (new_speed, new_speed, alpha, alpha, track_id)
    return query, values


# Add a track to the Tracks table
def addNewTrack(track_id, name, n_name, speed):
    query = """
    INSERT INTO Tracks (track_id, name, normalized_name, ewma_speed)
    VALUES (%s, %s, %s, %s)
    """
    return query, (track_id, name, n_name, speed)

# Add a jockey to the Jockeys table
def addNewJockey(name, avg_position_gain=None, avg_late_position_gain=None, avg_last_position_gain=None,
                 total_races=None, wins=None, places=None, shows=None, avg_pos_factor=None, ewma_perf_factor=None):
    
    query = """
    INSERT INTO Jockeys (name, avg_position_gain, avg_late_position_gain, avg_last_position_gain, total_races, 
                         wins, places, shows, avg_pos_factor, ewma_perf_factor)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    return query

# Add a trainer to the Trainers table
def addNewTrainer(name, total_races=None, wins=None, places=None, shows=None, avg_pos_factor=None,
                  ewma_perf_factor=None, ewma_dirt_perf_factor=None, ewma_turf_perf_factor=None, ewma_awt_perf_factor=None):
    
    query = """
    INSERT INTO Trainers (name, total_races, wins, places, shows, avg_pos_factor, ewma_perf_factor, 
                          ewma_dirt_perf_factor, ewma_turf_perf_factor, ewma_awt_perf_factor)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    return query

# Add an owner to the Owners table
def addNewOwner(name, total_races=None, wins=None, places=None, shows=None, avg_pos_factor=None, ewma_perf_factor=None):
    
    query = """
    INSERT INTO Owners (name, total_races, wins, places, shows, avg_pos_factor, ewma_perf_factor)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    return query

# Add a race to the Races table
def addNewRace(race_id, track_id, race_num, date, race_type=None, surface=None, weather=None,
               temperature=None, track_state=None, distance=None, final_time=None, speed=None,
               frac_time_1=None, frac_time_2=None, frac_time_3=None, frac_time_4=None, 
               frac_time_5=None, frac_time_6=None, split_time_1=None, split_time_2=None, 
               split_time_3=None, split_time_4=None, split_time_5=None, split_time_6=None):
    
    query = """
    INSERT INTO Races (race_id, track_id, race_num, date, race_type, surface, weather, temperature, track_state, 
                       distance, final_time, speed, frac_time_1, frac_time_2, frac_time_3, frac_time_4, frac_time_5, 
                       frac_time_6, split_time_1, split_time_2, split_time_3, split_time_4, split_time_5, split_time_6)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    return query, (race_id, track_id, race_num, date, race_type, surface, weather, 
                   temperature, track_state, distance, final_time, speed,
                   frac_time_1, frac_time_2, frac_time_3, frac_time_4, 
                   frac_time_5, frac_time_6, split_time_1, split_time_2, 
                   split_time_3, split_time_4, split_time_5, split_time_6)


# Add a performance to the Performances table
def addNewPerformance(race_id, horse_id, program_number, weight=None, start_pos=None, final_pos=None,
                      jockey_id=None, trainer_id=None, owner_id=None, pos_gained=None, late_pos_gained=None,
                      last_pos_gained=None, pos_factor=None, perf_factor=None, use=None):
    
    query = """
    INSERT INTO Performances (race_id, horse_id, program_number, weight, start_pos, final_pos, jockey_id,
                              trainer_id, owner_id, pos_gained, late_pos_gained, last_pos_gained, 
                              pos_factor, perf_factor, use)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    return query

# Add an owner_trainer relationship
def addNewOwnerTrainer(owner_id, trainer_id, total_races=None, wins=None, places=None, shows=None, avg_pos_factor=None, ewma_perf_factor=None):
    
    query = """
    INSERT INTO owner_trainer (owner_id, trainer_id, total_races, wins, places, shows, avg_pos_factor, ewma_perf_factor)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """
    return query

# Add a horse_jockey relationship
def addNewHorseJockey(horse_id, jockey_id, total_races=None, wins=None, places=None, shows=None, avg_pos_factor=None, ewma_perf_factor=None):
    
    query = """
    INSERT INTO horse_jockey (horse_id, jockey_id, total_races, wins, places, shows, avg_pos_factor, ewma_perf_factor)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """
    return query

# Add a horse_trainer relationship
def addNewHorseTrainer(horse_id, trainer_id, total_races=None, wins=None, places=None, shows=None, avg_pos_factor=None, ewma_perf_factor=None):
    
    query = """
    INSERT INTO horse_trainer (horse_id, trainer_id, total_races, wins, places, shows, avg_pos_factor, ewma_perf_factor)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """
    return query

# Add a trainer_track relationship
def addNewTrainerTrack(trainer_id, track_id, total_races=None, wins=None, places=None, shows=None, avg_pos_factor=None, ewma_perf_factor=None):
    
    query = """
    INSERT INTO trainer_track (trainer_id, track_id, total_races, wins, places, shows, avg_pos_factor, ewma_perf_factor)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """
    return query

# Add a horse_track relationship
def addNewHorseTrack(horse_id, track_id, total_races=None, wins=None, places=None, shows=None, avg_pos_factor=None, ewma_perf_factor=None):
    
    query = """
    INSERT INTO horse_track (horse_id, track_id, total_races, wins, places, shows, avg_pos_factor, ewma_perf_factor)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """
    return query

# Add a jockey_track relationship
def addNewJockeyTrack(jockey_id, track_id, total_races=None, wins=None, places=None, shows=None, avg_pos_factor=None, ewma_perf_factor=None):
    
    query = """
    INSERT INTO jockey_track (jockey_id, track_id, total_races, wins, places, shows, avg_pos_factor, ewma_perf_factor)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """
    return query

# Normalize a name string for fuzzy comparison
def normalize(name):
    return re.sub(r'[^a-z0-9]', '', unidecode.unidecode(name.strip().lower()))

# Check if two names are similar using fuzzy logic
def is_similar(name1, name2, threshold=90):
    return fuzz.ratio(name1, name2) > threshold

# This method converts date strings in the form "Month Date, Year" to sql date variables
def convertDate(date_str):
    # Convert the string date into a datetime object
    date_obj = datetime.strptime(date_str, '%B %d, %Y')
    
    # Convert the datetime object into the SQL 'YYYY-MM-DD' format
    return date_obj.strftime('%Y-%m-%d')

# This method converts strings of temperatures to floats and accounts for celsius temps
def convertTemp(temp):
    if '° C' in temp:
        return (float(temp.replace('° C', '')) * (9/5)) + 32
    else:
        return float(temp)
    
# This method converts times to floats
def convertTime(time):
    if time != 'N/A':
        nums = [float(n) for n in re.split('[:.]', time)]
        if len(nums) == 1:
            return nums[0] / 100
        elif len(nums) == 2:
            return nums[0] + nums[1] / 100
        else:
            return sum(nums[:-2]) * 60 + nums[-2] + nums[-1] / 100