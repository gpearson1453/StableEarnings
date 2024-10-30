import psycopg2
import re
import unidecode
from fuzzywuzzy import fuzz
from datetime import datetime
from decimal import Decimal

# Establish connection to CockroachDB
def connect_db():
    return psycopg2.connect(
        dbname="defaultdb",
        user="molomala",
        password="aPyds3qPNhslU5xV8H-pMw",
        host="stable-earnings-3899.j77.aws-us-east-1.cockroachlabs.cloud",
        port="26257"
    )

# Add a new horse to the Horses table
def addHorse(name, n_name, pos, pos_factor, pos_gain, late_pos_gain, last_pos_gain, speed, track_n_name, surface, distance, num_horses):
    alpha = Decimal(0.25)
    d_factor_max_alpha = 0.5
    d_factor_alpha = Decimal((1 - ((int(pos) - 1) / (int(num_horses) - 1))) * d_factor_max_alpha)

    query = """
    WITH TrackSpeed AS (
        SELECT ewma_speed
        FROM Tracks
        WHERE normalized_name = %s
    ),
    PerformanceFactor AS (
        SELECT 
            CASE 
                WHEN %s IS NULL OR (SELECT ewma_speed FROM TrackSpeed) IS NULL THEN NULL
                ELSE %s + (10 * ((%s / (SELECT ewma_speed FROM TrackSpeed)) - 1))
            END AS p_factor
        FROM TrackSpeed
    )
    INSERT INTO Horses (
        name, normalized_name, total_races, wins, places, shows, 
        ewma_pos_factor, ewma_pos_gain, ewma_late_pos_gain, 
        ewma_last_pos_gain, perf_factor_count, ewma_perf_factor, 
        recent_perf_factor, ewma_dirt_perf_factor, ewma_turf_perf_factor, 
        ewma_awt_perf_factor, distance_factor
    )
    SELECT
        %s, %s, 1, -- name, n_name, total_races
        CASE WHEN %s = 1 THEN 1 ELSE 0 END, -- wins
        CASE WHEN %s < 3 THEN 1 ELSE 0 END, -- places
        CASE WHEN %s < 4 THEN 1 ELSE 0 END, -- shows
        %s, %s, %s, %s, -- ewma_pos_factor, ewma_pos_gain, ewma_late_pos_gain, ewma_last_pos_gain
        CASE WHEN (SELECT p_factor FROM PerformanceFactor) IS NULL THEN 0 ELSE 1 END, -- perf_factor_count
        (SELECT p_factor FROM PerformanceFactor), -- ewma_perf_factor
        (SELECT p_factor FROM PerformanceFactor), -- recent_perf_factor
        CASE WHEN %s = 'Dirt' THEN (SELECT p_factor FROM PerformanceFactor) ELSE NULL END, -- ewma_dirt_perf_factor
        CASE WHEN %s = 'Turf' THEN (SELECT p_factor FROM PerformanceFactor) ELSE NULL END, -- ewma_turf_perf_factor
        CASE WHEN %s = 'AWT' THEN (SELECT p_factor FROM PerformanceFactor) ELSE NULL END, -- ewma_awt_perf_factor
        CASE WHEN %s < 2 THEN NULL ELSE %s END -- distance_factor
    ON CONFLICT (normalized_name) DO UPDATE SET
        total_races = Horses.total_races + 1,
        wins = Horses.wins + excluded.wins,
        places = Horses.places + excluded.places,
        shows = Horses.shows + excluded.shows,
        ewma_pos_factor = CASE 
            WHEN excluded.ewma_pos_factor IS NULL THEN Horses.ewma_pos_factor
            WHEN Horses.ewma_pos_factor IS NULL THEN excluded.ewma_pos_factor
            ELSE (excluded.ewma_pos_factor * %s) + ((1 - %s) * Horses.ewma_pos_factor)
        END,
        ewma_pos_gain = CASE 
            WHEN excluded.ewma_pos_gain IS NULL THEN Horses.ewma_pos_gain
            WHEN Horses.ewma_pos_gain IS NULL THEN excluded.ewma_pos_gain
            ELSE (excluded.ewma_pos_gain * %s) + ((1 - %s) * Horses.ewma_pos_gain)
        END,
        ewma_late_pos_gain = CASE 
            WHEN excluded.ewma_late_pos_gain IS NULL THEN Horses.ewma_late_pos_gain
            WHEN Horses.ewma_late_pos_gain IS NULL THEN excluded.ewma_late_pos_gain
            ELSE (excluded.ewma_late_pos_gain * %s) + ((1 - %s) * Horses.ewma_late_pos_gain)
        END,
        ewma_last_pos_gain = CASE 
            WHEN excluded.ewma_last_pos_gain IS NULL THEN Horses.ewma_last_pos_gain
            WHEN Horses.ewma_last_pos_gain IS NULL THEN excluded.ewma_last_pos_gain
            ELSE (excluded.ewma_last_pos_gain * %s) + ((1 - %s) * Horses.ewma_last_pos_gain)
        END,
        perf_factor_count = Horses.perf_factor_count + excluded.perf_factor_count,
        ewma_perf_factor = CASE 
            WHEN excluded.ewma_perf_factor IS NULL THEN Horses.ewma_perf_factor
            WHEN Horses.ewma_perf_factor IS NULL THEN excluded.ewma_perf_factor
            ELSE (excluded.ewma_perf_factor * %s) + ((1 - %s) * Horses.ewma_perf_factor)
        END,
        recent_perf_factor = CASE
            WHEN excluded.recent_perf_factor IS NULL THEN Horses.recent_perf_factor
            ELSE excluded.recent_perf_factor
        END,
        ewma_dirt_perf_factor = CASE 
            WHEN excluded.ewma_dirt_perf_factor IS NULL THEN Horses.ewma_dirt_perf_factor
            WHEN Horses.ewma_dirt_perf_factor IS NULL THEN excluded.ewma_dirt_perf_factor
            ELSE (excluded.ewma_dirt_perf_factor * %s) + ((1 - %s) * Horses.ewma_dirt_perf_factor)
        END,
        ewma_turf_perf_factor = CASE 
            WHEN excluded.ewma_turf_perf_factor IS NULL THEN Horses.ewma_turf_perf_factor
            WHEN Horses.ewma_turf_perf_factor IS NULL THEN excluded.ewma_turf_perf_factor
            ELSE (excluded.ewma_turf_perf_factor * %s) + ((1 - %s) * Horses.ewma_turf_perf_factor)
        END,
        ewma_awt_perf_factor = CASE 
            WHEN excluded.ewma_awt_perf_factor IS NULL THEN Horses.ewma_awt_perf_factor
            WHEN Horses.ewma_awt_perf_factor IS NULL THEN excluded.ewma_awt_perf_factor
            ELSE (excluded.ewma_awt_perf_factor * %s) + ((1 - %s) * Horses.ewma_awt_perf_factor)
        END,
        distance_factor = CASE
            WHEN excluded.distance_factor IS NULL THEN Horses.distance_factor
            WHEN Horses.distance_factor IS NULL THEN excluded.distance_factor
            ELSE (excluded.distance_factor * %s) + ((1 - %s) * Horses.distance_factor)
        END
    """
    
    values = (
        # CTEs
        track_n_name, # TrackSpeed CTE
        speed, pos_factor, speed, # PerformanceFactor CTE
        
        # ADDING
        name, n_name, # name, n_name, total_races
        pos, # wins
        pos, # places
        pos, # shows
        pos_factor, pos_gain, late_pos_gain, last_pos_gain, # ewma_pos_factor, ewma_pos_gain, ewma_late_pos_gain, ewma_last_pos_gain
        surface, # ewma_dirt_perf_factor
        surface, # ewma_turf_perf_factor
        surface, # ewma_awt_perf_factor
        num_horses, distance, # distance_factor
        
        # UPDATING
        alpha, alpha, # ewma_pos_factor
        alpha, alpha, # ewma_pos_gain
        alpha, alpha, # ewma_late_pos_gain
        alpha, alpha, # ewma_last_pos_gain
        alpha, alpha, # ewma_perf_factor
        alpha, alpha, # ewma_dirt_perf_factor
        alpha, alpha, # ewma_turf_perf_factor
        alpha, alpha, # ewma_awt_perf_factor
        d_factor_alpha, d_factor_alpha # distance_factor
    )
    
    return query, values

# Add a track to the Tracks table
def addTrack(name, n_name, speed):
    alpha = Decimal(0.15)
    query = """
    INSERT INTO Tracks (name, normalized_name, ewma_speed)
    VALUES (%s, %s, %s)
    ON CONFLICT (normalized_name) DO UPDATE SET
    ewma_speed = CASE
        WHEN excluded.ewma_speed IS NULL THEN Tracks.ewma_speed
        WHEN Tracks.ewma_speed IS NULL THEN excluded.ewma_speed
        ELSE (excluded.ewma_speed * %s) + ((1 - %s) * Tracks.ewma_speed)
    END
    """
    return query, (name, n_name, speed, alpha, alpha)

# Add a jockey to the Jockeys table
def addJockey(name, n_name, pos_gain, late_pos_gain, last_pos_gain, pos, pos_factor, speed, track_n_name):
    alpha = Decimal(0.15)
    query = """
    WITH TrackSpeed AS (
        SELECT ewma_speed
        FROM Tracks
        WHERE normalized_name = %s
    ),
    PerformanceFactor AS (
        SELECT 
            CASE 
                WHEN %s IS NULL OR (SELECT ewma_speed FROM TrackSpeed) IS NULL THEN NULL
                ELSE %s + (10 * ((%s / (SELECT ewma_speed FROM TrackSpeed)) - 1))
            END AS p_factor
        FROM TrackSpeed
    )
    INSERT INTO Jockeys (name, normalized_name, ewma_pos_gain, ewma_late_pos_gain, ewma_last_pos_gain, 
                        total_races, wins, places, shows, ewma_pos_factor, perf_factor_count, ewma_perf_factor)
    SELECT
        %s, %s, %s, %s, %s, 1, -- name, n_name, ewma_pos_gain, ewma_late_pos_gain, ewma_last_pos_gain, total_races
        CASE WHEN %s = 1 THEN 1 ELSE 0 END, -- wins
        CASE WHEN %s < 3 THEN 1 ELSE 0 END, -- places
        CASE WHEN %s < 4 THEN 1 ELSE 0 END, -- shows
        %s, -- ewma_pos_factor
        CASE WHEN (SELECT p_factor FROM PerformanceFactor) IS NULL THEN 0 ELSE 1 END, -- perf_factor_count
        (SELECT p_factor FROM PerformanceFactor) -- ewma_perf_factor
    ON CONFLICT (normalized_name) DO UPDATE SET
        ewma_pos_gain = CASE 
            WHEN excluded.ewma_pos_gain IS NULL THEN Jockeys.ewma_pos_gain
            WHEN Jockeys.ewma_pos_gain IS NULL THEN excluded.ewma_pos_gain
            ELSE (excluded.ewma_pos_gain * %s) + ((1 - %s) * Jockeys.ewma_pos_gain)
        END,
        ewma_late_pos_gain = CASE 
            WHEN excluded.ewma_late_pos_gain IS NULL THEN Jockeys.ewma_late_pos_gain
            WHEN Jockeys.ewma_late_pos_gain IS NULL THEN excluded.ewma_late_pos_gain
            ELSE (excluded.ewma_late_pos_gain * %s) + ((1 - %s) * Jockeys.ewma_late_pos_gain)
        END,
        ewma_last_pos_gain = CASE 
            WHEN excluded.ewma_last_pos_gain IS NULL THEN Jockeys.ewma_last_pos_gain
            WHEN Jockeys.ewma_last_pos_gain IS NULL THEN excluded.ewma_last_pos_gain
            ELSE (excluded.ewma_last_pos_gain * %s) + ((1 - %s) * Jockeys.ewma_last_pos_gain)
        END,
        total_races = Jockeys.total_races + 1,
        wins = Jockeys.wins + excluded.wins,
        places = Jockeys.places + excluded.places,
        shows = Jockeys.shows + excluded.shows,
        ewma_pos_factor = CASE 
            WHEN excluded.ewma_pos_factor IS NULL THEN Jockeys.ewma_pos_factor
            WHEN Jockeys.ewma_pos_factor IS NULL THEN excluded.ewma_pos_factor
            ELSE (excluded.ewma_pos_factor * %s) + ((1 - %s) * Jockeys.ewma_pos_factor)
        END,
        perf_factor_count = Jockeys.perf_factor_count + excluded.perf_factor_count,
        ewma_perf_factor = CASE 
            WHEN excluded.ewma_perf_factor IS NULL THEN Jockeys.ewma_perf_factor
            WHEN Jockeys.ewma_perf_factor IS NULL THEN excluded.ewma_perf_factor
            ELSE (excluded.ewma_perf_factor * %s) + ((1 - %s) * Jockeys.ewma_perf_factor)
        END
    """
    
    values = (
        # CTEs
        track_n_name, # TrackSpeed CTE
        speed, pos_factor, speed, # PerformanceFactor CTE
        
        # ADDING
        name, n_name, pos_gain, late_pos_gain, last_pos_gain, # name, n_name, ewma_pos_gain, late_pos_gain, last_pos_gain
        pos, # wins
        pos, # places
        pos, # shows
        pos_factor, # ewma_pos_factor
        
        # UPDATING
        alpha, alpha, # ewma_pos_gain
        alpha, alpha, # ewma_late_pos_gain
        alpha, alpha, # ewma_last_pos_gain
        alpha, alpha, # ewma_pos_factor
        alpha, alpha, # ewma_perf_factor
    )
    
    return query, values

# Add a trainer to the Trainers table
def addTrainer(name, total_races=None, wins=None, places=None, shows=None, avg_pos_factor=None,
                  ewma_perf_factor=None, ewma_dirt_perf_factor=None, ewma_turf_perf_factor=None, ewma_awt_perf_factor=None):
    
    query = """
    INSERT INTO Trainers (name, total_races, wins, places, shows, avg_pos_factor, ewma_perf_factor, 
                          ewma_dirt_perf_factor, ewma_turf_perf_factor, ewma_awt_perf_factor)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    values = ()
    
    return query

# Add an owner to the Owners table
def addNewOwner(name, total_races=None, wins=None, places=None, shows=None, avg_pos_factor=None, ewma_perf_factor=None):
    
    query = """
    INSERT INTO Owners (name, total_races, wins, places, shows, avg_pos_factor, ewma_perf_factor)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    return query

# Add a race to the Races table
def addRace(track_n_name, race_num, date, race_type=None, surface=None, weather=None,
               temperature=None, track_state=None, distance=None, final_time=None, speed=None,
               frac_time_1=None, frac_time_2=None, frac_time_3=None, frac_time_4=None, 
               frac_time_5=None, frac_time_6=None, split_time_1=None, split_time_2=None, 
               split_time_3=None, split_time_4=None, split_time_5=None, split_time_6=None):
    
    query = """
    INSERT INTO Races (track_n_name, race_num, date, race_type, surface, weather, temperature, track_state, 
                       distance, final_time, speed, frac_time_1, frac_time_2, frac_time_3, frac_time_4, frac_time_5, 
                       frac_time_6, split_time_1, split_time_2, split_time_3, split_time_4, split_time_5, split_time_6)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    return query, (track_n_name, race_num, date, race_type, surface, weather, 
                   temperature, track_state, distance, final_time, speed,
                   frac_time_1, frac_time_2, frac_time_3, frac_time_4, 
                   frac_time_5, frac_time_6, split_time_1, split_time_2, 
                   split_time_3, split_time_4, split_time_5, split_time_6)


# Add a performance to the Performances table
def addPerformance(date, race_num, track_n_name, horse_n_name, program_number, weight, odds, start_pos, 
                   final_pos, jockey_n_name, trainer_n_name, owner_n_name, pos_gained, late_pos_gained, 
                   last_pos_gained, pos_factor, speed, use):
    
    query = """
    WITH TrackSpeed AS (
        SELECT ewma_speed
        FROM Tracks
        WHERE normalized_name = %s
    ),
    PerformanceFactor AS (
        SELECT 
            CASE 
                WHEN %s IS NULL OR (SELECT ewma_speed FROM TrackSpeed) IS NULL THEN NULL
                ELSE %s + (10 * ((%s / (SELECT ewma_speed FROM TrackSpeed)) - 1))
            END AS p_factor
        FROM TrackSpeed
    )
    INSERT INTO Performances (date, race_num, track_n_name, horse_n_name, program_number, weight, odds, 
                            start_pos, final_pos, jockey_n_name, trainer_n_name, owner_n_name, pos_gained, 
                            late_pos_gained, last_pos_gained, pos_factor, perf_factor, use)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, (SELECT p_factor FROM PerformanceFactor), %s)
    """
    return query, (track_n_name, speed, pos_factor, speed, date, race_num, track_n_name, horse_n_name, program_number, weight, 
                   odds, start_pos, final_pos, jockey_n_name, trainer_n_name, owner_n_name, pos_gained, late_pos_gained, 
                   last_pos_gained, pos_factor, use)

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