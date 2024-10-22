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

# Add a horse to the Horses table
def addNewHorse(name, avg_pos_factor=None, st_dev_pos_factor=None, avg_position_gain=None,
                st_dev_position_gain=None, avg_late_position_gain=None, avg_last_position_gain=None,
                ewma_perf_factor=None, most_recent_perf_factor=None, ewma_dirt_perf_factor=None,
                ewma_turf_perf_factor=None, ewma_awt_perf_factor=None, distance_factor=None, conn=None, cur=None):
    
    query = """
    INSERT INTO Horses (name, avg_pos_factor, st_dev_pos_factor, avg_position_gain, st_dev_position_gain,
                        avg_late_position_gain, avg_last_position_gain, ewma_perf_factor, 
                        most_recent_perf_factor, ewma_dirt_perf_factor, ewma_turf_perf_factor,
                        ewma_awt_perf_factor, distance_factor)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    RETURNING horse_id;
    """
    
    cur.execute(query, (
        name, avg_pos_factor, st_dev_pos_factor, avg_position_gain, st_dev_position_gain, 
        avg_late_position_gain, avg_last_position_gain, ewma_perf_factor, most_recent_perf_factor, 
        ewma_dirt_perf_factor, ewma_turf_perf_factor, ewma_awt_perf_factor, distance_factor
    ))
    
    horse_id = cur.fetchone()[0]
    conn.commit()
    return horse_id

# Check for a track in the Tracks table
def checkTrack(normalized_name, cur):
    cur.execute("SELECT track_id, normalized_name FROM Tracks")
    tracks = cur.fetchall()

    for track_id, n_name in tracks:
        if is_similar(normalized_name, n_name):
            return track_id
    
    return None

# Add a track to the Tracks table
def addNewTrack(name, n_name, conn, cur):
    query = """
    INSERT INTO Tracks (name, normalized_name)
    VALUES (%s, %s)
    RETURNING track_id;
    """
    
    cur.execute(query, (name, n_name))
    track_id = cur.fetchone()[0]
    conn.commit()
    return track_id

# Add a jockey to the Jockeys table
def addNewJockey(name, avg_position_gain=None, avg_late_position_gain=None, avg_last_position_gain=None,
                 total_races=None, wins=None, places=None, shows=None, avg_pos_factor=None, ewma_perf_factor=None, conn=None, cur=None):
    
    query = """
    INSERT INTO Jockeys (name, avg_position_gain, avg_late_position_gain, avg_last_position_gain, total_races, 
                         wins, places, shows, avg_pos_factor, ewma_perf_factor)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    RETURNING jockey_id;
    """
    
    cur.execute(query, (
        name, avg_position_gain, avg_late_position_gain, avg_last_position_gain, total_races,
        wins, places, shows, avg_pos_factor, ewma_perf_factor
    ))
    
    jockey_id = cur.fetchone()[0]
    conn.commit()
    return jockey_id

# Add a trainer to the Trainers table
def addNewTrainer(name, total_races=None, wins=None, places=None, shows=None, avg_pos_factor=None,
                  ewma_perf_factor=None, ewma_dirt_perf_factor=None, ewma_turf_perf_factor=None, ewma_awt_perf_factor=None, conn=None, cur=None):
    
    query = """
    INSERT INTO Trainers (name, total_races, wins, places, shows, avg_pos_factor, ewma_perf_factor, 
                          ewma_dirt_perf_factor, ewma_turf_perf_factor, ewma_awt_perf_factor)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    RETURNING trainer_id;
    """
    
    cur.execute(query, (
        name, total_races, wins, places, shows, avg_pos_factor, ewma_perf_factor, 
        ewma_dirt_perf_factor, ewma_turf_perf_factor, ewma_awt_perf_factor
    ))
    
    trainer_id = cur.fetchone()[0]
    conn.commit()
    return trainer_id

# Add an owner to the Owners table
def addNewOwner(name, total_races=None, wins=None, places=None, shows=None, avg_pos_factor=None, ewma_perf_factor=None, conn=None, cur=None):
    
    query = """
    INSERT INTO Owners (name, total_races, wins, places, shows, avg_pos_factor, ewma_perf_factor)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    RETURNING owner_id;
    """
    
    cur.execute(query, (name, total_races, wins, places, shows, avg_pos_factor, ewma_perf_factor))
    owner_id = cur.fetchone()[0]
    conn.commit()
    return owner_id

# Add a race to the Races table
def addNewRace(track_id, race_num, date, race_type=None, surface=None, weather=None,
               temperature=None, track_state=None, distance=None, final_time=None,
               frac_time_1=None, frac_time_2=None, frac_time_3=None, frac_time_4=None, 
               frac_time_5=None, frac_time_6=None, split_time_1=None, split_time_2=None, 
               split_time_3=None, split_time_4=None, split_time_5=None, split_time_6=None, conn=None, cur=None):
    
    query = """
    INSERT INTO Races (track_id, race_num, date, race_type, surface, weather, temperature, track_state, 
                       distance, final_time, frac_time_1, frac_time_2, frac_time_3, frac_time_4, frac_time_5, 
                       frac_time_6, split_time_1, split_time_2, split_time_3, split_time_4, split_time_5, split_time_6)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    RETURNING race_id;
    """
    
    cur.execute(query, (
        track_id, race_num, date, race_type, surface, weather, temperature, track_state, 
        distance, final_time, frac_time_1, frac_time_2, frac_time_3, frac_time_4, frac_time_5, 
        frac_time_6, split_time_1, split_time_2, split_time_3, split_time_4, split_time_5, split_time_6
    ))

    race_id = cur.fetchone()[0]
    conn.commit()
    return race_id


# Add a performance to the Performances table
def addNewPerformance(race_id, horse_id, program_number, weight=None, start_pos=None, final_pos=None,
                      jockey_id=None, trainer_id=None, owner_id=None, pos_gained=None, late_pos_gained=None,
                      last_pos_gained=None, pos_factor=None, perf_factor=None, use=None, conn=None, cur=None):
    
    query = """
    INSERT INTO Performances (race_id, horse_id, program_number, weight, start_pos, final_pos, jockey_id,
                              trainer_id, owner_id, pos_gained, late_pos_gained, last_pos_gained, 
                              pos_factor, perf_factor, use)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    RETURNING race_id, horse_id;
    """
    
    cur.execute(query, (
        race_id, horse_id, program_number, weight, start_pos, final_pos, jockey_id, trainer_id, owner_id, 
        pos_gained, late_pos_gained, last_pos_gained, pos_factor, perf_factor, use
    ))

    result = cur.fetchone()
    conn.commit()
    return result  # Returns race_id and horse_id as a tuple

# Add an owner_trainer relationship
def addNewOwnerTrainer(owner_id, trainer_id, total_races=None, wins=None, places=None, shows=None, avg_pos_factor=None, ewma_perf_factor=None, conn=None, cur=None):
    
    query = """
    INSERT INTO owner_trainer (owner_id, trainer_id, total_races, wins, places, shows, avg_pos_factor, ewma_perf_factor)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """
    
    cur.execute(query, (owner_id, trainer_id, total_races, wins, places, shows, avg_pos_factor, ewma_perf_factor))
    conn.commit()

# Add a horse_jockey relationship
def addNewHorseJockey(horse_id, jockey_id, total_races=None, wins=None, places=None, shows=None, avg_pos_factor=None, ewma_perf_factor=None, conn=None, cur=None):
    
    query = """
    INSERT INTO horse_jockey (horse_id, jockey_id, total_races, wins, places, shows, avg_pos_factor, ewma_perf_factor)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """
    
    cur.execute(query, (horse_id, jockey_id, total_races, wins, places, shows, avg_pos_factor, ewma_perf_factor))
    conn.commit()

# Add a horse_trainer relationship
def addNewHorseTrainer(horse_id, trainer_id, total_races=None, wins=None, places=None, shows=None, avg_pos_factor=None, ewma_perf_factor=None, conn=None, cur=None):
    
    query = """
    INSERT INTO horse_trainer (horse_id, trainer_id, total_races, wins, places, shows, avg_pos_factor, ewma_perf_factor)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """
    
    cur.execute(query, (horse_id, trainer_id, total_races, wins, places, shows, avg_pos_factor, ewma_perf_factor))
    conn.commit()

# Add a trainer_track relationship
def addNewTrainerTrack(trainer_id, track_id, total_races=None, wins=None, places=None, shows=None, avg_pos_factor=None, ewma_perf_factor=None, conn=None, cur=None):
    
    query = """
    INSERT INTO trainer_track (trainer_id, track_id, total_races, wins, places, shows, avg_pos_factor, ewma_perf_factor)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """
    
    cur.execute(query, (trainer_id, track_id, total_races, wins, places, shows, avg_pos_factor, ewma_perf_factor))
    conn.commit()

# Add a horse_track relationship
def addNewHorseTrack(horse_id, track_id, total_races=None, wins=None, places=None, shows=None, avg_pos_factor=None, ewma_perf_factor=None, conn=None, cur=None):
    
    query = """
    INSERT INTO horse_track (horse_id, track_id, total_races, wins, places, shows, avg_pos_factor, ewma_perf_factor)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """
    
    cur.execute(query, (horse_id, track_id, total_races, wins, places, shows, avg_pos_factor, ewma_perf_factor))
    conn.commit()

# Add a jockey_track relationship
def addNewJockeyTrack(jockey_id, track_id, total_races=None, wins=None, places=None, shows=None, avg_pos_factor=None, ewma_perf_factor=None, conn=None, cur=None):
    
    query = """
    INSERT INTO jockey_track (jockey_id, track_id, total_races, wins, places, shows, avg_pos_factor, ewma_perf_factor)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """
    
    cur.execute(query, (jockey_id, track_id, total_races, wins, places, shows, avg_pos_factor, ewma_perf_factor))
    conn.commit()

# Normalize a name string for fuzzy comparison
def normalize(name):
    return re.sub(r'[^a-z0-9\s]', '', unidecode.unidecode(name.strip().lower()))

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
    if time == 'N/A':
        return None
    else:
        nums = [float(n) for n in re.split('[:.]', time)]
        if len(nums) == 1:
            return nums[0] / 100
        elif len(nums) == 2:
            return nums[0] + nums[1] / 100
        else:
            return sum(nums[:-2]) * 60 + nums[-2] + nums[-1] / 100