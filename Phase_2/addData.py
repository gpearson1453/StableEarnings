import psycopg2

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
def addHorse(name, avg_pos_factor=None, st_dev_pos_factor=None, avg_position_gain=None,
             st_dev_position_gain=None, avg_late_position_gain=None, avg_last_position_gain=None,
             ewma_perf_factor=None, most_recent_perf_factor=None, ewma_dirt_perf_factor=None,
             ewma_turf_perf_factor=None, ewma_awt_perf_factor=None, distance_factor=None):
    
    conn = connect_db()
    cur = conn.cursor()

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
    cur.close()
    conn.close()
    return horse_id

# Add a track to the Tracks table
def addTrack(name, track_speed_factor=None):
    conn = connect_db()
    cur = conn.cursor()

    query = """
    INSERT INTO Tracks (name, track_speed_factor)
    VALUES (%s, %s)
    RETURNING track_id;
    """
    
    cur.execute(query, (name, track_speed_factor))
    track_id = cur.fetchone()[0]
    conn.commit()
    cur.close()
    conn.close()
    return track_id

# Add a jockey to the Jockeys table
def addJockey(name, avg_position_gain=None, avg_late_position_gain=None, avg_last_position_gain=None,
              total_races=None, wins=None, places=None, shows=None, avg_pos_factor=None, ewma_perf_factor=None):
    
    conn = connect_db()
    cur = conn.cursor()

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
    cur.close()
    conn.close()
    return jockey_id

# Add a trainer to the Trainers table
def addTrainer(name, total_races=None, wins=None, places=None, shows=None, avg_pos_factor=None,
               ewma_perf_factor=None, ewma_dirt_perf_factor=None, ewma_turf_perf_factor=None, ewma_awt_perf_factor=None):
    
    conn = connect_db()
    cur = conn.cursor()

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
    cur.close()
    conn.close()
    return trainer_id

# Add an owner to the Owners table
def addOwner(name, total_races=None, wins=None, places=None, shows=None, avg_pos_factor=None, ewma_perf_factor=None):
    conn = connect_db()
    cur = conn.cursor()

    query = """
    INSERT INTO Owners (name, total_races, wins, places, shows, avg_pos_factor, ewma_perf_factor)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    RETURNING owner_id;
    """
    
    cur.execute(query, (name, total_races, wins, places, shows, avg_pos_factor, ewma_perf_factor))
    owner_id = cur.fetchone()[0]
    conn.commit()
    cur.close()
    conn.close()
    return owner_id

# Add a race to the Races table
def addRace(track_id, race_num, date, race_type=None, surface=None, weather=None,
            temperature=None, track_state=None, distance=None, final_time=None,
            frac_time_1=None, frac_time_2=None, frac_time_3=None, frac_time_4=None, 
            frac_time_5=None, frac_time_6=None, split_time_1=None, split_time_2=None, 
            split_time_3=None, split_time_4=None, split_time_5=None, split_time_6=None):
    
    conn = connect_db()
    cur = conn.cursor()

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
    cur.close()
    conn.close()
    return race_id

# Add a performance to the Performances table
def addPerformance(race_id, horse_id, program_number, weight=None, start_pos=None, final_pos=None,
                   jockey_id=None, trainer_id=None, owner_id=None, pos_gained=None, late_pos_gained=None,
                   last_pos_gained=None, pos_factor=None, perf_factor=None, use=None):
    
    conn = connect_db()
    cur = conn.cursor()

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
    cur.close()
    conn.close()
    return result  # Returns race_id and horse_id as a tuple


# Add an owner_trainer relationship
def addOwnerTrainer(owner_id, trainer_id, total_races=None, wins=None, places=None, shows=None, avg_pos_factor=None, ewma_perf_factor=None):
    conn = connect_db()
    cur = conn.cursor()

    query = """
    INSERT INTO owner_trainer (owner_id, trainer_id, total_races, wins, places, shows, avg_pos_factor, ewma_perf_factor)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """
    
    cur.execute(query, (owner_id, trainer_id, total_races, wins, places, shows, avg_pos_factor, ewma_perf_factor))
    conn.commit()
    cur.close()
    conn.close()

# Add a horse_jockey relationship
def addHorseJockey(horse_id, jockey_id, total_races=None, wins=None, places=None, shows=None, avg_pos_factor=None, ewma_perf_factor=None):
    conn = connect_db()
    cur = conn.cursor()

    query = """
    INSERT INTO horse_jockey (horse_id, jockey_id, total_races, wins, places, shows, avg_pos_factor, ewma_perf_factor)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """
    
    cur.execute(query, (horse_id, jockey_id, total_races, wins, places, shows, avg_pos_factor, ewma_perf_factor))
    conn.commit()
    cur.close()
    conn.close()

# Add a horse_trainer relationship
def addHorseTrainer(horse_id, trainer_id, total_races=None, wins=None, places=None, shows=None, avg_pos_factor=None, ewma_perf_factor=None):
    conn = connect_db()
    cur = conn.cursor()

    query = """
    INSERT INTO horse_trainer (horse_id, trainer_id, total_races, wins, places, shows, avg_pos_factor, ewma_perf_factor)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """
    
    cur.execute(query, (horse_id, trainer_id, total_races, wins, places, shows, avg_pos_factor, ewma_perf_factor))
    conn.commit()
    cur.close()
    conn.close()

# Add a trainer_track relationship
def addTrainerTrack(trainer_id, track_id, total_races=None, wins=None, places=None, shows=None, avg_pos_factor=None, ewma_perf_factor=None):
    conn = connect_db()
    cur = conn.cursor()

    query = """
    INSERT INTO trainer_track (trainer_id, track_id, total_races, wins, places, shows, avg_pos_factor, ewma_perf_factor)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """
    
    cur.execute(query, (trainer_id, track_id, total_races, wins, places, shows, avg_pos_factor, ewma_perf_factor))
    conn.commit()
    cur.close()
    conn.close()

# Add a horse_track relationship
def addHorseTrack(horse_id, track_id, total_races=None, wins=None, places=None, shows=None, avg_pos_factor=None, ewma_perf_factor=None):
    conn = connect_db()
    cur = conn.cursor()

    query = """
    INSERT INTO horse_track (horse_id, track_id, total_races, wins, places, shows, avg_pos_factor, ewma_perf_factor)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """
    
    cur.execute(query, (horse_id, track_id, total_races, wins, places, shows, avg_pos_factor, ewma_perf_factor))
    conn.commit()
    cur.close()
    conn.close()

# Add a jockey_track relationship
def addJockeyTrack(jockey_id, track_id, total_races=None, wins=None, places=None, shows=None, avg_pos_factor=None, ewma_perf_factor=None):
    conn = connect_db()
    cur = conn.cursor()

    query = """
    INSERT INTO jockey_track (jockey_id, track_id, total_races, wins, places, shows, avg_pos_factor, ewma_perf_factor)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """
    
    cur.execute(query, (jockey_id, track_id, total_races, wins, places, shows, avg_pos_factor, ewma_perf_factor))
    conn.commit()
    cur.close()
    conn.close()
