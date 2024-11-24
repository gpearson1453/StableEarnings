import psycopg2
import re
import unidecode
from datetime import datetime
from decimal import Decimal

# Establish connection to local postgres db
def local_connect(db_name):
    return psycopg2.connect(
        dbname=db_name,
        user="postgres",
        password="B!h8Cjxa37!78Yh",
        host="localhost",
        port="5432"
    )

# Establish connection to CockroachDB
def cockroach_connect(db_name):
    return psycopg2.connect(
        dbname=db_name,
        user="molomala",
        password="aPyds3qPNhslU5xV8H-pMw",
        host="stable-earnings-3899.j77.aws-us-east-1.cockroachlabs.cloud",
        port="26257"
    )

# Drops Trainers Table
def dropTrainers():
    return "DROP TABLE IF EXISTS Trainers CASCADE;"
    
# Builds Trainers table
def createTrainers():
    return """
        CREATE TABLE IF NOT EXISTS Trainers (
            name VARCHAR(255) NOT NULL,
            normalized_name VARCHAR(255) PRIMARY KEY,
            total_races INT,
            wins INT,
            places INT,
            shows INT,
            ewma_pos_factor DECIMAL(10, 6),
            perf_factor_count INT DEFAULT 0,
            ewma_perf_factor DECIMAL(10, 6),
            ewma_dirt_perf_factor DECIMAL(10, 6),
            ewma_turf_perf_factor DECIMAL(10, 6),
            ewma_awt_perf_factor DECIMAL(10, 6),
            distance_factor DECIMAL(10, 6)
        );
        """

# Add a trainer to the Trainers table
def addTrainer(name, n_name, pos, pos_factor, speed, track_n_name, surface, distance, num_horses):
    alpha = Decimal(0.15)
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
    INSERT INTO Trainers (name, normalized_name, total_races, wins, places, shows, ewma_pos_factor, 
                            perf_factor_count, ewma_perf_factor, ewma_dirt_perf_factor, 
                            ewma_turf_perf_factor, ewma_awt_perf_factor, distance_factor)
    SELECT
        %s, %s, 1, -- name, n_name, total_races
        CASE WHEN %s = 1 THEN 1 ELSE 0 END, -- wins
        CASE WHEN %s < 3 THEN 1 ELSE 0 END, -- places
        CASE WHEN %s < 4 THEN 1 ELSE 0 END, -- shows
        %s, -- ewma_pos_factor
        CASE WHEN (SELECT p_factor FROM PerformanceFactor) IS NULL THEN 0 ELSE 1 END, -- perf_factor_count
        (SELECT p_factor FROM PerformanceFactor), -- ewma_perf_factor
        CASE WHEN %s = 'Dirt' THEN (SELECT p_factor FROM PerformanceFactor) ELSE NULL END, -- ewma_dirt_perf_factor
        CASE WHEN %s = 'Turf' THEN (SELECT p_factor FROM PerformanceFactor) ELSE NULL END, -- ewma_turf_perf_factor
        CASE WHEN %s = 'AWT' THEN (SELECT p_factor FROM PerformanceFactor) ELSE NULL END, -- ewma_awt_perf_factor
        CASE WHEN %s < 2 THEN NULL ELSE %s END -- distance_factor
    ON CONFLICT (normalized_name) DO UPDATE SET
        total_races = Trainers.total_races + 1,
        wins = Trainers.wins + excluded.wins,
        places = Trainers.places + excluded.places,
        shows = Trainers.shows + excluded.shows,
        ewma_pos_factor = CASE 
            WHEN excluded.ewma_pos_factor IS NULL THEN Trainers.ewma_pos_factor
            WHEN Trainers.ewma_pos_factor IS NULL THEN excluded.ewma_pos_factor
            ELSE (excluded.ewma_pos_factor * %s) + ((1 - %s) * Trainers.ewma_pos_factor)
        END,
        perf_factor_count = Trainers.perf_factor_count + excluded.perf_factor_count,
        ewma_perf_factor = CASE 
            WHEN excluded.ewma_perf_factor IS NULL THEN Trainers.ewma_perf_factor
            WHEN Trainers.ewma_perf_factor IS NULL THEN excluded.ewma_perf_factor
            ELSE (excluded.ewma_perf_factor * %s) + ((1 - %s) * Trainers.ewma_perf_factor)
        END,
        ewma_dirt_perf_factor = CASE 
            WHEN excluded.ewma_dirt_perf_factor IS NULL THEN Trainers.ewma_dirt_perf_factor
            WHEN Trainers.ewma_dirt_perf_factor IS NULL THEN excluded.ewma_dirt_perf_factor
            ELSE (excluded.ewma_dirt_perf_factor * %s) + ((1 - %s) * Trainers.ewma_dirt_perf_factor)
        END,
        ewma_turf_perf_factor = CASE 
            WHEN excluded.ewma_turf_perf_factor IS NULL THEN Trainers.ewma_turf_perf_factor
            WHEN Trainers.ewma_turf_perf_factor IS NULL THEN excluded.ewma_turf_perf_factor
            ELSE (excluded.ewma_turf_perf_factor * %s) + ((1 - %s) * Trainers.ewma_turf_perf_factor)
        END,
        ewma_awt_perf_factor = CASE 
            WHEN excluded.ewma_awt_perf_factor IS NULL THEN Trainers.ewma_awt_perf_factor
            WHEN Trainers.ewma_awt_perf_factor IS NULL THEN excluded.ewma_awt_perf_factor
            ELSE (excluded.ewma_awt_perf_factor * %s) + ((1 - %s) * Trainers.ewma_awt_perf_factor)
        END,
        distance_factor = CASE
            WHEN excluded.distance_factor IS NULL THEN Trainers.distance_factor
            WHEN Trainers.distance_factor IS NULL THEN excluded.distance_factor
            ELSE (excluded.distance_factor * %s) + ((1 - %s) * Trainers.distance_factor)
        END
    """
    
    values = (
        # CTEs
        track_n_name, # TrackSpeed CTE
        speed, pos_factor, speed, # PerformanceFactor CTE
        
        # ADDING
        name, n_name, # name, n_name
        pos, # wins
        pos, # places
        pos, # shows
        pos_factor, # ewma_pos_factor
        surface, # ewma_dirt_perf_factor
        surface, # ewma_turf_perf_factor
        surface, # ewma_awt_perf_factor
        num_horses, distance, # distance_factor
        
        # UPDATING
        alpha, alpha, # ewma_pos_factor
        alpha, alpha, # ewma_perf_factor
        alpha, alpha, # ewma_dirt_perf_factor
        alpha, alpha, # ewma_turf_perf_factor
        alpha, alpha, # ewma_awt_perf_factor
        d_factor_alpha, d_factor_alpha # distance_factor
    )
    
    return query, values

# Drops Owners table
def dropOwners():
    return "DROP TABLE IF EXISTS Owners CASCADE;"

# Builds Owners table
def createOwners():
    return """
        CREATE TABLE IF NOT EXISTS Owners (
            name VARCHAR(255) NOT NULL,
            normalized_name VARCHAR(255) PRIMARY KEY,
            total_races INT,
            wins INT,
            places INT,
            shows INT,
            ewma_pos_factor DECIMAL(10, 6),
            perf_factor_count INT DEFAULT 0,
            ewma_perf_factor DECIMAL(10, 6)
        );
        """

# Add an owner to the Owners table
def addOwner(name, n_name, pos, pos_factor, speed, track_n_name):
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
    INSERT INTO Owners (name, normalized_name, total_races, wins, places, shows, ewma_pos_factor, perf_factor_count, ewma_perf_factor)
    SELECT
        %s, %s, 1, -- name, n_name, total_races
        CASE WHEN %s = 1 THEN 1 ELSE 0 END, -- wins
        CASE WHEN %s < 3 THEN 1 ELSE 0 END, -- places
        CASE WHEN %s < 4 THEN 1 ELSE 0 END, -- shows
        %s, -- ewma_pos_factor
        CASE WHEN (SELECT p_factor FROM PerformanceFactor) IS NULL THEN 0 ELSE 1 END, -- perf_factor_count
        (SELECT p_factor FROM PerformanceFactor) -- ewma_perf_factor
    ON CONFLICT (normalized_name) DO UPDATE SET
        total_races = Owners.total_races + 1,
        wins = Owners.wins + excluded.wins,
        places = Owners.places + excluded.places,
        shows = Owners.shows + excluded.shows,
        ewma_pos_factor = CASE 
            WHEN excluded.ewma_pos_factor IS NULL THEN Owners.ewma_pos_factor
            WHEN Owners.ewma_pos_factor IS NULL THEN excluded.ewma_pos_factor
            ELSE (excluded.ewma_pos_factor * %s) + ((1 - %s) * Owners.ewma_pos_factor)
        END,
        perf_factor_count = Owners.perf_factor_count + excluded.perf_factor_count,
        ewma_perf_factor = CASE 
            WHEN excluded.ewma_perf_factor IS NULL THEN Owners.ewma_perf_factor
            WHEN Owners.ewma_perf_factor IS NULL THEN excluded.ewma_perf_factor
            ELSE (excluded.ewma_perf_factor * %s) + ((1 - %s) * Owners.ewma_perf_factor)
        END
    """
    
    values = (
        # CTEs
        track_n_name, # TrackSpeed CTE
        speed, pos_factor, speed, # PerformanceFactor CTE
        
        # ADDING
        name, n_name, # name, n_name
        pos, # wins
        pos, # places
        pos, # shows
        pos_factor,
        
        # UPDATING
        alpha, alpha, # ewma_pos_factor
        alpha, alpha # ewma_perf_factor
    )
    
    return query, values

# Drops Performances table
def dropPerformances():
    return "DROP TABLE IF EXISTS Performances CASCADE;"

# Creates use_type for Performances, Trainables, Testables tables
def createPerformancesUseType():
    return """
        DROP TYPE IF EXISTS use_type CASCADE;
        CREATE TYPE use_type AS ENUM ('SETUP', 'TRAINING', 'TESTING');
    """

# Builds Performances table
def createPerformances():
    return """
        CREATE TABLE IF NOT EXISTS Performances (
            race_id VARCHAR(255),
            file_num INT,
            track_n_name VARCHAR(255) REFERENCES Tracks(normalized_name) ON DELETE CASCADE,
            race_num INT,
            horse_n_name VARCHAR(255),
            date DATE,
            program_number VARCHAR(255),
            weight DECIMAL(10, 6),
            odds DECIMAL(10, 6),
            start_pos INT,
            final_pos INT,
            jockey_n_name VARCHAR(255) REFERENCES Jockeys(normalized_name) ON DELETE CASCADE,
            trainer_n_name VARCHAR(255) REFERENCES Trainers(normalized_name) ON DELETE CASCADE,
            owner_n_name VARCHAR(255) REFERENCES Owners(normalized_name) ON DELETE CASCADE,
            pos_gained DECIMAL(10, 6),
            late_pos_gained DECIMAL(10, 6),
            last_pos_gained DECIMAL(10, 6),
            pos_factor DECIMAL(10, 6),
            perf_factor DECIMAL(10, 6),
            use use_type,
            PRIMARY KEY (race_id, horse_n_name),
            FOREIGN KEY (race_id) REFERENCES Races(race_id) ON DELETE CASCADE,
            FOREIGN KEY (horse_n_name) REFERENCES Horses(normalized_name) ON DELETE CASCADE
        );
        """

# Add a performance to the Performances table
def addPerformance(race_id, file_num, date, race_num, track_n_name, horse_n_name, program_number, weight, odds, start_pos, 
                   final_pos, jockey_n_name, trainer_n_name, owner_n_name, pos_gain, late_pos_gain, 
                   last_pos_gain, pos_factor, speed, use):
    
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
    INSERT INTO Performances (race_id, file_num, date, track_n_name, race_num, horse_n_name, program_number, weight, odds, 
                            start_pos, final_pos, jockey_n_name, trainer_n_name, owner_n_name, pos_gained, 
                            late_pos_gained, last_pos_gained, pos_factor, perf_factor, use)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, (SELECT p_factor FROM PerformanceFactor), %s)
    """
    return query, (track_n_name, speed, pos_factor, speed, race_id, file_num, date, track_n_name, race_num, horse_n_name, program_number, weight, 
                   odds, start_pos, final_pos, jockey_n_name, trainer_n_name, owner_n_name, pos_gain, late_pos_gain, 
                   last_pos_gain, pos_factor, use)
    
# Drops Trainables table
def dropTrainables():
    return "DROP TABLE IF EXISTS Trainables CASCADE;"

# Builds Trainables table
def createTrainables():
    return """
        CREATE TABLE IF NOT EXISTS Trainables (
            race_id VARCHAR(255),
            final_pos INT,
            horse_n_name VARCHAR(255),
            race_type VARCHAR(255),
            
            -- Track stats
            track_ewma_speed DECIMAL(10, 6),
            
            -- Horse stats
            weight DECIMAL(10, 6),
            horse_total_races INT DEFAULT 0,
            horse_wins INT DEFAULT 0,
            horse_places INT DEFAULT 0,
            horse_shows INT DEFAULT 0,
            horse_ewma_pos_factor DECIMAL(10, 6),
            horse_ewma_pos_gain DECIMAL(10, 6),
            horse_ewma_late_pos_gain DECIMAL(10, 6),
            horse_ewma_last_pos_gain DECIMAL(10, 6),
            horse_perf_factor_count INT DEFAULT 0,
            horse_ewma_perf_factor DECIMAL(10, 6),
            horse_recent_perf_factor DECIMAL(10, 6),
            horse_ewma_surface_perf_factor DECIMAL(10, 6),
            horse_distance_factor DECIMAL(10, 6),
            
            -- Jockey stats
            jockey_ewma_pos_gain DECIMAL(10, 6),
            jockey_ewma_late_pos_gain DECIMAL(10, 6),
            jockey_ewma_last_pos_gain DECIMAL(10, 6),
            jockey_total_races INT,
            jockey_wins INT,
            jockey_places INT,
            jockey_shows INT,
            jockey_ewma_pos_factor DECIMAL(10, 6),
            jockey_perf_factor_count INT DEFAULT 0,
            jockey_ewma_perf_factor DECIMAL(10, 6),
            
            -- Trainer stats
            trainer_total_races INT,
            trainer_wins INT,
            trainer_places INT,
            trainer_shows INT,
            trainer_ewma_pos_factor DECIMAL(10, 6),
            trainer_perf_factor_count INT DEFAULT 0,
            trainer_ewma_perf_factor DECIMAL(10, 6),
            trainer_ewma_surface_perf_factor DECIMAL(10, 6),
            trainer_distance_factor DECIMAL(10, 6),
            
            -- Owner stats
            owner_total_races INT,
            owner_wins INT,
            owner_places INT,
            owner_shows INT,
            owner_ewma_pos_factor DECIMAL(10, 6),
            owner_perf_factor_count INT DEFAULT 0,
            owner_ewma_perf_factor DECIMAL(10, 6),
            
            -- Race stats
            weather INT,
            temperature DECIMAL(10, 6),
            track_state INT,
            distance DECIMAL(10, 6),
            
            -- Horse-Jockey stats
            horse_jockey_total_races INT,
            horse_jockey_wins INT,
            horse_jockey_places INT,
            horse_jockey_shows INT,
            horse_jockey_ewma_pos_factor DECIMAL(10, 6),
            horse_jockey_perf_factor_count INT DEFAULT 0,
            horse_jockey_ewma_perf_factor DECIMAL(10, 6),
            
            -- Horse-Trainer stats
            horse_trainer_total_races INT,
            horse_trainer_wins INT,
            horse_trainer_places INT,
            horse_trainer_shows INT,
            horse_trainer_ewma_pos_factor DECIMAL(10, 6),
            horse_trainer_perf_factor_count INT DEFAULT 0,
            horse_trainer_ewma_perf_factor DECIMAL(10, 6),
            
            -- Trainer-Track stats
            trainer_track_total_races INT,
            trainer_track_wins INT,
            trainer_track_places INT,
            trainer_track_shows INT,
            trainer_track_ewma_pos_factor DECIMAL(10, 6),
            trainer_track_perf_factor_count INT DEFAULT 0,
            trainer_track_ewma_perf_factor DECIMAL(10, 6),
            
            -- Owner-Trainer stats
            owner_trainer_total_races INT,
            owner_trainer_wins INT,
            owner_trainer_places INT,
            owner_trainer_shows INT,
            owner_trainer_ewma_pos_factor DECIMAL(10, 6),
            owner_trainer_perf_factor_count INT DEFAULT 0,
            owner_trainer_ewma_perf_factor DECIMAL(10, 6),
            
            -- Horse-Track stats
            horse_track_total_races INT,
            horse_track_wins INT,
            horse_track_places INT,
            horse_track_shows INT,
            horse_track_ewma_pos_factor DECIMAL(10, 6),
            horse_track_perf_factor_count INT DEFAULT 0,
            horse_track_ewma_perf_factor DECIMAL(10, 6),
            
            -- Jockey-Trainer stats
            jockey_trainer_total_races INT,
            jockey_trainer_wins INT,
            jockey_trainer_places INT,
            jockey_trainer_shows INT,
            jockey_trainer_ewma_pos_factor DECIMAL(10, 6),
            jockey_trainer_perf_factor_count INT DEFAULT 0,
            jockey_trainer_ewma_perf_factor DECIMAL(10, 6),
            
            -- Outcomes (will either be 0 or 100)
            won DECIMAL(10, 6),
            placed DECIMAL(10, 6),
            showed DECIMAL(10, 6),
            
            PRIMARY KEY (race_id, final_pos),
            FOREIGN KEY (race_id) REFERENCES Races(race_id) ON DELETE CASCADE
        );
        """

# Add a Trainable to the Trainables table
def addTrainable(horse_n_name, track_n_name, jockey_n_name, trainer_n_name, owner_n_name, surface, race_id, final_pos, race_type, weight, weather, temp, track_state, distance, won, placed, showed):
    
    query = """
    WITH TrackSpeed AS (
        SELECT ewma_speed
        FROM Tracks
        WHERE normalized_name = %s
    ), 
    HorseStats AS (
        SELECT 
            total_races, wins, places, shows, 
            ewma_pos_factor, ewma_pos_gain, ewma_late_pos_gain, 
            ewma_last_pos_gain, perf_factor_count, ewma_perf_factor, 
            recent_perf_factor, 
            CASE 
                WHEN %s = 'Dirt' THEN ewma_dirt_perf_factor
                WHEN %s = 'Turf' THEN ewma_turf_perf_factor
                WHEN %s = 'AWT' THEN ewma_awt_perf_factor
            END as ewma_surface_perf_factor, 
            distance_factor
        FROM Horses
        WHERE normalized_name = %s
    ), 
    JockeyStats AS (
        SELECT *
        FROM Jockeys
        WHERE normalized_name = %s
    ), 
    TrainerStats AS (
        SELECT 
            total_races, wins, places, shows, ewma_pos_factor, 
            perf_factor_count, ewma_perf_factor, 
            CASE 
                WHEN %s = 'Dirt' THEN ewma_dirt_perf_factor
                WHEN %s = 'Turf' THEN ewma_turf_perf_factor
                WHEN %s = 'AWT' THEN ewma_awt_perf_factor
            END as ewma_surface_perf_factor, 
            distance_factor
        FROM Trainers
        WHERE normalized_name = %s
    ), 
    OwnerStats AS (
        SELECT *
        FROM Owners
        WHERE normalized_name = %s
    ), 
    HorseJockeyStats AS (
        SELECT *
        FROM horse_jockey
        WHERE horse_n_name = %s AND jockey_n_name = %s
    ), 
    HorseTrainerStats AS (
        SELECT *
        FROM horse_trainer
        WHERE horse_n_name = %s AND trainer_n_name = %s
    ), 
    TrainerTrackStats AS (
        SELECT *
        FROM trainer_track
        WHERE trainer_n_name = %s AND track_n_name = %s AND surface = %s
    ), 
    OwnerTrainerStats AS (
        SELECT *
        FROM owner_trainer
        WHERE owner_n_name = %s AND trainer_n_name = %s
    ), 
    HorseTrackStats AS (
        SELECT *
        FROM horse_track
        WHERE horse_n_name = %s AND track_n_name = %s AND surface = %s
    ), 
    JockeyTrainerStats AS (
        SELECT *
        FROM jockey_trainer
        WHERE jockey_n_name = %s AND trainer_n_name = %s
    )
    INSERT INTO Trainables (
        race_id, final_pos, horse_n_name, race_type,
        
        track_ewma_speed,
        
        weight, horse_total_races, horse_wins, horse_places, horse_shows, 
        horse_ewma_pos_factor, horse_ewma_pos_gain, horse_ewma_late_pos_gain, 
        horse_ewma_last_pos_gain, horse_perf_factor_count, horse_ewma_perf_factor, 
        horse_recent_perf_factor, horse_ewma_surface_perf_factor, horse_distance_factor,
        
        jockey_ewma_pos_gain, jockey_ewma_late_pos_gain, jockey_ewma_last_pos_gain, 
        jockey_total_races, jockey_wins, jockey_places, jockey_shows, 
        jockey_ewma_pos_factor, jockey_perf_factor_count, jockey_ewma_perf_factor,
        
        trainer_total_races, trainer_wins, trainer_places, trainer_shows, 
        trainer_ewma_pos_factor, trainer_perf_factor_count, trainer_ewma_perf_factor, 
        trainer_ewma_surface_perf_factor, trainer_distance_factor,
        
        owner_total_races, owner_wins, owner_places, owner_shows, owner_ewma_pos_factor, 
        owner_perf_factor_count, owner_ewma_perf_factor,
        
        weather, temperature, track_state, distance,
        
        horse_jockey_total_races, horse_jockey_wins, horse_jockey_places, 
        horse_jockey_shows, horse_jockey_ewma_pos_factor, horse_jockey_perf_factor_count, 
        horse_jockey_ewma_perf_factor,
        
        horse_trainer_total_races, horse_trainer_wins, horse_trainer_places, 
        horse_trainer_shows, horse_trainer_ewma_pos_factor, horse_trainer_perf_factor_count, 
        horse_trainer_ewma_perf_factor,
        
        trainer_track_total_races, trainer_track_wins, trainer_track_places, 
        trainer_track_shows, trainer_track_ewma_pos_factor, trainer_track_perf_factor_count, 
        trainer_track_ewma_perf_factor,
        
        owner_trainer_total_races, owner_trainer_wins, owner_trainer_places, 
        owner_trainer_shows, owner_trainer_ewma_pos_factor, owner_trainer_perf_factor_count, 
        owner_trainer_ewma_perf_factor,
        
        horse_track_total_races, horse_track_wins, horse_track_places, 
        horse_track_shows, horse_track_ewma_pos_factor, horse_track_perf_factor_count, 
        horse_track_ewma_perf_factor,
        
        jockey_trainer_total_races, jockey_trainer_wins, jockey_trainer_places, 
        jockey_trainer_shows, jockey_trainer_ewma_pos_factor, jockey_trainer_perf_factor_count, 
        jockey_trainer_ewma_perf_factor,
        
        won, placed, showed)
    
    SELECT
        %s, %s, %s, %s, -- race_id, final_pos, horse_n_name, race_type
        
        (SELECT ewma_speed FROM TrackSpeed), -- track_ewma_speed
        
        %s, -- weight
        
        (SELECT total_races FROM HorseStats), 
        (SELECT wins FROM HorseStats), 
        (SELECT places FROM HorseStats), 
        (SELECT shows FROM HorseStats), 
        (SELECT ewma_pos_factor FROM HorseStats), 
        (SELECT ewma_pos_gain FROM HorseStats), 
        (SELECT ewma_late_pos_gain FROM HorseStats), 
        (SELECT ewma_last_pos_gain FROM HorseStats), 
        (SELECT perf_factor_count FROM HorseStats), 
        (SELECT ewma_perf_factor FROM HorseStats), 
        (SELECT recent_perf_factor FROM HorseStats), 
        (SELECT ewma_surface_perf_factor FROM HorseStats), 
        (SELECT distance_factor FROM HorseStats), -- horse stats
        
        (SELECT ewma_pos_gain FROM JockeyStats), 
        (SELECT ewma_late_pos_gain FROM JockeyStats), 
        (SELECT ewma_last_pos_gain FROM JockeyStats), 
        (SELECT total_races FROM JockeyStats), 
        (SELECT wins FROM JockeyStats), 
        (SELECT places FROM JockeyStats), 
        (SELECT shows FROM JockeyStats), 
        (SELECT ewma_pos_factor FROM JockeyStats), 
        (SELECT perf_factor_count FROM JockeyStats), 
        (SELECT ewma_perf_factor FROM JockeyStats), -- jockey stats
        
        (SELECT total_races FROM TrainerStats), 
        (SELECT wins FROM TrainerStats), 
        (SELECT places FROM TrainerStats), 
        (SELECT shows FROM TrainerStats), 
        (SELECT ewma_pos_factor FROM TrainerStats), 
        (SELECT perf_factor_count FROM TrainerStats), 
        (SELECT ewma_perf_factor FROM TrainerStats), 
        (SELECT ewma_surface_perf_factor FROM TrainerStats), 
        (SELECT distance_factor FROM TrainerStats), -- trainer stats
        
        (SELECT total_races FROM OwnerStats), 
        (SELECT wins FROM OwnerStats), 
        (SELECT places FROM OwnerStats), 
        (SELECT shows FROM OwnerStats), 
        (SELECT ewma_pos_factor FROM OwnerStats), 
        (SELECT perf_factor_count FROM OwnerStats), 
        (SELECT ewma_perf_factor FROM OwnerStats), -- owner stats
        
        %s, %s, %s, %s, -- weather, temperature, track_state, distance
        
        (SELECT total_races FROM HorseJockeyStats), 
        (SELECT wins FROM HorseJockeyStats), 
        (SELECT places FROM HorseJockeyStats), 
        (SELECT shows FROM HorseJockeyStats), 
        (SELECT ewma_pos_factor FROM HorseJockeyStats), 
        (SELECT perf_factor_count FROM HorseJockeyStats), 
        (SELECT ewma_perf_factor FROM HorseJockeyStats), -- horse-jockey stats
        
        (SELECT total_races FROM HorseTrainerStats), 
        (SELECT wins FROM HorseTrainerStats), 
        (SELECT places FROM HorseTrainerStats), 
        (SELECT shows FROM HorseTrainerStats), 
        (SELECT ewma_pos_factor FROM HorseTrainerStats), 
        (SELECT perf_factor_count FROM HorseTrainerStats), 
        (SELECT ewma_perf_factor FROM HorseTrainerStats), -- horse-trainer stats
        
        (SELECT total_races FROM TrainerTrackStats), 
        (SELECT wins FROM TrainerTrackStats), 
        (SELECT places FROM TrainerTrackStats), 
        (SELECT shows FROM TrainerTrackStats), 
        (SELECT ewma_pos_factor FROM TrainerTrackStats), 
        (SELECT perf_factor_count FROM TrainerTrackStats), 
        (SELECT ewma_perf_factor FROM TrainerTrackStats), -- trainer-track stats
        
        (SELECT total_races FROM OwnerTrainerStats), 
        (SELECT wins FROM OwnerTrainerStats), 
        (SELECT places FROM OwnerTrainerStats), 
        (SELECT shows FROM OwnerTrainerStats), 
        (SELECT ewma_pos_factor FROM OwnerTrainerStats), 
        (SELECT perf_factor_count FROM OwnerTrainerStats), 
        (SELECT ewma_perf_factor FROM OwnerTrainerStats), -- owner-trainer stats
        
        (SELECT total_races FROM HorseTrackStats), 
        (SELECT wins FROM HorseTrackStats), 
        (SELECT places FROM HorseTrackStats), 
        (SELECT shows FROM HorseTrackStats), 
        (SELECT ewma_pos_factor FROM HorseTrackStats), 
        (SELECT perf_factor_count FROM HorseTrackStats), 
        (SELECT ewma_perf_factor FROM HorseTrackStats), -- horse-track stats
        
        (SELECT total_races FROM JockeyTrainerStats), 
        (SELECT wins FROM JockeyTrainerStats), 
        (SELECT places FROM JockeyTrainerStats), 
        (SELECT shows FROM JockeyTrainerStats), 
        (SELECT ewma_pos_factor FROM JockeyTrainerStats), 
        (SELECT perf_factor_count FROM JockeyTrainerStats), 
        (SELECT ewma_perf_factor FROM JockeyTrainerStats), -- jockey-trainer stats
        
        %s, %s, %s
    """
    
    values = (
        # CTEs
        track_n_name, # TrackSpeed
        surface, surface, surface, horse_n_name, # HorseStats
        jockey_n_name, # JockeyStats
        surface, surface, surface, trainer_n_name, # TrainerStats
        owner_n_name, # OwnerStats
        
        horse_n_name, jockey_n_name, # HorseJockeyStats
        horse_n_name, trainer_n_name, # HorseTrainerStats
        trainer_n_name, track_n_name, surface, # TrainerTrackStats
        owner_n_name, trainer_n_name, # OwnerTrainerStats
        horse_n_name, track_n_name, surface, # HorseTrackStats
        jockey_n_name, trainer_n_name, # JockeyTrainerStats
        
        # ADDING
        race_id, final_pos, horse_n_name, race_type,
        weight,
        weather, temp, track_state, distance,
        won, placed, showed
    )
    
    return query, values

# Drops Testables table
def dropTestables():
    return "DROP TABLE IF EXISTS Testables CASCADE;"

# Builds Testables table
def createTestables():
    return """
        CREATE TABLE IF NOT EXISTS Testables (
            race_id VARCHAR(255),
            final_pos INT,
            horse_n_name VARCHAR(255),
            race_type VARCHAR(255),
            
            -- Track stats
            track_ewma_speed DECIMAL(10, 6),
            
            -- Horse stats
            weight DECIMAL(10, 6),
            horse_total_races INT DEFAULT 0,
            horse_wins INT DEFAULT 0,
            horse_places INT DEFAULT 0,
            horse_shows INT DEFAULT 0,
            horse_ewma_pos_factor DECIMAL(10, 6),
            horse_ewma_pos_gain DECIMAL(10, 6),
            horse_ewma_late_pos_gain DECIMAL(10, 6),
            horse_ewma_last_pos_gain DECIMAL(10, 6),
            horse_perf_factor_count INT DEFAULT 0,
            horse_ewma_perf_factor DECIMAL(10, 6),
            horse_recent_perf_factor DECIMAL(10, 6),
            horse_ewma_surface_perf_factor DECIMAL(10, 6),
            horse_distance_factor DECIMAL(10, 6),
            
            -- Jockey stats
            jockey_ewma_pos_gain DECIMAL(10, 6),
            jockey_ewma_late_pos_gain DECIMAL(10, 6),
            jockey_ewma_last_pos_gain DECIMAL(10, 6),
            jockey_total_races INT,
            jockey_wins INT,
            jockey_places INT,
            jockey_shows INT,
            jockey_ewma_pos_factor DECIMAL(10, 6),
            jockey_perf_factor_count INT DEFAULT 0,
            jockey_ewma_perf_factor DECIMAL(10, 6),
            
            -- Trainer stats
            trainer_total_races INT,
            trainer_wins INT,
            trainer_places INT,
            trainer_shows INT,
            trainer_ewma_pos_factor DECIMAL(10, 6),
            trainer_perf_factor_count INT DEFAULT 0,
            trainer_ewma_perf_factor DECIMAL(10, 6),
            trainer_ewma_surface_perf_factor DECIMAL(10, 6),
            trainer_distance_factor DECIMAL(10, 6),
            
            -- Owner stats
            owner_total_races INT,
            owner_wins INT,
            owner_places INT,
            owner_shows INT,
            owner_ewma_pos_factor DECIMAL(10, 6),
            owner_perf_factor_count INT DEFAULT 0,
            owner_ewma_perf_factor DECIMAL(10, 6),
            
            -- Race stats
            weather INT,
            temperature DECIMAL(10, 6),
            track_state INT,
            distance DECIMAL(10, 6),
            
            -- Horse-Jockey stats
            horse_jockey_total_races INT,
            horse_jockey_wins INT,
            horse_jockey_places INT,
            horse_jockey_shows INT,
            horse_jockey_ewma_pos_factor DECIMAL(10, 6),
            horse_jockey_perf_factor_count INT DEFAULT 0,
            horse_jockey_ewma_perf_factor DECIMAL(10, 6),
            
            -- Horse-Trainer stats
            horse_trainer_total_races INT,
            horse_trainer_wins INT,
            horse_trainer_places INT,
            horse_trainer_shows INT,
            horse_trainer_ewma_pos_factor DECIMAL(10, 6),
            horse_trainer_perf_factor_count INT DEFAULT 0,
            horse_trainer_ewma_perf_factor DECIMAL(10, 6),
            
            -- Trainer-Track stats
            trainer_track_total_races INT,
            trainer_track_wins INT,
            trainer_track_places INT,
            trainer_track_shows INT,
            trainer_track_ewma_pos_factor DECIMAL(10, 6),
            trainer_track_perf_factor_count INT DEFAULT 0,
            trainer_track_ewma_perf_factor DECIMAL(10, 6),
            
            -- Owner-Trainer stats
            owner_trainer_total_races INT,
            owner_trainer_wins INT,
            owner_trainer_places INT,
            owner_trainer_shows INT,
            owner_trainer_ewma_pos_factor DECIMAL(10, 6),
            owner_trainer_perf_factor_count INT DEFAULT 0,
            owner_trainer_ewma_perf_factor DECIMAL(10, 6),
            
            -- Horse-Track stats
            horse_track_total_races INT,
            horse_track_wins INT,
            horse_track_places INT,
            horse_track_shows INT,
            horse_track_ewma_pos_factor DECIMAL(10, 6),
            horse_track_perf_factor_count INT DEFAULT 0,
            horse_track_ewma_perf_factor DECIMAL(10, 6),
            
            -- Jockey-Trainer stats
            jockey_trainer_total_races INT,
            jockey_trainer_wins INT,
            jockey_trainer_places INT,
            jockey_trainer_shows INT,
            jockey_trainer_ewma_pos_factor DECIMAL(10, 6),
            jockey_trainer_perf_factor_count INT DEFAULT 0,
            jockey_trainer_ewma_perf_factor DECIMAL(10, 6),
            
            -- Odds (will be tested against the model's predictions)
            odds DECIMAL(10, 6),
            
            PRIMARY KEY (race_id, final_pos),
            FOREIGN KEY (race_id) REFERENCES Races(race_id) ON DELETE CASCADE
        );
        """

# Add a Testable to the Testables table
def addTestable(horse_n_name, track_n_name, jockey_n_name, trainer_n_name, owner_n_name, surface, race_id, final_pos, race_type, weight, weather, temp, track_state, distance, odds):
    
    query = """
    WITH TrackSpeed AS (
        SELECT ewma_speed
        FROM Tracks
        WHERE normalized_name = %s
    ), 
    HorseStats AS (
        SELECT 
            total_races, wins, places, shows, 
            ewma_pos_factor, ewma_pos_gain, ewma_late_pos_gain, 
            ewma_last_pos_gain, perf_factor_count, ewma_perf_factor, 
            recent_perf_factor, 
            CASE 
                WHEN %s = 'Dirt' THEN ewma_dirt_perf_factor
                WHEN %s = 'Turf' THEN ewma_turf_perf_factor
                WHEN %s = 'AWT' THEN ewma_awt_perf_factor
            END as ewma_surface_perf_factor, 
            distance_factor
        FROM Horses
        WHERE normalized_name = %s
    ), 
    JockeyStats AS (
        SELECT *
        FROM Jockeys
        WHERE normalized_name = %s
    ), 
    TrainerStats AS (
        SELECT 
            total_races, wins, places, shows, ewma_pos_factor, 
            perf_factor_count, ewma_perf_factor, 
            CASE 
                WHEN %s = 'Dirt' THEN ewma_dirt_perf_factor
                WHEN %s = 'Turf' THEN ewma_turf_perf_factor
                WHEN %s = 'AWT' THEN ewma_awt_perf_factor
            END as ewma_surface_perf_factor, 
            distance_factor
        FROM Trainers
        WHERE normalized_name = %s
    ), 
    OwnerStats AS (
        SELECT *
        FROM Owners
        WHERE normalized_name = %s
    ), 
    HorseJockeyStats AS (
        SELECT *
        FROM horse_jockey
        WHERE horse_n_name = %s AND jockey_n_name = %s
    ), 
    HorseTrainerStats AS (
        SELECT *
        FROM horse_trainer
        WHERE horse_n_name = %s AND trainer_n_name = %s
    ), 
    TrainerTrackStats AS (
        SELECT *
        FROM trainer_track
        WHERE trainer_n_name = %s AND track_n_name = %s AND surface = %s
    ), 
    OwnerTrainerStats AS (
        SELECT *
        FROM owner_trainer
        WHERE owner_n_name = %s AND trainer_n_name = %s
    ), 
    HorseTrackStats AS (
        SELECT *
        FROM horse_track
        WHERE horse_n_name = %s AND track_n_name = %s AND surface = %s
    ), 
    JockeyTrainerStats AS (
        SELECT *
        FROM jockey_trainer
        WHERE jockey_n_name = %s AND trainer_n_name = %s
    )
    INSERT INTO Testables (
        race_id, final_pos, horse_n_name, race_type,
        
        track_ewma_speed,
        
        weight, horse_total_races, horse_wins, horse_places, horse_shows, 
        horse_ewma_pos_factor, horse_ewma_pos_gain, horse_ewma_late_pos_gain, 
        horse_ewma_last_pos_gain, horse_perf_factor_count, horse_ewma_perf_factor, 
        horse_recent_perf_factor, horse_ewma_surface_perf_factor, horse_distance_factor,
        
        jockey_ewma_pos_gain, jockey_ewma_late_pos_gain, jockey_ewma_last_pos_gain, 
        jockey_total_races, jockey_wins, jockey_places, jockey_shows, 
        jockey_ewma_pos_factor, jockey_perf_factor_count, jockey_ewma_perf_factor,
        
        trainer_total_races, trainer_wins, trainer_places, trainer_shows, 
        trainer_ewma_pos_factor, trainer_perf_factor_count, trainer_ewma_perf_factor, 
        trainer_ewma_surface_perf_factor, trainer_distance_factor,
        
        owner_total_races, owner_wins, owner_places, owner_shows, owner_ewma_pos_factor, 
        owner_perf_factor_count, owner_ewma_perf_factor,
        
        weather, temperature, track_state, distance,
        
        horse_jockey_total_races, horse_jockey_wins, horse_jockey_places, 
        horse_jockey_shows, horse_jockey_ewma_pos_factor, horse_jockey_perf_factor_count, 
        horse_jockey_ewma_perf_factor,
        
        horse_trainer_total_races, horse_trainer_wins, horse_trainer_places, 
        horse_trainer_shows, horse_trainer_ewma_pos_factor, horse_trainer_perf_factor_count, 
        horse_trainer_ewma_perf_factor,
        
        trainer_track_total_races, trainer_track_wins, trainer_track_places, 
        trainer_track_shows, trainer_track_ewma_pos_factor, trainer_track_perf_factor_count, 
        trainer_track_ewma_perf_factor,
        
        owner_trainer_total_races, owner_trainer_wins, owner_trainer_places, 
        owner_trainer_shows, owner_trainer_ewma_pos_factor, owner_trainer_perf_factor_count, 
        owner_trainer_ewma_perf_factor,
        
        horse_track_total_races, horse_track_wins, horse_track_places, 
        horse_track_shows, horse_track_ewma_pos_factor, horse_track_perf_factor_count, 
        horse_track_ewma_perf_factor,
        
        jockey_trainer_total_races, jockey_trainer_wins, jockey_trainer_places, 
        jockey_trainer_shows, jockey_trainer_ewma_pos_factor, jockey_trainer_perf_factor_count, 
        jockey_trainer_ewma_perf_factor,
        
        odds)
    
    SELECT
        %s, %s, %s, %s, -- race_id, final_pos, horse_n_name, race_type
        
        (SELECT ewma_speed FROM TrackSpeed), -- track_ewma_speed
        
        %s, -- weight
        
        (SELECT total_races FROM HorseStats), 
        (SELECT wins FROM HorseStats), 
        (SELECT places FROM HorseStats), 
        (SELECT shows FROM HorseStats), 
        (SELECT ewma_pos_factor FROM HorseStats), 
        (SELECT ewma_pos_gain FROM HorseStats), 
        (SELECT ewma_late_pos_gain FROM HorseStats), 
        (SELECT ewma_last_pos_gain FROM HorseStats), 
        (SELECT perf_factor_count FROM HorseStats), 
        (SELECT ewma_perf_factor FROM HorseStats), 
        (SELECT recent_perf_factor FROM HorseStats), 
        (SELECT ewma_surface_perf_factor FROM HorseStats), 
        (SELECT distance_factor FROM HorseStats), -- horse stats
        
        (SELECT ewma_pos_gain FROM JockeyStats), 
        (SELECT ewma_late_pos_gain FROM JockeyStats), 
        (SELECT ewma_last_pos_gain FROM JockeyStats), 
        (SELECT total_races FROM JockeyStats), 
        (SELECT wins FROM JockeyStats), 
        (SELECT places FROM JockeyStats), 
        (SELECT shows FROM JockeyStats), 
        (SELECT ewma_pos_factor FROM JockeyStats), 
        (SELECT perf_factor_count FROM JockeyStats), 
        (SELECT ewma_perf_factor FROM JockeyStats), -- jockey stats
        
        (SELECT total_races FROM TrainerStats), 
        (SELECT wins FROM TrainerStats), 
        (SELECT places FROM TrainerStats), 
        (SELECT shows FROM TrainerStats), 
        (SELECT ewma_pos_factor FROM TrainerStats), 
        (SELECT perf_factor_count FROM TrainerStats), 
        (SELECT ewma_perf_factor FROM TrainerStats), 
        (SELECT ewma_surface_perf_factor FROM TrainerStats), 
        (SELECT distance_factor FROM TrainerStats), -- trainer stats
        
        (SELECT total_races FROM OwnerStats), 
        (SELECT wins FROM OwnerStats), 
        (SELECT places FROM OwnerStats), 
        (SELECT shows FROM OwnerStats), 
        (SELECT ewma_pos_factor FROM OwnerStats), 
        (SELECT perf_factor_count FROM OwnerStats), 
        (SELECT ewma_perf_factor FROM OwnerStats), -- owner stats
        
        %s, %s, %s, %s, -- weather, temperature, track_state, distance
        
        (SELECT total_races FROM HorseJockeyStats), 
        (SELECT wins FROM HorseJockeyStats), 
        (SELECT places FROM HorseJockeyStats), 
        (SELECT shows FROM HorseJockeyStats), 
        (SELECT ewma_pos_factor FROM HorseJockeyStats), 
        (SELECT perf_factor_count FROM HorseJockeyStats), 
        (SELECT ewma_perf_factor FROM HorseJockeyStats), -- horse-jockey stats
        
        (SELECT total_races FROM HorseTrainerStats), 
        (SELECT wins FROM HorseTrainerStats), 
        (SELECT places FROM HorseTrainerStats), 
        (SELECT shows FROM HorseTrainerStats), 
        (SELECT ewma_pos_factor FROM HorseTrainerStats), 
        (SELECT perf_factor_count FROM HorseTrainerStats), 
        (SELECT ewma_perf_factor FROM HorseTrainerStats), -- horse-trainer stats
        
        (SELECT total_races FROM TrainerTrackStats), 
        (SELECT wins FROM TrainerTrackStats), 
        (SELECT places FROM TrainerTrackStats), 
        (SELECT shows FROM TrainerTrackStats), 
        (SELECT ewma_pos_factor FROM TrainerTrackStats), 
        (SELECT perf_factor_count FROM TrainerTrackStats), 
        (SELECT ewma_perf_factor FROM TrainerTrackStats), -- trainer-track stats
        
        (SELECT total_races FROM OwnerTrainerStats), 
        (SELECT wins FROM OwnerTrainerStats), 
        (SELECT places FROM OwnerTrainerStats), 
        (SELECT shows FROM OwnerTrainerStats), 
        (SELECT ewma_pos_factor FROM OwnerTrainerStats), 
        (SELECT perf_factor_count FROM OwnerTrainerStats), 
        (SELECT ewma_perf_factor FROM OwnerTrainerStats), -- owner-trainer stats
        
        (SELECT total_races FROM HorseTrackStats), 
        (SELECT wins FROM HorseTrackStats), 
        (SELECT places FROM HorseTrackStats), 
        (SELECT shows FROM HorseTrackStats), 
        (SELECT ewma_pos_factor FROM HorseTrackStats), 
        (SELECT perf_factor_count FROM HorseTrackStats), 
        (SELECT ewma_perf_factor FROM HorseTrackStats), -- horse-track stats
        
        (SELECT total_races FROM JockeyTrainerStats), 
        (SELECT wins FROM JockeyTrainerStats), 
        (SELECT places FROM JockeyTrainerStats), 
        (SELECT shows FROM JockeyTrainerStats), 
        (SELECT ewma_pos_factor FROM JockeyTrainerStats), 
        (SELECT perf_factor_count FROM JockeyTrainerStats), 
        (SELECT ewma_perf_factor FROM JockeyTrainerStats), -- jockey-trainer stats
        
        %s
    """
    
    values = (
        # CTEs
        track_n_name, # TrackSpeed
        surface, surface, surface, horse_n_name, # HorseStats
        jockey_n_name, # JockeyStats
        surface, surface, surface, trainer_n_name, # TrainerStats
        owner_n_name, # OwnerStats
        
        horse_n_name, jockey_n_name, # HorseJockeyStats
        horse_n_name, trainer_n_name, # HorseTrainerStats
        trainer_n_name, track_n_name, surface, # TrainerTrackStats
        owner_n_name, trainer_n_name, # OwnerTrainerStats
        horse_n_name, track_n_name, surface, # HorseTrackStats
        jockey_n_name, trainer_n_name, # JockeyTrainerStats
        
        # ADDING
        race_id, final_pos, horse_n_name, race_type,
        weight,
        weather, temp, track_state, distance,
        odds
    )
    
    return query, values

# Drops Horses table
def dropHorses():
    return "DROP TABLE IF EXISTS Horses CASCADE;"

# Builds Horses table
def createHorses():
    return """
        CREATE TABLE IF NOT EXISTS Horses (
            name VARCHAR(255) NOT NULL,
            normalized_name VARCHAR(255) PRIMARY KEY,
            total_races INT DEFAULT 0,
            wins INT DEFAULT 0,
            places INT DEFAULT 0,
            shows INT DEFAULT 0,
            ewma_pos_factor DECIMAL(10, 6),
            ewma_pos_gain DECIMAL(10, 6),
            ewma_late_pos_gain DECIMAL(10, 6),
            ewma_last_pos_gain DECIMAL(10, 6),
            perf_factor_count INT DEFAULT 0,
            ewma_perf_factor DECIMAL(10, 6),
            recent_perf_factor DECIMAL(10, 6),
            ewma_dirt_perf_factor DECIMAL(10, 6),
            ewma_turf_perf_factor DECIMAL(10, 6),
            ewma_awt_perf_factor DECIMAL(10, 6),
            distance_factor DECIMAL(10, 6)
        );
        """

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
        name, n_name, # name, n_name
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

# Drops Tracks table
def dropTracks():
    return "DROP TABLE IF EXISTS Tracks CASCADE;"

# Builds Tracks table
def createTracks():
    return """
        CREATE TABLE IF NOT EXISTS Tracks (
            name VARCHAR(255) NOT NULL,
            normalized_name VARCHAR(255) PRIMARY KEY,
            ewma_speed DECIMAL(10, 6)
        );
        """

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

# Drops Jockeys Table
def dropJockeys():
    return "DROP TABLE IF EXISTS Jockeys CASCADE;"

# Builds Jockeys table
def createJockeys():
    return """
        CREATE TABLE IF NOT EXISTS Jockeys (
            name VARCHAR(255) NOT NULL,
            normalized_name VARCHAR(255) PRIMARY KEY,
            ewma_pos_gain DECIMAL(10, 6),
            ewma_late_pos_gain DECIMAL(10, 6),
            ewma_last_pos_gain DECIMAL(10, 6),
            total_races INT,
            wins INT,
            places INT,
            shows INT,
            ewma_pos_factor DECIMAL(10, 6),
            perf_factor_count INT DEFAULT 0,
            ewma_perf_factor DECIMAL(10, 6)
        );
        """

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

# Drops Races table
def dropRaces():
    return "DROP TABLE IF EXISTS Races CASCADE;"

# Builds Races table
def createRaces():
    return """
        CREATE TABLE IF NOT EXISTS Races (
            race_id VARCHAR(255) PRIMARY KEY,
            file_num INT,
            race_num INT,
            track_n_name VARCHAR(255),
            date DATE,
            race_type VARCHAR(255),
            surface VARCHAR(255),
            weather VARCHAR(255),
            temperature DECIMAL(10, 6),
            track_state VARCHAR(255),
            distance DECIMAL(10, 6),
            final_time DECIMAL(10, 6),
            speed DECIMAL(10, 6),
            frac_time_1 DECIMAL(10, 6),
            frac_time_2 DECIMAL(10, 6),
            frac_time_3 DECIMAL(10, 6),
            frac_time_4 DECIMAL(10, 6),
            frac_time_5 DECIMAL(10, 6),
            frac_time_6 DECIMAL(10, 6),
            split_time_1 DECIMAL(10, 6),
            split_time_2 DECIMAL(10, 6),
            split_time_3 DECIMAL(10, 6),
            split_time_4 DECIMAL(10, 6),
            split_time_5 DECIMAL(10, 6),
            split_time_6 DECIMAL(10, 6)
        );

        """

# Add a race to the Races table
def addRace(race_id, file_num, track_n_name, race_num, date, race_type=None, surface=None, weather=None,
               temperature=None, track_state=None, distance=None, final_time=None, speed=None,
               frac_time_1=None, frac_time_2=None, frac_time_3=None, frac_time_4=None, 
               frac_time_5=None, frac_time_6=None, split_time_1=None, split_time_2=None, 
               split_time_3=None, split_time_4=None, split_time_5=None, split_time_6=None):
    
    query = """
    INSERT INTO Races (race_id, file_num, track_n_name, race_num, date, race_type, surface, weather, temperature, track_state, 
                       distance, final_time, speed, frac_time_1, frac_time_2, frac_time_3, frac_time_4, frac_time_5, 
                       frac_time_6, split_time_1, split_time_2, split_time_3, split_time_4, split_time_5, split_time_6)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    return query, (race_id, file_num, track_n_name, race_num, date, race_type, surface, weather, 
                   temperature, track_state, distance, final_time, speed,
                   frac_time_1, frac_time_2, frac_time_3, frac_time_4, 
                   frac_time_5, frac_time_6, split_time_1, split_time_2, 
                   split_time_3, split_time_4, split_time_5, split_time_6)

# Drops horse_jockey table
def dropHorseJockey():
    return "DROP TABLE IF EXISTS horse_jockey CASCADE;"

# Builds horse_jockey table
def createHorseJockey():
    return """
        CREATE TABLE IF NOT EXISTS horse_jockey (
            horse_n_name VARCHAR(255),
            jockey_n_name VARCHAR(255),
            total_races INT,
            wins INT,
            places INT,
            shows INT,
            ewma_pos_factor DECIMAL(10, 6),
            perf_factor_count INT DEFAULT 0,
            ewma_perf_factor DECIMAL(10, 6),
            PRIMARY KEY (horse_n_name, jockey_n_name),
            FOREIGN KEY (horse_n_name) REFERENCES Horses(normalized_name) ON DELETE CASCADE,
            FOREIGN KEY (jockey_n_name) REFERENCES Jockeys(normalized_name) ON DELETE CASCADE
        );
        """

# Add a horse_jockey relationship
def addHorseJockey(horse_n_name, jockey_n_name, pos, pos_factor, speed, track_n_name):
    alpha = Decimal(0.2)
    
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
    INSERT INTO horse_jockey (horse_n_name, jockey_n_name, total_races, wins, places, shows, ewma_pos_factor, perf_factor_count, ewma_perf_factor)
    SELECT
        %s, %s, 1, -- horse_n_name, jockey_n_name, total_races
        CASE WHEN %s = 1 THEN 1 ELSE 0 END, -- wins
        CASE WHEN %s < 3 THEN 1 ELSE 0 END, -- places
        CASE WHEN %s < 4 THEN 1 ELSE 0 END, -- shows
        %s, -- ewma_pos_factor
        CASE WHEN (SELECT p_factor FROM PerformanceFactor) IS NULL THEN 0 ELSE 1 END, -- perf_factor_count
        (SELECT p_factor FROM PerformanceFactor) -- ewma_perf_factor
    ON CONFLICT (horse_n_name, jockey_n_name) DO UPDATE SET
        total_races = horse_jockey.total_races + 1,
        wins = horse_jockey.wins + excluded.wins,
        places = horse_jockey.places + excluded.places,
        shows = horse_jockey.shows + excluded.shows,
        ewma_pos_factor = CASE 
            WHEN excluded.ewma_pos_factor IS NULL THEN horse_jockey.ewma_pos_factor
            WHEN horse_jockey.ewma_pos_factor IS NULL THEN excluded.ewma_pos_factor
            ELSE (excluded.ewma_pos_factor * %s) + ((1 - %s) * horse_jockey.ewma_pos_factor)
        END,
        perf_factor_count = horse_jockey.perf_factor_count + excluded.perf_factor_count,
        ewma_perf_factor = CASE 
            WHEN excluded.ewma_perf_factor IS NULL THEN horse_jockey.ewma_perf_factor
            WHEN horse_jockey.ewma_perf_factor IS NULL THEN excluded.ewma_perf_factor
            ELSE (excluded.ewma_perf_factor * %s) + ((1 - %s) * horse_jockey.ewma_perf_factor)
        END
    """
    
    values = (
        # CTEs
        track_n_name, # TrackSpeed CTE
        speed, pos_factor, speed, # PerformanceFactor CTE
        
        # ADDING
        horse_n_name, jockey_n_name, # horse_n_name, jockey_n_name
        pos, # wins
        pos, # places
        pos, # shows
        pos_factor, # ewma_pos_factor
        
        # UPDATING
        alpha, alpha, # ewma_pos_factor
        alpha, alpha, # ewma_perf_factor
        )
    
    return query, values

# Drops horse_trainer table
def dropHorseTrainer():
    return "DROP TABLE IF EXISTS horse_trainer CASCADE;"

# Builds horse_trainer table
def createHorseTrainer():
    return """
        CREATE TABLE IF NOT EXISTS horse_trainer (
            horse_n_name VARCHAR(255),
            trainer_n_name VARCHAR(255),
            total_races INT,
            wins INT,
            places INT,
            shows INT,
            ewma_pos_factor DECIMAL(10, 6),
            perf_factor_count INT DEFAULT 0,
            ewma_perf_factor DECIMAL(10, 6),
            PRIMARY KEY (horse_n_name, trainer_n_name),
            FOREIGN KEY (horse_n_name) REFERENCES Horses(normalized_name) ON DELETE CASCADE,
            FOREIGN KEY (trainer_n_name) REFERENCES Trainers(normalized_name) ON DELETE CASCADE
        );
        """

# Add a horse_trainer relationship
def addHorseTrainer(horse_n_name, trainer_n_name, pos, pos_factor, speed, track_n_name):
    alpha = Decimal(0.2)
    
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
    INSERT INTO horse_trainer (horse_n_name, trainer_n_name, total_races, wins, places, shows, ewma_pos_factor, perf_factor_count, ewma_perf_factor)
    SELECT
        %s, %s, 1, -- horse_n_name, trainer_n_name, total_races
        CASE WHEN %s = 1 THEN 1 ELSE 0 END, -- wins
        CASE WHEN %s < 3 THEN 1 ELSE 0 END, -- places
        CASE WHEN %s < 4 THEN 1 ELSE 0 END, -- shows
        %s, -- ewma_pos_factor
        CASE WHEN (SELECT p_factor FROM PerformanceFactor) IS NULL THEN 0 ELSE 1 END, -- perf_factor_count
        (SELECT p_factor FROM PerformanceFactor) -- ewma_perf_factor
    ON CONFLICT (horse_n_name, trainer_n_name) DO UPDATE SET
        total_races = horse_trainer.total_races + 1,
        wins = horse_trainer.wins + excluded.wins,
        places = horse_trainer.places + excluded.places,
        shows = horse_trainer.shows + excluded.shows,
        ewma_pos_factor = CASE 
            WHEN excluded.ewma_pos_factor IS NULL THEN horse_trainer.ewma_pos_factor
            WHEN horse_trainer.ewma_pos_factor IS NULL THEN excluded.ewma_pos_factor
            ELSE (excluded.ewma_pos_factor * %s) + ((1 - %s) * horse_trainer.ewma_pos_factor)
        END,
        perf_factor_count = horse_trainer.perf_factor_count + excluded.perf_factor_count,
        ewma_perf_factor = CASE 
            WHEN excluded.ewma_perf_factor IS NULL THEN horse_trainer.ewma_perf_factor
            WHEN horse_trainer.ewma_perf_factor IS NULL THEN excluded.ewma_perf_factor
            ELSE (excluded.ewma_perf_factor * %s) + ((1 - %s) * horse_trainer.ewma_perf_factor)
        END
    """
    
    values = (
        # CTEs
        track_n_name, # TrackSpeed CTE
        speed, pos_factor, speed, # PerformanceFactor CTE
        
        # ADDING
        horse_n_name, trainer_n_name, # horse_n_name, trainer_n_name
        pos, # wins
        pos, # places
        pos, # shows
        pos_factor, # ewma_pos_factor
        
        # UPDATING
        alpha, alpha, # ewma_pos_factor
        alpha, alpha, # ewma_perf_factor
        )
    
    return query, values

# Drops trainer_track table
def dropTrainerTrack():
    return "DROP TABLE IF EXISTS trainer_track CASCADE;"

# Builds trainer_track table
def createTrainerTrack():
    return """
        CREATE TABLE IF NOT EXISTS trainer_track (
            trainer_n_name VARCHAR(255),
            track_n_name VARCHAR(255),
            surface VARCHAR(255),
            total_races INT,
            wins INT,
            places INT,
            shows INT,
            ewma_pos_factor DECIMAL(10, 6),
            perf_factor_count INT DEFAULT 0,
            ewma_perf_factor DECIMAL(10, 6),
            PRIMARY KEY (trainer_n_name, track_n_name, surface),
            FOREIGN KEY (trainer_n_name) REFERENCES Trainers(normalized_name) ON DELETE CASCADE,
            FOREIGN KEY (track_n_name) REFERENCES Tracks(normalized_name) ON DELETE CASCADE
        );
        """

# Add a trainer_track relationship
def addTrainerTrack(trainer_n_name, track_n_name, pos, pos_factor, speed, surface):
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
    INSERT INTO trainer_track (trainer_n_name, track_n_name, surface, total_races, wins, places, shows, ewma_pos_factor, perf_factor_count, ewma_perf_factor)
    SELECT
        %s, %s, %s, 1, -- trainer_n_name, track_n_name, surface, total_races
        CASE WHEN %s = 1 THEN 1 ELSE 0 END, -- wins
        CASE WHEN %s < 3 THEN 1 ELSE 0 END, -- places
        CASE WHEN %s < 4 THEN 1 ELSE 0 END, -- shows
        %s, -- ewma_pos_factor
        CASE WHEN (SELECT p_factor FROM PerformanceFactor) IS NULL THEN 0 ELSE 1 END, -- perf_factor_count
        (SELECT p_factor FROM PerformanceFactor) -- ewma_perf_factor
    ON CONFLICT (trainer_n_name, track_n_name, surface) DO UPDATE SET
        total_races = trainer_track.total_races + 1,
        wins = trainer_track.wins + excluded.wins,
        places = trainer_track.places + excluded.places,
        shows = trainer_track.shows + excluded.shows,
        ewma_pos_factor = CASE 
            WHEN excluded.ewma_pos_factor IS NULL THEN trainer_track.ewma_pos_factor
            WHEN trainer_track.ewma_pos_factor IS NULL THEN excluded.ewma_pos_factor
            ELSE (excluded.ewma_pos_factor * %s) + ((1 - %s) * trainer_track.ewma_pos_factor)
        END,
        perf_factor_count = trainer_track.perf_factor_count + excluded.perf_factor_count,
        ewma_perf_factor = CASE 
            WHEN excluded.ewma_perf_factor IS NULL THEN trainer_track.ewma_perf_factor
            WHEN trainer_track.ewma_perf_factor IS NULL THEN excluded.ewma_perf_factor
            ELSE (excluded.ewma_perf_factor * %s) + ((1 - %s) * trainer_track.ewma_perf_factor)
        END
    """
    
    values = (
        # CTEs
        track_n_name, # TrackSpeed CTE
        speed, pos_factor, speed, # PerformanceFactor CTE
        
        # ADDING
        trainer_n_name, track_n_name, surface, # trainer_n_name, track_n_name, surface
        pos, # wins
        pos, # places
        pos, # shows
        pos_factor, # ewma_pos_factor
        
        # UPDATING
        alpha, alpha, # ewma_pos_factor
        alpha, alpha, # ewma_perf_factor
        )
    
    return query, values

# Drops horse_track table
def dropHorseTrack():
    return "DROP TABLE IF EXISTS horse_track CASCADE;"

# Builds horse_tracke table
def createHorseTrack():
    return """
        CREATE TABLE IF NOT EXISTS horse_track (
            horse_n_name VARCHAR(255),
            track_n_name VARCHAR(255),
            surface VARCHAR(255),
            total_races INT,
            wins INT,
            places INT,
            shows INT,
            ewma_pos_factor DECIMAL(10, 6),
            perf_factor_count INT DEFAULT 0,
            ewma_perf_factor DECIMAL(10, 6),
            PRIMARY KEY (horse_n_name, track_n_name, surface),
            FOREIGN KEY (horse_n_name) REFERENCES Horses(normalized_name) ON DELETE CASCADE,
            FOREIGN KEY (track_n_name) REFERENCES Tracks(normalized_name) ON DELETE CASCADE
        );
        """

# Add a horse_track relationship
def addHorseTrack(horse_n_name, track_n_name, pos, pos_factor, speed, surface):
    alpha = Decimal(0.2)
    
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
    INSERT INTO horse_track (horse_n_name, track_n_name, surface, total_races, wins, places, shows, ewma_pos_factor, perf_factor_count, ewma_perf_factor)
    SELECT
        %s, %s, %s, 1, -- horse_n_name, track_n_name, surface, total_races
        CASE WHEN %s = 1 THEN 1 ELSE 0 END, -- wins
        CASE WHEN %s < 3 THEN 1 ELSE 0 END, -- places
        CASE WHEN %s < 4 THEN 1 ELSE 0 END, -- shows
        %s, -- ewma_pos_factor
        CASE WHEN (SELECT p_factor FROM PerformanceFactor) IS NULL THEN 0 ELSE 1 END, -- perf_factor_count
        (SELECT p_factor FROM PerformanceFactor) -- ewma_perf_factor
    ON CONFLICT (horse_n_name, track_n_name, surface) DO UPDATE SET
        total_races = horse_track.total_races + 1,
        wins = horse_track.wins + excluded.wins,
        places = horse_track.places + excluded.places,
        shows = horse_track.shows + excluded.shows,
        ewma_pos_factor = CASE 
            WHEN excluded.ewma_pos_factor IS NULL THEN horse_track.ewma_pos_factor
            WHEN horse_track.ewma_pos_factor IS NULL THEN excluded.ewma_pos_factor
            ELSE (excluded.ewma_pos_factor * %s) + ((1 - %s) * horse_track.ewma_pos_factor)
        END,
        perf_factor_count = horse_track.perf_factor_count + excluded.perf_factor_count,
        ewma_perf_factor = CASE 
            WHEN excluded.ewma_perf_factor IS NULL THEN horse_track.ewma_perf_factor
            WHEN horse_track.ewma_perf_factor IS NULL THEN excluded.ewma_perf_factor
            ELSE (excluded.ewma_perf_factor * %s) + ((1 - %s) * horse_track.ewma_perf_factor)
        END
    """
    
    values = (
        # CTEs
        track_n_name, # TrackSpeed CTE
        speed, pos_factor, speed, # PerformanceFactor CTE
        
        # ADDING
        horse_n_name, track_n_name, surface, # horse_n_name, track_n_name, surface
        pos, # wins
        pos, # places
        pos, # shows
        pos_factor, # ewma_pos_factor
        
        # UPDATING
        alpha, alpha, # ewma_pos_factor
        alpha, alpha, # ewma_perf_factor
        )
    
    return query, values

# Drops jockey_trainer table
def dropJockeyTrainer():
    return "DROP TABLE IF EXISTS jockey_trainer CASCADE;"

# Builds jockey_trainer table
def createJockeyTrainer():
    return """
        CREATE TABLE IF NOT EXISTS jockey_trainer (
            jockey_n_name VARCHAR(255),
            trainer_n_name VARCHAR(255),
            total_races INT,
            wins INT,
            places INT,
            shows INT,
            ewma_pos_factor DECIMAL(10, 6),
            perf_factor_count INT DEFAULT 0,
            ewma_perf_factor DECIMAL(10, 6),
            PRIMARY KEY (jockey_n_name, trainer_n_name),
            FOREIGN KEY (jockey_n_name) REFERENCES Jockeys(normalized_name) ON DELETE CASCADE,
            FOREIGN KEY (trainer_n_name) REFERENCES Trainers(normalized_name) ON DELETE CASCADE
        );
        """

# Add a jockey_track relationship
def addJockeyTrainer(jockey_n_name, trainer_n_name, pos, pos_factor, speed, track_n_name):
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
    INSERT INTO jockey_trainer (jockey_n_name, trainer_n_name, total_races, wins, places, shows, ewma_pos_factor, perf_factor_count, ewma_perf_factor)
    SELECT
        %s, %s, 1, -- jockey_n_name, trainer_n_name, total_races
        CASE WHEN %s = 1 THEN 1 ELSE 0 END, -- wins
        CASE WHEN %s < 3 THEN 1 ELSE 0 END, -- places
        CASE WHEN %s < 4 THEN 1 ELSE 0 END, -- shows
        %s, -- ewma_pos_factor
        CASE WHEN (SELECT p_factor FROM PerformanceFactor) IS NULL THEN 0 ELSE 1 END, -- perf_factor_count
        (SELECT p_factor FROM PerformanceFactor) -- ewma_perf_factor
    ON CONFLICT (jockey_n_name, trainer_n_name) DO UPDATE SET
        total_races = jockey_trainer.total_races + 1,
        wins = jockey_trainer.wins + excluded.wins,
        places = jockey_trainer.places + excluded.places,
        shows = jockey_trainer.shows + excluded.shows,
        ewma_pos_factor = CASE 
            WHEN excluded.ewma_pos_factor IS NULL THEN jockey_trainer.ewma_pos_factor
            WHEN jockey_trainer.ewma_pos_factor IS NULL THEN excluded.ewma_pos_factor
            ELSE (excluded.ewma_pos_factor * %s) + ((1 - %s) * jockey_trainer.ewma_pos_factor)
        END,
        perf_factor_count = jockey_trainer.perf_factor_count + excluded.perf_factor_count,
        ewma_perf_factor = CASE 
            WHEN excluded.ewma_perf_factor IS NULL THEN jockey_trainer.ewma_perf_factor
            WHEN jockey_trainer.ewma_perf_factor IS NULL THEN excluded.ewma_perf_factor
            ELSE (excluded.ewma_perf_factor * %s) + ((1 - %s) * jockey_trainer.ewma_perf_factor)
        END
    """
    
    values = (
        # CTEs
        track_n_name, # TrackSpeed CTE
        speed, pos_factor, speed, # PerformanceFactor CTE
        
        # ADDING
        jockey_n_name, trainer_n_name, # jockey_n_name, trainer_n_name
        pos, # wins
        pos, # places
        pos, # shows
        pos_factor, # ewma_pos_factor
        
        # UPDATING
        alpha, alpha, # ewma_pos_factor
        alpha, alpha, # ewma_perf_factor
        )
    
    return query, values

# Drops owner_trainer table
def dropOwnerTrainer():
    return "DROP TABLE IF EXISTS owner_trainer CASCADE;"

# Builds owner_trainer table
def createOwnerTrainer():
    return """
        CREATE TABLE IF NOT EXISTS owner_trainer (
            owner_n_name VARCHAR(255),
            trainer_n_name VARCHAR(255),
            total_races INT,
            wins INT,
            places INT,
            shows INT,
            ewma_pos_factor DECIMAL(10, 6),
            perf_factor_count INT DEFAULT 0,
            ewma_perf_factor DECIMAL(10, 6),
            PRIMARY KEY (owner_n_name, trainer_n_name),
            FOREIGN KEY (owner_n_name) REFERENCES Owners(normalized_name) ON DELETE CASCADE,
            FOREIGN KEY (trainer_n_name) REFERENCES Trainers(normalized_name) ON DELETE CASCADE
        );
        """

# Add an owner_trainer relationship
def addOwnerTrainer(owner_n_name, trainer_n_name, pos, pos_factor, speed, track_n_name):
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
    INSERT INTO owner_trainer (owner_n_name, trainer_n_name, total_races, wins, places, shows, ewma_pos_factor, perf_factor_count, ewma_perf_factor)
    SELECT
        %s, %s, 1, -- owner_n_name, trainer_n_name, total_races
        CASE WHEN %s = 1 THEN 1 ELSE 0 END, -- wins
        CASE WHEN %s < 3 THEN 1 ELSE 0 END, -- places
        CASE WHEN %s < 4 THEN 1 ELSE 0 END, -- shows
        %s, -- ewma_pos_factor
        CASE WHEN (SELECT p_factor FROM PerformanceFactor) IS NULL THEN 0 ELSE 1 END, -- perf_factor_count
        (SELECT p_factor FROM PerformanceFactor) -- ewma_perf_factor
    ON CONFLICT (owner_n_name, trainer_n_name) DO UPDATE SET
        total_races = owner_trainer.total_races + 1,
        wins = owner_trainer.wins + excluded.wins,
        places = owner_trainer.places + excluded.places,
        shows = owner_trainer.shows + excluded.shows,
        ewma_pos_factor = CASE 
            WHEN excluded.ewma_pos_factor IS NULL THEN owner_trainer.ewma_pos_factor
            WHEN owner_trainer.ewma_pos_factor IS NULL THEN excluded.ewma_pos_factor
            ELSE (excluded.ewma_pos_factor * %s) + ((1 - %s) * owner_trainer.ewma_pos_factor)
        END,
        perf_factor_count = owner_trainer.perf_factor_count + excluded.perf_factor_count,
        ewma_perf_factor = CASE 
            WHEN excluded.ewma_perf_factor IS NULL THEN owner_trainer.ewma_perf_factor
            WHEN owner_trainer.ewma_perf_factor IS NULL THEN excluded.ewma_perf_factor
            ELSE (excluded.ewma_perf_factor * %s) + ((1 - %s) * owner_trainer.ewma_perf_factor)
        END
    """
    
    values = (
        # CTEs
        track_n_name, # TrackSpeed CTE
        speed, pos_factor, speed, # PerformanceFactor CTE
        
        # ADDING
        owner_n_name, trainer_n_name, # owner_n_name, trainer_n_name
        pos, # wins
        pos, # places
        pos, # shows
        pos_factor, # ewma_pos_factor
        
        # UPDATING
        alpha, alpha, # ewma_pos_factor
        alpha, alpha, # ewma_perf_factor
        )
    
    return query, values

# Normalize a name string
def normalize(name):
    return re.sub(r'[^a-z0-9]', '', unidecode.unidecode(name.strip().lower()))

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