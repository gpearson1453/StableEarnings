"""
dataMethods.py provides a collection of utility functions for database interactions and some additional supportive functions.

This script contains reusable methods for connecting to and interacting with the database, defining schema creation and
modification queries, and implementing complex SQL commands for inserting and updating records. These methods are designed
to ensure consistency and efficiency across database operations.

Functions:
    - Connection Methods: Establish connections to local or remote databases.
    - Schema Definitions: SQL commands to create and drop tables and types, ensuring database consistency.
    - Data Insertion/Update: Methods to add and update records for entities like horses, jockeys, trainers, and owners.
    - Statistical Updates: Advanced queries to calculate and update metrics such as performance factors and position gains.
    - Relationship Management: Methods to handle many-to-many relationships (e.g., horse-jockey or trainer-track).

Usage:
    Import this module to execute database operations from other scripts. Ensure database connection parameters are
    configured correctly before use.
"""
import psycopg2
import re
import unidecode
from datetime import datetime
from decimal import Decimal


def local_connect(db_name):
    """
    Establish a connection to a PostgreSQL database on the local host.

    Args:
        db_name (str): The name of the database to connect to.

    Returns:
        psycopg2.connection: A connection object for the specified database.

    Raises:
        psycopg2.Error: If the connection to the database fails.
    """
    return psycopg2.connect(
        dbname=db_name,
        user="postgres",
        password="B!h8Cjxa37!78Yh",
        host="localhost",
        port="5432",
    )


def cockroach_connect(db_name):
    """
    Establish a connection to a CockroachDB cluster.

    Args:
        db_name (str): The name of the database to connect to.

    Returns:
        psycopg2.connection: A connection object for the specified CockroachDB database.

    Raises:
        psycopg2.Error: If the connection to the database fails.
    """
    return psycopg2.connect(
        dbname=db_name,
        user="molomala",
        password="aPyds3qPNhslU5xV8H-pMw",
        host="stable-earnings-3899.j77.aws-us-east-1.cockroachlabs.cloud",
        port="26257",
    )


def dropTrainers():
    """
    Generate a SQL query to drop the 'Trainers' table if it exists.

    Returns:
        str: A SQL query string to drop the 'Trainers' table, including all its
        dependent objects.
    """
    return "DROP TABLE IF EXISTS Trainers CASCADE;"


def createTrainers():
    """
    Generate a SQL query to create the 'Trainers' table.

    Returns:
        str: A SQL query string to create the 'Trainers' table with its schema definition.
    """
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


def addTrainer(
    name, n_name, pos, pos_factor, speed, track_n_name, surface, distance, num_horses
):
    """
    Add or update a trainer's record in the 'Trainers' table.

    Args:
        name (str): Full name of the trainer.
        n_name (str): Normalized name of the trainer.
        pos (int): The final position of the trainer's horse in the race.
        pos_factor (Decimal): Position factor for trainer's horse in the race.
        speed (Decimal): Speed of the race.
        track_n_name (str): Normalized name of the track.
        surface (str): Surface type of the track (e.g., 'Dirt', 'Turf', 'AWT').
        distance (Decimal): Distance of the race.
        num_horses (int): Total number of horses in the race.

    Returns:
        tuple: A SQL query string and its parameters for adding or updating a trainer's record.
    """
    # Define alpha values for performance factor calculations
    alpha = Decimal(0.15)
    d_factor_max_alpha = 0.5
    d_factor_alpha = Decimal(
        (1 - ((int(pos) - 1) / (int(num_horses) - 1))) * d_factor_max_alpha
    )

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

    # Parameters for the SQL query
    values = (
        # CTEs
        track_n_name,  # TrackSpeed CTE
        speed,
        pos_factor,
        speed,  # PerformanceFactor CTE
        # ADDING
        name,
        n_name,  # name, n_name
        pos,  # wins
        pos,  # places
        pos,  # shows
        pos_factor,  # ewma_pos_factor
        surface,  # ewma_dirt_perf_factor
        surface,  # ewma_turf_perf_factor
        surface,  # ewma_awt_perf_factor
        num_horses,
        distance,  # distance_factor
        # UPDATING
        alpha,
        alpha,  # ewma_pos_factor
        alpha,
        alpha,  # ewma_perf_factor
        alpha,
        alpha,  # ewma_dirt_perf_factor
        alpha,
        alpha,  # ewma_turf_perf_factor
        alpha,
        alpha,  # ewma_awt_perf_factor
        d_factor_alpha,
        d_factor_alpha,  # distance_factor
    )

    return query, values


def dropOwners():
    """
    Generate a SQL query to drop the 'Owners' table if it exists.

    Returns:
        str: A SQL query string to drop the 'Owners' table, including all its dependent objects.
    """
    return "DROP TABLE IF EXISTS Owners CASCADE;"


def createOwners():
    """
    Generate a SQL query to create the 'Owners' table.

    Returns:
        str: A SQL query string to create the 'Owners' table with its schema definition.
    """
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


def addOwner(name, n_name, pos, pos_factor, speed, track_n_name):
    """
    Add or update an owner's record in the 'Owners' table.

    Args:
        name (str): Full name of the owner.
        n_name (str): Normalized name of the owner.
        pos (int): Final position of the owner's horse in the race.
        pos_factor (Decimal): Position factor of the owner's horse in the race.
        speed (Decimal): Average speed of the race.
        track_n_name (str): Normalized name of the track where the race occurred.

    Returns:
        tuple: A SQL query string and its parameters for adding or updating the owner's record.
    """
    alpha = Decimal(0.15)  # Weight for EWMA calculations

    # SQL query to calculate performance factor and insert or update owner's record
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
    INSERT INTO Owners (
        name, normalized_name, total_races, wins, places, shows, ewma_pos_factor, perf_factor_count, ewma_perf_factor
    )
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

    # Parameters for the SQL query
    values = (
        # CTEs
        track_n_name,  # TrackSpeed CTE
        speed,
        pos_factor,
        speed,  # PerformanceFactor CTE
        # ADDING
        name,
        n_name,  # name, n_name
        pos,  # wins
        pos,  # places
        pos,  # shows
        pos_factor,
        # UPDATING
        alpha,
        alpha,  # ewma_pos_factor
        alpha,
        alpha,  # ewma_perf_factor
    )

    return query, values


def dropPerformances():
    """
    Generate a SQL query to drop the 'Performances' table if it exists.

    Returns:
        str: A SQL query string to drop the 'Performances' table, including all dependent objects.
    """
    return "DROP TABLE IF EXISTS Performances CASCADE;"


def createPerformancesUseType():
    """
    Create the 'use_type' ENUM type for categorizing race data.

    Returns:
        str: A SQL query string to create the 'use_type' ENUM type with possible values 'SETUP', 'TRAINING', and 'TESTING'.
    """
    return """
        DROP TYPE IF EXISTS use_type CASCADE;
        CREATE TYPE use_type AS ENUM ('SETUP', 'TRAINING', 'TESTING');
    """


def createPerformances():
    """
    Create the 'Performances' table in the database.

    Returns:
        str: A SQL query string to create the 'Performances' table with its schema definition.
    """
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


def addPerformance(
    race_id,
    file_num,
    date,
    race_num,
    track_n_name,
    horse_n_name,
    program_number,
    weight,
    odds,
    start_pos,
    final_pos,
    jockey_n_name,
    trainer_n_name,
    owner_n_name,
    pos_gain,
    late_pos_gain,
    last_pos_gain,
    pos_factor,
    speed,
    use,
):
    """
    Add a new performance record to the 'Performances' table.

    Args:
        race_id (str): Unique identifier for the race.
        file_num (int): File number associated with the race data.
        date (date): Date of the race.
        race_num (int): Number of the race in the file.
        track_n_name (str): Normalized name of the track where the race occurred.
        horse_n_name (str): Normalized name of the horse.
        program_number (str): Program number assigned to the horse in the race.
        weight (Decimal): Weight carried by the horse in the race.
        odds (Decimal): Odds for the horse in the race.
        start_pos (int): Starting position of the horse in the race.
        final_pos (int): Final position of the horse in the race.
        jockey_n_name (str): Normalized name of the jockey.
        trainer_n_name (str): Normalized name of the trainer.
        owner_n_name (str): Normalized name of the owner.
        pos_gain (Decimal): Total positions gained by the horse in the race.
        late_pos_gain (Decimal): Positions gained by the horse in the later stages of the race.
        last_pos_gain (Decimal): Positions gained by the horse in the final stages of the race.
        pos_factor (Decimal): Performance factor based on the horse's position in the race.
        speed (Decimal): Average speed of the race.
        use (str): Use type of the record ('SETUP', 'TRAINING', or 'TESTING').

    Returns:
        tuple: A SQL query string and a tuple of parameters for executing the query.
    """
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
    VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, (SELECT p_factor FROM PerformanceFactor), %s
    )
    """
    # Values to be inserted into the query
    values = (
        track_n_name,
        speed,
        pos_factor,
        speed,
        race_id,
        file_num,
        date,
        track_n_name,
        race_num,
        horse_n_name,
        program_number,
        weight,
        odds,
        start_pos,
        final_pos,
        jockey_n_name,
        trainer_n_name,
        owner_n_name,
        pos_gain,
        late_pos_gain,
        last_pos_gain,
        pos_factor,
        use,
    )

    return query, values


def dropTrainables():
    """
    Drop the 'Trainables' table if it exists.

    Returns:
        str: A SQL query string to drop the 'Trainables' table, including all dependent objects.
    """
    return "DROP TABLE IF EXISTS Trainables CASCADE;"


def createTrainables():
    """
    Create the 'Trainables' table in the database.

    Returns:
        str: A SQL query string to create the 'Trainables' table with its schema definition.
    """
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


def addTrainable(
    horse_n_name,
    track_n_name,
    jockey_n_name,
    trainer_n_name,
    owner_n_name,
    surface,
    race_id,
    final_pos,
    race_type,
    weight,
    weather,
    temp,
    track_state,
    distance,
    won,
    placed,
    showed,
):
    """
    Add a new trainable record to the 'Trainables' table.

    Args:
        horse_n_name (str): Normalized name of the horse.
        track_n_name (str): Normalized name of the track.
        jockey_n_name (str): Normalized name of the jockey.
        trainer_n_name (str): Normalized name of the trainer.
        owner_n_name (str): Normalized name of the owner.
        surface (str): Surface type of the track (e.g., 'Dirt', 'Turf').
        race_id (str): Unique identifier for the race.
        final_pos (int): Final position of the horse in the race.
        race_type (str): Type of the race.
        weight (Decimal): Weight carried by the horse.
        weather (int): Weather condition code during the race.
        temp (Decimal): Temperature during the race.
        track_state (int): Track condition code during the race.
        distance (Decimal): Distance of the race.
        won (Decimal): 'Odds' for winning the race (0 if yes, 100 otherwise).
        placed (Decimal): 'Odds' for placing in the race (0 if yes, 100 otherwise).
        showed (Decimal): 'Odds' for showing in the race (0 if yes, 100 otherwise).

    Returns:
        tuple: A SQL query string and a tuple of parameters for executing the query.
    """
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

    # Values to be used in the query
    values = (
        # CTEs
        track_n_name,  # TrackSpeed
        surface,
        surface,
        surface,
        horse_n_name,  # HorseStats
        jockey_n_name,  # JockeyStats
        surface,
        surface,
        surface,
        trainer_n_name,  # TrainerStats
        owner_n_name,  # OwnerStats
        horse_n_name,
        jockey_n_name,  # HorseJockeyStats
        horse_n_name,
        trainer_n_name,  # HorseTrainerStats
        trainer_n_name,
        track_n_name,
        surface,  # TrainerTrackStats
        owner_n_name,
        trainer_n_name,  # OwnerTrainerStats
        horse_n_name,
        track_n_name,
        surface,  # HorseTrackStats
        jockey_n_name,
        trainer_n_name,  # JockeyTrainerStats
        # ADDING
        race_id,
        final_pos,
        horse_n_name,
        race_type,
        weight,
        weather,
        temp,
        track_state,
        distance,
        won,
        placed,
        showed,
    )

    return query, values


def dropTestables():
    """
    Drop the 'Testables' table if it exists.

    Returns:
        str: A SQL query string to drop the 'Testables' table, including all dependent objects.
    """
    return "DROP TABLE IF EXISTS Testables CASCADE;"


def createTestables():
    """
    Create the 'Testables' table in the database.

    Returns:
        str: A SQL query string to create the 'Testables' table with its schema definition.
    """
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


def addTestable(
    horse_n_name,
    track_n_name,
    jockey_n_name,
    trainer_n_name,
    owner_n_name,
    surface,
    race_id,
    final_pos,
    race_type,
    weight,
    weather,
    temp,
    track_state,
    distance,
    odds,
):
    """
    Add a new testable record to the 'Testables' table.

    Args:
        horse_n_name (str): Normalized name of the horse.
        track_n_name (str): Normalized name of the track.
        jockey_n_name (str): Normalized name of the jockey.
        trainer_n_name (str): Normalized name of the trainer.
        owner_n_name (str): Normalized name of the owner.
        surface (str): Surface type of the track (e.g., 'Dirt', 'Turf').
        race_id (str): Unique identifier for the race.
        final_pos (int): Final position of the horse in the race.
        race_type (str): Type of the race.
        weight (Decimal): Weight carried by the horse.
        weather (int): Weather condition code during the race.
        temp (Decimal): Temperature during the race.
        track_state (int): Track condition code during the race.
        distance (Decimal): Distance of the race.
        odds (Decimal): Odds associated with the horse's performance.

    Returns:
        tuple: A SQL query string and a tuple of parameters for executing the query.
    """
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

    # Values to be used in the query
    values = (
        # CTEs
        track_n_name,  # TrackSpeed
        surface,
        surface,
        surface,
        horse_n_name,  # HorseStats
        jockey_n_name,  # JockeyStats
        surface,
        surface,
        surface,
        trainer_n_name,  # TrainerStats
        owner_n_name,  # OwnerStats
        horse_n_name,
        jockey_n_name,  # HorseJockeyStats
        horse_n_name,
        trainer_n_name,  # HorseTrainerStats
        trainer_n_name,
        track_n_name,
        surface,  # TrainerTrackStats
        owner_n_name,
        trainer_n_name,  # OwnerTrainerStats
        horse_n_name,
        track_n_name,
        surface,  # HorseTrackStats
        jockey_n_name,
        trainer_n_name,  # JockeyTrainerStats
        # ADDING
        race_id,
        final_pos,
        horse_n_name,
        race_type,
        weight,
        weather,
        temp,
        track_state,
        distance,
        odds,
    )

    return query, values


def copyBadTestables():
    """
    Copy invalid testable records to the Trainables table.

    An testable is invalid if any other testables from the same race, which will have the same race_id, do not have a
    valid Odds value. This often means that soemthing went wrong with a certain horse's race performance, and in these cases,
    we do not want to test our model against this data. Instead, we will convert these testables into trainables. There is
    still some value in training based off of these unusual results because the other horses in most cases still compete
    normally against each other.

    Returns:
        str: A SQL query string to insert invalid testable records into the Trainables table.
    """
    return """
    WITH BadEntries AS (
        SELECT *
        FROM Testables
        WHERE race_id IN (
            SELECT race_id
            FROM Testables
            WHERE odds IS NULL
        )
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
        won, placed, showed
    )
    SELECT
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
        CASE
            WHEN final_pos = 1 THEN 0
            ELSE 100
        END AS won,
        CASE
            WHEN final_pos <= 2 THEN 0
            ELSE 100
        END AS placed,
        CASE
            WHEN final_pos <= 3 THEN 0
            ELSE 100
        END AS showed
    FROM BadEntries;
    """


def deleteBadTestables():
    """
    Delete invalid testable records from the 'Testables' table.

    Returns:
        str: A SQL query string to delete invalid testable records.
    """
    return """
    WITH BadEntries AS (
        SELECT *
        FROM Testables
        WHERE race_id IN (
            SELECT race_id
            FROM Testables
            WHERE odds IS NULL
        )
    ) DELETE FROM Testables
    WHERE race_id IN (
        SELECT race_id
        FROM BadEntries
    );
    """


def fixPerformances():
    """
    Change invalid testable performances to be recorded as trainables.

    Returns:
        str: A SQL query string to update invalid testable performances.
    """
    return """
    UPDATE Performances
    SET use = 'TRAINING'
    WHERE race_id IN (
        SELECT race_id
        FROM Testables
        WHERE odds IS NULL
    );
    """


def dropHorses():
    """
    Generate a SQL query to drop the 'Horses' table if it exists.

    Returns:
        str: A SQL query string to drop the 'Horses' table, including all dependent objects.
    """
    return "DROP TABLE IF EXISTS Horses CASCADE;"


def createHorses():
    """
    Generate a SQL query to create the 'Horses' table.

    Returns:
        str: A SQL query string to create the 'Horses' table with its schema definition.
    """
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


def addHorse(
    name,
    n_name,
    pos,
    pos_factor,
    pos_gain,
    late_pos_gain,
    last_pos_gain,
    speed,
    track_n_name,
    surface,
    distance,
    num_horses,
):
    """
    Add or update a horse's record in the 'Horses' table.

    Args:
        name (str): Full name of the horse.
        n_name (str): Normalized name of the horse (unique identifier).
        pos (int): Final position of the horse in the race.
        pos_factor (Decimal): Position factor for the horse in the race.
        pos_gain (Decimal): Positions gained by the horse during the race.
        late_pos_gain (Decimal): Positions gained by the horse in the later stages of the race.
        last_pos_gain (Decimal): Positions gained by the horse in the final stretch of the race.
        speed (Decimal): Average speed of the race.
        track_n_name (str): Normalized name of the track where the race occurred.
        surface (str): Surface type of the track (e.g., 'Dirt', 'Turf', 'AWT').
        distance (Decimal): Distance of the race.
        num_horses (int): Total number of horses participating in the race.

    Returns:
        tuple: A SQL query string and a tuple of parameters for executing the query.
    """
    # Define alpha values for EWMA calculations
    alpha = Decimal(0.25)
    d_factor_max_alpha = 0.5
    d_factor_alpha = Decimal(
        (1 - ((int(pos) - 1) / (int(num_horses) - 1))) * d_factor_max_alpha
    )

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

    # Parameters for the SQL query
    values = (
        # CTEs
        track_n_name,  # TrackSpeed CTE
        speed,
        pos_factor,
        speed,  # PerformanceFactor CTE
        # ADDING
        name,
        n_name,  # name, n_name
        pos,  # wins
        pos,  # places
        pos,  # shows
        pos_factor,
        pos_gain,
        late_pos_gain,
        last_pos_gain,  # ewma_pos_factor, ewma_pos_gain, ewma_late_pos_gain, ewma_last_pos_gain
        surface,  # ewma_dirt_perf_factor
        surface,  # ewma_turf_perf_factor
        surface,  # ewma_awt_perf_factor
        num_horses,
        distance,  # distance_factor
        # UPDATING
        alpha,
        alpha,  # ewma_pos_factor
        alpha,
        alpha,  # ewma_pos_gain
        alpha,
        alpha,  # ewma_late_pos_gain
        alpha,
        alpha,  # ewma_last_pos_gain
        alpha,
        alpha,  # ewma_perf_factor
        alpha,
        alpha,  # ewma_dirt_perf_factor
        alpha,
        alpha,  # ewma_turf_perf_factor
        alpha,
        alpha,  # ewma_awt_perf_factor
        d_factor_alpha,
        d_factor_alpha,  # distance_factor
    )

    return query, values


def dropTracks():
    """
    Generate a SQL query to drop the 'Tracks' table if it exists.

    Returns:
        str: A SQL query string to drop the 'Tracks' table, including all dependent objects.
    """
    return "DROP TABLE IF EXISTS Tracks CASCADE;"


def createTracks():
    """
    Generate a SQL query to create the 'Tracks' table.

    Returns:
        str: A SQL query string to create the 'Tracks' table.
    """
    return """
        CREATE TABLE IF NOT EXISTS Tracks (
            name VARCHAR(255) NOT NULL,
            normalized_name VARCHAR(255) PRIMARY KEY,
            ewma_speed DECIMAL(10, 6)
        );
        """


def addTrack(name, n_name, speed):
    """
    Generate a SQL query to add or update a track in the 'Tracks' table.

    Args:
        name (str): Full name of the track.
        n_name (str): Normalized unique identifier for the track.
        speed (Decimal): Observed speed to update the ewma_speed.

    Returns:
        tuple: A SQL query string and parameters for execution.
    """
    # Alpha value for EWMA calculation
    alpha = Decimal(0.1)

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


def dropJockeys():
    """
    Generate a SQL query to drop the 'Jockeys' table.

    Returns:
        str: A SQL query string to drop the 'Jockeys' table.
    """
    return "DROP TABLE IF EXISTS Jockeys CASCADE;"


def createJockeys():
    """
    Generate a SQL query to create the 'Jockeys' table.

    Returns:
        str: A SQL query string to create the 'Jockeys' table.
    """
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


def addJockey(
    name,
    n_name,
    pos_gain,
    late_pos_gain,
    last_pos_gain,
    pos,
    pos_factor,
    speed,
    track_n_name,
):
    """
    Generate a SQL query to add or update a jockey's record in the 'Jockeys' table.

    Args:
        name (str): Full name of the jockey.
        n_name (str): Normalized unique identifier for the jockey.
        pos_gain (Decimal): Positions gained in the race.
        late_pos_gain (Decimal): Positions gained in the later stages of the race.
        last_pos_gain (Decimal): Positions gained in the final stretch.
        pos (int): Final position in the race.
        pos_factor (Decimal): Position factor for the race.
        speed (Decimal): Average speed of the race.
        track_n_name (str): Normalized name of the track.

    Returns:
        tuple: A SQL query string and parameters for execution.
    """
    # Alpha value for EWMA calculations
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

    # Parameters for the SQL query
    values = (
        # CTEs
        track_n_name,  # TrackSpeed CTE
        speed,
        pos_factor,
        speed,  # PerformanceFactor CTE
        # ADDING
        name,
        n_name,
        pos_gain,
        late_pos_gain,
        last_pos_gain,  # name, n_name, ewma_pos_gain, late_pos_gain, last_pos_gain
        pos,  # wins
        pos,  # places
        pos,  # shows
        pos_factor,  # ewma_pos_factor
        # UPDATING
        alpha,
        alpha,  # ewma_pos_gain
        alpha,
        alpha,  # ewma_late_pos_gain
        alpha,
        alpha,  # ewma_last_pos_gain
        alpha,
        alpha,  # ewma_pos_factor
        alpha,
        alpha,  # ewma_perf_factor
    )

    return query, values


def dropRaces():
    """
    Generate a SQL query to drop the 'Races' table.

    Returns:
        str: A SQL query string to drop the 'Races' table.
    """
    return "DROP TABLE IF EXISTS Races CASCADE;"


def createRaces():
    """
    Generate a SQL query to create the 'Races' table.

    Returns:
        str: A SQL query string to create the 'Races' table.
    """
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


def addRace(
    race_id,
    file_num,
    track_n_name,
    race_num,
    date,
    race_type=None,
    surface=None,
    weather=None,
    temperature=None,
    track_state=None,
    distance=None,
    final_time=None,
    speed=None,
    frac_time_1=None,
    frac_time_2=None,
    frac_time_3=None,
    frac_time_4=None,
    frac_time_5=None,
    frac_time_6=None,
    split_time_1=None,
    split_time_2=None,
    split_time_3=None,
    split_time_4=None,
    split_time_5=None,
    split_time_6=None,
):
    """
    Generate a SQL query to add or update a race in the 'Races' table.

    Args:
        race_id (str): Unique identifier for the race.
        file_num (int): File number associated with the race.
        track_n_name (str): Normalized track name where the race occurred.
        race_num (int): Race number within the file.
        date (str): Date of the race in 'YYYY-MM-DD' format.
        race_type (str): Type of race (e.g., thoroughbred, harness).
        surface (str): Track surface type (e.g., Dirt, Turf).
        weather (int): Weather condition description.
        temperature (float): Temperature during the race.
        track_state (str): Track condition description (e.g., Fast, Muddy).
        distance (float): Distance of the race in miles.
        final_time (float): Final time of the race in seconds.
        speed (float): Speed figure for the race.
        frac_time_1 to frac_time_6 (float): Fractional times at various points in the race.
        split_time_1 to split_time_6 (float): Split times at various segments of the race.

    Returns:
        tuple: A SQL query string and parameters for execution.
    """
    query = """
    INSERT INTO Races (
        race_id, file_num, track_n_name, race_num, date, race_type, surface, weather, temperature, track_state, distance,
        final_time, speed, frac_time_1, frac_time_2, frac_time_3, frac_time_4, frac_time_5, frac_time_6, split_time_1,
        split_time_2, split_time_3, split_time_4, split_time_5, split_time_6
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    # Parameters for the query
    values = (
        race_id,
        file_num,
        track_n_name,
        race_num,
        date,
        race_type,
        surface,
        weather,
        temperature,
        track_state,
        distance,
        final_time,
        speed,
        frac_time_1,
        frac_time_2,
        frac_time_3,
        frac_time_4,
        frac_time_5,
        frac_time_6,
        split_time_1,
        split_time_2,
        split_time_3,
        split_time_4,
        split_time_5,
        split_time_6,
    )

    return query, values


def dropHorseJockey():
    """
    Generate a SQL query to drop the 'HorseJockey' table.

    Returns:
        str: A SQL query string to drop the 'HorseJockey' table.
    """
    return "DROP TABLE IF EXISTS horse_jockey CASCADE;"


def createHorseJockey():
    """
    Generate a SQL query to create the 'HorseJockey' table.

    Returns:
        str: A SQL query string to create the 'HorseJockey' table.
    """
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


def addHorseJockey(horse_n_name, jockey_n_name, pos, pos_factor, speed, track_n_name):
    """
    Generate a SQL query to add a record to the 'HorseJockey' table.

    Args:
        horse_n_name (str): Normalized name of the horse.
        jockey_n_name (str): Normalized name of the jockey.
        pos (int): Final position of the horse in the race.
        pos_factor (Decimal): Position factor for the horse in the race.
        speed (Decimal): Speed of the race.
        track_n_name (str): Normalized name of the track where the race occurred.

    Returns:
        tuple: A SQL query string and parameters for execution.
    """
    # Alpha value for EWMA calculations
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
    INSERT INTO horse_jockey (
        horse_n_name, jockey_n_name, total_races, wins, places, shows, ewma_pos_factor, perf_factor_count, ewma_perf_factor
    )
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

    # Parameters for the SQL query
    values = (
        # CTEs
        track_n_name,  # TrackSpeed CTE
        speed,
        pos_factor,
        speed,  # PerformanceFactor CTE
        # ADDING
        horse_n_name,
        jockey_n_name,  # horse_n_name, jockey_n_name
        pos,  # wins
        pos,  # places
        pos,  # shows
        pos_factor,  # ewma_pos_factor
        # UPDATING
        alpha,
        alpha,  # ewma_pos_factor
        alpha,
        alpha,  # ewma_perf_factor
    )

    return query, values


def dropHorseTrainer():
    """
    Generate a SQL query to drop the 'horse_trainer' table if it exists.

    Returns:
        str: A SQL query string to drop the 'horse_trainer' table, including cascading deletion.
    """
    return "DROP TABLE IF EXISTS horse_trainer CASCADE;"


def createHorseTrainer():
    """
    Generate a SQL query to create the 'horse_trainer' table.

    Returns:
        str: A SQL query string to create the 'horse_trainer' table with its schema definition.
    """
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


def addHorseTrainer(horse_n_name, trainer_n_name, pos, pos_factor, speed, track_n_name):
    """
    Generate a SQL query to add or update a record in the 'horse_trainer' table.

    Args:
        horse_n_name (str): Normalized name of the horse.
        trainer_n_name (str): Normalized name of the trainer.
        pos (int): Final position of the horse in the race.
        pos_factor (Decimal): Position factor for the horse in the race.
        speed (Decimal): Speed of the race.
        track_n_name (str): Normalized name of the track where the race occurred.

    Returns:
        tuple: A SQL query string and its parameters for adding or updating a record.
    """
    # Alpha value for EWMA calculations
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
    INSERT INTO horse_trainer (
        horse_n_name, trainer_n_name, total_races, wins, places, shows, ewma_pos_factor, perf_factor_count, ewma_perf_factor
    )
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

    # Parameters for the SQL query
    values = (
        # CTEs
        track_n_name,  # TrackSpeed CTE
        speed,
        pos_factor,
        speed,  # PerformanceFactor CTE
        # ADDING
        horse_n_name,
        trainer_n_name,  # horse_n_name, trainer_n_name
        pos,  # wins
        pos,  # places
        pos,  # shows
        pos_factor,  # ewma_pos_factor
        # UPDATING
        alpha,
        alpha,  # ewma_pos_factor
        alpha,
        alpha,  # ewma_perf_factor
    )

    return query, values


def dropTrainerTrack():
    """
    Generate a SQL query to drop the 'trainer_track' table if it exists.

    Returns:
        str: A SQL query string to drop the 'trainer_track' table, including cascading deletion.
    """
    return "DROP TABLE IF EXISTS trainer_track CASCADE;"


def createTrainerTrack():
    """
    Generate a SQL query to create the 'trainer_track' table.

    Returns:
        str: A SQL query string to create the 'trainer_track' table with its schema definition.
    """
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


def addTrainerTrack(trainer_n_name, track_n_name, pos, pos_factor, speed, surface):
    """
    Generate a SQL query to add or update a record in the 'trainer_track' table.

    Args:
        trainer_n_name (str): Normalized name of the trainer.
        track_n_name (str): Normalized name of the track.
        pos (int): Final position of the trainer's horse in the race.
        pos_factor (Decimal): Position factor for the trainer's horse in the race.
        speed (Decimal): Average speed of the race.
        surface (str): Surface type of the track (e.g., 'Dirt', 'Turf', 'AWT').

    Returns:
        tuple: A SQL query string and its parameters for adding or updating a record.
    """
    # Alpha value for EWMA calculations
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
    INSERT INTO trainer_track (
        trainer_n_name, track_n_name, surface, total_races, wins, places, shows, ewma_pos_factor, perf_factor_count,
        ewma_perf_factor
    )
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

    # Parameters for the SQL query
    values = (
        # CTEs
        track_n_name,  # TrackSpeed CTE
        speed,
        pos_factor,
        speed,  # PerformanceFactor CTE
        # ADDING
        trainer_n_name,
        track_n_name,
        surface,  # trainer_n_name, track_n_name, surface
        pos,  # wins
        pos,  # places
        pos,  # shows
        pos_factor,  # ewma_pos_factor
        # UPDATING
        alpha,
        alpha,  # ewma_pos_factor
        alpha,
        alpha,  # ewma_perf_factor
    )

    return query, values


def dropHorseTrack():
    """
    Generate a SQL query to drop the 'horse_track' table if it exists.

    Returns:
        str: A SQL query string to drop the 'horse_track' table, including cascading deletion.
    """
    return "DROP TABLE IF EXISTS horse_track CASCADE;"


def createHorseTrack():
    """
    Generate a SQL query to create the 'horse_track' table.

    Returns:
        str: A SQL query string to create the 'horse_track' table with its schema definition.
    """
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


def addHorseTrack(horse_n_name, track_n_name, pos, pos_factor, speed, surface):
    """
    Generate a SQL query to add or update a record in the 'horse_track' table.

    Args:
        horse_n_name (str): Normalized name of the horse.
        track_n_name (str): Normalized name of the track.
        pos (int): Final position of the horse in the race.
        pos_factor (Decimal): Position factor for the horse in the race.
        speed (Decimal): Average speed of the race.
        surface (str): Surface type of the track (e.g., 'Dirt', 'Turf', 'AWT').

    Returns:
        tuple: A SQL query string and its parameters for adding or updating a record.
    """
    # Alpha value for EWMA calculations
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
    INSERT INTO horse_track (
        horse_n_name, track_n_name, surface, total_races, wins, places, shows, ewma_pos_factor, perf_factor_count,
        ewma_perf_factor
    )
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

    # Parameters for the SQL query
    values = (
        # CTEs
        track_n_name,  # TrackSpeed CTE
        speed,
        pos_factor,
        speed,  # PerformanceFactor CTE
        # ADDING
        horse_n_name,
        track_n_name,
        surface,  # horse_n_name, track_n_name, surface
        pos,  # wins
        pos,  # places
        pos,  # shows
        pos_factor,  # ewma_pos_factor
        # UPDATING
        alpha,
        alpha,  # ewma_pos_factor
        alpha,
        alpha,  # ewma_perf_factor
    )

    return query, values


def dropJockeyTrainer():
    """
    Generate a SQL query to drop the 'jockey_trainer' table if it exists.

    Returns:
        str: A SQL query string to drop the 'jockey_trainer' table, including cascading deletion.
    """
    return "DROP TABLE IF EXISTS jockey_trainer CASCADE;"


def createJockeyTrainer():
    """
    Generate a SQL query to create the 'jockey_trainer' table.

    Returns:
        str: A SQL query string to create the 'jockey_trainer' table with its schema definition.
    """
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


def addJockeyTrainer(
    jockey_n_name, trainer_n_name, pos, pos_factor, speed, track_n_name
):
    """
    Generate a SQL query to add or update a record in the 'jockey_trainer' table.

    Args:
        jockey_n_name (str): Normalized name of the jockey.
        trainer_n_name (str): Normalized name of the trainer.
        pos (int): Final position of the horse in the race.
        pos_factor (Decimal): Position factor for the jockey-trainer pair in the race.
        speed (Decimal): Average speed of the race.
        track_n_name (str): Normalized name of the track.

    Returns:
        tuple: A SQL query string and its parameters for adding or updating a record.
    """
    # Alpha value for EWMA calculations
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
    INSERT INTO jockey_trainer (
        jockey_n_name, trainer_n_name, total_races, wins, places, shows, ewma_pos_factor, perf_factor_count, ewma_perf_factor
    )
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

    # Parameters for the SQL query
    values = (
        # CTEs
        track_n_name,  # TrackSpeed CTE
        speed,
        pos_factor,
        speed,  # PerformanceFactor CTE
        # ADDING
        jockey_n_name,
        trainer_n_name,  # jockey_n_name, trainer_n_name
        pos,  # wins
        pos,  # places
        pos,  # shows
        pos_factor,  # ewma_pos_factor
        # UPDATING
        alpha,
        alpha,  # ewma_pos_factor
        alpha,
        alpha,  # ewma_perf_factor
    )

    return query, values


def dropOwnerTrainer():
    """
    Generate a SQL query to drop the 'owner_trainer' table if it exists.

    Returns:
        str: A SQL query string to drop the 'owner_trainer' table, including cascading deletion.
    """
    return "DROP TABLE IF EXISTS owner_trainer CASCADE;"


def createOwnerTrainer():
    """
    Generate a SQL query to create the 'owner_trainer' table.

    Returns:
        str: A SQL query string to create the 'owner_trainer' table with its schema definition.
    """
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


def addOwnerTrainer(owner_n_name, trainer_n_name, pos, pos_factor, speed, track_n_name):
    """
    Generate a SQL query to add or update a record in the 'owner_trainer' table.

    Args:
        owner_n_name (str): Normalized name of the owner.
        trainer_n_name (str): Normalized name of the trainer.
        pos (int): Final position of the horse in the race.
        pos_factor (Decimal): Position factor for the owner-trainer pair in the race.
        speed (Decimal): Average speed of the race.
        track_n_name (str): Normalized name of the track.

    Returns:
        tuple: A SQL query string and its parameters for adding or updating a record.
    """
    # Alpha value for EWMA calculations
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
    INSERT INTO owner_trainer (
        owner_n_name, trainer_n_name, total_races, wins, places, shows, ewma_pos_factor, perf_factor_count, ewma_perf_factor
    )
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

    # Parameters for the SQL query
    values = (
        # CTEs
        track_n_name,  # TrackSpeed CTE
        speed,
        pos_factor,
        speed,  # PerformanceFactor CTE
        # ADDING
        owner_n_name,
        trainer_n_name,  # owner_n_name, trainer_n_name
        pos,  # wins
        pos,  # places
        pos,  # shows
        pos_factor,  # ewma_pos_factor
        # UPDATING
        alpha,
        alpha,  # ewma_pos_factor
        alpha,
        alpha,  # ewma_perf_factor
    )

    return query, values


def normalize(name):
    """
    Normalize a string.

    This method normalizes a string by converting it to lowercase, stripping extra whitespace, and removing any
    non-alphanumeric characters.

    Args:
        value (str): The string to be normalized.

    Returns:
        str: The normalized string, or None if the input value is None.
    """
    return re.sub(r"[^a-z0-9]", "", unidecode.unidecode(name.strip().lower()))


def convertDate(date_str):
    """
    Convert a date string from 'Month Day, Year' format to a datetime object and then to SQL 'YYYY-MM-DD' format.

    Args:
        value (str): The date string to be converted.

    Returns:
        str: The converted date string in SQL 'YYYY-MM-DD' format.
    """
    # Convert the string date into a datetime object
    date_obj = datetime.strptime(date_str, "%B %d, %Y")

    # Convert the datetime object into the SQL 'YYYY-MM-DD' format
    return date_obj.strftime("%Y-%m-%d")


def convertTemp(temp):
    """
    Convert a temperature string to a float value in Fahrenheit.

    Args:
        value (str): The temperature string.

    Returns:
        float: The temperature in Fahrenheit as a float.
    """
    if "° C" in temp:
        return (float(temp.replace("° C", "")) * (9 / 5)) + 32
    else:
        return float(temp)


def convertTime(time):
    """
    Convert a time string from 'M:SS.s' format to total seconds as a float.

    Args:
        value (str): The time string to be converted (e.g., '1:45.3').

    Returns:
        float: The total time in seconds.
    """
    if time != "N/A":
        nums = [float(n) for n in re.split("[:.]", time)]
        if len(nums) == 1:
            return nums[0] / 100
        elif len(nums) == 2:
            return nums[0] + nums[1] / 100
        else:
            return sum(nums[:-2]) * 60 + nums[-2] + nums[-1] / 100
