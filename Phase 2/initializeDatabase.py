import psycopg2
from psycopg2 import sql

# Establish connection to CockroachDB
conn = psycopg2.connect(
    dbname="defaultdb",
    user="molomala",
    password="aPyds3qPNhslU5xV8H-pMw",
    host="stable-earnings-3899.j77.aws-us-east-1.cockroachlabs.cloud",
    port="26257"
)

# Create a cursor object
cur = conn.cursor()

# SQL statements for creating tables
tables = [
    """
    CREATE TABLE IF NOT EXISTS Track (
        track_id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        track_speed_factor DECIMAL(5, 2),
        surfaces VARCHAR(255)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS Horse (
        horse_id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        total_races INT,
        win_rate DECIMAL(5, 2),
        place_rate DECIMAL(5, 2),
        show_rate DECIMAL(5, 2),
        avg_final_position_factor DECIMAL(5, 2),
        stddev_final_position_factor DECIMAL(5, 2),
        avg_positions_gained_start DECIMAL(5, 2),
        stddev_positions_gained DECIMAL(5, 2),
        avg_positions_gained_last_2_3_legs DECIMAL(5, 2),
        avg_positions_gained_last_leg DECIMAL(5, 2),
        median_performance_factor DECIMAL(5, 2),
        recent_performance_factor DECIMAL(5, 2),
        dirt_median_performance_factor DECIMAL(5, 2),
        turf_median_performance_factor DECIMAL(5, 2),
        awt_median_performance_factor DECIMAL(5, 2),
        distance_factor DECIMAL(5, 2)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS Race (
        race_id SERIAL PRIMARY KEY,
        date DATE NOT NULL,
        location VARCHAR(255),
        race_number INT NOT NULL,
        race_type VARCHAR(100),
        surface VARCHAR(100),
        weather VARCHAR(100),
        temperature DECIMAL(5, 2),
        track_state VARCHAR(100),
        distance DECIMAL(5, 2),
        fractional_times_1_6 VARCHAR(255),
        final_time DECIMAL(5, 2),
        split_time_1_6 VARCHAR(255),
        track_id INT,
        FOREIGN KEY (track_id) REFERENCES Track(track_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS Jockey (
        jockey_id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        total_races INT,
        win_rate DECIMAL(5, 2),
        place_rate DECIMAL(5, 2),
        show_rate DECIMAL(5, 2),
        avg_final_position_factor DECIMAL(5, 2),
        stddev_final_position_factor DECIMAL(5, 2),
        median_performance_factor DECIMAL(5, 2),
        avg_positions_gained_start DECIMAL(5, 2),
        avg_positions_gained_last_2_3_legs DECIMAL(5, 2),
        avg_positions_gained_last_leg DECIMAL(5, 2)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS Trainer (
        trainer_id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        total_races INT,
        win_rate DECIMAL(5, 2),
        place_rate DECIMAL(5, 2),
        show_rate DECIMAL(5, 2),
        dirt_median_performance_factor DECIMAL(5, 2),
        turf_median_performance_factor DECIMAL(5, 2),
        awt_median_performance_factor DECIMAL(5, 2)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS Owner (
        owner_id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        total_races INT,
        win_rate DECIMAL(5, 2),
        place_rate DECIMAL(5, 2),
        show_rate DECIMAL(5, 2)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS HorseRace (
        horse_race_id SERIAL PRIMARY KEY,
        horse_id INT,
        race_id INT,
        jockey_id INT,
        trainer_id INT,
        owner_id INT,
        program_number INT,
        final_position INT,
        FOREIGN KEY (horse_id) REFERENCES Horse(horse_id),
        FOREIGN KEY (race_id) REFERENCES Race(race_id),
        FOREIGN KEY (jockey_id) REFERENCES Jockey(jockey_id),
        FOREIGN KEY (trainer_id) REFERENCES Trainer(trainer_id),
        FOREIGN KEY (owner_id) REFERENCES Owner(owner_id)
    );
    """
]

# Execute each SQL statement to create the tables
for table in tables:
    cur.execute(table)

# Commit the transaction
conn.commit()

# Close the cursor and connection
cur.close()
conn.close()

print("Tables created successfully in CockroachDB.")
