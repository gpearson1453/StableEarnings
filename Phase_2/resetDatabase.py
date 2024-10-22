import psycopg2

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

# SQL statements for dropping tables if they exist (with CASCADE)
drop_tables = [
    "DROP TABLE IF EXISTS owner_trainer CASCADE;",
    "DROP TABLE IF EXISTS horse_track CASCADE;",
    "DROP TABLE IF EXISTS jockey_track CASCADE;",
    "DROP TABLE IF EXISTS horse_jockey CASCADE;",
    "DROP TABLE IF EXISTS horse_trainer CASCADE;",
    "DROP TABLE IF EXISTS trainer_track CASCADE;",
    "DROP TABLE IF EXISTS Performances CASCADE;",
    "DROP TABLE IF EXISTS Races CASCADE;",
    "DROP TABLE IF EXISTS Owners CASCADE;",
    "DROP TABLE IF EXISTS Trainers CASCADE;",
    "DROP TABLE IF EXISTS Jockeys CASCADE;",
    "DROP TABLE IF EXISTS Horses CASCADE;",
    "DROP TABLE IF EXISTS Tracks CASCADE;"
]

# Execute each SQL statement to drop the tables
for table in drop_tables:
    cur.execute(table)

# SQL statements for creating tables
tables = [
    """
    CREATE TABLE IF NOT EXISTS Tracks (
        name VARCHAR(255) NOT NULL,
        normalized_name VARCHAR(255) NOT NULL,
        track_id VARCHAR(255) PRIMARY KEY
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS Horses (
        name VARCHAR(255) NOT NULL,
        normalized_name VARCHAR(255) NOT NULL,
        horse_id VARCHAR(255) PRIMARY KEY,
        total_races INT,
        wins INT,
        places INT,
        shows INT,
        avg_pos_factor DECIMAL,
        st_dev_pos_factor DECIMAL,
        avg_position_gain DECIMAL,
        st_dev_position_gain DECIMAL,
        avg_late_position_gain DECIMAL,
        avg_last_position_gain DECIMAL,
        ewma_perf_factor DECIMAL,
        most_recent_perf_factor DECIMAL,
        ewma_dirt_perf_factor DECIMAL,
        ewma_turf_perf_factor DECIMAL,
        ewma_awt_perf_factor DECIMAL,
        distance_factor DECIMAL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS Jockeys (
        name VARCHAR(255) NOT NULL,
        normalized_name VARCHAR(255) NOT NULL,
        jockey_id VARCHAR(255) PRIMARY KEY,
        avg_position_gain DECIMAL,
        avg_late_position_gain DECIMAL,
        avg_last_position_gain DECIMAL,
        total_races INT,
        wins INT,
        places INT,
        shows INT,
        avg_pos_factor DECIMAL,
        ewma_perf_factor DECIMAL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS Trainers (
        name VARCHAR(255) NOT NULL,
        normalized_name VARCHAR(255) NOT NULL,
        trainer_id VARCHAR(255) PRIMARY KEY,
        total_races INT,
        wins INT,
        places INT,
        shows INT,
        avg_pos_factor DECIMAL,
        ewma_perf_factor DECIMAL,
        ewma_dirt_perf_factor DECIMAL,
        ewma_turf_perf_factor DECIMAL,
        ewma_awt_perf_factor DECIMAL,
        distance_factor DECIMAL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS Owners (
        name VARCHAR(255) NOT NULL,
        normalized_name VARCHAR(255) NOT NULL,
        owner_id VARCHAR(255) PRIMARY KEY,
        total_races INT,
        wins INT,
        places INT,
        shows INT,
        avg_pos_factor DECIMAL,
        ewma_perf_factor DECIMAL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS Races (
        date DATE,
        race_num INT,
        race_id VARCHAR(255) PRIMARY KEY,
        track_id VARCHAR(255) REFERENCES Tracks(track_id),
        race_type VARCHAR(255),
        surface VARCHAR(255),
        weather VARCHAR(255),
        temperature DECIMAL,
        track_state VARCHAR(255),
        distance DECIMAL,
        final_time DECIMAL,
        frac_time_1 DECIMAL,
        frac_time_2 DECIMAL,
        frac_time_3 DECIMAL,
        frac_time_4 DECIMAL,
        frac_time_5 DECIMAL,
        frac_time_6 DECIMAL,
        split_time_1 DECIMAL,
        split_time_2 DECIMAL,
        split_time_3 DECIMAL,
        split_time_4 DECIMAL,
        split_time_5 DECIMAL,
        split_time_6 DECIMAL
    );
    """,
    """
    CREATE TYPE IF NOT EXISTS use_type AS ENUM ('SETUP', 'TRAINING', 'TESTING');
    """,
    """
    CREATE TABLE IF NOT EXISTS Performances (
        race_id VARCHAR(255) REFERENCES Races(race_id),
        horse_id VARCHAR(255) REFERENCES Horses(horse_id),
        program_number INT,
        weight DECIMAL,
        odds DECIMAL,
        start_pos INT,
        final_pos INT,
        jockey_id VARCHAR(255) REFERENCES Jockeys(jockey_id),
        trainer_id VARCHAR(255) REFERENCES Trainers(trainer_id),
        owner_id VARCHAR(255) REFERENCES Owners(owner_id),
        pos_gained DECIMAL,
        late_pos_gained DECIMAL,
        last_pos_gained DECIMAL,
        pos_factor DECIMAL,
        perf_factor DECIMAL,
        use use_type,
        PRIMARY KEY (race_id, horse_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS horse_jockey (
        horse_id VARCHAR(255) REFERENCES Horses(horse_id),
        jockey_id VARCHAR(255) REFERENCES Jockeys(jockey_id),
        total_races INT,
        wins INT,
        places INT,
        shows INT,
        avg_pos_factor DECIMAL,
        ewma_perf_factor DECIMAL,
        PRIMARY KEY (horse_id, jockey_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS horse_trainer (
        horse_id VARCHAR(255) REFERENCES Horses(horse_id),
        trainer_id VARCHAR(255) REFERENCES Trainers(trainer_id),
        total_races INT,
        wins INT,
        places INT,
        shows INT,
        avg_pos_factor DECIMAL,
        ewma_perf_factor DECIMAL,
        PRIMARY KEY (horse_id, trainer_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS trainer_track (
        trainer_id VARCHAR(255) REFERENCES Trainers(trainer_id),
        track_id VARCHAR(255) REFERENCES Tracks(track_id),
        surface VARCHAR(255),
        total_races INT,
        wins INT,
        places INT,
        shows INT,
        avg_pos_factor DECIMAL,
        ewma_perf_factor DECIMAL,
        PRIMARY KEY (trainer_id, track_id, surface)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS owner_trainer (
        owner_id VARCHAR(255) REFERENCES Owners(owner_id),
        trainer_id VARCHAR(255) REFERENCES Trainers(trainer_id),
        total_races INT,
        wins INT,
        places INT,
        shows INT,
        avg_pos_factor DECIMAL,
        ewma_perf_factor DECIMAL,
        PRIMARY KEY (owner_id, trainer_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS horse_track (
        horse_id VARCHAR(255) REFERENCES Horses(horse_id),
        track_id VARCHAR(255) REFERENCES Tracks(track_id),
        surface VARCHAR(255),
        total_races INT,
        wins INT,
        places INT,
        shows INT,
        avg_pos_factor DECIMAL,
        ewma_perf_factor DECIMAL,
        PRIMARY KEY (horse_id, track_id, surface)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS jockey_trainer (
        jockey_id VARCHAR(255) REFERENCES Jockeys(jockey_id),
        trainer_id VARCHAR(255) REFERENCES Trainers(trainer_id),
        total_races INT,
        wins INT,
        places INT,
        shows INT,
        avg_pos_factor DECIMAL,
        ewma_perf_factor DECIMAL,
        PRIMARY KEY (jockey_id, trainer_id)
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
