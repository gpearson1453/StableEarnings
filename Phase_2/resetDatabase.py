import psycopg2

def resetDatabase():
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
            normalized_name VARCHAR(255) PRIMARY KEY,
            ewma_speed DECIMAL(10, 6)
        );
        """,
        """
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
        """,
        """
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
        """,
        """
        CREATE TABLE IF NOT EXISTS Trainers (
            name VARCHAR(255) NOT NULL,
            normalized_name VARCHAR(255) PRIMARY KEY,
            total_races INT,
            wins INT,
            places INT,
            shows INT,
            avg_pos_factor DECIMAL(10, 6),
            ewma_perf_factor DECIMAL(10, 6),
            ewma_dirt_perf_factor DECIMAL(10, 6),
            ewma_turf_perf_factor DECIMAL(10, 6),
            ewma_awt_perf_factor DECIMAL(10, 6),
            distance_factor DECIMAL(10, 6)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS Owners (
            name VARCHAR(255) NOT NULL,
            normalized_name VARCHAR(255) PRIMARY KEY,
            total_races INT,
            wins INT,
            places INT,
            shows INT,
            avg_pos_factor DECIMAL(10, 6),
            ewma_perf_factor DECIMAL(10, 6)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS Races (
            date DATE,
            race_num INT,
            track_n_name VARCHAR(255) REFERENCES Tracks(normalized_name) ON DELETE CASCADE,
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
            split_time_6 DECIMAL(10, 6),
            PRIMARY KEY (date, race_num, track_n_name)
        );

        """,
        """
        CREATE TYPE IF NOT EXISTS use_type AS ENUM ('SETUP', 'TRAINING', 'TESTING');
        """,
        """
        CREATE TABLE IF NOT EXISTS Performances (
            date DATE,
            race_num INT,
            track_n_name VARCHAR(255),
            horse_n_name VARCHAR(255),
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
            PRIMARY KEY (date, race_num, track_n_name, horse_n_name),
            FOREIGN KEY (date, race_num, track_n_name) REFERENCES Races(date, race_num, track_n_name) ON DELETE CASCADE,
            FOREIGN KEY (horse_n_name) REFERENCES Horses(normalized_name) ON DELETE CASCADE
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS horse_jockey (
            horse_n_name VARCHAR(255) REFERENCES Horses(normalized_name) ON DELETE CASCADE,
            jockey_n_name VARCHAR(255) REFERENCES Jockeys(normalized_name) ON DELETE CASCADE,
            total_races INT,
            wins INT,
            places INT,
            shows INT,
            avg_pos_factor DECIMAL(10, 6),
            ewma_perf_factor DECIMAL(10, 6),
            PRIMARY KEY (horse_n_name, jockey_n_name)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS horse_trainer (
            horse_n_name VARCHAR(255) REFERENCES Horses(normalized_name) ON DELETE CASCADE,
            trainer_n_name VARCHAR(255) REFERENCES Trainers(normalized_name) ON DELETE CASCADE,
            total_races INT,
            wins INT,
            places INT,
            shows INT,
            avg_pos_factor DECIMAL(10, 6),
            ewma_perf_factor DECIMAL(10, 6),
            PRIMARY KEY (horse_n_name, trainer_n_name)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS trainer_track (
            trainer_n_name VARCHAR(255) REFERENCES Trainers(normalized_name) ON DELETE CASCADE,
            track_n_name VARCHAR(255) REFERENCES Tracks(normalized_name) ON DELETE CASCADE,
            surface VARCHAR(255),
            total_races INT,
            wins INT,
            places INT,
            shows INT,
            avg_pos_factor DECIMAL(10, 6),
            ewma_perf_factor DECIMAL(10, 6),
            PRIMARY KEY (trainer_n_name, track_n_name, surface)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS owner_trainer (
            owner_n_name VARCHAR(255) REFERENCES Owners(normalized_name) ON DELETE CASCADE,
            trainer_n_name VARCHAR(255) REFERENCES Trainers(normalized_name) ON DELETE CASCADE,
            total_races INT,
            wins INT,
            places INT,
            shows INT,
            avg_pos_factor DECIMAL(10, 6),
            ewma_perf_factor DECIMAL(10, 6),
            PRIMARY KEY (owner_n_name, trainer_n_name)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS horse_track (
            horse_n_name VARCHAR(255) REFERENCES Horses(normalized_name) ON DELETE CASCADE,
            track_n_name VARCHAR(255) REFERENCES Tracks(normalized_name) ON DELETE CASCADE,
            surface VARCHAR(255),
            total_races INT,
            wins INT,
            places INT,
            shows INT,
            avg_pos_factor DECIMAL(10, 6),
            ewma_perf_factor DECIMAL(10, 6),
            PRIMARY KEY (horse_n_name, track_n_name, surface)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS jockey_trainer (
            jockey_n_name VARCHAR(255) REFERENCES Jockeys(normalized_name) ON DELETE CASCADE,
            trainer_n_name VARCHAR(255) REFERENCES Trainers(normalized_name) ON DELETE CASCADE,
            total_races INT,
            wins INT,
            places INT,
            shows INT,
            avg_pos_factor DECIMAL(10, 6),
            ewma_perf_factor DECIMAL(10, 6),
            PRIMARY KEY (jockey_n_name, trainer_n_name)
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

if __name__ == "__main__":
    resetDatabase()
