import dataMethods as dm

def clearDatabase():
    # Establish connection to CockroachDB
    conn = dm.local_connect("StableEarnings")

    # Create a cursor object
    cur = conn.cursor()

    # SQL statements for dropping tables if they exist (with CASCADE)
    drop_tables = [
        "DROP TABLE IF EXISTS owner_trainer CASCADE;",
        "DROP TABLE IF EXISTS horse_track CASCADE;",
        "DROP TABLE IF EXISTS jockey_trainer CASCADE;",
        "DROP TABLE IF EXISTS horse_jockey CASCADE;",
        "DROP TABLE IF EXISTS horse_trainer CASCADE;",
        "DROP TABLE IF EXISTS trainer_track CASCADE;",
        "DROP TABLE IF EXISTS Performances CASCADE;",
        "DROP TABLE IF EXISTS Races CASCADE;",
        "DROP TABLE IF EXISTS Owners CASCADE;",
        "DROP TABLE IF EXISTS Trainers CASCADE;",
        "DROP TABLE IF EXISTS Jockeys CASCADE;",
        "DROP TABLE IF EXISTS Horses CASCADE;",
        "DROP TABLE IF EXISTS Tracks CASCADE;",
        "DROP TYPE IF EXISTS use_type CASCADE;",
        "DROP TABLE IF EXISTS Trainables CASCADE;",
        "DROP TABLE IF EXISTS Testables CASCADE;"
    ]

    # Execute each SQL statement to drop the tables
    for table in drop_tables:
        cur.execute(table)
    
    # Commit the changes
    conn.commit()
    print("Database cleared successfully.")

    # Close the connection
    conn.close()

if __name__ == "__main__":
    clearDatabase()
