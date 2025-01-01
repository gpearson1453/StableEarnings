"""
clearDatabase.py resets the database to a completely empty state.

This script is used to clear all tables and types from the StableEarnings database. It drops all existing tables, their
dependencies, and custom types to reset the database to a clean state. This is useful for reinitializing the database during
development or testing.

Steps:
    - Establish a connection to the StableEarnings database.
    - Execute SQL commands to drop each table and custom type, ensuring dependencies are handled.
    - Commit the changes to apply the deletions.
    - Close the database connection.

Functions:
    - clearDatabase: Drops all relevant tables and types from the database.

Usage:
    Execute this script directly to clear the StableEarnings database. Ensure the
    database connection parameters in dataMethods are correctly configured.
"""
import dataMethods as dm


def clearDatabase():
    """
    Drop all tables and custom types from the StableEarnings database.

    This function connects to the database, executes a series of SQL commands to drop tables
    and types, and commits the changes. Any dependent objects (e.g., foreign key constraints)
    are also removed due to the CASCADE option.
    """
    # Connect to the StableEarnings database
    conn = dm.local_connect("StableEarnings")
    cur = conn.cursor()

    # List of SQL commands to drop tables and types
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
        "DROP TABLE IF EXISTS AltTrainables CASCADE;",
        "DROP TABLE IF EXISTS Testables CASCADE;",
    ]

    # Execute each drop command
    for table in drop_tables:
        cur.execute(table)

    # Commit the changes to apply all deletions
    conn.commit()
    print("Database cleared successfully.")

    # Close the connection
    conn.close()


if __name__ == "__main__":
    """
    Main script execution. Clears the StableEarnings database by dropping all tables and custom types.
    """
    clearDatabase()
