# Phase 2
In Phase 2, the extracted information is converted into usable data for the learning model that will be developed in Phase 3. This involves cleaning and reformatting data, designing a database schema and populating the database, and compiling information into statistical profiles for entities and training and testing units. The data is currently stored in a local PostgreSQL database and can be migrated to an online database service at a later date.

---

## Python Files

### `Phase 2/addSetup.py`
`addSetup.py` processes `setup.csv` (or `testing.csv`) to populate the database with setup data.

This script adds setup data, such as tracks, horses, jockeys, trainers, owners, races, and relationships between these entities. Data is read from a CSV file and inserted into the corresponding database tables in batches for efficient processing. Setup data is added before training and testing data to allow for horses, jockeys, etc. to have established statistical profiles before the model is trained on how those profiles compare to each other.

Steps:
- Optionally reset specific database tables before inserting new data.
- Process rows from a CSV file in batches of 1000.
- Normalize and transform data where needed (e.g., locations, horse names).
- Add data for tracks, horses, jockeys, trainers, owners, races, and entity relationships.
- Commit batches to the database to optimize performance and avoid memory overload.

### `Phase 2/addTrainTest.py`
`addTrainTest.py` processes `traintest.csv` (or `testing.csv`) to populate the database with trainable and testable data.

This script reads data from a CSV file, builds caches for weather and track state encoding, and inserts rows into the database in batches. Data is split into training and testing datasets based on a calculated test ratio. The script also normalizes data values, encodes weather and track state values, and handles progress tracking.

Unlike `addSetup.py`, which processes all track data first, then performances and races, then horses, etc., this file adds all new data and updates concurrently. This is because a training or testing data point must be created using data from other fields from a specific point in time. For example, if we are trying to train the model to predict a horse's performance in a certain race, we cannot train the model using the horse's win rate at the end of that year because the model will not have access to that type of future data when it is being used to predict an upcoming race. Instead, training and testing data should contain all entities' statistical profiles at the time just before the race, which is when the model would actualy be used to make the predictions. Therefore, updates to the database must happen for each race, rather than processing all data for a certain entity type at once and then moving on to the next.

Steps:
- Reset relevant database tables if specified.
- Build caches for weather and track state encodings.
- Calculate the ratio for testable rows based on odds data.
- Process rows in the CSV file and insert data into Trainables and Testables tables.
- Batch process queries to optimize database insertion.

### `Phase 2/clearDatabase.py`
`clearDatabase.py` resets the database to a completely empty state.

This script is used to clear all tables and types from the StableEarnings database. It drops all existing tables, their dependencies, and custom types to reset the database to a clean state. This is useful for reinitializing the database during development or testing.

Steps:
- Establish a connection to the StableEarnings database.
- Execute SQL commands to drop each table and custom type, ensuring dependencies are handled.
- Commit the changes to apply the deletions.
- Close the database connection.

### `Phase 2/createCSVs.py`
`createCSVs.py` extracts data from `all_race_data.csv` into three files, `testing.csv`, `setup.csv`, and `traintest.csv`.

This script takes race data from a large input file, processes it into the required format, and calculates additional fields like position gains and race speed. Data is divided into three categories:
- Setup data (before the train-test start year)
- Train/Test data (from the train-test start year onward)
- Testing data (specific conditions like dates in August 2022)

Steps:
- Read data from the input CSV file.
- Calculate new fields such as position gains, speed, and race IDs.
- Write rows to the appropriate output file based on the year and testing conditions.
- Perform column conversions (e.g., time, temperature, and date) using threaded processing.

### `Phase 2/dataMethods.py`
`dataMethods.py` provides a collection of utility functions for database interactions and some additional supportive functions.

This script contains reusable methods for connecting to and interacting with the database, defining schema creation and modification queries, and implementing complex SQL commands for inserting and updating records. These methods are designed to ensure consistency and efficiency across database operations.

---

## Other Files

### `Phase 2/testing.csv`
`testing.csv` is created by the `createCSVs.py` script and contains race data specifically filtered for testing purposes. It includes races from a specific month. This dataset is used to evaluate the system's performance on a distinct subset of data.

### `Phase 2/setup.csv`
`setup.csv` is created by the `createCSVs.py` script and contains race data used to populate the database with setup information. This dataset includes data from all races held before the defined training and testing cutoff.

### `Phase 2/traintest.csv`
`traintest.csv` is created by the `createCSVs.py` script and contains race data used to build training and testing data. This dataset includes data from all races held after the defined training and testing cutoff.
