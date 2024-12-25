"""
creating batches:
    max batch size of 32
    each batch should have the same number of horses per race
    


embedding layer:
    reduce dimensionality from 94 down to 64
"""

from sqlalchemy import create_engine
import pandas as pd


def local_connect(db_name):
    """
    Establish a connection to a PostgreSQL database using SQLAlchemy.

    Args:
        db_name (str): The name of the database to connect to.

    Returns:
        sqlalchemy.engine.base.Engine: An SQLAlchemy engine object for the database.
    """
    return create_engine(f"postgresql+psycopg2://postgres:B!h8Cjxa37!78Yh@localhost:5432/{db_name}")


# Example: Load from PostgreSQL database
engine = local_connect("StableEarnings")
query = """
SELECT *
FROM trainables
ORDER BY race_id
LIMIT 100;
"""
df = pd.read_sql_query(query, engine)
engine.dispose()

grouped = df.groupby('race_id')

for race_id, group in grouped:
    print(f"Race ID: {race_id}")
    print(group)
    print("-" * 40)  # Separator between groups
